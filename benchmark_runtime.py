"""Run isolated, resumable index experiments; never reuse an unowned trial DB."""

import argparse
import fcntl
import hashlib
import importlib.metadata
import json
import math
import os
import shutil
import subprocess
import sys
import time
import traceback
import uuid
import zipfile
from collections import Counter
from dataclasses import asdict, is_dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import psycopg
from psycopg import sql

from OceanDB.config import Config
from OceanDB.etl import AlongTrackETL
from OceanDB.index_experiment import (
    BenchmarkCleanupError,
    BenchmarkFailure,
    BenchmarkTimeout,
    _cleanup_index_performance_sessions,
    index_database_is_reusable,
    index_definition,
    index_signature,
    ocean_db_init_for_test_db,
    run_index_performance_test_with_timeout,
)
from OceanDB.managed_index_oceandb import DatabaseIndex
from OceanDB.managed_indices import ManagedIndices
from OceanDB.OceanDB_Initializer import OceanDBInit
from OceanDB.schemas.along_track_schema import along_track_columns_schema
from optimize_geo_points_r_dt import (
    TIMEOUT_SECONDS,
    basic_indexes,
    build_scenarios,
    candidate_specs,
)

ROOT = Path(__file__).resolve().parent
GIB = 1024**3
TERMINAL = {"completed", "timed_out", "failed"}
PLANNER_SETTINGS = [
    "shared_buffers",
    "effective_cache_size",
    "work_mem",
    "maintenance_work_mem",
    "random_page_cost",
    "seq_page_cost",
    "effective_io_concurrency",
    "max_parallel_workers_per_gather",
    "max_parallel_workers",
    "jit",
    "default_statistics_target",
    "enable_bitmapscan",
    "enable_indexscan",
    "enable_indexonlyscan",
    "enable_seqscan",
    "plan_cache_mode",
]


def now():
    return datetime.now(timezone.utc).isoformat()


def json_default(value):
    if is_dataclass(value):
        return asdict(value)
    if isinstance(value, (set, frozenset)):
        return sorted(value)
    if isinstance(value, (datetime, Path)):
        return str(value)
    if isinstance(value, timedelta):
        return value.total_seconds()
    if isinstance(value, np.ndarray):
        return value.tolist()
    raise TypeError(f"Cannot serialize {type(value).__name__}")


def canonical(value):
    return json.dumps(value, default=json_default, sort_keys=True, allow_nan=False)


def digest(value):
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def read_json(path):
    return json.loads(Path(path).read_text())


def atomic_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    with temporary.open("w") as stream:
        stream.write(canonical(value) + "\n")
        stream.flush()
        os.fsync(stream.fileno())
    temporary.replace(path)


def code_hashes():
    paths = [
        ROOT / name
        for name in (
            "optimize_geo_points_r_dt.py",
            "benchmark_runtime.py",
            "graph_index_performance.py",
            "pyproject.toml",
            "requirements.txt",
        )
    ]
    paths += [
        p
        for p in (ROOT / "src").rglob("*")
        if p.is_file() and "__pycache__" not in p.parts and ".egg-info" not in str(p)
    ]
    return {
        p.relative_to(ROOT).as_posix(): hashlib.sha256(p.read_bytes()).hexdigest()
        for p in sorted(paths)
    }


def environment():
    return {
        "python": sys.version,
        "packages": sorted(
            f"{d.metadata['Name']}=={d.version}"
            for d in importlib.metadata.distributions()
        ),
    }


def workload(scenarios):
    definitions = []
    for i, scenario in enumerate(scenarios):
        kwargs = dict(scenario.kwargs)
        grid = {key: kwargs.pop(key) for key in ("latitudes", "longitudes", "dates")}
        definitions.append(
            {
                "id": f"scenario_{i + 1}",
                "method": scenario.name,
                "parameters": kwargs,
                "grid_sha256": digest(grid),
                "points": len(grid["latitudes"]),
            }
        )
    if len(definitions) != 6 or any(d["points"] != 31966 for d in definitions):
        raise RuntimeError("Expected six scenarios with 31,966 ocean points each")
    return json.loads(
        canonical(
            {
                "scenarios": definitions,
                "grid_degrees": 1,
                "passes": 1,
                "nearest_limit": 3,
                "timing": "batch including result processing; EXPLAIN separate",
            }
        )
    )


def specs_for_run(run_id):
    included, excluded = candidate_specs()
    if Counter(len(s["fields"]) for s in included) != {2: 7, 3: 12, 4: 12, 0: 1}:
        raise RuntimeError("Unexpected candidate counts")
    for spec in included:
        spec["database_name"] = f"idx_{run_id}_{spec['id']}"
        spec["trial_indexes"] = (
            [asdict(index_definition("experiment", tuple(spec["fields"])))]
            if spec["fields"]
            else []
        )
    return included, excluded


def definitions(spec):
    return basic_indexes() + (
        [index_definition("experiment", tuple(spec["fields"]))]
        if spec["fields"]
        else []
    )


def dataset_facts(database):
    with database.cursor() as cur:
        cur.execute(database.load_sql_file("queries/benchmark/dataset_facts.sql"))
        observations = cur.fetchall()
        cur.execute(
            "SELECT file_name, observation_count FROM public.along_track_metadata ORDER BY file_name"
        )
        metadata = cur.fetchall()
        cur.execute(
            "SELECT column_name, data_type, udt_name, is_nullable FROM information_schema.columns "
            "WHERE table_schema='public' AND table_name='along_track' ORDER BY ordinal_position"
        )
        columns = cur.fetchall()
    if not observations or sum(r[1] for r in observations) <= 0:
        raise RuntimeError("Source database is empty")
    if len(metadata) != len({r[0] for r in metadata}) or dict(metadata) != {
        r[0]: r[1] for r in observations
    }:
        raise RuntimeError(
            "Incomplete ingestion: observation counts do not match metadata for every file"
        )
    return json.loads(canonical({"files": observations, "columns": columns}))


def source_facts(source, *, check_files=True):
    if not source.database_exists():
        raise RuntimeError(
            "Source database is missing. Use --ingest-source to create and ingest it explicitly."
        )
    facts = dataset_facts(source)
    expected_types = {
        field.postgres_column_name: (
            "timestamp without time zone"
            if field.postgres_type == "timestamp"
            else field.postgres_type
        )
        for field in along_track_columns_schema.values()
    }
    expected_types.update(id="bigint", along_track_point="geography")
    actual_types = {
        r[0]: r[2] if r[1] == "USER-DEFINED" else r[1] for r in facts["columns"]
    }
    if any(actual_types.get(name) != kind for name, kind in expected_types.items()):
        raise RuntimeError(
            "Source along_track schema does not match the package's column types"
        )
    if check_files:
        files = AlongTrackETL(config=source.config).discover_files(
            missions=["all"],
            start_date=datetime(2022, 10, 5),
            end_date=datetime(2022, 10, 25),
        )["files"]
        names = [p.name for p in files]
        if (
            not names
            or len(names) != len(set(names))
            or set(names) != {r[0] for r in facts["files"]}
        ):
            raise RuntimeError(
                "Source file inventory differs from the discovered October 2022 files"
            )
    indexes = source.inventory_indexes()
    parent_constraints = {
        (i.constraint_type, index_signature(i.index_definition))
        for i in indexes
        if i.table_name == "along_track" and i.is_constraint_owned
    }
    if not {
        ("p", "unique btree(date_time,id)"),
        ("u", "unique btree(date_time,latitude,longitude)"),
    }.issubset(parent_constraints):
        raise RuntimeError("Source is missing the expected primary/unique constraints")
    # Constraint indexes and the declared singleton indexes are the only allowed
    # inherited along-track indexes. Other tables keep their shared indexes.
    inherited = [
        i
        for i in indexes
        if i.table_name == "along_track" or i.parent_table_name == "along_track"
    ]
    constraints = [i for i in inherited if i.is_constraint_owned]
    present = [
        d for d in basic_indexes() if any(i.index_name == d.name for i in inherited)
    ]
    if not index_database_is_reusable(constraints, present, inherited):
        raise RuntimeError(
            "Source has unexpected/invalid along-track indexes; baseline would be contaminated"
        )
    with source.cursor() as cur:
        cur.execute("SELECT pg_get_partkeydef('public.along_track'::regclass)")
        if cur.fetchone()[0] != "RANGE (date_time)":
            raise RuntimeError(
                "Source along_track must be range-partitioned on date_time"
            )
        cur.execute(
            "SELECT pid FROM pg_stat_activity WHERE datname=current_database() "
            "AND pid<>pg_backend_pid() AND backend_type='client backend'"
        )
        if cur.fetchall():
            raise RuntimeError(
                "Source has other client sessions; close them before cloning"
            )
        cur.execute("SELECT version(), pg_database_size(current_database())")
        version, size = cur.fetchone()
        cur.execute("SHOW data_directory")
        directory = cur.fetchone()[0]
        cur.execute("SELECT extname, extversion FROM pg_extension ORDER BY extname")
        extensions = cur.fetchall()
        cur.execute(
            "SELECT name, setting, unit FROM pg_settings WHERE name = ANY(%s) ORDER BY name",
            (PLANNER_SETTINGS,),
        )
        settings = cur.fetchall()
    return json.loads(
        canonical(
            {
                "dataset": facts,
                "indexes": indexes,
                "version": version,
                "extensions": extensions,
                "planner_settings": settings,
                "source_bytes": size,
                "data_directory": directory,
            }
        )
    )


def connection_identity(config):
    return {
        "host": config.postgres_host,
        "port": config.postgres_port,
        "username": config.postgres_username,
        "database": config.postgres_database,
    }


def statistics(database):
    with database.cursor() as cur:
        cur.execute(database.load_sql_file("queries/benchmark/table_statistics.sql"))
        tables = cur.fetchall()
        cur.execute(
            "SELECT attname, inherited, n_distinct FROM pg_stats WHERE schemaname='public' "
            "AND tablename='along_track' ORDER BY attname, inherited"
        )
        columns = cur.fetchall()
        cur.execute(
            "SELECT name, setting, unit FROM pg_settings WHERE name = ANY(%s) ORDER BY name",
            (PLANNER_SETTINGS,),
        )
        settings = cur.fetchall()
    return {
        "captured_at": now(),
        "tables": tables,
        "columns": columns,
        "settings": settings,
        "indexes": database.inventory_indexes(),
    }


def check_disk(manifest, results):
    # Conservative allowance for singleton + composite indexes and WAL. Local
    # storage only: checking the client filesystem for a remote DB is misleading.
    pending = [r for r in results if r["status"] not in TERMINAL]
    with psycopg.connect(Config().postgres_dsn_admin) as conn:
        sizes = dict(
            conn.execute(
                "SELECT datname, pg_database_size(oid) FROM pg_database WHERE datname = ANY(%s)",
                ([r["database_name"] for r in pending],),
            ).fetchall()
        )
    clone_bytes = max(5.2 * GIB, manifest["source"]["source_bytes"] * 2.5)
    required = (
        sum(max(0, clone_bytes - sizes.get(r["database_name"], 0)) for r in pending)
        + 20 * GIB
    )
    free = shutil.disk_usage(manifest["source"]["data_directory"]).free
    if free < required:
        raise RuntimeError(
            f"Insufficient disk: {free/GIB:.1f} GiB free, {required/GIB:.1f} GiB estimated required"
        )
    return {"free_gib": free / GIB, "required_gib": required / GIB}


class Recorder:
    def __init__(self, directory, envelope):
        self.directory, self.envelope = directory, envelope

    def save(self):
        atomic_json(self.directory / "results.json", self.envelope)

    def event(self, kind, **values):
        line = canonical({"timestamp": now(), "event": kind, **values})
        with (self.directory / "progress.log").open("a") as stream:
            stream.write(line + "\n")
            stream.flush()
        print(line, flush=True)


def prepare(directory, *, ingest_source=False, timeout=TIMEOUT_SECONDS):
    if not math.isfinite(timeout) or timeout <= 0:
        raise ValueError("Timeout must be finite and positive")
    config = Config()
    if config.postgres_host not in {"localhost", "127.0.0.1", "::1"}:
        raise RuntimeError(
            "Disk preflight currently requires a local PostgreSQL server"
        )
    directory = Path(directory).resolve()
    directory.mkdir(parents=True, exist_ok=False)
    source = OceanDBInit(
        config=config, managed_indices=ManagedIndices(tuple(basic_indexes()))
    )
    recorder = Recorder(directory, {})
    recorder.event("validating_source")
    if not source.database_exists() and ingest_source:
        etl = AlongTrackETL(config=config)
        selection = dict(
            missions=["all"],
            start_date=datetime(2022, 10, 5),
            end_date=datetime(2022, 10, 25),
        )
        if not etl.discover_files(**selection)["files"]:
            raise RuntimeError("No source files discovered; refusing empty ingestion")
        source.initialize_database(
            partition_start="2022-09-01", partition_end="2022-11-01"
        )
        etl.ingest(
            **selection,
            workers=4,
            on_progress=lambda event: recorder.event("ingest", detail=event),
        )
    facts = source_facts(source)
    scenarios = build_scenarios()
    run_id = (
        datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        + "_"
        + uuid.uuid4().hex[:8]
    )
    included, excluded = specs_for_run(run_id)
    hashes = code_hashes()
    # Comparing these hashes rejects query/schema changes as well as workload changes.
    comparison = {
        "workload": workload(scenarios),
        "query_deadline_seconds": timeout,
        "dataset": facts["dataset"],
        "source_indexes": facts["indexes"],
        "postgres": facts["version"],
        "extensions": facts["extensions"],
        "planner_settings": facts["planner_settings"],
        "code_hashes": hashes,
        "environment": environment(),
    }
    manifest = {
        "format_version": 2,
        "run_id": run_id,
        "created_at": now(),
        "connection": connection_identity(config),
        "source": facts,
        "timeout_seconds": timeout,
        "included": included,
        "excluded": excluded,
        "comparison": comparison,
        "git_commit": subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True
        ).strip(),
    }
    results = [
        {
            **s,
            "status": "pending",
            "performance": [],
            "index_sizes": None,
            "error": None,
            "exception": None,
        }
        for s in included
    ]
    manifest["disk_preflight"] = check_disk(manifest, results)
    atomic_json(directory / "manifest.json", manifest)
    with zipfile.ZipFile(
        directory / "source-code.zip", "x", zipfile.ZIP_DEFLATED
    ) as archive:
        for relative in hashes:
            archive.write(ROOT / relative, relative)
    envelope = {
        "format_version": 2,
        "manifest_sha256": digest(manifest),
        "comparison": comparison,
        "results": results,
    }
    Recorder(directory, envelope).save()
    recorder.event(
        "prepared", configurations=len(results), **manifest["disk_preflight"]
    )
    return directory


def validate_resume(manifest, envelope, config, scenarios):
    if envelope.get("manifest_sha256") != digest(manifest):
        raise RuntimeError("Manifest changed since preparation")
    if manifest["connection"] != connection_identity(config):
        raise RuntimeError("Database connection differs from the prepared run")
    comparison = manifest["comparison"]
    if comparison != envelope.get("comparison"):
        raise RuntimeError("Result metadata does not match manifest")
    if (
        comparison["code_hashes"] != code_hashes()
        or comparison["environment"] != environment()
    ):
        raise RuntimeError("Code or Python environment changed; start a new run")
    if comparison["workload"] != workload(scenarios):
        raise RuntimeError("Workload changed; start a new run")
    included, excluded = specs_for_run(manifest["run_id"])
    if included != manifest["included"] or excluded != manifest["excluded"]:
        raise RuntimeError("Candidate definitions differ from the prepared run")
    results = envelope["results"]
    if len(results) != len(included):
        raise RuntimeError("Results do not account for every configuration")
    for spec, result in zip(included, results, strict=True):
        if any(result.get(key) != value for key, value in spec.items()):
            raise RuntimeError("Trial identity differs from the manifest")
        if result["status"] not in TERMINAL | {
            "pending",
            "prepared",
            "running",
            "interrupted",
        }:
            raise RuntimeError("Unknown configuration status")
        if result["status"] == "completed" and len(result["performance"]) != len(
            scenarios
        ):
            raise RuntimeError("Successful configuration is missing scenario results")


def prepare_trial(source, manifest, result, recorder):
    desired = definitions(result)
    database = ocean_db_init_for_test_db(source, result["database_name"], desired)
    owner = "OceanDB benchmark " + manifest["run_id"]
    with source.cursor(
        autocommit=True, connection_string=source.config.postgres_dsn_admin
    ) as cur:
        cur.execute(
            "SELECT oid, shobj_description(oid, 'pg_database') FROM pg_database WHERE datname=%s",
            (database.db_name,),
        )
        existing = cur.fetchone()
        if existing:
            if existing[1] != owner or (
                result.get("database_oid") is not None
                and result["database_oid"] != existing[0]
            ):
                raise RuntimeError(
                    "Trial database is not owned by this run; refusing to modify it"
                )
        else:
            if result.get("database_oid") is not None:
                raise RuntimeError("Previously prepared trial database is missing")
            cur.execute(
                sql.SQL("CREATE DATABASE {} TEMPLATE {}").format(
                    sql.Identifier(database.db_name), sql.Identifier(source.db_name)
                )
            )
            cur.execute(
                sql.SQL("COMMENT ON DATABASE {} IS {}").format(
                    sql.Identifier(database.db_name), sql.Literal(owner)
                )
            )
            cur.execute(
                "SELECT oid FROM pg_database WHERE datname=%s", (database.db_name,)
            )
            existing = (cur.fetchone()[0], owner)
    result["database_oid"] = existing[0]
    recorder.save()
    # Validate before adding any missing indexes to a resumed, partially built DB.
    if dataset_facts(database) != manifest["source"]["dataset"]:
        raise RuntimeError("Trial dataset differs from the source snapshot")
    database.create_indexes(desired)
    actual = database.inventory_indexes()
    source_indexes = [DatabaseIndex(**i) for i in manifest["source"]["indexes"]]
    if not index_database_is_reusable(source_indexes, desired, actual):
        raise RuntimeError(
            "Trial index definitions do not match the requested inventory"
        )
    result["index_sizes"] = {
        d.name: sum(
            database.get_index_size(i.index_name)
            for i in actual
            if i.index_name == d.name or i.parent_index_name == d.name
        )
        for d in desired
    }
    atomic_json(
        recorder.directory / "trials" / result["id"] / "prepared.json",
        statistics(database),
    )
    return database


def archive_path(directory):
    return directory.parent / (directory.name + ".zip")


def package(directory):
    target = archive_path(directory)
    if target.exists():
        with zipfile.ZipFile(target) as archive:
            previous = json.loads(archive.read(directory.name + "/manifest.json"))
        if previous != read_json(directory / "manifest.json"):
            raise RuntimeError("Refusing to overwrite another run's ZIP")
    files = sorted(
        p
        for p in directory.rglob("*")
        if p.is_file()
        and p.name != ".runner.lock"
        and ".matplotlib" not in p.parts
        and not p.name.endswith(".tmp")
        and p.name != "SHA256SUMS.txt"
    )
    checksum = directory / "SHA256SUMS.txt"
    checksum.write_text(
        "\n".join(
            hashlib.sha256(p.read_bytes()).hexdigest()
            + "  "
            + p.relative_to(directory).as_posix()
            for p in files
        )
        + "\n"
    )
    temporary = target.with_suffix(".zip.tmp")
    with zipfile.ZipFile(temporary, "w", zipfile.ZIP_DEFLATED) as archive:
        for path in [*files, checksum]:
            archive.write(
                path, directory.name + "/" + path.relative_to(directory).as_posix()
            )
    with zipfile.ZipFile(temporary) as archive:
        if archive.testzip():
            raise RuntimeError("ZIP integrity check failed")
    temporary.replace(target)
    return target


def run(directory):
    directory = Path(directory).resolve()
    with (directory / ".runner.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        manifest, envelope = read_json(directory / "manifest.json"), read_json(
            directory / "results.json"
        )
        config, scenarios = Config(), build_scenarios()
        validate_resume(manifest, envelope, config, scenarios)
        recorder = Recorder(directory, envelope)
        results = envelope["results"]
        source = OceanDBInit(config=config)
        if envelope.get("exception"):
            envelope.setdefault("interruptions", []).append(envelope.pop("exception"))
        envelope["status"] = "running"
        recorder.save()
        try:
            # Clean up interrupted workers before any source scans or new work.
            for result in results:
                if result.get("application_name"):
                    database = ocean_db_init_for_test_db(
                        source, result["database_name"], definitions(result)
                    )
                    pid = result.get("worker_pid")
                    if pid:
                        try:
                            os.kill(pid, 0)
                        except ProcessLookupError:
                            pass
                        else:
                            raise BenchmarkCleanupError(
                                f"Previous worker PID {pid} still exists; refusing resume"
                            )
                    _cleanup_index_performance_sessions(
                        database.config, result["application_name"]
                    )
            facts = source_facts(source)
            for key in (
                "dataset",
                "indexes",
                "version",
                "extensions",
                "planner_settings",
            ):
                if facts[key] != manifest["source"][key]:
                    raise RuntimeError(f"Source changed since preparation: {key}")
            recorder.event("run_started", **check_disk(manifest, results))
            prepared = {}
            # Finish all cloning and index construction before any query timing.
            for result in results:
                if result["status"] in TERMINAL:
                    continue
                if result["performance"]:
                    result.setdefault("interrupted_attempts", []).append(
                        {
                            "performance": result["performance"],
                            "started_at": result.get("started_at"),
                        }
                    )
                result.update(
                    performance=[], error=None, status="pending", exception=None
                )
                recorder.save()
                recorder.event("preparation_started", configuration=result["id"])
                try:
                    if shutil.disk_usage(facts["data_directory"]).free < 26 * GIB:
                        raise RuntimeError("Disk reserve reached")
                    prepared[result["id"]] = prepare_trial(
                        source, manifest, result, recorder
                    )
                    result["status"] = "prepared"
                    recorder.event("preparation_completed", configuration=result["id"])
                except Exception:
                    result.update(
                        status="failed",
                        failure_stage="preparation",
                        exception=traceback.format_exc(),
                    )
                    recorder.event(
                        "configuration_failed",
                        configuration=result["id"],
                        exception=result["exception"],
                    )
                recorder.save()
            for result in results:
                if result["status"] in TERMINAL:
                    continue
                if code_hashes() != manifest["comparison"]["code_hashes"]:
                    raise RuntimeError("Code changed during the run")
                database = prepared[result["id"]]
                result.update(
                    status="running",
                    started_at=now(),
                    worker_pid=None,
                    application_name="idx_" + uuid.uuid4().hex,
                    failure_stage="maintenance",
                    query_phase_wall_seconds=None,
                )
                recorder.save()
                started = None

                def event(kind, **values):
                    nonlocal started
                    if kind == "worker_started":
                        result["worker_pid"] = values["pid"]
                    elif kind == "query_phase_started":
                        started = time.monotonic()
                    elif kind == "query_phase_finished":
                        result["query_phase_wall_seconds"] = values["wall_seconds"]
                    elif kind == "scenario_completed":
                        row = asdict(values.pop("result"))
                        row["scenario_id"] = f"scenario_{values['number'] + 1}"
                        result["performance"].append(row)
                    recorder.save()
                    recorder.event(kind, configuration=result["id"], **values)

                try:
                    event("maintenance_started")
                    database.vacuum_analyze("along_track")
                    snapshot = statistics(database)
                    if digest(snapshot["settings"]) != digest(
                        manifest["source"]["planner_settings"]
                    ):
                        raise RuntimeError(
                            "Trial planner settings differ from the prepared source settings"
                        )
                    if not {
                        "along_track_point",
                        "date_time",
                        "basin_id",
                        "mission",
                    }.issubset(r[0] for r in snapshot["columns"] if r[1]):
                        raise RuntimeError("Inherited statistics missing after ANALYZE")
                    atomic_json(
                        directory / "trials" / result["id"] / "analyzed.json", snapshot
                    )
                    event("maintenance_completed")
                    result["failure_stage"] = "queries"
                    performance = run_index_performance_test_with_timeout(
                        database,
                        scenarios,
                        manifest["timeout_seconds"],
                        on_event=event,
                        application_name=result["application_name"],
                        maintenance=False,
                    )
                    if len(performance) != len(scenarios):
                        raise RuntimeError(
                            "Worker returned incomplete scenario results"
                        )
                    result.update(
                        status="completed",
                        error=sum(r.total_time for r in performance),
                        failure_stage=None,
                    )
                except BenchmarkCleanupError:
                    result.update(
                        status="interrupted",
                        error=None,
                        failure_stage="cleanup",
                        exception=traceback.format_exc(),
                    )
                    raise
                except BenchmarkFailure as exc:
                    result.update(
                        status=(
                            "timed_out"
                            if isinstance(exc, BenchmarkTimeout)
                            else "failed"
                        ),
                        error=None,
                        exception=str(exc),
                    )
                except Exception:
                    result.update(
                        status="failed", error=None, exception=traceback.format_exc()
                    )
                finally:
                    if result["status"] != "interrupted":
                        result["worker_pid"] = None
                    if result["query_phase_wall_seconds"] is None and started:
                        result["query_phase_wall_seconds"] = time.monotonic() - started
                    result["finished_at"] = now()
                    recorder.save()
                recorder.event(
                    "configuration_finished",
                    configuration=result["id"],
                    status=result["status"],
                    completed_scenarios=len(result["performance"]),
                    exception=result["exception"],
                )
            if any(r["status"] not in TERMINAL for r in results):
                raise RuntimeError("Run did not account for every configuration")
            if any(
                r["status"] == "completed" and len(r["performance"]) != len(scenarios)
                for r in results
            ):
                raise RuntimeError("Successful configuration is incomplete")
            envelope["status"] = "complete"
            recorder.event(
                "run_complete", counts=dict(Counter(r["status"] for r in results))
            )
        except BaseException:
            envelope.update(status="interrupted", exception=traceback.format_exc())
            recorder.event("run_interrupted", exception=envelope["exception"])
            raise
        finally:
            recorder.save()
            try:
                os.environ.setdefault("MPLBACKEND", "Agg")
                os.environ.setdefault("MPLCONFIGDIR", str(directory / ".matplotlib"))
                from graph_index_performance import generate_outputs

                generate_outputs((directory / "results.json",), directory / "analysis")
                recorder.event("report_completed")
            except Exception:
                recorder.event("report_failed", exception=traceback.format_exc())
            recorder.event("packaging", archive=str(archive_path(directory)))
            package(directory)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "--prepare",
        type=Path,
        help="Validate and snapshot a new run; do not clone or time queries",
    )
    group.add_argument(
        "--resume",
        type=Path,
        help="Resume only pending/interrupted trials from this run",
    )
    parser.add_argument(
        "--ingest-source",
        action="store_true",
        help="Create and ingest source DB only if missing",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=TIMEOUT_SECONDS,
        help="Combined query deadline per configuration",
    )
    args = parser.parse_args()
    if args.resume:
        if args.ingest_source or args.timeout != TIMEOUT_SECONDS:
            parser.error("--resume uses the saved source and timeout")
        run(args.resume)
    else:
        directory = args.prepare or ROOT / "notes" / (
            "index-benchmark-"
            + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
            + "_"
            + uuid.uuid4().hex[:8]
        )
        directory = prepare(
            directory, ingest_source=args.ingest_source, timeout=args.timeout
        )
        print(f"Progress log: {directory / 'progress.log'}", flush=True)
        if not args.prepare:
            run(directory)


if __name__ == "__main__":
    main()
