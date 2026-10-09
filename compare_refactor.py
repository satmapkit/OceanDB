"""Compare legacy and current radius/NN APIs with equivalent BMA indexes."""

import argparse
import ast
import fcntl
import hashlib
import html
import json
import os
import selectors
import shutil
import statistics as stats
import subprocess
import sys
import time
import traceback
import uuid
import zipfile
from collections import Counter
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import psycopg
from psycopg import sql

import benchmark_runtime as common
from OceanDB.config import Config
from OceanDB.etl import AlongTrackETL
from OceanDB.index_experiment import (
    BenchmarkCleanupError,
    _cleanup_index_performance_sessions,
    index_database_is_reusable,
    index_definition,
)
from OceanDB.managed_indices import IndexDefinition, ManagedIndices
from OceanDB.ocean_data.basins import BasinConnections, BasinMask
from OceanDB.OceanDB_Initializer import OceanDBInit
from OceanDB.schemas.along_track_schema import sla_filtered
from optimize_geo_points_r_dt import batch_scenario_grid
from refactor_comparison_worker import canonical_rows

ROOT = Path(__file__).resolve().parent
WORKER = ROOT / "refactor_comparison_worker.py"
EXPECTED_FILES = 209
EXPECTED_ROWS = 9519966
TIMEOUT = 900


def definitions(version):
    fields = [
        ("mission",),
        ("basin_id",),
        ("along_track_point",),
        ("date_time",),
        ("basin_id", "mission", "along_track_point"),
    ]
    result = []
    for field in fields:
        definition = index_definition(
            "static" if len(field) == 1 else "experiment", field
        )
        if version == "old":
            definition = IndexDefinition(
                name=definition.name,
                table=definition.table,
                create_sql=definition.create_sql.replace(
                    "mission", "split_part(file_name, '_'::text, 3)"
                ),
            )
        result.append(definition)
    return result


def code_hashes(old_root):
    hashes = {"new/" + key: value for key, value in common.code_hashes().items()}
    for path in (Path(__file__), WORKER):
        hashes["new/" + path.name] = hashlib.sha256(path.read_bytes()).hexdigest()
    for folder in ("python/OceanDB", "sql", "data"):
        for path in (old_root / folder).rglob("*"):
            if (
                path.is_file()
                and "__pycache__" not in path.parts
                and path.suffix in {".py", ".sql", ".csv", ".nc"}
            ):
                hashes["old/" + path.relative_to(old_root).as_posix()] = hashlib.sha256(
                    path.read_bytes()
                ).hexdigest()
    return hashes


def schedule():
    jobs = []
    for query, versions in (("radius", ("old", "new")), ("nn", ("new", "old"))):
        for version in versions:
            jobs.append(
                {
                    "id": f"warmup_{query}_{version}",
                    "query": query,
                    "version": version,
                    "validation": True,
                    "round": 0,
                    "status": "pending",
                }
            )
    for repeat in range(1, 6):
        queries = ("radius", "nn") if repeat % 2 else ("nn", "radius")
        for query in queries:
            old_first = (repeat % 2 == 1) == (query == "radius")
            for version in (("old", "new") if old_first else ("new", "old")):
                jobs.append(
                    {
                        "id": f"pass_{repeat}_{query}_{version}",
                        "query": query,
                        "version": version,
                        "validation": False,
                        "round": repeat,
                        "status": "pending",
                    }
                )
    return jobs


def pg_environment(config, application):
    env = dict(
        os.environ,
        PGHOST=config.postgres_host,
        PGPORT=str(config.postgres_port),
        PGUSER=config.postgres_username,
        PGPASSWORD=config.postgres_password,
        PGDATABASE=config.postgres_database,
        PGAPPNAME=application,
        PYTHONDONTWRITEBYTECODE="1",
        MPLBACKEND="Agg",
    )
    return env


def supervise(job, config, directory, event, timeout=TIMEOUT, worker_path=WORKER):
    """Bound a fresh interpreter's query phase and clean up only its DB sessions."""
    directory.mkdir(parents=True, exist_ok=False)
    application = "cmp_" + uuid.uuid4().hex
    specification = {**job, "directory": str(directory)}
    common.atomic_json(directory / "job.json", specification)
    started = deadline = None
    status, exception = "failed", "Worker exited without a terminal result"
    payload = None
    with (directory / "stderr.log").open("w") as errors:
        process = subprocess.Popen(
            [sys.executable, "-B", str(worker_path), str(directory / "job.json")],
            env=pg_environment(config, application),
            cwd=ROOT,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=errors,
            text=True,
            bufsize=1,
        )
        selector = selectors.DefaultSelector()
        selector.register(process.stdout, selectors.EVENT_READ)
        startup_deadline = time.monotonic() + 60
        try:
            event("worker_started", pid=process.pid, application_name=application)
            while True:
                remaining = (deadline or startup_deadline) - time.monotonic()
                if remaining <= 0:
                    status = "timed_out" if started else "failed"
                    exception = (
                        f"Query phase exceeded {timeout:g} seconds"
                        if started
                        else "Worker startup exceeded 60 seconds"
                    )
                    break
                if selector.select(min(remaining, 0.1)):
                    line = process.stdout.readline()
                    if not line:
                        break
                    message = json.loads(line)
                    kind = message.pop("event")
                    if kind == "ready":
                        started = time.monotonic()
                        deadline = started + timeout
                        process.stdin.write("go\n")
                        process.stdin.flush()
                        event("query_started", **message)
                    elif kind == "completed":
                        if time.monotonic() > deadline:
                            status, exception = (
                                "timed_out",
                                f"Query phase exceeded {timeout:g} seconds",
                            )
                        else:
                            payload = common.read_json(directory / "result.json")
                            status, exception = "completed", None
                        break
                    elif kind == "failed":
                        exception = (directory / "exception.txt").read_text()
                        break
                elif process.poll() is not None:
                    break
        finally:
            wall = time.monotonic() - started if started else None
            selector.close()
            try:
                if process.poll() is None:
                    process.terminate()
                    try:
                        process.wait(timeout=2)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        try:
                            process.wait(timeout=2)
                        except subprocess.TimeoutExpired as exc:
                            raise BenchmarkCleanupError(
                                "Comparison worker survived termination"
                            ) from exc
                else:
                    process.wait()
            finally:
                process.stdin.close()
                process.stdout.close()
                _cleanup_index_performance_sessions(config, application)
    result = {
        "status": status,
        "exception": exception,
        "wall_seconds": wall,
        "seconds": payload["seconds"] if status == "completed" else None,
        "finished_at": common.now(),
        "application_name": application,
        "worker_pid": None,
    }
    common.atomic_json(directory / "supervisor-result.json", result)
    return result


def nearest_selection_matches(rows, candidates):
    """Require all closer rows and permit only genuine ties at the third distance."""
    if len(rows) != min(3, len(candidates)):
        return False
    if not candidates:
        return True
    distances = np.asarray([r[4] for r in candidates], dtype=float)
    cutoff = distances[-1]
    values = np.asarray([r[:4] for r in candidates], dtype=float)
    values[:, 2] = sla_filtered.apply_scaling(values[:, 2])

    def counts(array):
        return Counter(row.tobytes() for row in canonical_rows(array.T))

    chosen, allowed = counts(rows), counts(values)
    required = counts(values[distances < cutoff])
    return not (chosen - allowed) and not (required - chosen)


def match_outputs(first, second, *, nearest_reference=None):
    # Materialize once: repeated reads of an NPZ member repeatedly decompress it.
    with np.load(first) as left, np.load(second) as right:
        a = {key: left[key] for key in left.files}
        b = {key: right[key] for key in right.files}
        if a["counts"].shape != b["counts"].shape:
            return {"matched": False, "reason": "location count differs"}
        mismatches = np.flatnonzero(
            (a["counts"] != b["counts"]) | (a["hashes"] != b["hashes"])
        )
        samples, rejected, accepted = [], [], []
        for i in mismatches:
            left = a["rows"][a["offsets"][i] : a["offsets"][i + 1]]
            right = b["rows"][b["offsets"][i] : b["offsets"][i + 1]]
            if nearest_reference is not None:
                candidates = nearest_reference(int(i))
                if nearest_selection_matches(
                    left, candidates
                ) and nearest_selection_matches(right, candidates):
                    accepted.append(int(i))
                    continue
            rejected.append(int(i))
            if len(samples) >= 10:
                continue
            samples.append(
                {
                    "location_index": int(i),
                    "old_count": len(left),
                    "new_count": len(right),
                    "old_preview": [
                        [float(v) if np.isfinite(v) else None for v in row]
                        for row in left[:5]
                    ],
                    "new_preview": [
                        [float(v) if np.isfinite(v) else None for v in row]
                        for row in right[:5]
                    ],
                }
            )
        return {
            "matched": not rejected,
            "points": len(a["counts"]),
            "old_rows": int(a["counts"].sum()),
            "new_rows": int(b["counts"].sum()),
            "exact_mismatched_locations": len(mismatches),
            "verified_boundary_ties": len(accepted),
            "mismatched_locations": len(rejected),
            "examples": samples,
        }


def validate_outputs(directory, target, recorder):
    def paths(query):
        return [
            directory / "jobs" / f"warmup_{query}_{v}" / "validation.npz"
            for v in ("old", "new")
        ]

    validation = {"radius": match_outputs(*paths("radius"))}
    inputs = common.read_json(directory / "inputs.json")
    mask = BasinMask()
    statement = target.load_sql_file(
        "queries/benchmark/nearest_boundary_validation.sql"
    )
    proofs = []
    started = time.monotonic()
    # This independent reference check is outside every reported API batch timer.
    with psycopg.connect(
        target.config.postgres_dsn,
        options="-c default_transaction_read_only=on -c statement_timeout=30000",
        application_name="comparison_tie_validation",
    ) as connection:

        def reference(i):
            if time.monotonic() - started > TIMEOUT:
                raise RuntimeError(
                    "Nearest-neighbor reference validation exceeded its deadline"
                )
            lat, lon = inputs["latitudes"][i], inputs["longitudes"][i]
            params = {
                "latitude": lat,
                "longitude": lon,
                "central_date_time": datetime.fromisoformat(inputs["central_date"]),
                "time_delta": timedelta(days=10),
                "connected_basin_ids": inputs["basin_map"][
                    str(int(mask.lookup(lat, lon)))
                ],
            }
            rows = connection.execute(statement, params).fetchall()
            proofs.append(
                {
                    "location_index": i,
                    "parameters": params,
                    "candidates": [
                        [float(v) if v is not None else None for v in row]
                        for row in rows
                    ],
                }
            )
            if len(proofs) % 500 == 0:
                recorder.event("nearest_ties_checked", locations=len(proofs))
            return rows

        validation["nn"] = match_outputs(*paths("nn"), nearest_reference=reference)
    common.atomic_json(
        directory / "nearest-tie-proofs.json", {"sql": statement, "checks": proofs}
    )
    common.atomic_json(directory / "validation.json", validation)
    return validation


def prepare(old_root, directory, reuse_from=None):
    old_root, directory = Path(old_root).resolve(), Path(directory).resolve()
    config = Config()
    if config.postgres_host not in {"localhost", "127.0.0.1", "::1"}:
        raise RuntimeError(
            "Comparison requires the local PostgreSQL server for disk checks"
        )
    if subprocess.check_output(
        ["git", "status", "--porcelain", "--untracked-files=no"],
        cwd=old_root,
        text=True,
    ).strip():
        raise RuntimeError("Legacy checkout has tracked changes")
    with psycopg.connect(config.postgres_dsn_admin) as connection:
        data_directory = connection.execute("SHOW data_directory").fetchone()[0]
    if shutil.disk_usage(data_directory).free < 40 * common.GIB:
        raise RuntimeError(
            "Need at least 40 GiB free for source, two clones and reserve"
        )
    files = AlongTrackETL(config=config).discover_files(
        missions=["all"],
        start_date=datetime(2022, 10, 5),
        end_date=datetime(2022, 10, 25),
    )["files"]
    if len(files) != EXPECTED_FILES:
        raise RuntimeError(
            f"Expected {EXPECTED_FILES} raw files, discovered {len(files)}"
        )
    tree = ast.parse((old_root / "python/OceanDB/AlongTrack.py").read_text())
    legacy_class = next(
        n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == "AlongTrack"
    )
    missions = next(
        ast.literal_eval(n.value)
        for n in legacy_class.body
        if isinstance(n, ast.Assign)
        and any(isinstance(t, ast.Name) and t.id == "missions" for t in n.targets)
    )
    if any(p.name.split("_")[2] not in missions for p in files):
        raise RuntimeError("Legacy all-missions filter would exclude input data")
    old_mask = old_root / "data/basin_masks/new_basin_mask.nc"
    new_mask = ROOT / "src/OceanDB/data/basin_masks/new_basin_mask.nc"
    if old_mask.read_bytes() != new_mask.read_bytes():
        raise RuntimeError("Basin masks differ")
    grid = batch_scenario_grid(
        method_name="geographic_point_in_r_dt_batch",
        central_date=datetime(2022, 10, 15),
        time_window=timedelta(days=10),
        scenario_kwargs={"radius": 50000},
    )
    if len(grid.kwargs["latitudes"]) != 31966:
        raise RuntimeError("Unexpected ocean grid")
    directory.mkdir(parents=True, exist_ok=False)
    run_id = (
        datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ").lower()
        + "_"
        + uuid.uuid4().hex[:6]
    )
    manifest = {
        "format_version": 1,
        "run_id": run_id,
        "created_at": common.now(),
        "old_root": str(old_root),
        "new_root": str(ROOT),
        "connection": common.connection_identity(config),
        "old_commit": subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=old_root, text=True
        ).strip(),
        "new_commit": subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True
        ).strip(),
        "code_hashes": code_hashes(old_root),
        "environment": common.environment(),
        "databases": {
            version: f"cmp_{run_id}_{version}" for version in ("source", "old", "new")
        },
        "timeout_seconds": TIMEOUT,
        "expected_files": EXPECTED_FILES,
        "expected_rows": EXPECTED_ROWS,
        "data_directory": data_directory,
        "ingest_mode": "copy",
        "legacy_missions": missions,
        "files": [
            {
                "name": p.name,
                "path": str(p),
                "bytes": p.stat().st_size,
                "mtime_ns": p.stat().st_mtime_ns,
            }
            for p in sorted(files)
        ],
        "indexes": {
            version: [asdict(d) for d in definitions(version)]
            for version in ("old", "new")
        },
        "workload": {
            "central_date": "2022-10-15T00:00:00",
            "radius_m": 50000,
            "time_window_days_each_side": 10,
            "nearest_limit": 3,
            "nearest_max_radius": None,
            "missions": "all",
            "fields": list(("longitude", "latitude", "sla_filtered", "delta_t")),
            "points": 31966,
            "warmup_passes": 1,
            "measured_passes": 5,
        },
    }
    inputs = {
        "latitudes": grid.kwargs["latitudes"],
        "longitudes": grid.kwargs["longitudes"],
        "central_date": manifest["workload"]["central_date"],
        "radius_m": 50000,
    }
    common.atomic_json(directory / "grid.json", inputs)
    manifest["grid_sha256"] = common.digest(common.read_json(directory / "grid.json"))
    if reuse_from is not None:
        configure_reuse(Path(reuse_from).resolve(), directory, manifest)
    common.atomic_json(directory / "manifest.json", manifest)
    state = {
        "status": "prepared",
        "manifest_sha256": common.digest(manifest),
        "jobs": schedule(),
    }
    common.Recorder(directory, state).save()
    with zipfile.ZipFile(
        directory / "source-code.zip", "x", zipfile.ZIP_DEFLATED
    ) as archive:
        for name in manifest["code_hashes"]:
            version, relative = name.split("/", 1)
            archive.write((old_root if version == "old" else ROOT) / relative, name)
    common.Recorder(directory, state).event(
        "prepared",
        configurations=2,
        queries=2,
        jobs=len(state["jobs"]),
        points=31966,
        timeout_seconds=TIMEOUT,
        database_names=manifest["databases"],
    )
    return directory


def configure_reuse(previous, directory, manifest):
    """Pin the prior preparation, while starting a fresh validation/timing schedule."""
    prior = common.read_json(previous / "manifest.json")
    state = common.read_json(previous / "results.json")
    if state["status"] not in {"failed", "completed", "completed_with_failures"}:
        raise RuntimeError("The previous run is not terminal")
    if state["manifest_sha256"] != common.digest(prior):
        raise RuntimeError("Previous manifest changed")
    for key in (
        "connection",
        "environment",
        "old_root",
        "new_root",
        "old_commit",
        "files",
        "indexes",
        "workload",
        "grid_sha256",
        "legacy_missions",
    ):
        if prior[key] != manifest[key]:
            raise RuntimeError(f"Cannot reuse preparation: {key} differs")
    # Only validation/restart tooling may change; query, ingestion and schema code must match.
    allowed = {
        "new/compare_refactor.py",
        "new/src/OceanDB/sql/queries/benchmark/nearest_boundary_validation.sql",
    }
    for name in set(prior["code_hashes"]) | set(manifest["code_hashes"]):
        if name not in allowed and prior["code_hashes"].get(name) != manifest[
            "code_hashes"
        ].get(name):
            raise RuntimeError(f"Cannot reuse preparation after code change: {name}")
    saved = directory / "reused-preparation"
    saved.mkdir()
    for name in (
        "manifest.json",
        "source-database.json",
        "old-database.json",
        "new-database.json",
        "inputs.json",
        "ingestion.json",
    ):
        shutil.copyfile(previous / name, saved / name)
    manifest["databases"] = prior["databases"]
    manifest["database_owner_run_id"] = prior.get(
        "database_owner_run_id", prior["run_id"]
    )
    manifest["reuse_preparation"] = {
        "previous_directory": str(previous),
        "manifest_sha256": common.digest(prior),
        "files": {
            p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in saved.iterdir()
        },
    }


def database(config, name, version=None):
    return OceanDBInit(
        config=config.model_copy(
            update={"postgres_database": name, "ingest_mode": "copy"}
        ),
        managed_indices=ManagedIndices(tuple(definitions(version)) if version else ()),
    )


def create_source(config, manifest, recorder):
    source = database(config, manifest["databases"]["source"])
    if source.database_exists():
        raise RuntimeError("Source name already exists; refusing to ingest into it")
    recorder.event("source_initializing", database=source.db_name)
    source.initialize_database(partition_start="2022-09-01", partition_end="2022-11-01")
    with source.cursor(
        autocommit=True, connection_string=config.postgres_dsn_admin
    ) as cur:
        cur.execute(
            sql.SQL("COMMENT ON DATABASE {} IS {}").format(
                sql.Identifier(source.db_name),
                sql.Literal("OceanDB comparison " + manifest["run_id"]),
            )
        )
    recorder.event("ingestion_started", files=EXPECTED_FILES, mode="copy", workers=4)
    info = AlongTrackETL(config=source.config).ingest(
        missions=["all"],
        start_date=datetime(2022, 10, 5),
        end_date=datetime(2022, 10, 25),
        workers=4,
        on_progress=lambda event: recorder.event("ingest", detail=event),
    )
    common.atomic_json(recorder.directory / "ingestion.json", info)
    recorder.event("ingestion_finished")
    facts = common.source_facts(source)
    if (
        sum(r[1] for r in facts["dataset"]["files"]) != EXPECTED_ROWS
        or len(facts["dataset"]["files"]) != EXPECTED_FILES
    ):
        raise RuntimeError(
            "Ingested dataset differs from the previous benchmark dataset"
        )
    with source.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM along_track WHERE mission IS DISTINCT FROM split_part(file_name, '_', 3) OR NOT (mission = ANY(%s))",
            (manifest["legacy_missions"],),
        )
        if cur.fetchone()[0]:
            raise RuntimeError(
                "Mission column and legacy filename filter are not equivalent"
            )
    common.atomic_json(recorder.directory / "source-database.json", facts)
    mapping = BasinConnections(config=source.config).connection_map
    inputs = common.read_json(recorder.directory / "grid.json")
    inputs["basin_map"] = {str(int(k)): [int(i) for i in v] for k, v in mapping.items()}
    common.atomic_json(recorder.directory / "inputs.json", inputs)
    recorder.event("source_validated", rows=EXPECTED_ROWS, files=EXPECTED_FILES)
    return source, facts


def clone(source, config, manifest, version, facts, recorder):
    if shutil.disk_usage(manifest["data_directory"]).free < 26 * common.GIB:
        raise RuntimeError("Disk reserve reached")
    target = database(config, manifest["databases"][version], version)
    if target.database_exists():
        raise RuntimeError("Trial name already exists; refusing to modify it")
    with source.cursor(
        autocommit=True, connection_string=config.postgres_dsn_admin
    ) as cur:
        cur.execute(
            sql.SQL("CREATE DATABASE {} TEMPLATE {}").format(
                sql.Identifier(target.db_name), sql.Identifier(source.db_name)
            )
        )
        cur.execute(
            sql.SQL("COMMENT ON DATABASE {} IS {}").format(
                sql.Identifier(target.db_name),
                sql.Literal("OceanDB comparison " + manifest["run_id"]),
            )
        )
    target.create_indexes(definitions(version))
    if not index_database_is_reusable(
        source.inventory_indexes(), definitions(version), target.inventory_indexes()
    ):
        raise RuntimeError(f"Actual {version} index definitions differ from the plan")
    if common.dataset_facts(target) != facts["dataset"]:
        raise RuntimeError(f"{version} clone does not contain identical observations")
    target.vacuum_analyze("along_track")
    snapshot = common.statistics(target)
    if common.digest(snapshot["settings"]) != common.digest(facts["planner_settings"]):
        raise RuntimeError("Planner settings differ between source and clone")
    if not {"mission", "basin_id", "along_track_point", "date_time"}.issubset(
        r[0] for r in snapshot["columns"] if r[1]
    ):
        raise RuntimeError("Inherited parent-table statistics are missing")
    snapshot["index_sizes"] = {
        d.name: sum(
            target.get_index_size(i.index_name)
            for i in target.inventory_indexes()
            if i.index_name == d.name or i.parent_index_name == d.name
        )
        for d in definitions(version)
    }
    common.atomic_json(recorder.directory / f"{version}-database.json", snapshot)
    recorder.event(
        "database_ready",
        version=version,
        database=target.db_name,
        index_sizes=snapshot["index_sizes"],
    )
    return target


def reuse_databases(config, manifest, recorder):
    saved = recorder.directory / "reused-preparation"
    for name, expected in manifest["reuse_preparation"]["files"].items():
        if hashlib.sha256((saved / name).read_bytes()).hexdigest() != expected:
            raise RuntimeError("Saved preparation changed")
    names = list(manifest["databases"].values())
    with psycopg.connect(config.postgres_dsn_admin) as connection:
        owners = dict(
            connection.execute(
                "SELECT datname, shobj_description(oid, 'pg_database') FROM pg_database WHERE datname=ANY(%s)",
                (names,),
            ).fetchall()
        )
        if any(
            owners.get(name)
            != "OceanDB comparison " + manifest["database_owner_run_id"]
            for name in names
        ):
            raise RuntimeError("Reused database is missing or belongs to another run")
        if connection.execute(
            "SELECT pid FROM pg_stat_activity WHERE datname=ANY(%s) AND backend_type='client backend'",
            (names,),
        ).fetchall():
            raise RuntimeError("Reused databases still have client sessions")
    recorder.event("preparation_reuse_started", databases=manifest["databases"])
    source = database(config, manifest["databases"]["source"])
    facts = common.source_facts(source)
    previous = common.read_json(saved / "source-database.json")
    for key in (
        "dataset",
        "indexes",
        "version",
        "extensions",
        "planner_settings",
        "data_directory",
    ):
        if facts[key] != previous[key]:
            raise RuntimeError(f"Source preparation changed: {key}")
    common.atomic_json(recorder.directory / "source-database.json", facts)
    inputs = common.read_json(saved / "inputs.json")
    if {k: v for k, v in inputs.items() if k != "basin_map"} != common.read_json(
        recorder.directory / "grid.json"
    ):
        raise RuntimeError("Reused inputs differ from the grid")
    mapping = {
        str(int(k)): list(map(int, v))
        for k, v in BasinConnections(config=source.config).connection_map.items()
    }
    if inputs["basin_map"] != mapping:
        raise RuntimeError("Basin connectivity changed")
    common.atomic_json(recorder.directory / "inputs.json", inputs)
    targets = {}
    for version in ("old", "new"):
        target = database(config, manifest["databases"][version], version)
        if common.dataset_facts(target) != facts["dataset"]:
            raise RuntimeError(f"{version} observations changed")
        if not index_database_is_reusable(
            source.inventory_indexes(), definitions(version), target.inventory_indexes()
        ):
            raise RuntimeError(f"{version} indexes changed")
        target.vacuum_analyze("along_track")
        snapshot = common.statistics(target)
        if common.digest(snapshot["settings"]) != common.digest(
            facts["planner_settings"]
        ):
            raise RuntimeError("Planner settings differ")
        if not {"mission", "basin_id", "along_track_point", "date_time"}.issubset(
            r[0] for r in snapshot["columns"] if r[1]
        ):
            raise RuntimeError("Inherited parent statistics are missing")
        snapshot["index_sizes"] = {
            d.name: sum(
                target.get_index_size(i.index_name)
                for i in target.inventory_indexes()
                if i.index_name == d.name or i.parent_index_name == d.name
            )
            for d in definitions(version)
        }
        common.atomic_json(recorder.directory / f"{version}-database.json", snapshot)
        recorder.event("database_revalidated", version=version, database=target.db_name)
        targets[version] = target
    return targets


def summary_rows(state):
    rows = []
    for query in ("radius", "nn"):
        row = {
            "query": query,
            "old_median": None,
            "new_median": None,
            "speedup": None,
            "saved_percent": None,
        }
        for version in ("old", "new"):
            jobs = [
                j
                for j in state["jobs"]
                if j["query"] == query
                and j["version"] == version
                and not j["validation"]
            ]
            values = [j["seconds"] for j in jobs if j["status"] == "completed"]
            row[version + "_completed"] = len(values)
            if len(values) == 5:
                row[version + "_median"] = stats.median(values)
                row[version + "_range"] = [min(values), max(values)]
        if (
            row["old_median"]
            and row["new_median"]
            and state.get("validation", {}).get(query, {}).get("matched")
        ):
            row["speedup"] = row["old_median"] / row["new_median"]
            row["saved_percent"] = 100 * (1 - row["new_median"] / row["old_median"])
        rows.append(row)
    return rows


def report(directory, manifest, state):
    rows = summary_rows(state)
    common.atomic_json(directory / "summary.json", rows)
    headers = [
        "Query",
        "Old median (range), s",
        "New median (range), s",
        "Old / new speedup",
        "Time saved",
    ]
    body = []
    for row in rows:
        cells = [
            (
                "Radius 50 km"
                if row["query"] == "radius"
                else "Nearest 3, unlimited radius"
            )
        ]
        for version in ("old", "new"):
            if row[version + "_median"] is None:
                cells.append(
                    f"Unavailable ({row[version + '_completed']}/5 passes completed)"
                )
            else:
                lo, hi = row[version + "_range"]
                cells.append(f"{row[version + '_median']:.3f} ({lo:.3f}–{hi:.3f})")
        cells.extend(
            [
                f"{row['speedup']:.3f}×" if row["speedup"] else "—",
                f"{row['saved_percent']:+.1f}%" if row["speedup"] else "—",
            ]
        )
        body.append(cells)
    description = (
        f"Run status: {state['status']}. Old commit {manifest['old_commit'][:12]}; new commit {manifest['new_commit'][:12]}. "
        "31,966 locations per pass, October 15, 2022 ±10 days, all missions, equivalent BMA plus four singleton GiST indexes. "
        "One validation/warm-up pass and five measured passes with alternating version order. "
        "Seconds cover native batched API calls, fetching, result construction, and access to all four output arrays. "
        "Imports, object initialization, cached mask/connectivity loading, maintenance, validation sorting, and EXPLAIN are outside reported times. "
        "The same canonical basin lists are supplied to both versions. Old uses an expression mission index; new uses the stored mission column. "
        "This measures code versions on equivalent observations/index keys, not historical server or ingestion performance. "
        "Median speedup is old median / new median. All five passes and matching validation are required for a speedup. "
        "Each worker has a 900-second wall cap; failed/timed-out passes have no successful timing and are not retried. Warm-up failures stop the comparison. "
        "Warm-cache results; caches and thermals are not experimentally reset. Old radius/NN methods already use executemany."
    )
    if state.get("validation"):
        description += (
            f" Radius validation: {state['validation']['radius']['mismatched_locations']} unmatched locations. "
            f"NN validation: {state['validation']['nn'].get('verified_boundary_ties', 0)} locations accepted after "
            "independent PostgreSQL verification of exact third-neighbor ties; "
            f"{state['validation']['nn']['mismatched_locations']} unmatched locations. "
            "The timed queries keep their original distance-only ordering."
        )
    lines = [
        "# Pre-refactor versus current OceanDB",
        "",
        description,
        "",
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join(["---"] * len(headers)) + " |",
        *["| " + " | ".join(c) + " |" for c in body],
    ]
    if state.get("exception"):
        lines.extend(["", "Run exception:", "```", state["exception"], "```"])
    lines.extend(
        ["", "## Every pass", "", "| Pass | Status | Seconds |", "| --- | --- | --- |"]
    )
    for job in state["jobs"]:
        seconds = (
            f"{job['seconds']:.3f}"
            if job["status"] == "completed" and not job["validation"]
            else "—"
        )
        lines.append(f"| {job['id']} | {job['status']} | {seconds} |")
    (directory / "summary.md").write_text("\n".join(lines) + "\n")
    table = (
        "<table><tr>"
        + "".join("<th>" + html.escape(h) + "</th>" for h in headers)
        + "</tr>"
    )
    for cells in body:
        table += (
            "<tr>" + "".join("<td>" + html.escape(c) + "</td>" for c in cells) + "</tr>"
        )
    page = (
        '<!doctype html><html lang="en"><meta charset="utf-8"><title>OceanDB comparison</title>'
        "<style>body{font:16px/1.5 system-ui;max-width:1200px;margin:32px auto;padding:0 24px}table{border-collapse:collapse;width:100%}th,td{padding:12px;border:1px solid #ccc;text-align:left}th{background:#eef3f6}</style>"
        "<h1>Pre-refactor versus current OceanDB</h1><p>"
        + html.escape(description)
        + "</p>"
        + table
        + "</table>"
        "<p>See summary.md and results.json for every pass and failure details.</p></html>"
    )
    (directory / "summary.html").write_text(page)


def run(directory):
    directory = Path(directory).resolve()
    with (directory / ".runner.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        manifest, state = common.read_json(
            directory / "manifest.json"
        ), common.read_json(directory / "results.json")
        config = Config()
        if state["manifest_sha256"] != common.digest(manifest) or manifest[
            "code_hashes"
        ] != code_hashes(Path(manifest["old_root"])):
            raise RuntimeError("Manifest or code changed after preparation")
        if (
            manifest["connection"] != common.connection_identity(config)
            or manifest["environment"] != common.environment()
        ):
            raise RuntimeError("Connection or Python environment changed")
        if (
            common.digest(common.read_json(directory / "grid.json"))
            != manifest["grid_sha256"]
        ):
            raise RuntimeError("Grid changed")
        if state["status"] != "prepared":
            raise RuntimeError(
                "Run already started; inspect its checkpoints instead of automatically retrying"
            )
        recorder = common.Recorder(directory, state)
        state["status"] = "preparing_databases"
        recorder.save()
        try:
            for item in manifest["files"]:
                path = Path(item["path"])
                if (
                    path.stat().st_size != item["bytes"]
                    or path.stat().st_mtime_ns != item["mtime_ns"]
                ):
                    raise RuntimeError("Raw input files changed")
            if manifest.get("reuse_preparation"):
                databases = reuse_databases(config, manifest, recorder)
            else:
                source, facts = create_source(config, manifest, recorder)
                databases = {
                    version: clone(source, config, manifest, version, facts, recorder)
                    for version in ("old", "new")
                }
            state["status"] = "validating"
            recorder.save()
            failed_cases = set()
            for job in state["jobs"]:
                if (job["version"], job["query"]) in failed_cases:
                    job.update(
                        status="skipped",
                        exception="Earlier measured pass of this case failed; no automatic retry",
                    )
                    recorder.save()
                    continue
                if manifest["code_hashes"] != code_hashes(Path(manifest["old_root"])):
                    raise RuntimeError("Code changed during the comparison")
                if not job["validation"] and state["status"] == "validating":
                    recorder.event("correctness_check_started")
                    validation = validate_outputs(directory, databases["new"], recorder)
                    state["validation"] = validation
                    common.atomic_json(directory / "validation.json", validation)
                    if not all(v["matched"] for v in validation.values()):
                        raise RuntimeError(
                            "Legacy/current query results differ; timings are blocked. See validation.json"
                        )
                    state["status"] = "measuring"
                    recorder.event("correctness_passed", validation=validation)
                job.update(status="running", started_at=common.now(), seconds=None)
                recorder.save()
                recorder.event(
                    "pass_started", pass_id=job["id"], validation=job["validation"]
                )
                specification = {
                    **job,
                    "code_root": (
                        manifest["old_root"]
                        if job["version"] == "old"
                        else manifest["new_root"]
                    ),
                    "inputs_path": str(directory / "inputs.json"),
                }

                def event(kind, **values):
                    if kind == "worker_started":
                        job.update(
                            worker_pid=values["pid"],
                            application_name=values["application_name"],
                        )
                        recorder.save()
                    recorder.event(kind, pass_id=job["id"], **values)

                job.update(
                    supervise(
                        specification,
                        databases[job["version"]].config,
                        directory / "jobs" / job["id"],
                        event,
                        manifest["timeout_seconds"],
                    )
                )
                recorder.save()
                recorder.event(
                    "pass_finished",
                    pass_id=job["id"],
                    status=job["status"],
                    seconds=job["seconds"] if not job["validation"] else None,
                    exception=job["exception"],
                )
                if job["status"] != "completed":
                    if job["validation"]:
                        raise RuntimeError(
                            "Validation/warm-up failed; comparison stopped"
                        )
                    failed_cases.add((job["version"], job["query"]))
            state["status"] = (
                "completed"
                if all(j["status"] == "completed" for j in state["jobs"])
                else "completed_with_failures"
            )
            recorder.event(
                "comparison_finished",
                counts=dict(Counter(j["status"] for j in state["jobs"])),
            )
        except BaseException:
            state.update(status="failed", exception=traceback.format_exc())
            for job in state["jobs"]:
                if job["status"] == "pending":
                    job.update(
                        status="skipped", exception="Run stopped before this pass"
                    )
                elif job["status"] == "running":
                    job.update(
                        status="interrupted", seconds=None, exception=state["exception"]
                    )
            recorder.event("comparison_failed", exception=state["exception"])
            raise
        finally:
            state["finished_at"] = common.now()
            recorder.save()
            report(directory, manifest, state)
            recorder.event(
                "report_written",
                html=str(directory / "summary.html"),
                archive=str(common.archive_path(directory)),
            )
            common.package(directory)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--prepare", type=Path)
    group.add_argument("--run", type=Path)
    parser.add_argument("--old-root", type=Path)
    parser.add_argument("--reuse-databases-from", type=Path)
    args = parser.parse_args()
    if args.prepare:
        if not args.old_root:
            parser.error("--prepare requires --old-root")
        print(prepare(args.old_root, args.prepare, args.reuse_databases_from))
    else:
        if args.reuse_databases_from:
            parser.error("--reuse-databases-from requires --prepare")
        run(args.run)
