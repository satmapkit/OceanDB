"""Private execution support for optimize_geo_points_r_dt.py (not a library API)."""

import argparse
from collections import Counter
from dataclasses import asdict
from datetime import datetime, timezone
import fcntl
import hashlib
import html
import importlib.metadata
import json
import multiprocessing
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import time
import traceback
import uuid
import zipfile

import psycopg
from psycopg import sql

from OceanDB.config import Config
from OceanDB.index_experiment import index_database_is_reusable, index_definition
from OceanDB.managed_indices import ManagedIndices
from OceanDB.OceanDB_Initializer import OceanDBInit
from OceanDB.query_analysis import QueryAnalysisRunner
from optimize_geo_points_r_dt import (
    EXPECTED_FILES, EXPECTED_POINTS, EXPECTED_ROWS, GRID_DEGREES, SCENARIO_LABELS,
    TIMEOUT_SECONDS, basic_indexes, build_scenarios, candidate_specs,
)

ROOT = Path(__file__).resolve().parent
GIB = 1024 ** 3
TERMINAL = {"completed", "timed_out", "failed"}


def now():
    return datetime.now(timezone.utc).isoformat()


def json_default(value):
    if isinstance(value, set):
        return sorted(value)
    if isinstance(value, (datetime, Path)):
        return str(value)
    raise TypeError(f"Cannot serialize {type(value).__name__}")


def atomic_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(json.dumps(value, indent=2, default=json_default, allow_nan=False) + "\n")
    temporary.replace(path)


def read_json(path):
    return json.loads(Path(path).read_text())


def code_files():
    tracked = subprocess.check_output(["git", "ls-files", "-z"], cwd=ROOT).decode().split("\0")
    extra = ["benchmark_runtime.py", "markdown/index-benchmark.md",
             "tests/benchmark/test_expanded_benchmark.py"]
    return sorted({p for p in tracked + extra if p and (ROOT / p).is_file()
                   and not Path(p).name.startswith(".env")})


def code_hashes():
    return {p: hashlib.sha256((ROOT / p).read_bytes()).hexdigest() for p in code_files()}


def validate_manifest(manifest):
    suite = manifest.get("suite", "expanded")
    included, excluded = candidate_specs(suite)
    token = manifest["run_id"]
    if not re.fullmatch(r"\d{8}_\d{6}_[0-9a-f]{6}", token):
        raise RuntimeError("Invalid run identifier")
    for spec in included:
        spec["database_name"] = f"{'idx2' if suite == 'pairs' else 'idx34'}_{token}_{spec['id']}"
        spec["indexes"] = [asdict(d) for d in definitions(spec)]
    if manifest["included"] != included or manifest["excluded"] != excluded:
        raise RuntimeError("Candidate definitions or database ownership differ from this run's plan")
    for key, expected in {"format_version": 1, "grid_degrees": GRID_DEGREES,
                          "ocean_points_per_scenario": EXPECTED_POINTS, "timeout_seconds": TIMEOUT_SECONDS,
                          "passes": 1, "central_date": "2022-10-15", "time_window_days_each_side": 10,
                          "radius_m": 50000, "nearest_max_radius_m": 500000, "nearest_limit": 3,
                          "scenarios": list(SCENARIO_LABELS)}.items():
        if manifest.get(key) != expected:
            raise RuntimeError(f"Run configuration mismatch: {key}")


def definitions(spec):
    return basic_indexes() + ([index_definition("experiment", tuple(spec["fields"]))]
                             if spec["fields"] else [])


def database_for(config, spec):
    return OceanDBInit(config=config.model_copy(update={"postgres_database": spec["database_name"]}),
                       managed_indices=ManagedIndices(tuple(definitions(spec))))


def verify_inventory(database, source_indexes, desired):
    actual = database.inventory_indexes()
    if not index_database_is_reusable(source_indexes, desired, actual):
        raise RuntimeError(f"Unexpected, missing, or invalid indexes in {database.db_name}")
    # The shared reuse helper checks names; also verify actual column order and type.
    wanted = {d.name: tuple(re.search(r"USING gist \(([^)]+)\)", d.create_sql)[1].split(", "))
              for d in desired}
    for index in actual:
        name = index.parent_index_name or index.index_name
        if name in wanted:
            match = re.search(r"USING gist \(([^)]+)\)", index.index_definition)
            if index.access_method != "gist" or not match or tuple(match[1].split(", ")) != wanted[name]:
                raise RuntimeError(f"Wrong index definition: {index.index_definition}")
    return actual


def metadata(database):
    with database.cursor() as cur:
        cur.execute("SELECT attname, inherited, n_distinct FROM pg_stats "
                    "WHERE schemaname='public' AND tablename='along_track' ORDER BY attname, inherited")
        parent_stats = cur.fetchall()
        cur.execute("SELECT relname, last_analyze, last_autoanalyze, n_live_tup FROM pg_stat_user_tables "
                    "WHERE relname='along_track' OR relname LIKE 'along_track_2022%' ORDER BY relname")
        tables = cur.fetchall()
    return {"captured_at": now(), "parent_statistics": parent_stats, "table_statistics": tables,
            "indexes": [asdict(index) for index in database.inventory_indexes()]}


def source_facts(config):
    database = OceanDBInit(config=config)
    if config.postgres_host not in {"localhost", "127.0.0.1"}:
        raise RuntimeError("This benchmark is configured for the existing local PostgreSQL server")
    if not database.database_exists():
        raise RuntimeError("The prepared source database is missing; this runner never ingests data")
    with database.cursor() as cur:
        cur.execute("SELECT pid, application_name FROM pg_stat_activity "
                    "WHERE datname=current_database() AND pid<>pg_backend_pid()")
        other_sessions = cur.fetchall()
        if other_sessions:
            raise RuntimeError(f"Source database has other sessions that would prevent cloning: {other_sessions}")
        cur.execute("SELECT count(*), min(date_time), max(date_time) FROM along_track")
        count, first, last = cur.fetchone()
        cur.execute("SELECT file_name FROM along_track_metadata ORDER BY file_name")
        files = [row[0] for row in cur.fetchall()]
        cur.execute("SELECT version(), pg_database_size(current_database())")
        version, size = cur.fetchone()
        cur.execute("SHOW data_directory")
        data_directory = cur.fetchone()[0]
        cur.execute("SELECT extname, extversion FROM pg_extension ORDER BY extname")
        extensions = cur.fetchall()
    if count != EXPECTED_ROWS or len(files) != EXPECTED_FILES:
        raise RuntimeError(f"Unexpected source data: {count} rows and {len(files)} files")
    identity = {"host": config.postgres_host, "port": config.postgres_port,
                "username": config.postgres_username, "database": config.postgres_database,
                "rows": count, "first": str(first), "last": str(last), "files": files,
                "indexes": [asdict(index) for index in database.inventory_indexes()]}
    return {"identity": identity, "postgres_version": version, "extensions": extensions,
            "source_bytes": size, "data_directory": data_directory}


def check_disk(config, manifest):
    with psycopg.connect(config.postgres_dsn_admin, autocommit=True) as conn:
        sizes = dict(conn.execute("SELECT datname, pg_database_size(oid) FROM pg_database "
                                  "WHERE datname = ANY(%s)",
                                  ([s["database_name"] for s in manifest["included"]],)).fetchall())
    required = sum(max(0, 5.2 * GIB - sizes.get(s["database_name"], 0))
                   for s in manifest["included"]) + 20 * GIB
    free = shutil.disk_usage(manifest["source"]["data_directory"]).free
    if free < required:
        raise RuntimeError(f"Insufficient disk space: {free/GIB:.1f} GiB free, {required/GIB:.1f} GiB required")
    return {"free_gib": free / GIB, "required_gib_including_reserve": required / GIB}


class Recorder:
    def __init__(self, run_dir, state):
        self.run_dir, self.state = Path(run_dir), state

    def event(self, event, **values):
        item = {"timestamp": now(), "event": event, **values}
        print(json.dumps(item, default=json_default), flush=True)
        self.state.update(updated_at=item["timestamp"], last_event=item)
        atomic_json(self.run_dir / "status.json", self.state)

    def results(self, results):
        atomic_json(self.run_dir / "results.json", results)


def prepare(run_dir, suite="expanded", compare_with=None):
    run_dir = Path(run_dir).resolve()
    run_dir.mkdir(parents=True, exist_ok=False)
    config = Config()
    included, excluded = candidate_specs(suite)
    expected_counts = {2: 9, 3: 1, 4: 1, 0: 1} if suite == "pairs" else {3: 16, 4: 12, 0: 1}
    assert Counter(s["column_count"] for s in included) == expected_counts
    build_scenarios()
    token = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S") + "_" + uuid.uuid4().hex[:6]
    for spec in included:
        spec["database_name"] = f"{'idx2' if suite == 'pairs' else 'idx34'}_{token}_{spec['id']}"
        spec["indexes"] = [asdict(d) for d in definitions(spec)]
    manifest = {"format_version": 1, "suite": suite, "created_at": now(), "run_id": token,
                "code_hashes": code_hashes(), "commit": subprocess.check_output(
                    ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
                "grid_degrees": GRID_DEGREES, "ocean_points_per_scenario": EXPECTED_POINTS,
                "timeout_seconds": TIMEOUT_SECONDS, "timeout_scope": "all four query scenarios combined",
                "passes": 1, "central_date": "2022-10-15", "time_window_days_each_side": 10,
                "radius_m": 50000, "nearest_max_radius_m": 500000, "nearest_limit": 3,
                "scenarios": list(SCENARIO_LABELS), "included": included, "excluded": excluded,
                "source": source_facts(config)}
    if compare_with is not None:
        compare_with = Path(compare_with).resolve()
        previous_manifest = read_json(compare_with.parent / "manifest.json")
        for key in ("grid_degrees", "ocean_points_per_scenario", "scenarios", "passes", "central_date",
                    "time_window_days_each_side", "radius_m", "nearest_max_radius_m", "nearest_limit"):
            if previous_manifest[key] != manifest[key]:
                raise RuntimeError(f"Comparison workload differs: {key}")
        if previous_manifest["source"]["identity"] != manifest["source"]["identity"]:
            raise RuntimeError("Comparison uses a different source dataset")
        previous = read_json(compare_with)
        baseline = [r for r in previous if not r["trial_indexes"]]
        if len(baseline) != 1 or baseline[0]["status"] != "completed" or len(baseline[0]["performance"]) != 4:
            raise RuntimeError("Comparison requires a completed four-scenario baseline")
        reference = run_dir / "reference"
        reference.mkdir()
        shutil.copyfile(compare_with, reference / "results.json")
        shutil.copyfile(compare_with.parent / "manifest.json", reference / "manifest.json")
        manifest["comparison"] = {"original_results": str(compare_with), "snapshot": "reference/results.json",
                                  "sha256": hashlib.sha256(compare_with.read_bytes()).hexdigest()}
    manifest["disk_preflight"] = check_disk(config, manifest)
    validate_manifest(manifest)
    atomic_json(run_dir / "manifest.json", manifest)
    with zipfile.ZipFile(run_dir / "source-code.zip", "x", compression=zipfile.ZIP_DEFLATED) as archive:
        for path in manifest["code_hashes"]:
            archive.write(ROOT / path, path)
    (run_dir / "source.patch").write_text(subprocess.check_output(["git", "diff"], cwd=ROOT, text=True))
    (run_dir / "python-packages.txt").write_text("\n".join(sorted(
        f"{d.metadata['Name']}=={d.version}" for d in importlib.metadata.distributions())) + "\n")
    atomic_json(run_dir / "results.json", [
        {"id": s["id"], "database_name": s["database_name"], "fields": s["fields"],
         "trial_indexes": [asdict(index_definition("experiment", tuple(s["fields"])))] if s["fields"] else [],
         "status": "pending", "performance": [], "index_sizes": None, "error": None}
        for s in included])
    Recorder(run_dir, {"phase": "prepared", "run_id": token}).event("prepared", configurations=len(included),
        **manifest["disk_preflight"])
    return run_dir


class CleanupError(RuntimeError):
    pass


def cleanup_sessions(config, app_name):
    """Cancel only the stopped worker's sessions; fail closed if cleanup is incomplete."""
    try:
        with psycopg.connect(config.postgres_dsn_admin, autocommit=True, connect_timeout=5,
                             application_name="idx34_cleanup", options="-c statement_timeout=5000") as conn:
            args = (config.postgres_database, app_name)
            predicate = "datname=%s AND application_name=%s AND backend_type='client backend'"
            conn.execute("SELECT pg_cancel_backend(pid) FROM pg_stat_activity WHERE " + predicate, args)
            conn.execute("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE " + predicate, args)
            until = time.monotonic() + 5
            while conn.execute("SELECT count(*) FROM pg_stat_activity WHERE " + predicate, args).fetchone()[0]:
                if time.monotonic() >= until:
                    raise CleanupError(f"Sessions remain for {app_name} in {config.postgres_database}")
                time.sleep(0.05)
    except Exception as exc:
        raise CleanupError(f"Could not verify session cleanup for {app_name}: {exc}") from exc


def query_worker(config, spec, directory, pipe, app_name):
    os.environ["PGAPPNAME"] = app_name
    os.environ["PGOPTIONS"] = "-c default_transaction_read_only=on -c statement_timeout=900000"
    try:
        scenarios = build_scenarios()
        runner = QueryAnalysisRunner(config=config, scenarios=scenarios,
                                     managed_indices=ManagedIndices(tuple(definitions(spec))))
        pipe.send({"event": "ready"})
        if pipe.recv() != "go":
            return
        for number, scenario in enumerate(scenarios):
            pipe.send({"event": "scenario_started", "scenario": SCENARIO_LABELS[number]})
            result = asdict(runner.analyze_statement(scenario))
            atomic_json(Path(directory) / f"scenario_{number}.json", result)
            pipe.send({"event": "scenario_completed", "scenario": SCENARIO_LABELS[number],
                       "number": number, "query_seconds": result["total_time"]})
        pipe.send({"event": "finished", "finished_monotonic": time.monotonic()})
    except BaseException:
        pipe.send({"event": "worker_failed", "exception": traceback.format_exc()})
    finally:
        pipe.close()


def supervise(config, spec, directory, app_name, event, timeout=TIMEOUT_SECONDS,
              target=query_worker):
    """Start the deadline only after worker initialization; enforce it outside libpq."""
    context = multiprocessing.get_context("spawn")
    parent, child = context.Pipe()
    worker = context.Process(target=target, args=(config, spec, str(directory), child, app_name), daemon=True)
    started = deadline = None
    status, exception = "failed", None
    worker.start()
    child.close()
    startup_deadline = time.monotonic() + 60
    try:
        event("worker_started", worker_pid=worker.pid, application_name=app_name)
        while True:
            remaining = (deadline if deadline is not None else startup_deadline) - time.monotonic()
            if remaining <= 0:
                status = "timed_out" if started is not None else "failed"
                exception = "900-second query-phase limit exceeded" if started is not None else "Worker startup timed out"
                if started is not None:
                    exception = f"{timeout:g}-second query-phase limit exceeded"
                break
            if parent.poll(min(remaining, 0.2)):
                try:
                    message = parent.recv()
                except EOFError:
                    exception = f"Worker exited without a terminal result (exit code {worker.exitcode})"
                    break
                kind = message.pop("event")
                if kind == "ready":
                    started = time.monotonic()
                    deadline = started + timeout
                    parent.send("go")
                    event("query_phase_started", timeout_seconds=timeout)
                elif kind == "finished":
                    status = "completed" if message["finished_monotonic"] <= deadline else "timed_out"
                    if status == "timed_out":
                        exception = f"{timeout:g}-second query-phase limit exceeded"
                    break
                elif kind == "worker_failed":
                    exception = message["exception"]
                    break
                else:
                    event(kind, **message)
            elif not worker.is_alive():
                exception = f"Worker exited unexpectedly (exit code {worker.exitcode})"
                break
    finally:
        query_finished = time.monotonic()
        # Stop the producer before cancelling sessions, preventing it opening another query.
        if worker.is_alive():
            if status == "completed":
                worker.join(timeout=2)
            if worker.is_alive():
                worker.terminate()
                worker.join(timeout=2)
            if worker.is_alive():
                worker.kill()
                worker.join(timeout=2)
        parent.close()
        cleanup_sessions(config, app_name)
    performance = [read_json(p) for p in sorted(Path(directory).glob("scenario_[0-3].json"))]
    if status == "completed" and len(performance) != 4:
        status, exception = "failed", "Worker finished without all four scenario results"
    return {"status": status, "exception": exception, "performance": performance,
            "error": sum(p["total_time"] for p in performance) if status == "completed" else None,
            "query_phase_wall_seconds": query_finished - started if started is not None else None,
            "worker_pid": None, "finished_at": now()}


def prepare_database(config, source, spec, run_dir):
    database = database_for(config, spec)
    if not database.database_exists():
        with source.cursor(autocommit=True, connection_string=config.postgres_dsn_admin) as cur:
            cur.execute(sql.SQL("CREATE DATABASE {} TEMPLATE {}").format(
                sql.Identifier(database.db_name), sql.Identifier(source.db_name)))
    # Only this run's names can reach here. Partial builds resume with IF NOT EXISTS.
    database.create_indexes(definitions(spec))
    actual = verify_inventory(database, source.inventory_indexes(), definitions(spec))
    directory = run_dir / "trials" / spec["id"]
    atomic_json(directory / "database-before.json", metadata(database))
    sizes = {d.name: sum(database.get_index_size(i.index_name) for i in actual
                         if i.index_name == d.name or i.parent_index_name == d.name)
             for d in definitions(spec)}
    return database, sizes


def report(run_dir, manifest, results):
    headers = ["Configuration", "Additional GiST columns (in order)", "Status", "Query total", "Scenarios completed"]
    rows = []
    for result in results:
        seconds = f"{result['error']:.2f} s" if result["status"] == "completed" else "—"
        rows.append([result["id"], ", ".join(result["fields"]) or "None (baseline)",
                     result["status"], seconds, f"{len(result['performance'])}/4"])
    explanation = (
        "One pass on a 1° grid, 31,966 ocean locations per scenario, using 9,519,966 observations. "
        "Each configuration includes separate GiST indexes on mission, basin_id, along_track_point, and date_time, "
        "plus the same inherited constraint indexes. Four scenarios cover radius and nearest-neighbor searches, "
        "with all missions and with s6a/j3n. VACUUM ANALYZE precedes every configuration. "
        "The 900-second limit covers its four scenarios and EXPLAIN replays combined. Query totals include "
        "Python/result processing but exclude maintenance, setup, and EXPLAIN replays. Partial results are "
        "preserved; timed-out or failed configurations have no comparable total."
    )
    title = ("OceanDB pair index benchmark with BMA and BADM controls" if manifest.get("suite") == "pairs"
             else "OceanDB triplet and quadruplet index benchmark")
    markdown = "# " + title + "\n\n" + explanation + "\n\n"
    markdown += "| " + " | ".join(headers) + " |\n|---|---|---|---:|---:|\n"
    markdown += "\n".join("| " + " | ".join(row) + " |" for row in rows) + "\n"
    (run_dir / "email-summary.md").write_text(markdown)
    rendered = ('<!doctype html><html lang="en"><meta charset="utf-8"><title>OceanDB benchmark</title>'
                '<style>body{font:16px/1.5 Arial,sans-serif;margin:30px}table{border-collapse:collapse}'
                'td,th{border:1px solid #bbc;padding:8px;text-align:left}th{background:#edf2f7}</style>'
                '<h1>' + title + '</h1><p>' + html.escape(explanation) + '</p>')
    rendered += "<table><thead><tr>" + "".join("<th>" + html.escape(h) + "</th>" for h in headers) + "</tr></thead><tbody>"
    rendered += "".join("<tr>" + "".join("<td>" + html.escape(c) + "</td>" for c in row) + "</tr>" for row in rows)
    (run_dir / "email-summary.html").write_text(rendered + "</tbody></table></html>")
    (run_dir / "README.txt").write_text(
        "Open email-summary.html in a browser to copy its table into email.\n"
        "results.json preserves all scenario results and terminal statuses.\n"
        "manifest.json records exact indexes, exclusions, inputs, settings, and code hashes.\n"
        "source-code.zip is the exact working source; .env credentials and raw observations are excluded.\n"
        "The timeout includes EXPLAIN replays; reported query totals do not.\n"
        "Database clones are retained for subsequent inspection.\n"
        "This is one pass per scenario, not repeated estimates of timing variability.\n")


def analyze_results(run_dir, manifest, recorder):
    """Build tables and figures after timing, entirely within the detached runner."""
    baseline = next(r for r in read_json(run_dir / "results.json") if not r["trial_indexes"])
    if baseline["status"] != "completed":
        state = {"status": "unavailable", "reason": "Fresh baseline did not complete; relative scores are undefined"}
        atomic_json(run_dir / "analysis-status.json", state)
        recorder.event("analysis_unavailable", **state)
        return
    command = [sys.executable, str(ROOT / "graph_index_performance.py"), str(run_dir / "results.json"),
               "--output-dir", str(run_dir / "analysis"), "--title", "Pair GiST indexes and controls · 1° sampling"]
    if manifest.get("comparison"):
        reference = run_dir / manifest["comparison"]["snapshot"]
        if hashlib.sha256(reference.read_bytes()).hexdigest() != manifest["comparison"]["sha256"]:
            raise RuntimeError("Comparison snapshot changed")
        command.extend(["--compare-with", str(reference)])
    environment = dict(os.environ, MPLBACKEND="Agg", MPLCONFIGDIR=str(run_dir / ".matplotlib"))
    recorder.event("analysis_started")
    with (run_dir / "analysis.log").open("w") as output:
        try:
            subprocess.run(command, cwd=ROOT, env=environment, stdout=output, stderr=subprocess.STDOUT,
                           check=True, timeout=300)
        except Exception:
            atomic_json(run_dir / "analysis-status.json", {"status": "failed", "exception": traceback.format_exc()})
            raise
    atomic_json(run_dir / "analysis-status.json", {"status": "completed", "finished_at": now(),
                                                  "directory": str(run_dir / "analysis")})
    recorder.event("analysis_completed", directory=str(run_dir / "analysis"))


def package(run_dir):
    export = ROOT.parent / "benchmark-results"
    export.mkdir(exist_ok=True)
    target = export / (run_dir.name + ".zip")
    # Flush all records before taking a stable copy of the progress log.
    sys.stdout.flush()
    sys.stderr.flush()
    files = sorted(p for p in run_dir.rglob("*") if p.is_file()
                   and p.name not in {".runner.lock", "SHA256SUMS.txt"}
                   and not p.name.endswith(".tmp") and "__pycache__" not in p.parts and ".matplotlib" not in p.parts)
    checksums = "\n".join(hashlib.sha256(p.read_bytes()).hexdigest() + "  " + p.relative_to(run_dir).as_posix()
                          for p in files) + "\n"
    (run_dir / "SHA256SUMS.txt").write_text(checksums)
    temporary = target.with_suffix(".zip.tmp")
    with zipfile.ZipFile(temporary, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for path in files + [run_dir / "SHA256SUMS.txt"]:
            archive.write(path, Path(run_dir.name) / path.relative_to(run_dir))
    with zipfile.ZipFile(temporary) as archive:
        if archive.testzip() is not None:
            raise RuntimeError("ZIP integrity check failed")
    temporary.replace(target)
    return target


def run(run_dir):
    run_dir = Path(run_dir).resolve()
    with (run_dir / ".runner.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        manifest = read_json(run_dir / "manifest.json")
        validate_manifest(manifest)
        if manifest["code_hashes"] != code_hashes():
            raise RuntimeError("Code differs from the prepared run; refusing to resume")
        config = Config()
        if source_facts(config)["identity"] != manifest["source"]["identity"]:
            raise RuntimeError("Source database differs from the prepared run")
        results = read_json(run_dir / "results.json")
        if len(results) != len(manifest["included"]) or any(
                (r["id"], r["database_name"], r["fields"]) != (s["id"], s["database_name"], s["fields"])
                for r, s in zip(results, manifest["included"], strict=True)):
            raise RuntimeError("Results do not match the run manifest")
        recorder = Recorder(run_dir, read_json(run_dir / "status.json"))
        source = OceanDBInit(config=config)
        recorder.state.update(phase="preparing_databases", coordinator_pid=os.getpid())
        recorder.event("run_started", configurations=len(results), **check_disk(config, manifest))
        try:
            # All clone/index work finishes before any benchmark query is timed.
            for spec, result in zip(manifest["included"], results, strict=True):
                if result["status"] in TERMINAL:
                    continue
                prior_pid = result.get("worker_pid")
                if prior_pid:
                    try:
                        os.kill(prior_pid, 0)
                    except ProcessLookupError:
                        pass
                    else:
                        raise RuntimeError(f"Previous worker PID {prior_pid} still exists; refusing concurrent resume")
                if result.get("application_name"):
                    cleanup_sessions(database_for(config, spec).config, result["application_name"])
                recorder.event("preparing_configuration", configuration=spec["id"], database=spec["database_name"])
                try:
                    if shutil.disk_usage(manifest["source"]["data_directory"]).free < 26 * GIB:
                        raise RuntimeError("Disk reserve reached")
                    _, sizes = prepare_database(config, source, spec, run_dir)
                    result.update(status="prepared", index_sizes=sizes)
                    recorder.results(results)
                    recorder.event("configuration_prepared", configuration=spec["id"], index_sizes=sizes)
                except Exception:
                    result.update(status="failed", failure_stage="preparation", exception=traceback.format_exc())
                    recorder.results(results)
                    recorder.event("configuration_failed", configuration=spec["id"], exception=result["exception"])
            recorder.state["phase"] = "benchmarking"
            for spec, result in zip(manifest["included"], results, strict=True):
                if result["status"] in TERMINAL:
                    continue
                if manifest["code_hashes"] != code_hashes():
                    raise RuntimeError("Source code changed during the run")
                database = database_for(config, spec)
                directory = run_dir / "trials" / spec["id"]
                # Preserve an interrupted attempt without mixing its scenarios into the rerun.
                if list(directory.glob("scenario_[0-3].json")):
                    archived = directory / ("interrupted_" + uuid.uuid4().hex[:8])
                    archived.mkdir()
                    for path in directory.glob("scenario_[0-3].json"):
                        path.rename(archived / path.name)
                result.update(status="running", performance=[], error=None, started_at=now(),
                              application_name=f"idx34_{manifest['run_id']}_{spec['id']}")
                recorder.results(results)
                try:
                    recorder.event("vacuum_analyze_started", configuration=spec["id"])
                    database.vacuum_analyze("along_track")
                    stats = metadata(database)
                    if not {"along_track_point", "date_time", "basin_id", "mission"}.issubset(
                            row[0] for row in stats["parent_statistics"] if row[1]):
                        raise RuntimeError("Inherited parent-table statistics are missing after ANALYZE")
                    atomic_json(directory / "database-analyzed.json", stats)
                    recorder.event("vacuum_analyze_completed", configuration=spec["id"])

                    def event(kind, **values):
                        if kind == "worker_started":
                            result.update(worker_pid=values["worker_pid"])
                        if kind == "scenario_completed":
                            result["performance"] = [read_json(p) for p in sorted(directory.glob("scenario_[0-3].json"))]
                        recorder.results(results)
                        recorder.event(kind, configuration=spec["id"], **values)

                    result.update(supervise(database.config, spec, directory, result["application_name"], event))
                    atomic_json(directory / "database-after.json", metadata(database))
                except CleanupError:
                    raise
                except Exception:
                    result.update(status="failed", error=None, exception=traceback.format_exc(), worker_pid=None)
                recorder.results(results)
                recorder.event("configuration_finished", configuration=spec["id"], status=result["status"],
                               completed_scenarios=len(result["performance"]), query_seconds=result["error"],
                               exception=result.get("exception"))
            if len(results) != len(manifest["included"]) or any(r["status"] not in TERMINAL for r in results):
                raise RuntimeError("Not all configurations have terminal results")
            if any(r["status"] == "completed" and len(r["performance"]) != 4 for r in results):
                raise RuntimeError("A completed configuration is missing scenario results")
            recorder.state.update(phase="analyzing", counts=dict(Counter(r["status"] for r in results)))
            recorder.event("queries_complete", counts=recorder.state["counts"])
            analyze_results(run_dir, manifest, recorder)
            recorder.state.update(phase="complete", finished_at=now())
            recorder.event("run_complete", counts=recorder.state["counts"])
        except BaseException:
            recorder.state.update(phase="failed", exception=traceback.format_exc())
            recorder.event("run_failed", exception=recorder.state["exception"])
            raise
        finally:
            report(run_dir, manifest, results)
            recorder.event("packaging_results", archive=str(ROOT.parent / "benchmark-results" / (run_dir.name + ".zip")))
            target = package(run_dir)
            # Keep this final notification separate so the ZIP's checksums remain stable.
            atomic_json(run_dir / "archive-status.json", {"archive": str(target), "verified_at": now(),
                                                       "bytes": target.stat().st_size})


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--prepare", type=Path, help="Validate inputs and save a new run manifest; do not build indexes or run queries")
    group.add_argument("--resume", type=Path, help="Execute pending configurations from a matching prepared run")
    parser.add_argument("--suite", choices=("expanded", "pairs"), default="expanded",
                        help="Candidate set for --prepare; --resume uses the saved manifest")
    parser.add_argument("--compare-with", type=Path,
                        help="With --prepare, snapshot a prior compatible run for automatic combined analysis")
    args = parser.parse_args()
    if args.prepare:
        print(prepare(args.prepare, suite=args.suite, compare_with=args.compare_with))
    else:
        if args.compare_with or args.suite != "expanded":
            parser.error("--suite and --compare-with only apply to --prepare")
        run(args.resume)


if __name__ == "__main__":
    main()
