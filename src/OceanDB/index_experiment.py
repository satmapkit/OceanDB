import json
import multiprocessing
import os
import re
import tempfile
import time
import traceback
import uuid
from collections.abc import Sequence
from dataclasses import asdict, dataclass
from pathlib import Path

import psycopg
from psycopg import sql

from OceanDB.managed_index_oceandb import DatabaseIndex
from OceanDB.managed_indices import IndexDefinition, ManagedIndices
from OceanDB.OceanDB_Initializer import OceanDBInit
from OceanDB.query_analysis import (
    BaseQueryScenario,
    QueryAnalysisRow,
    QueryAnalysisRunner,
)


def fields_short_name(fields: tuple[str, ...]) -> str:
    return "".join(field[0] for field in fields)


def index_definition(kind: str, fields: tuple[str, ...]) -> IndexDefinition:
    name = f"{kind}_{fields_short_name(fields)}"
    create_sql = f"""
        CREATE INDEX IF NOT EXISTS {name}
        ON along_track USING gist ({", ".join(fields)})
        WITH (buffering=auto);
    """
    return IndexDefinition(name=name, table="along_track", create_sql=create_sql)


def ocean_db_init_for_test_db(
    source_db: OceanDBInit,
    test_database: str,
    indexes: Sequence[IndexDefinition],
) -> OceanDBInit:
    test_config = source_db.config.model_copy(
        update={"postgres_database": test_database}
    )
    return OceanDBInit(
        config=test_config,
        managed_indices=ManagedIndices(tuple(indexes)),
    )


def _index_def_to_key(index: IndexDefinition) -> tuple[str, str]:
    return (index.table, index.name)


def _index_db_to_key(index: DatabaseIndex) -> tuple[str, str]:
    return (index.table_name, index.index_name)


def index_signature(statement: str) -> str | None:
    """Conservatively compare index keys, order, opclasses, options and predicates.

    Ignore only the index/table name and PostgreSQL's formatting of identifiers
    and the buffering storage option. Unknown syntax fails closed.
    """
    match = re.fullmatch(
        r"\s*CREATE\s+(UNIQUE\s+)?INDEX\s+(?:IF\s+NOT\s+EXISTS\s+)?"
        r"\S+\s+ON\s+(?:ONLY\s+)?\S+\s+USING\s+(.*?);?\s*",
        statement,
        re.IGNORECASE | re.DOTALL,
    )
    if not match:
        return None
    tail = re.sub(r'"([a-z_][a-z_0-9]*)"', r"\1", match[2]).strip().rstrip(";")
    tail = re.sub(r"\bbuffering\s*=\s*'auto'", "buffering=auto", tail)
    tail = re.sub(r"\s*([(),=])\s*", r"\1", " ".join(tail.split()))
    return ("unique " if match[1] else "") + tail


def index_database_is_reusable(
    source_indexes: Sequence[DatabaseIndex],
    desired_indexes: Sequence[IndexDefinition],
    actual_indexes: Sequence[DatabaseIndex],
) -> bool:
    """Return whether a test database has only its baseline and desired indexes."""

    source_by_key = {_index_db_to_key(index): index for index in source_indexes}
    database_by_key = {_index_db_to_key(index): index for index in actual_indexes}

    source_indexes_match = all(
        (database_index := database_by_key.get(key)) is not None
        and database_index.index_definition == source_index.index_definition
        and database_index.is_valid
        and database_index.is_ready
        for key, source_index in source_by_key.items()
    )

    desired_keys = {_index_def_to_key(index) for index in desired_indexes}
    desired_by_name = {definition.name: definition for definition in desired_indexes}
    desired_names = set(desired_by_name)
    desired_indexes_match = all(
        (database_index := database_by_key.get(key)) is not None
        and database_index.is_valid
        and database_index.is_ready
        and database_index.schema_name == "public"
        and index_signature(database_index.index_definition) is not None
        and index_signature(database_index.index_definition)
        == index_signature(definition.create_sql)
        for definition in desired_indexes
        for key in [_index_def_to_key(definition)]
    )

    children_match = all(
        index.is_valid
        and index.is_ready
        and index.schema_name == "public"
        and index_signature(index.index_definition)
        == index_signature(desired_by_name[index.parent_index_name].create_sql)
        for index in actual_indexes
        if index.parent_index_name in desired_names
    )

    allowed_keys = source_by_key.keys() | desired_keys
    has_unexpected_indexes = any(
        key not in allowed_keys and index.parent_index_name not in desired_names
        for key, index in database_by_key.items()
    )

    return (
        source_indexes_match
        and desired_indexes_match
        and children_match
        and not has_unexpected_indexes
    )


def setup_index_performance_test(
    source_db: OceanDBInit,
    indexes: Sequence[IndexDefinition],
    test_database: str,
) -> tuple[OceanDBInit, dict[str, int]]:
    """Clone the source database and create the indexes for a performance test."""

    print("cloning data into ", test_database, "with indexes", indexes)
    t1 = time.time()
    test_db = ocean_db_init_for_test_db(source_db, test_database, indexes)

    source_indexes = source_db.inventory_indexes()
    database_indexes: Sequence[DatabaseIndex] = ()
    needs_create = True

    if test_db.database_exists():
        database_indexes = test_db.inventory_indexes()
        if index_database_is_reusable(source_indexes, indexes, database_indexes):
            print("already exists with the desired indexes")
            needs_create = False
        else:
            raise RuntimeError(
                f"Existing database '{test_database}' has unexpected or missing indexes; "
                "refusing to drop it. Use a new trial database name."
            )

    if needs_create:
        with source_db.cursor(
            autocommit=True,
            connection_string=source_db.config.postgres_dsn_admin,
        ) as cur:
            cur.execute(
                sql.SQL("CREATE DATABASE {} TEMPLATE {}").format(
                    sql.Identifier(test_database),
                    sql.Identifier(source_db.db_name),
                )
            )

        try:
            test_db.create_indexes(indexes)
            database_indexes = test_db.inventory_indexes()
            if not index_database_is_reusable(
                source_indexes, indexes, database_indexes
            ):
                raise RuntimeError(
                    f"Database '{test_database}' does not have the expected indexes"
                )
        except Exception:
            test_db.drop_database()
            raise

    index_sizes = {
        definition.name: sum(
            test_db.get_index_size(database_index.index_name)
            for database_index in database_indexes
            if database_index.index_name == definition.name
            or database_index.parent_index_name == definition.name
        )
        for definition in indexes
    }
    print("done in", time.time() - t1)
    return test_db, index_sizes


def run_index_performance_test(
    ocean_db_init: OceanDBInit,
    scenarios: list[BaseQueryScenario],
) -> list[QueryAnalysisRow]:
    """Run query performance scenarios against a prepared test database."""
    ocean_db_init.vacuum_analyze("along_track")
    runner = QueryAnalysisRunner(
        config=ocean_db_init.config,
        scenarios=scenarios,
        managed_indices=ocean_db_init.managed_indices,
    )
    return runner.analyze_queries()


class BenchmarkFailure(RuntimeError):
    """A failed query phase, with any fully completed scenarios retained."""

    def __init__(self, message, performance=(), wall_seconds=None):
        super().__init__(message)
        self.performance = list(performance)
        self.wall_seconds = wall_seconds


class BenchmarkTimeout(BenchmarkFailure, TimeoutError):
    pass


class BenchmarkCleanupError(BenchmarkFailure):
    """Fatal: another configuration must not start until cleanup succeeds."""


def _run_index_performance_test_worker(
    config,
    indexes,
    scenarios,
    connection,
    application_name,
    directory,
) -> None:
    os.environ["PGAPPNAME"] = application_name
    try:
        runner = QueryAnalysisRunner(
            config=config,
            scenarios=scenarios,
            managed_indices=ManagedIndices(indexes),
        )
        connection.send(("ready", None))
        if connection.recv() != "go":
            return
        for number, scenario in enumerate(scenarios):
            connection.send(("scenario_started", number))
            row = runner.analyze_statement(scenario)
            payload = {"row": asdict(row), "finished": time.monotonic()}
            temporary = Path(directory) / f"{number:03d}.tmp"
            temporary.write_text(json.dumps(payload, default=sorted, allow_nan=False))
            temporary.replace(temporary.with_suffix(".json"))
            # Keep pipe messages small: EXPLAIN results can be arbitrarily large.
            connection.send(("scenario_completed", number))
        connection.send(("completed", time.monotonic()))
    except BaseException:
        # A file avoids a large traceback blocking the deadline supervisor.
        (Path(directory) / "failure.txt").write_text(traceback.format_exc())
        connection.send(("failed", None))
    finally:
        connection.close()


def _stop_index_performance_worker(worker: multiprocessing.Process) -> None:
    if worker.is_alive():
        worker.terminate()
        worker.join(timeout=2)
    if worker.is_alive():
        worker.kill()
    worker.join(timeout=2)
    if worker.is_alive():
        raise BenchmarkCleanupError("Benchmark worker survived termination")


def _cleanup_index_performance_sessions(config, application_name: str) -> None:
    """Cancel only the stopped benchmark worker's PostgreSQL sessions."""

    try:
        with psycopg.connect(
            config.postgres_dsn_admin,
            autocommit=True,
            connect_timeout=5,
            application_name="index_performance_cleanup",
            options="-c statement_timeout=5000",
        ) as connection:
            args = (config.postgres_database, application_name)
            predicate = (
                "datname=%s AND application_name=%s "
                "AND backend_type='client backend'"
            )
            connection.execute(
                "SELECT pg_cancel_backend(pid) FROM pg_stat_activity WHERE "
                + predicate,
                args,
            )
            connection.execute(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE "
                + predicate,
                args,
            )
            deadline = time.monotonic() + 5
            while connection.execute(
                "SELECT count(*) FROM pg_stat_activity WHERE " + predicate,
                args,
            ).fetchone()[0]:
                if time.monotonic() >= deadline:
                    raise RuntimeError(
                        f"PostgreSQL sessions remain for {application_name}"
                    )
                time.sleep(0.05)
    except Exception as exc:
        raise BenchmarkCleanupError(
            f"Could not clean up PostgreSQL sessions for {application_name}"
        ) from exc


def run_index_performance_test_with_timeout(
    ocean_db_init: OceanDBInit,
    scenarios: list[BaseQueryScenario],
    timeout_seconds: float,
    *,
    on_event=None,
    application_name: str | None = None,
    maintenance: bool = True,
) -> list[QueryAnalysisRow]:
    """Time the complete query phase, including EXPLAIN, at one wall deadline.

    Maintenance and worker initialization are outside the deadline. Events deliver
    completed rows for checkpoints. Failures carry partial results; cleanup errors
    are fatal and must not be treated as ordinary failed trials by a caller.
    """
    if not 0 < timeout_seconds < float("inf"):
        raise ValueError("timeout_seconds must be finite and positive")
    if not scenarios:
        raise ValueError("At least one scenario is required")
    event = on_event or (lambda *args, **kwargs: None)
    if maintenance:
        event("maintenance_started")
        ocean_db_init.vacuum_analyze("along_track")
        event("maintenance_completed")
    application_name = application_name or f"index_performance_{uuid.uuid4().hex}"
    if len(application_name.encode()) > 63:
        raise ValueError("application_name exceeds PostgreSQL's 63-byte limit")
    context = multiprocessing.get_context("spawn")
    parent, child = context.Pipe()
    performance = []
    started = deadline = None
    status, reason = "failed", "Worker exited without a result"
    with tempfile.TemporaryDirectory(prefix="oceandb-benchmark-") as directory:
        worker = context.Process(
            target=_run_index_performance_test_worker,
            args=(
                ocean_db_init.config,
                ocean_db_init.managed_indices.index_definitions,
                scenarios,
                child,
                application_name,
                directory,
            ),
            daemon=True,
        )

        def collect():
            for number in range(len(performance), len(scenarios)):
                path = Path(directory) / f"{number:03d}.json"
                if not path.exists():
                    break
                payload = json.loads(path.read_text())
                if deadline is None or payload["finished"] > deadline:
                    break
                row = payload["row"]
                for key in ("tables", "candidate_indices", "used_indices"):
                    row[key] = set(row[key])
                performance.append(QueryAnalysisRow(**row))
                event("scenario_completed", number=number, result=performance[-1])

        worker.start()
        child.close()
        startup_deadline = time.monotonic() + 60
        try:
            event("worker_started", pid=worker.pid, application_name=application_name)
            while True:
                remaining = (deadline or startup_deadline) - time.monotonic()
                if remaining <= 0:
                    status = "timed_out" if started is not None else "failed"
                    reason = (
                        f"Query phase exceeded {timeout_seconds:g} seconds"
                        if started is not None
                        else "Worker startup exceeded 60 seconds"
                    )
                    break
                if parent.poll(min(remaining, 0.1)):
                    try:
                        kind, value = parent.recv()
                    except EOFError:
                        break
                    if kind == "ready":
                        started = time.monotonic()
                        deadline = started + timeout_seconds
                        parent.send("go")
                        event("query_phase_started", timeout_seconds=timeout_seconds)
                    elif kind == "completed":
                        status = "completed" if value <= deadline else "timed_out"
                        reason = f"Query phase exceeded {timeout_seconds:g} seconds"
                        break
                    elif kind == "failed":
                        reason = (Path(directory) / "failure.txt").read_text()
                        break
                    elif kind == "scenario_completed":
                        collect()
                    elif kind == "scenario_started":
                        event(kind, number=value)
                elif not worker.is_alive():
                    break
        finally:
            wall = time.monotonic() - started if started is not None else None
            try:
                _stop_index_performance_worker(worker)
            finally:
                parent.close()
                try:
                    collect()
                finally:
                    try:
                        _cleanup_index_performance_sessions(
                            ocean_db_init.config, application_name
                        )
                    except BenchmarkCleanupError as exc:
                        exc.performance = performance
                        exc.wall_seconds = wall
                        raise
            event("query_phase_finished", wall_seconds=wall)
        if status == "completed" and len(performance) == len(scenarios):
            return performance
        failure = BenchmarkTimeout if status == "timed_out" else BenchmarkFailure
        raise failure(reason, performance, wall)


@dataclass
class IndexNode:
    database_name: str
    trial_indexes: list[IndexDefinition]
    # fields: tuple[str, ...]
    index_sizes: dict[str, int] | None = None
    performance: list[QueryAnalysisRow] | None = None
    error: float | None = None

    def pretty_name(self):
        return "_".join(x.name for x in self.trial_indexes)
