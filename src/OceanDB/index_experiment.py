import multiprocessing
import os
import time
import traceback
import uuid
from collections.abc import Sequence
from dataclasses import dataclass

import psycopg
from psycopg import sql

from OceanDB.managed_index_oceandb import DatabaseIndex
from OceanDB.managed_indices import IndexDefinition, ManagedIndices
from OceanDB.OceanDB_Initializer import OceanDBInit
from OceanDB.query_analysis import (BaseQueryScenario, QueryAnalysisRow,
                                    QueryAnalysisRunner)


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

def index_definitons_short_name(indexes: list[IndexDefinition]) -> str:
    prefixes = ["experiment_", "static_"]
    def remove_prefix(s: str) -> str:
        for p in prefixes:
            if s.startswith(p):
                return s[len(p):]
        return p
    return "_".join(remove_prefix(x.name) for x in indexes)


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
    desired_names = {definition.name for definition in desired_indexes}
    desired_indexes_match = all(
        (database_index := database_by_key.get(key)) is not None
        and database_index.is_valid
        and database_index.is_ready
        for key in desired_keys
    )

    allowed_keys = source_by_key.keys() | desired_keys
    has_unexpected_indexes = any(
        key not in allowed_keys and index.parent_index_name not in desired_names
        for key, index in database_by_key.items()
    )

    return source_indexes_match and desired_indexes_match and not has_unexpected_indexes


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
            print("existing database has unexpected or missing indexes; recreating")
            test_db.drop_database()

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


def _run_index_performance_test_worker(
    config,
    indexes: tuple[IndexDefinition, ...],
    scenarios: list[BaseQueryScenario],
    connection,
    application_name: str,
) -> None:
    os.environ["PGAPPNAME"] = application_name
    try:
        test_db = OceanDBInit(
            config=config,
            managed_indices=ManagedIndices(indexes),
        )
        connection.send(("completed", run_index_performance_test(test_db, scenarios)))
    except BaseException:
        connection.send(("failed", traceback.format_exc()))
    finally:
        connection.close()


def _stop_index_performance_worker(worker: multiprocessing.Process) -> None:
    if worker.is_alive():
        worker.terminate()
        worker.join(timeout=2)
    if worker.is_alive():
        worker.kill()
    worker.join(timeout=2)


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
        raise RuntimeError(
            f"Could not clean up PostgreSQL sessions for {application_name}"
        ) from exc


def run_index_performance_test_with_timeout(
    ocean_db_init: OceanDBInit,
    scenarios: list[BaseQueryScenario],
    timeout_seconds: float,
) -> list[QueryAnalysisRow]:
    """Run all scenarios in a worker that can be stopped at one deadline."""

    if timeout_seconds <= 0:
        raise ValueError("timeout_seconds must be positive")

    context = multiprocessing.get_context("spawn")
    parent, child = context.Pipe(duplex=False)
    application_name = f"index_performance_{uuid.uuid4().hex}"
    worker = context.Process(
        target=_run_index_performance_test_worker,
        args=(
            ocean_db_init.config,
            ocean_db_init.managed_indices.index_definitions,
            scenarios,
            child,
            application_name,
        ),
        daemon=True,
    )
    worker.start()
    child.close()

    deadline = time.monotonic() + timeout_seconds
    try:
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError(
                    f"Index performance test exceeded {timeout_seconds:g} seconds"
                )
            if parent.poll(min(remaining, 0.2)):
                break
            if not worker.is_alive():
                raise RuntimeError("Index performance worker exited without a result")

        status, result = parent.recv()
        if status == "failed":
            raise RuntimeError(result)
        return result
    finally:
        _stop_index_performance_worker(worker)
        parent.close()
        _cleanup_index_performance_sessions(
            ocean_db_init.config,
            application_name,
        )


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
