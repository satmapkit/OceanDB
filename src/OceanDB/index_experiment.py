from collections.abc import Sequence
from dataclasses import dataclass

from psycopg import sql

from OceanDB.managed_index_oceandb import DatabaseIndex
from OceanDB.managed_indices import IndexDefinition, ManagedIndices
from OceanDB.OceanDB_Initializer import OceanDBInit
from OceanDB.query_analysis import (BaseQueryScenario, QueryAnalysisRow,
                                    QueryAnalysisRunner)

def fields_short_name(fields: tuple[str, ...]) -> str:
    return ''.join(field[0] for field in fields)

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

    return (
        source_indexes_match
        and desired_indexes_match
        and not has_unexpected_indexes
    )


def setup_index_performance_test(
    source_db: OceanDBInit,
    indexes: Sequence[IndexDefinition],
    test_database: str,
) -> tuple[OceanDBInit, dict[str, int]]:
    """Clone the source database and create the indexes for a performance test."""

    print("cloning data into ", test_database, "with indexes", indexes)
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
    return test_db, index_sizes


def run_index_performance_test(
    ocean_db_init: OceanDBInit,
    scenarios: list[BaseQueryScenario],
) -> list[QueryAnalysisRow]:
    """Run query performance scenarios against a prepared test database."""
    runner = QueryAnalysisRunner(
        config=ocean_db_init.config,
        scenarios=scenarios,
        managed_indices=ocean_db_init.managed_indices,
    )
    return runner.analyze_queries()


@dataclass
class IndexNode:
    database_name: str
    trial_indexes: list[IndexDefinition]
    # fields: tuple[str, ...]
    index_sizes: dict[str, int] | None = None
    performance: list[QueryAnalysisRow] | None = None
    error: float | None = None
