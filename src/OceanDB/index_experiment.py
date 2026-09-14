from collections.abc import Sequence
from dataclasses import dataclass

from psycopg import sql

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


def setup_index_performance_test(
    source_db: OceanDBInit,
    indexes: Sequence[IndexDefinition],
    test_database: str,
) -> tuple[OceanDBInit, dict[str, int]]:
    """Clone the source database and create the indexes for a performance test."""

    print("cloning data into ", test_database, "with indexes", indexes)
    test_db = ocean_db_init_for_test_db(source_db, test_database, indexes)

    if not test_db.database_exists():
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
        except Exception:
            test_db.drop_database()
            raise
    else:
        print("already exists")

    database_indexes = test_db.inventory_indexes()
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
