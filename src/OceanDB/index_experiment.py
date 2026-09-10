from collections.abc import Sequence
from dataclasses import dataclass

from OceanDB.managed_indices import IndexDefinition, ManagedIndices
from OceanDB.OceanDB_Initializer import OceanDBInit
from OceanDB.query_analysis import (BaseQueryScenario, QueryAnalysisRow,
                                    QueryAnalysisRunner)


def index_definition(kind: str, fields: tuple[str, ...]) -> IndexDefinition:
    name = f"along_track_index_{kind}_{'_'.join(fields)}"
    create_sql = f"""
        CREATE INDEX IF NOT EXISTS {name}
        ON along_track USING gist ({", ".join(fields)})
        WITH (buffering=auto);
    """
    return IndexDefinition(name=name, table="along_track", create_sql=create_sql)


def run_index_performance_test(
    indexes: Sequence[IndexDefinition],
    ocean_db_init: OceanDBInit,
    scenarios: list[BaseQueryScenario],
) -> list[QueryAnalysisRow]:
    """
    Create an index, run query performance scenarios, and drop the index.

    The index is dropped in a finally block so failed scenarios do not leave the
    test index behind.
    """
    try:
        ocean_db_init.create_indexes(indexes)
        runner = QueryAnalysisRunner(
            config=ocean_db_init.config,
            scenarios=scenarios,
            managed_indices=ManagedIndices(tuple(indexes)),
        )
        return runner.analyze_queries()
    finally:
        ocean_db_init.drop_indexes_by_definition(indexes)


@dataclass
class IndexNode:
    trial_indexes: list[IndexDefinition] | None = None
    # fields: tuple[str, ...]
    performance: list[QueryAnalysisRow] | None = None
    error: float | None = None
