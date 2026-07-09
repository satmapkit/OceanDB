from dataclasses import dataclass
from typing import Callable, LiteralString, Optional

from psycopg import sql

from OceanDB.OceanDB_Initializer import OceanDBInit
from OceanDB.query_analysis import (BaseQueryScenario, QueryAnalysisRow,
                                    QueryAnalysisRunner)
from OceanDB.query_spec import QuerySpec

import optuna

class IndexSpec(QuerySpec):
    def __init__(self, sql_template: LiteralString):
        """
        Template should look like

        CREATE INDEX IF NOT EXISTS {name} ON table USING method ({fields}) WITH (buffering=auto);

        or

        DROP INDEX IF EXISTS {name}
        """
        super().__init__(sql_template, {}, [])

    def sql_projection_compiler(self, fields):
        fields_list = list(fields)
        if not len(fields_list) >= 1:
            raise ValueError(
                "IndexSpec requires at least one field to specify the name of the index"
            )

        fields_sql = sql.SQL(", ").join(
            sql.Identifier(field) for field in fields_list[1:]
        )
        return sql.SQL(self.sql_template).format(name=sql.Identifier(fields_list[0]), fields=fields_sql)


@dataclass(frozen=True)
class Index:
    name: str
    table: str
    fields: list[str]
    spec: IndexSpec

    def build(self, init: OceanDBInit):
        init.execute_write_query(self.spec, fields=[self.name, *self.fields])

    def drop(self, init: OceanDBInit):
        drop_query = IndexSpec("DROP INDEX IF EXISTS public.{name}")
        init.execute_write_query(drop_query, fields=[self.name])

def run_index_performance_test(
    indexes: list[Index],
    oceanDBInit: OceanDBInit,
    scenarios: list[BaseQueryScenario],
) -> list[QueryAnalysisRow]:
    """
    Create an index, run query performance scenarios, and drop the index.

    The index is dropped in a finally block so failed scenarios do not leave the
    test index behind.
    """
    print('building index', end='... ')
    try:
        for index in indexes:
            index.build(oceanDBInit)
        print('done')
        runner = QueryAnalysisRunner(
            config=oceanDBInit.config,
            scenarios=scenarios,
            indices=[
                {"index_name": index.name, "table_name": index.table}
                for index in indexes
            ],
        )
        return runner.analyze_queries()
    except Exception as ex:
        print("error")
        raise ex
    finally:
        print('dropping index', end='... ')
        for index in indexes:
            index.drop(oceanDBInit)
        print('done')


@dataclass
class IndexNode:
    indexes: list[Index]
    performance: Optional[list[QueryAnalysisRow]] = None
    error: Optional[float] = None

def optuna_search(
    node_sampler: Callable[[object], IndexNode],
    oceanDBInit: OceanDBInit,
    scenarios: list[BaseQueryScenario],
    *,
    n_trials: int = 50,
    study_name: str = "oceandb_index_search",
    storage: str | None = None,
    random_seed: int | None = None,
) -> tuple[list[IndexNode], optuna.Study]:

    tried_nodes = []

    def objective(trial):
        node = node_sampler(trial)
        tried_nodes.append(node)

        try:
            performance = run_index_performance_test(node.indexes, oceanDBInit, scenarios)
            error = sum(x.total_time for x in performance)
            node.performance = performance
            node.error = error
        except Exception as ex:
            error = float("inf")
            node.error = error
            trial.set_user_attr("exception", repr(ex))

        trial.set_user_attr("indexes", [index.name for index in node.indexes])
        trial.set_user_attr("fields", [index.fields for index in node.indexes])
        return error

    sampler = optuna.samplers.TPESampler(seed=random_seed)
    study = optuna.create_study(
        direction="minimize",
        sampler=sampler,
        study_name=study_name,
        storage=storage,
        load_if_exists=storage is not None,
    )
    study.optimize(objective, n_trials=n_trials)

    return tried_nodes, study
