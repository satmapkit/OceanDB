from dataclasses import dataclass
from typing import Callable, LiteralString, Optional

from psycopg import sql

from OceanDB.OceanDB_Initializer import OceanDBInit
from OceanDB.query_analysis import (BaseQueryScenario, QueryAnalysisRow,
                                    QueryAnalysisRunner)
from OceanDB.query_spec import QuerySpec

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
