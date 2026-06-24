from __future__ import annotations

import re
import time
from abc import ABC
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Iterable, Iterator, Mapping

import yaml
from psycopg import sql

from OceanDB.config import Config
from OceanDB.data_access.along_track import AlongTrack
from OceanDB.data_access.base_query import BaseReadQuery, QueryObserver
from OceanDB.data_access.eddy import Eddy
from OceanDB.managed_index_oceandb import ManagedIndexOceanDB
from OceanDB.schemas.along_track_schema import along_track_schema
from OceanDB.schemas.eddy_schema import eddy_columns_schema

SQL_TABLE_PATTERN = re.compile(
    r"\b(?:FROM|JOIN)\s+([A-Za-z_][A-Za-z0-9_]*)",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class QueryCapture:
    query: sql.Composed
    params: Mapping[str, Any]
    rendered: str


class BaseQueryScenario(ABC):
    def run(self, *, config: Config, observer: QueryObserver) -> None:
        raise NotImplementedError()

    @property
    def name(self) -> str:
        raise NotImplementedError()


@dataclass(frozen=True)
class QueryScenario(BaseQueryScenario):
    query_class: type[BaseReadQuery]
    method_name: str
    kwargs: dict[str, Any]

    def run(self, *, config: Config, observer: QueryObserver) -> None:
        query = self.query_class(config=config)
        query.start_debug(observer)
        try:
            getattr(query, self.method_name)(**self.kwargs)
        finally:
            query.stop_debug()

    @property
    def name(self) -> str:
        return f"{self.query_class.__name__}.{self.method_name}"


@dataclass(frozen=True)
class QueryAnalysisRow:
    scenario_name: str
    """
    Name of the scenario. Usually the name of method being called
    """

    tables: set[str]
    """
    Set of tables referenced in the query
    """

    candidate_indices: set[str]
    """
    Set of indices on the used tables
    """

    used_indices: set[str]
    """
    Set of indices used by the queries in the scenario
    """

    sql: str
    """
    Raw SQL of the queries in the scenario
    """

    explain_result_dict: list[Any]
    """
    List of SQL explain/analyze results for each query in the scenario
    """

    explain_result_str: str
    """
    SQL explain/analyze results for each query in the scenario, formatted as yaml
    """

    total_cost: float | None
    """
    Total cost of the first query in the scenario, as listed by explain/analyze
    """

    single_query_sql_time: float | None
    """
    Time for the SQL to run of the first query in the scenario, as listed by explain/analyze.
    If the scenario is a batch scenario, will only compute this value for the first item in the batch
    """

    total_time: float
    """
    Total time for the query to run, including both SQL and python.
    """


class QueryAnalysisRunner(ManagedIndexOceanDB):
    def __init__(
        self,
        config: Config | None = None,
        scenarios: list[BaseQueryScenario] | None = None,
        indices: Iterable[dict[str, Any]] | None = None,
    ):
        super().__init__(config=config)
        self.scenarios = scenarios or self.default_scenarios()
        self.indices = tuple(indices or self.default_indices())
        self.index_names: set[str] = {index["index_name"] for index in self.indices}

    def default_scenarios(self) -> list[BaseQueryScenario]:
        all_along_track_fields = list(along_track_schema.keys())
        all_eddy_fields = list(eddy_columns_schema.keys())
        return [
            QueryScenario(
                query_class=AlongTrack,
                method_name="geographic_point_in_r_dt",
                kwargs={
                    "fields": all_along_track_fields,
                    "latitude": -39.1,
                    "longitude": 54.7,
                    "date": datetime(2013, 1, 4, 23),
                    "radius": 500_000,
                    "time_window": timedelta(days=10),
                },
            ),
            QueryScenario(
                query_class=AlongTrack,
                method_name="geographic_nearest_neighbors",
                kwargs={
                    "fields": all_along_track_fields,
                    "latitude": -69,
                    "longitude": 28.1,
                    "date": datetime(2013, 1, 4, 23),
                    "time_window": timedelta(days=10),
                },
            ),
            QueryScenario(
                query_class=Eddy,
                method_name="eddy_with_track_id",
                kwargs={"fields": all_eddy_fields, "track_id": -1},
            ),
            QueryScenario(
                query_class=Eddy,
                method_name="eddy_envelope_query",
                kwargs={"track_id": -1},
            ),
            QueryScenario(
                query_class=Eddy,
                method_name="along_track_points_near_eddy",
                kwargs={"track_id": -1},
            ),
        ]

    def default_indices(self):
        return tuple(self.managed_indices.definitions)

    def analyze_queries(self) -> list[QueryAnalysisRow]:
        return [self.analyze_statement(scenario) for scenario in self.scenarios]

    def analyze_statement(
        self,
        scenario: BaseQueryScenario,
    ) -> QueryAnalysisRow:

        captured_all, total_time = self._capture_statement_sql(scenario)

        tables = set()
        explain_output: list[dict[str, Any]] = []
        for captured_query in captured_all:
            tables.update(self.extract_tables(captured_query.rendered))
            explain_output.extend(self.explain_analyze_sql(captured_query))
        rendered = (c.rendered for c in captured_all)

        return QueryAnalysisRow(
            scenario_name=scenario.name,
            tables=tables,
            candidate_indices=self.candidate_indices_for_tables(tables),
            used_indices=self.extract_used_indices(explain_output),
            sql="\n".join(rendered),
            explain_result_dict=explain_output,
            explain_result_str=yaml.safe_dump(explain_output),
            total_cost=self.extract_total_cost(explain_output),
            single_query_sql_time=self.extract_total_time(explain_output),
            total_time=total_time,
        )

    def explain_analyze_sql(self, capture: QueryCapture) -> list[dict[str, Any]]:
        query_prefix = sql.SQL("EXPLAIN (FORMAT yaml, ANALYZE true)")
        query = query_prefix + capture.query
        with self.cursor() as cur:
            cur.execute(query, capture.params)
            explain_yaml = "\n".join(row[0] for row in cur.fetchall() if row)

        explain_output = yaml.safe_load(explain_yaml)
        if not isinstance(explain_output, list):
            raise ValueError("Expected EXPLAIN YAML output to be a list")
        if not all(isinstance(doc, dict) for doc in explain_output):
            raise ValueError("Expected EXPLAIN YAML entries to be mappings")
        return explain_output

    def candidate_indices_for_tables(self, tables: Iterable[str]) -> set[str]:
        return set(
            index["index_name"]
            for index in self.indices
            if index["table_name"] in tables
        )

    def extract_tables(self, query: str) -> set[str]:
        # TODO: figure out a better way to do this other than
        # searching the string
        return set(match.group(1) for match in SQL_TABLE_PATTERN.finditer(query))

    def normalize_index_name(self, index_name: str) -> str | None:
        if index_name in self.index_names:
            return index_name
        return self.partition_index_name_map.get(index_name)

    def extract_used_indices(
        self, explain_output: Iterable[dict[str, Any]]
    ) -> set[str]:
        matched: set[str] = set()
        for node in self.iter_plan_nodes(explain_output):
            index_name = node.get("Index Name")
            if not isinstance(index_name, str):
                continue

            normalized_index_name = self.normalize_index_name(index_name)
            if normalized_index_name is not None:
                matched.add(normalized_index_name)
        return matched

    def extract_total_cost(
        self, explain_output: Iterable[dict[str, Any]]
    ) -> float | None:
        for explain_doc in explain_output:
            for node in self.iter_plan_nodes((explain_doc,)):
                return node.get("Total Cost")
        return None

    def extract_total_time(
        self, explain_output: Iterable[dict[str, Any]]
    ) -> float | None:
        for explain_doc in explain_output:
            for node in self.iter_plan_nodes((explain_doc,)):
                return node.get("Actual Total Time")
        return None

    def iter_plan_nodes(
        self, explain_output: Iterable[dict[str, Any]]
    ) -> Iterator[dict[str, Any]]:
        for explain_doc in explain_output:
            plan = explain_doc.get("Plan")
            if isinstance(plan, dict):
                yield from self._iter_plan_nodes(plan)

    def _iter_plan_nodes(self, node: dict[str, Any]) -> Iterator[dict[str, Any]]:
        yield node

        plans = node.get("Plans")
        if not isinstance(plans, list):
            return

        for child in plans:
            if isinstance(child, dict):
                yield from self._iter_plan_nodes(child)

    def _capture_statement_sql(
        self, scenario: BaseQueryScenario
    ) -> tuple[list[QueryCapture], float]:

        outputs: list[QueryCapture] = []

        def observer(
            query: sql.Composed, params: Mapping[str, Any], rendered: str
        ) -> None:
            outputs.append(QueryCapture(query, params, rendered))

        start_time = time.time()
        scenario.run(config=self.config, observer=observer)
        end_time = time.time()

        return outputs, end_time - start_time
