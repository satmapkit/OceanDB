from typing import LiteralString, cast

import pytest
from psycopg import sql

from OceanDB.config import Config
from OceanDB.data_access.along_track import AlongTrack
from OceanDB.data_access.base_query import BaseReadQuery
from OceanDB.data_access.eddy import Eddy
from OceanDB.query_analysis import QueryAnalysisRunner, QueryScenario

pytestmark = pytest.mark.unit


def test_default_scenarios_cover_expected_query_surfaces():
    runner = QueryAnalysisRunner()

    scenarios = runner.default_scenarios()
    scenario_names = set([scenario.name for scenario in scenarios])

    assert scenario_names == set(
        (
            "AlongTrack.geographic_point_in_r_dt",
            "AlongTrack.geographic_nearest_neighbors",
            "Eddy.eddy_with_track_id",
            "Eddy.eddy_envelope_query",
            "Eddy.along_track_points_near_eddy",
        )
    )
    assert scenarios[0].query_class is AlongTrack
    assert scenarios[0].method_name == "geographic_point_in_r_dt"
    assert scenarios[2].query_class is Eddy
    assert scenarios[2].method_name == "eddy_with_track_id"
    assert scenarios[2].kwargs["track_id"] == -1


class FakeQuery(BaseReadQuery):
    def __init__(self, *, config: Config):
        self.config = config
        self.query_observer = None

    def start_debug(self, query_observer):
        self.query_observer = query_observer

    def stop_debug(self):
        self.query_observer = None

    def emit_two(self, sql_one: str, sql_two: str):
        if self.query_observer is None:
            raise AssertionError("observer was not configured")
        self.query_observer(
            cast(sql.Composed, sql.SQL(cast(LiteralString, sql_one))), {}, sql_one
        )
        self.query_observer(
            cast(sql.Composed, sql.SQL(cast(LiteralString, sql_two))), {}, sql_two
        )


def test_runner_can_take_injected_scenarios():
    scenarios = [
        QueryScenario(
            query_class=FakeQuery,
            method_name="emit_two",
            kwargs={"sql_one": "SELECT target", "sql_two": "SELECT ignored"},
        )
    ]

    runner = QueryAnalysisRunner(scenarios=scenarios)

    assert runner.scenarios == scenarios


def test_extract_tables_returns_unique_from_and_join_targets():
    runner = QueryAnalysisRunner()

    sql_query = """
        SELECT *
        FROM eddy
        INNER JOIN along_track atk ON atk.date_time = eddy.date_time
        LEFT JOIN basin ON basin.id = atk.basin_id
        INNER JOIN along_track again ON again.track = atk.track
    """

    assert set(runner.extract_tables(sql_query)) == set(
        ("eddy", "along_track", "basin")
    )


def test_extract_used_indices_matches_known_index_names_from_plan():
    runner = QueryAnalysisRunner()

    explain_docs = [
        {
            "Plan": {
                "Node Type": "Nested Loop",
                "Plans": [
                    {
                        "Node Type": "Parallel Bitmap Heap Scan",
                        "Relation Name": "eddy",
                        "Plans": [
                            {
                                "Node Type": "Bitmap Index Scan",
                                "Index Name": "track_times_cyclonic_type_idx",
                            }
                        ],
                    },
                    {
                        "Node Type": "Index Scan",
                        "Relation Name": "basin",
                        "Index Name": "basin_geog_idx",
                    },
                ],
            }
        }
    ]

    assert runner.extract_used_indices(explain_docs) == set(
        (
            "basin_geog_idx",
            "track_times_cyclonic_type_idx",
        )
    )


def test_extract_used_indices_normalizes_partition_child_index_names():
    runner = QueryAnalysisRunner()
    runner._load_partition_index_name_map = lambda: {
        "along_track_2013_01_along_track_point_date_time_basin_id_sp_idx": (
            "along_track_point_date_mission_basin_idx"
        )
    }

    explain_docs = [
        {
            "Plan": {
                "Node Type": "Bitmap Heap Scan",
                "Relation Name": "along_track_2013_01",
                "Plans": [
                    {
                        "Node Type": "Bitmap Index Scan",
                        "Index Name": (
                            "along_track_2013_01_"
                            "along_track_point_date_time_basin_id_sp_idx"
                        ),
                    }
                ],
            }
        }
    ]

    assert runner.extract_used_indices(explain_docs) == set(
        ("along_track_point_date_mission_basin_idx",)
    )


def test_iter_plan_nodes_yields_nested_plan_nodes():
    runner = QueryAnalysisRunner()

    explain_docs = [
        {
            "Plan": {
                "Node Type": "Nested Loop",
                "Plans": [
                    {
                        "Node Type": "Index Scan",
                        "Index Name": "basin_geog_idx",
                    },
                    {
                        "Node Type": "Bitmap Heap Scan",
                        "Plans": [
                            {
                                "Node Type": "Bitmap Index Scan",
                                "Index Name": "track_times_cyclonic_type_idx",
                            }
                        ],
                    },
                ],
            }
        }
    ]

    assert [node["Node Type"] for node in runner.iter_plan_nodes(explain_docs)] == [
        "Nested Loop",
        "Index Scan",
        "Bitmap Heap Scan",
        "Bitmap Index Scan",
    ]


def test_extract_total_cost_and_time_read_first_plan_node_match():
    runner = QueryAnalysisRunner()

    explain_docs = [
        {
            "Plan": {
                "Node Type": "Gather",
                "Actual Total Time": 20.984,
                "Total Cost": 48778.67,
                "Plans": [
                    {
                        "Node Type": "Bitmap Heap Scan",
                        "Actual Total Time": 12.0,
                        "Total Cost": 100.0,
                    }
                ],
            },
        },
        {"Plan": {"Node Type": "Seq Scan"}},
    ]

    assert runner.extract_total_cost(explain_docs) == 48778.67
    assert runner.extract_total_time(explain_docs) == 20.984


def test_analyze_statement_includes_top_level_metrics(monkeypatch):
    scenario = QueryScenario(
        query_class=FakeQuery,
        method_name="emit_two",
        kwargs={"sql_one": "SELECT * FROM eddy", "sql_two": "SELECT * FROM basin"},
    )
    runner = QueryAnalysisRunner(scenarios=[scenario])

    captured_queries = [
        type("Captured", (), {"rendered": "SELECT * FROM eddy"})(),
        type("Captured", (), {"rendered": "SELECT * FROM basin"})(),
    ]
    explain_outputs = iter(
        [
            [
                {
                    "Execution Time": 2.5,
                    "Plan": {"Total Cost": 10.0, "Actual Total Time": 2.0},
                }
            ],
            [{"Plan": {"Total Cost": 20.0, "Actual Total Time": 4.0}}],
        ]
    )

    monkeypatch.setattr(runner, "_capture_statement_sql", lambda _: captured_queries)
    monkeypatch.setattr(runner, "explain_analyze_sql", lambda _: next(explain_outputs))
    monkeypatch.setattr(runner, "candidate_indices_for_tables", lambda _: set())
    monkeypatch.setattr(runner, "extract_used_indices", lambda _: set())

    row = runner.analyze_statement(scenario)

    assert row.total_cost == 10.0
    assert row.total_time == 2.0


def test_candidate_indices_for_tables_uses_initializer_metadata():
    runner = QueryAnalysisRunner()

    assert runner.candidate_indices_for_tables(("eddy", "along_track")) == set(
        (
            "along_track_basin_idx",
            "along_track_date_idx",
            "along_track_file_name_idx",
            "along_track_mission_idx",
            "along_track_point_date_idx",
            "along_track_point_date_mission_basin_idx",
            "along_track_point_date_mission_idx",
            "along_track_point_geom_idx",
            "along_track_point_idx",
            "along_track_time_idx",
            "eddy_point_idx",
            "track_times_cyclonic_type_idx",
        )
    )
