from datetime import datetime, timedelta

import pytest

from OceanDB.data_access.along_track import AlongTrack
from OceanDB.data_access.eddy import Eddy
from OceanDB.query_analysis import (BatchQueryScenario, QueryAnalysisRunner,
                                    QueryScenario)
from OceanDB.schemas.along_track_schema import along_track_schema
from OceanDB.schemas.eddy_schema import eddy_columns_schema
from tests.database.fixtures import *

pytestmark = pytest.mark.uses_database


def test_analyze_all(db_with_indices):
    runner = QueryAnalysisRunner(config=db_with_indices.config)
    output_all = runner.analyze_queries()
    for output, source_scenario in zip(output_all, runner.scenarios):
        assert output.scenario_name == source_scenario.name
        assert output.used_indices.issubset(output.candidate_indices)


def test_analyze_along_track_point_r_dt(db_with_indices):
    scenario = QueryScenario(
        query_class=AlongTrack,
        method_name="geographic_point_in_r_dt",
        kwargs={
            "fields": list(along_track_schema.keys()),
            "latitude": -39.1,
            "longitude": 54.7,
            "date": datetime(2013, 1, 4, 23),
            "radius": 500_000,
            "time_window": timedelta(days=10),
        },
    )
    runner = QueryAnalysisRunner(config=db_with_indices.config, scenarios=[scenario])
    output_all = runner.analyze_queries()
    assert len(output_all) > 0
    output = output_all[0]

    assert output.used_indices == {
        "along_track_time_idx",
        "along_track_point_date_mission_basin_idx",
    }
    assert output.total_cost is not None
    assert 66.5 < output.total_cost < 66.6


def test_analyze_along_track_nearest_neighbor(db_with_indices):
    scenario = QueryScenario(
        query_class=AlongTrack,
        method_name="geographic_nearest_neighbors",
        kwargs={
            "fields": list(along_track_schema.keys()),
            "latitude": -69,
            "longitude": 28.1,
            "date": datetime(2013, 1, 4, 23),
            "time_window": timedelta(days=10),
        },
    )
    runner = QueryAnalysisRunner(config=db_with_indices.config, scenarios=[scenario])
    output_all = runner.analyze_queries()
    assert len(output_all) > 0
    output = output_all[0]

    assert output.used_indices == {"along_track_point_date_mission_basin_idx"}
    assert output.total_cost is not None
    assert 23.2 < output.total_cost < 23.4


def test_analyze_eddy_with_track_id(db_with_indices):
    scenario = QueryScenario(
        query_class=Eddy,
        method_name="eddy_with_track_id",
        kwargs={"fields": (eddy_columns_schema.keys()), "track_id": -1},
    )
    runner = QueryAnalysisRunner(config=db_with_indices.config, scenarios=[scenario])
    output_all = runner.analyze_queries()
    assert len(output_all) > 0
    output = output_all[0]

    assert output.used_indices == set()
    assert output.total_cost is not None
    assert 8.1 < output.total_cost < 8.2


def test_analyze_eddy_envelope(db_with_indices):
    scenario = QueryScenario(
        query_class=Eddy,
        method_name="eddy_envelope_query",
        kwargs={"track_id": -1},
    )
    runner = QueryAnalysisRunner(config=db_with_indices.config, scenarios=[scenario])
    output_all = runner.analyze_queries()
    assert len(output_all) > 0
    output = output_all[0]

    assert output.used_indices == {
        "basin_geog_idx",
        "basin_id_idx",
        "track_times_cyclonic_type_idx",
    }
    assert output.total_cost is not None
    assert 29.3 < output.total_cost < 29.4


def test_analyze_eddy_along_near_eddy(db_with_indices):
    scenario = QueryScenario(
        query_class=Eddy,
        method_name="along_track_points_near_eddy",
        kwargs={"track_id": -1},
    )
    runner = QueryAnalysisRunner(config=db_with_indices.config, scenarios=[scenario])
    output_all = runner.analyze_queries()
    assert len(output_all) > 0
    output = output_all[0]

    assert output.used_indices == {
        "along_track_point_date_mission_basin_idx",
        "basin_geog_idx",
        "basin_id_idx",
        "track_times_cyclonic_type_idx",
    }
    assert output.total_cost is not None
    assert 29.3 < output.total_cost < 29.4


def test_analyze_batch_query(db_with_indices):
    scenario = BatchQueryScenario(
        query_class=AlongTrack,
        method_name="geographic_point_in_r_dt_batch",
        kwargs={
            "fields": list(along_track_schema.keys()),
            "latitudes": [-39.1, -39.2],
            "longitudes": [54.7, 54.7],
            "dates": [datetime(2013, 1, 4, 23), datetime(2013, 1, 4, 23)],
            "radius": 500_000,
            "time_window": timedelta(days=10),
        },
    )
    runner = QueryAnalysisRunner(config=db_with_indices.config, scenarios=[scenario])
    output_all = runner.analyze_queries()
    assert len(output_all) > 0
    output = output_all[0]

    assert output.used_indices == {
        "along_track_time_idx",
        "along_track_point_date_mission_basin_idx",
    }
    assert output.total_cost is not None
    assert 66.5 < output.total_cost < 66.6
