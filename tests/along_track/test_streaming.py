from datetime import datetime
from inspect import signature

from OceanDB.data_access.along_track import AlongTrack
from OceanDB.data_access.query_plan import BasinPlanner, LatitudePlanner
from OceanDB.schemas.along_track_schema import along_track_schema
from tests.database.fixtures import *

pytestmark = pytest.mark.unit


@pytest.mark.parametrize("planner_type", [BasinPlanner, LatitudePlanner])
def test_geographic_nearest_neighbors_stream(config, monkeypatch, planner_type):
    atdb = AlongTrack(config=config)
    latitudes = [71.0, 26.0, 22.0]
    longitudes = [-142.0, -45.0, 64.0]
    dates = [datetime(2020, 1, 1)] * 3

    dummy_data = [["data 1"], ["data 2"], ["data 3"]]

    def get_data(params):
        for lat, lon, date, data in zip(latitudes, longitudes, dates, dummy_data):
            print(params)
            if (
                params["latitude"] == lat
                and params["longitude"] == lon
                and params["central_date_time"] == date
            ):
                return [(x, params["connected_basin_ids"]) for x in data]
        raise ValueError("no corresponding data found")

    class DummyConnectionsMap:
        connection_map = {
            i: [i] for i in range(200)
        }  # only connect basins to themselves

    def execute_batch_read_query(*, query_spec, fields, params_batch, dataset_name):
        for params in params_batch:
            yield get_data(params)

    monkeypatch.setattr(atdb, "execute_batch_read_query", execute_batch_read_query)
    monkeypatch.setattr(atdb, "basin_connections", DummyConnectionsMap())

    result = list(
        atdb.geographic_nearest_neighbors_stream(
            fields=list(along_track_schema.keys()),
            latitudes=latitudes,
            longitudes=longitudes,
            dates=dates,
            planner=planner_type(2, 2),
        )
    )
    assert (0, [("data 1", [14])]) in result
    assert (1, [("data 2", [3])]) in result
    assert (2, [("data 3", [13])]) in result


@pytest.mark.parametrize("planner_type", [BasinPlanner, LatitudePlanner])
def test_geographic_r_dt_stream(config, monkeypatch, planner_type):
    atdb = AlongTrack(config=config)
    latitudes = [71.0, 26.0, 22.0]
    longitudes = [-142.0, -45.0, 64.0]
    dates = [datetime(2020, 1, 1)] * 3

    dummy_data = [["data 1"], ["data 2"], ["data 3"]]

    def get_data(params):
        for lat, lon, date, data in zip(latitudes, longitudes, dates, dummy_data):
            print(params)
            if (
                params["latitude"] == lat
                and params["longitude"] == lon
                and params["central_date_time"] == date
            ):
                return [(x, params["connected_basin_ids"]) for x in data]
        raise ValueError("no corresponding data found")

    class DummyConnectionsMap:
        connection_map = {
            i: [i] for i in range(200)
        }  # only connect basins to themselves

    def execute_batch_read_query(*, query_spec, fields, params_batch, dataset_name):
        for params in params_batch:
            yield get_data(params)

    monkeypatch.setattr(atdb, "execute_batch_read_query", execute_batch_read_query)
    monkeypatch.setattr(atdb, "basin_connections", DummyConnectionsMap())

    result = list(
        atdb.geographic_point_in_r_dt_stream(
            fields=list(along_track_schema.keys()),
            latitudes=latitudes,
            longitudes=longitudes,
            dates=dates,
            planner=planner_type(2, 2),
        )
    )
    assert (0, [("data 1", [14])]) in result
    assert (1, [("data 2", [3])]) in result
    assert (2, [("data 3", [13])]) in result


def test_ordered_batch_wrapper_reorders_stream_results(monkeypatch):
    query = AlongTrack.__new__(AlongTrack)
    planner = BasinPlanner(3, 1)
    first, last = object(), object()
    captured = {}

    def fake_stream(**kwargs):
        captured.update(kwargs)
        return iter([(2, last), (0, first), (1, None)])

    monkeypatch.setattr(query, "geographic_nearest_neighbors_stream", fake_stream)

    results = list(
        query.geographic_nearest_neighbors_batch(
            fields=list(along_track_schema.keys()),
            latitudes=[1.0, 2.0, 3.0],
            longitudes=[4.0, 5.0, 6.0],
            dates=[datetime(2020, 1, 1)] * 3,
            planner=planner,
        )
    )

    assert results == [first, None, last]
    assert captured["planner"] is planner


@pytest.mark.parametrize(
    "method_name,n_jobs",
    [
        ("geographic_point_in_r_dt", 1),
        ("geographic_point_in_r_dt_batch", 1),
        ("geographic_point_in_r_dt_stream", 4),
        ("geographic_nearest_neighbors", 1),
        ("geographic_nearest_neighbors_batch", 1),
        ("geographic_nearest_neighbors_stream", 4),
    ],
)
def test_default_planner_preserves_execution_settings(method_name, n_jobs):
    planner = signature(getattr(AlongTrack, method_name)).parameters["planner"].default
    assert isinstance(planner, BasinPlanner)
    assert (planner.n_jobs, planner.chunk_size) == (n_jobs, 16)
