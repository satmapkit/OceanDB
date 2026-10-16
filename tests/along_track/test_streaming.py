from collections.abc import Callable, Iterable
from datetime import datetime
from typing import NoReturn

import pytest

from OceanDB.data_access.along_track import Mission
from OceanDB.data_access.along_track import \
    PostgreSQLAlongTrackBackend as AlongTrack
from OceanDB.schemas.along_track_schema import along_track_schema
from tests.database.fixtures import *

pytestmark = pytest.mark.unit


def test_geographic_nearest_neighbors_stream(config, monkeypatch):
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
            n_jobs=2,
            chunk_size=2,
        )
    )
    assert (0, [("data 1", [14])]) in result
    assert (1, [("data 2", [3])]) in result
    assert (2, [("data 3", [13])]) in result


def test_geographic_r_dt_stream(config, monkeypatch):
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
            n_jobs=2,
            chunk_size=2,
        )
    )
    assert (0, [("data 1", [14])]) in result
    assert (1, [("data 2", [3])]) in result
    assert (2, [("data 3", [13])]) in result


def test_ordered_batch_wrapper_reorders_stream_results(monkeypatch):
    query = AlongTrack.__new__(AlongTrack)
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
            n_jobs=3,
            chunk_size=1,
        )
    )

    assert results == [first, None, last]
    assert captured["n_jobs"] == 3
    assert captured["chunk_size"] == 1


class _BasinMask:
    def lookup(self, latitude: float, longitude: float) -> int:
        return 1


class _BasinConnections:
    connection_map: dict[int, list[int]] = {1: [1]}


_BATCH_AND_STREAM_METHODS = (
    AlongTrack.geographic_point_in_r_dt_batch,
    AlongTrack.geographic_point_in_r_dt_stream,
    AlongTrack.geographic_nearest_neighbors_batch,
    AlongTrack.geographic_nearest_neighbors_stream,
)


@pytest.fixture
def query_without_execution(monkeypatch: pytest.MonkeyPatch) -> AlongTrack:
    query = AlongTrack.__new__(AlongTrack)
    query.__dict__["basin_mask_lookup"] = _BasinMask()
    query.__dict__["basin_connections"] = _BasinConnections()

    def fail_execution(*args: object, **kwargs: object) -> NoReturn:
        pytest.fail("Empty or invalid batches must not create an executor or cursor")

    monkeypatch.setattr(
        "OceanDB.data_access.base_query.ThreadPoolExecutor", fail_execution
    )
    monkeypatch.setattr(query, "cursor", fail_execution)
    return query


@pytest.mark.parametrize("method", _BATCH_AND_STREAM_METHODS)
@pytest.mark.parametrize("missions", [None, ["j2"]])
def test_empty_batches_and_streams_yield_no_results(
    query_without_execution: AlongTrack,
    method: Callable[..., Iterable[object]],
    missions: list[Mission] | None,
) -> None:
    assert (
        list(
            method(
                query_without_execution,
                fields=["track"],
                latitudes=[],
                longitudes=[],
                dates=[],
                missions=missions,
            )
        )
        == []
    )


@pytest.mark.parametrize("method", _BATCH_AND_STREAM_METHODS)
@pytest.mark.parametrize(
    "latitudes, longitudes, dates",
    [([0.0], [], []), ([], [0.0], []), ([], [], [datetime(2020, 1, 1)])],
)
def test_batches_and_streams_still_reject_mismatched_lengths(
    query_without_execution: AlongTrack,
    method: Callable[..., Iterable[object]],
    latitudes: list[float],
    longitudes: list[float],
    dates: list[datetime],
) -> None:
    with pytest.raises(ValueError, match="zip\\(\\) argument"):
        list(
            method(
                query_without_execution,
                fields=["track"],
                latitudes=latitudes,
                longitudes=longitudes,
                dates=dates,
            )
        )


@pytest.mark.parametrize("method", _BATCH_AND_STREAM_METHODS)
@pytest.mark.parametrize(
    "n_jobs, chunk_size, message",
    [(0, 16, "n_jobs must be at least 1"), (4, 0, "chunk_size must be at least 1")],
)
def test_empty_batches_and_streams_still_reject_invalid_limits(
    query_without_execution: AlongTrack,
    method: Callable[..., Iterable[object]],
    n_jobs: int,
    chunk_size: int,
    message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        list(
            method(
                query_without_execution,
                fields=["track"],
                latitudes=[],
                longitudes=[],
                dates=[],
                n_jobs=n_jobs,
                chunk_size=chunk_size,
            )
        )
