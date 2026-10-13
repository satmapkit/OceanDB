"""All-record query contract and opt-in, transaction-local PostgreSQL tests."""

import os
from contextlib import contextmanager
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import Mock

import numpy as np
import psycopg
import pytest

from OceanDB.data_access.along_track import AlongTrack

FIELDS = ["longitude", "latitude", "sla_filtered", "date_time"]


def reader():
    obj = object.__new__(AlongTrack)
    obj.query_observer = None
    obj.basin_mask_lookup = SimpleNamespace(lookup=lambda lat, lon: 1)
    obj.basin_connections = SimpleNamespace(connection_map={1: [1, 2]})
    return obj


@pytest.mark.unit
@pytest.mark.parametrize("missions", [None, ["s6a", "j3n"], []])
def test_all_record_keeps_spatial_basin_mission_filters_but_has_no_time_bound(missions):
    obj = reader()
    obj.execute_batch_read_query = Mock(return_value=iter([None, None]))
    result = list(
        obj.geographic_point_in_r_batch(
            FIELDS, [30, 30], [-40, -39.75], missions=missions
        )
    )
    assert result == [None, None]
    args = obj.execute_batch_read_query.call_args.kwargs
    query = args["query_spec"].sql_projection_compiler(FIELDS).as_string(None)
    assert "ST_DWithin" in query and "basin_id = ANY" in query
    assert ("mission = ANY" in query) is (missions is not None)
    assert (
        "BETWEEN" not in query
        and "time_delta" not in query
        and "central_date_time" not in query
    )
    assert "LIMIT" not in query and '"date_time"' in query
    assert [p["longitude"] for p in args["params_batch"]] == [-40, -39.75]
    assert all(
        p["distance"] == 50_000 and p["connected_basin_ids"] == [1, 2]
        for p in args["params_batch"]
    )
    assert all(
        ("missions" in p) is (missions is not None) for p in args["params_batch"]
    )


@pytest.mark.unit
@pytest.mark.parametrize(
    "kwargs,match",
    [
        ({"fields": ["delta_t"]}, "no reference date"),
        ({"fields": []}, "output field"),
        ({"radius": float("nan")}, "radius"),
        ({"radius": 0}, "radius"),
        ({"latitudes": [30, 31]}, "zip"),
        ({"latitudes": [90]}, "basin mask"),
    ],
)
def test_invalid_record_request_fails_before_opening_a_connection(kwargs, match):
    obj = reader()
    obj.execute_batch_read_query = Mock()
    params = dict(fields=FIELDS, latitudes=[30], longitudes=[-40]) | kwargs
    with pytest.raises(ValueError, match=match):
        obj.geographic_point_in_r_batch(**params)
    obj.execute_batch_read_query.assert_not_called()


@pytest.mark.unit
def test_single_record_query_delegates_and_preserves_empty_result():
    obj = reader()
    obj.geographic_point_in_r_batch = Mock(return_value=iter([None]))
    assert obj.geographic_point_in_r(FIELDS, 30, -40) is None
    assert obj.geographic_point_in_r_batch.call_args.kwargs == dict(
        fields=FIELDS, latitudes=[30], longitudes=[-40], radius=50_000, missions=None
    )


@pytest.fixture
def temporary_record_reader():
    """Only a temporary shadow table is written; no permanent table/DDL is touched."""
    dsn = os.environ.get("OCEANDB_RADIUS_TEST_DSN")
    if not dsn:
        pytest.skip(
            "Set OCEANDB_RADIUS_TEST_DSN to a PostGIS database for temporary-table SQL tests"
        )
    with psycopg.connect(dsn, application_name="oceandb_record_query_test") as conn:
        conn.execute("SET LOCAL statement_timeout = '5s'")
        conn.execute(
            "CREATE TEMP TABLE along_track (longitude float8,latitude float8,sla_filtered smallint,date_time timestamp,mission text,basin_id smallint,along_track_point geography(Point,4326)) ON COMMIT DROP"
        )
        conn.execute("SET LOCAL search_path = pg_temp, public")
        observations = [
            (0, 0, 100, datetime(1993, 1, 1), "s6a", 1),
            (0.2, 0, 200, datetime(2022, 9, 1), "j3n", 2),
            (0.4, 0, 300, datetime(2022, 11, 30), "h2b", 1),
            (0.1, 0, 400, datetime(2030, 1, 1), "s6a", 1),
            (0.1, 0, 500, datetime(2022, 10, 1), "s6a", 3),  # disconnected basin
            (2, 0, 600, datetime(2022, 10, 1), "s6a", 1),  # outside radius
        ]
        with conn.cursor() as cur:
            cur.executemany(
                "INSERT INTO pg_temp.along_track SELECT %s,%s,%s,%s,%s,%s,ST_SetSRID(ST_MakePoint(%s,%s),4326)::geography",
                [(*r, r[0], r[1]) for r in observations],
            )
        obj = reader()

        @contextmanager
        def cursor(**kwargs):
            with conn.cursor(row_factory=kwargs["row_factory"]) as cur:
                yield cur

        obj.cursor = cursor
        try:
            yield obj, conn
        finally:
            conn.rollback()


@pytest.mark.uses_database
def test_native_record_query_includes_all_years_and_matches_independent_radius_reference(
    temporary_record_reader,
):
    obj, conn = temporary_record_reader
    results = list(obj.geographic_point_in_r_batch(FIELDS, [0, 0, 20], [0, 0.2, 20]))
    assert results[-1] is None
    assert set(d.year for d in results[0]["date_time"]) == {1993, 2022, 2030}
    assert results[0].row_count == 4
    for lon, result in zip([0, 0.2], results[:2], strict=True):
        expected = conn.execute(
            "SELECT longitude,latitude,sla_filtered,date_time FROM pg_temp.along_track WHERE ST_Distance(along_track_point,ST_SetSRID(ST_MakePoint(%s,0),4326)::geography)<=50000 AND basin_id IN (1,2) ORDER BY date_time",
            (lon,),
        ).fetchall()
        actual = sorted(
            zip(*(result[f] for f in FIELDS), strict=True), key=lambda r: r[3]
        )
        assert [(r[0], r[1], r[3]) for r in actual] == [
            (r[0], r[1], r[3]) for r in expected
        ]
        np.testing.assert_allclose(
            [r[2] for r in actual], [r[2] * 0.001 for r in expected]
        )


@pytest.mark.uses_database
def test_record_mission_filter_and_single_batch_equivalence(temporary_record_reader):
    obj, _ = temporary_record_reader
    single = obj.geographic_point_in_r(FIELDS, 0, 0, missions=["s6a", "j3n"])
    batch = list(
        obj.geographic_point_in_r_batch(FIELDS, [0], [0], missions=["s6a", "j3n"])
    )[0]
    assert single.row_count == 3
    for f in FIELDS:
        np.testing.assert_array_equal(single[f], batch[f])
    assert obj.geographic_point_in_r(FIELDS, 0, 0, missions=[]) is None
