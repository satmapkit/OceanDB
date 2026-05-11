from datetime import datetime, timedelta

import numpy as np
import pytest

from OceanDB.data_access.base_query import BaseReadQuery, QuerySpec
from OceanDB.ocean_data.ocean_data import ColumnField, DerivedField
from tests.database.fixtures import *

pytestmark = pytest.mark.uses_database


def _make_along_track_query() -> QuerySpec:
    return QuerySpec(
        sql_template="""
            SELECT
                {fields}
            FROM along_track atk
            WHERE ST_DWithin(
                along_track_point::geography,
                ST_SetSRID(ST_MakePoint(%(longitude)s, %(latitude)s), 4326)::geography,
                %(distance)s
            )
            AND date_time BETWEEN %(central_date_time)s - %(time_delta)s::interval
                      AND %(central_date_time)s + %(time_delta)s::interval
        """,
        schema={
            "latitude": ColumnField(
                export_name="latitude",
                postgres_table_name="atk",
                postgres_column_name="latitude",
                python_type=float,
            ),
            "longitude": ColumnField(
                export_name="longitude",
                postgres_table_name="atk",
                postgres_column_name="longitude",
                python_type=float,
            ),
            "distance": DerivedField(
                export_name="distance",
                expression=f"""
                    ST_Distance(
                        ST_MakePoint(%(longitude)s, %(latitude)s),
                        atk.along_track_point
                    )
                """,
                python_type=np.float64,
                postgres_type="double precision",
            ),
        },
    )


def _make_along_track_params():
    return {
        "longitude": -65.9,
        "latitude": 58.9,
        "central_date_time": datetime(year=2013, month=1, day=4, hour=23),
        "time_delta": timedelta(days=10),
        "distance": 500_000,
    }


def test_execute_query(db_with_alongtrack_data):
    base_query = BaseReadQuery(db_with_alongtrack_data.config)
    res = base_query.execute_read_query(
        query_spec=_make_along_track_query(),
        params=_make_along_track_params(),
        fields=[
            "latitude",
            "distance",
        ],
    )
    assert res is not None
    assert "latitude" in res
    assert "distance" in res


def test_execute_query_with_start_debug_captures_rendered_query(
    db_with_alongtrack_data,
):
    rendered_queries: list[str] = []
    base_query = BaseReadQuery(db_with_alongtrack_data.config)
    base_query.start_debug(rendered_queries.append)

    res = base_query.execute_read_query(
        query_spec=_make_along_track_query(),
        params=_make_along_track_params(),
        fields=[
            "latitude",
            "distance",
        ],
    )

    assert res is not None
    assert len(rendered_queries) == 1
    assert "%(longitude)s" not in rendered_queries[0]
    assert "%(latitude)s" not in rendered_queries[0]
    assert "%(distance)s" not in rendered_queries[0]
    assert "-65.9" in rendered_queries[0]
    assert "58.9" in rendered_queries[0]
    assert "500000" in rendered_queries[0]
    base_query.stop_debug()


def test_stop_debug_disables_rendered_query_capture(
    db_with_alongtrack_data,
):
    rendered_queries: list[str] = []
    base_query = BaseReadQuery(db_with_alongtrack_data.config)
    base_query.start_debug(rendered_queries.append)
    base_query.stop_debug()

    res = base_query.execute_read_query(
        query_spec=_make_along_track_query(),
        params=_make_along_track_params(),
        fields=[
            "latitude",
            "distance",
        ],
    )

    assert res is not None
    assert rendered_queries == []
