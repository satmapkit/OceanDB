from datetime import datetime, timedelta
import pytest

import numpy as np

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


def test_execute_query_with_debug_sql_logs_rendered_query(
    db_with_alongtrack_data, caplog
):
    base_query = BaseReadQuery(db_with_alongtrack_data.config)
    base_query.debug = True

    res = base_query.execute_read_query(
        query_spec=_make_along_track_query(),
        params=_make_along_track_params(),
        fields=[
            "latitude",
            "distance",
        ],
    )
    log_text = caplog.text

    assert res is not None
    assert "--- SQL QUERY ---" in log_text
    assert "--- PARAMS ---" not in log_text
    assert "%(longitude)s" not in log_text
    assert "%(latitude)s" not in log_text
    assert "%(distance)s" not in log_text
    assert "-65.9" in log_text
    assert "58.9" in log_text
    assert "500000" in log_text
