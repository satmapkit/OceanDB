from datetime import datetime, timedelta

import pytest

from OceanDB.data_access.along_track import AlongTrack
from OceanDB.schemas.along_track_schema import along_track_schema
from tests.database.fixtures import *

pytestmark = pytest.mark.uses_database


def test_geographic_point_in_r_dt_all_fields(db_with_alongtrack_data):
    """
    TEST single point spatiotemporal query
    """
    along_track = AlongTrack(config=db_with_alongtrack_data.config)
    latitude = -39.1
    longitude = 54.7
    date = datetime(year=2013, month=1, day=4, hour=23)

    radius = 500_000
    time_window = timedelta(days=10)

    # fields = list(AlongTrack.schema.keys())
    fields = list(along_track_schema.keys())

    result = along_track.geographic_point_in_r_dt(
        latitude=latitude,
        longitude=longitude,
        date=date,
        fields=fields,
        radius=radius,
        time_window=time_window,
    )

    assert result is not None

    shape = result[fields[0]].shape
    for field in fields:
        assert field in result
        assert result[field].shape == shape


def test_geographic_point_in_r_dt_weird_field(db_with_alongtrack_data):
    along_track = AlongTrack(config=db_with_alongtrack_data.config)
    latitude = -39.1
    longitude = 54.7
    date = datetime(year=2013, month=1, day=4, hour=23)

    radius = 500_000
    time_window = timedelta(days=10)

    weird_fields = [
        [],
        ["distance"],
        ["sla_filtered"],
        ["distance", "sla_filtered"],
    ]

    for fields in weird_fields:
        result = along_track.geographic_point_in_r_dt(
            latitude=latitude,
            longitude=longitude,
            date=date,
            fields=fields,
            radius=radius,
            time_window=time_window,
        )

        assert result is not None

        for field in fields:
            assert field in result
