from datetime import datetime, timedelta

import numpy as np
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


def test_geographic_point_in_r_dt_batch_matches_single_queries(
    db_with_alongtrack_data,
):
    along_track = AlongTrack(config=db_with_alongtrack_data.config)
    fields = list(along_track_schema.keys())

    latitudes = [-39.1, 58.9]
    longitudes = [54.7, -65.9]
    dates = [
        datetime(year=2013, month=1, day=4, hour=23),
        datetime(year=2013, month=1, day=4, hour=23),
    ]

    expected = [
        along_track.geographic_point_in_r_dt(
            latitude=latitude,
            longitude=longitude,
            date=date,
            fields=fields,
            radius=500_000,
            time_window=timedelta(days=10),
        )
        for latitude, longitude, date in zip(latitudes, longitudes, dates, strict=True)
    ]

    result = list(
        along_track.geographic_point_in_r_dt_batch(
            latitudes=latitudes,
            longitudes=longitudes,
            dates=dates,
            fields=fields,
            radius=500_000,
            time_window=timedelta(days=10),
        )
    )

    assert len(result) == len(expected)

    for batch_item, single_item in zip(result, expected, strict=True):
        assert (batch_item is None) is (single_item is None)
        if batch_item is None or single_item is None:
            continue

        for field in fields:
            assert field in batch_item
            assert np.array_equal(batch_item[field], single_item[field])


def test_geographic_point_in_r_dt_batch_rejects_mismatched_input_lengths(
    db_with_alongtrack_data,
):
    along_track = AlongTrack(config=db_with_alongtrack_data.config)

    with pytest.raises(ValueError):
        list(
            along_track.geographic_point_in_r_dt_batch(
                fields=list(along_track_schema.keys()),
                latitudes=[-39.1, 58.9],
                longitudes=[54.7],
                dates=[datetime(year=2013, month=1, day=4, hour=23)],
            )
        )
