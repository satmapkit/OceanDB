from datetime import datetime, timedelta

import numpy as np
import pytest

from OceanDB.data_access.along_track import AlongTrack
from OceanDB.schemas.along_track_schema import along_track_schema
from tests.database.fixtures import *

pytestmark = pytest.mark.uses_database


def test_geographic_nearest_neighbor(db_with_alongtrack_data):
    """
    TEST single point spatiotemporal query
    """
    along_track = AlongTrack(db_with_alongtrack_data.config)
    latitude = -69
    longitude = 28.1
    date = datetime(year=2013, month=1, day=4, hour=23)

    time_window = timedelta(days=10)

    fields = list(along_track_schema.keys())

    result = along_track.geographic_nearest_neighbors(
        latitude=latitude,
        longitude=longitude,
        date=date,
        fields=fields,
        time_window=time_window,
        max_radius=None,
    )

    # result should have been gotten
    assert result is not None

    # all requested fields should exist and be identical shape
    shape = result[fields[0]].shape
    for field in fields:
        assert field in result
        assert result[field].shape == shape

    # distance should be monotonically increasing
    assert np.all(result["distance"][:-1] <= result["distance"][1:])

def test_geographic_nearest_neighbor_max_rad_none(db_with_alongtrack_data):
    """
    TEST single point spatiotemporal query
    """
    along_track = AlongTrack(db_with_alongtrack_data.config)
    latitude = -69
    longitude = 28.1
    date = datetime(year=2013, month=1, day=4, hour=23)

    time_window = timedelta(days=10)

    fields = list(along_track_schema.keys())

    result = along_track.geographic_nearest_neighbors(
        latitude=latitude,
        longitude=longitude,
        date=date,
        fields=fields,
        time_window=time_window,
        max_radius=500_000,
    )

    # result should have been gotten
    assert result is None

def test_geographic_nearest_neighbor_max_rad_not_none(db_with_alongtrack_data):
    """
    TEST single point spatiotemporal query
    """
    along_track = AlongTrack(db_with_alongtrack_data.config)
    latitude = -66
    longitude = 60
    date = datetime(year=2013, month=1, day=4, hour=23)

    time_window = timedelta(days=10)

    fields = list(along_track_schema.keys())

    result = along_track.geographic_nearest_neighbors(
        latitude=latitude,
        longitude=longitude,
        date=date,
        fields=fields,
        time_window=time_window,
        max_radius=500_000,
    )

    # result should have been gotten
    assert result is  not None

    # all requested fields should exist and be identical shape
    shape = result[fields[0]].shape
    for field in fields:
        assert field in result
        assert result[field].shape == shape

    # distance should be monotonically increasing
    assert np.all(result["distance"][:-1] <= result["distance"][1:])
