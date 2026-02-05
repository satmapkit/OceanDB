from datetime import datetime, timedelta
from OceanDB.data_access.along_track import AlongTrack
from OceanDB.schemas.along_track_schema import along_track_schema
import numpy as np


def test_geographic_nearest_neighbor():
    """
    TEST single point spatiotemporal query
    """
    along_track = AlongTrack()
    latitude = -69
    longitude = 28.1
    date = datetime(year=2013, month=3, day=14, hour=23)

    time_window = timedelta(days=10)

    fields = list(along_track_schema.keys())

    result = along_track.geographic_nearest_neighbors(
        latitude=latitude,
        longitude=longitude,
        date=date,
        fields=fields,
        time_window=time_window,
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
