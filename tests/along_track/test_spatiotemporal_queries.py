from datetime import datetime, timedelta
from OceanDB.data_access.along_track import AlongTrack
from OceanDB.schemas.along_track_schema import along_track_schema


def test_geographic_point_in_r_dt():
    """
    TEST single point spatiotemporal query
    """
    along_track = AlongTrack()
    latitude = -69
    longitude = 28.1
    date = datetime(year=2013, month=3, day=14, hour=23)

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
