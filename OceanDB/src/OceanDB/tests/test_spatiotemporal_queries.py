from datetime import datetime

from OceanDB.AlongTrack import AlongTrack

along_track = AlongTrack()

latitude = -69
longitude = 28

along_track.geographic_points_in_spatialtemporal_window(
    latitude=latitude,
    longitude=longitude,
    date= datetime(year=2013, month=3, day=14, hour=5),
    missions=['al']
)