from datetime import datetime

from OceanDB.AlongTrack import AlongTrack

along_track = AlongTrack()

latitude = [-69, -69.1]
longitude = [28, 28.1]
date = [ datetime(year=2013, month=3, day=14, hour=5), datetime(year=2013, month=3, day=14, hour=5)]

data = along_track.projected_geographic_points_in_spatialtemporal_windows(
    latitudes=latitude,
    longitudes=longitude,
    dates=date,
)