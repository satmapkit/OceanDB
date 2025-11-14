import numpy as np
from datetime import datetime
from OceanDB.AlongTrack import AlongTrack

along_track = AlongTrack()


"""
TEST single point spatiotemporal query
"""

latitude =  -63.77912
longitude = 291.794742
date = datetime(year=2013, month=12, day=31, hour=23)


sla_geographic = along_track.geographic_points_in_spatialtemporal_window(
    latitude=latitude,
    longitude=longitude,
    date=date
)

print(sla_geographic)


"""
TEST multipoint spatiotemporal query
"""

latitudes = np.array([latitude])
longitudes = np.array([longitude])
dates = [ date ]

sla_geographic = along_track.geographic_points_in_spatialtemporal_windows(
    latitudes=latitudes,
    longitudes=longitudes,
    dates=dates
)

for data in sla_geographic:
    print(data)
