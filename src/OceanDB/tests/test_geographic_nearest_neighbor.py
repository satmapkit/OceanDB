from datetime import datetime
import numpy as np

from OceanDB.AlongTrack import AlongTrack

along_track = AlongTrack()

latitude = -69
longitude = 28
date = datetime(year=2013, month=3, day=14, hour=5)


data = along_track.geographic_nearest_neighbors_dt(
    latitudes=np.array([latitude]),
    longitudes=np.array([longitude]),
    dates=[date],
    missions=['al']
)

for d in data:
    print(d)


