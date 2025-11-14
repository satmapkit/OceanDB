from datetime import datetime
import numpy as np

from OceanDB.AlongTrack import AlongTrack

along_track = AlongTrack()

latitude = -65
longitude = 115
date = datetime(year=2014, month=2, day=28, hour=5)

data = along_track.geographic_nearest_neighbor(
    latitude=latitude,
    longitude=longitude,
    date=date,
    missions=['al']
)

print(data)



data = along_track.geographic_nearest_neighbors(
    latitudes=np.array([latitude]),
    longitudes=np.array([longitude]),
    date=date,
    missions=['al']
)

for d in data:
    print(d)


