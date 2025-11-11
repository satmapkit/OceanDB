from datetime import datetime

from OceanDB.AlongTrack import AlongTrack

along_track = AlongTrack()

latitude = -69
longitude = 28
date = datetime(year=2013, month=3, day=14, hour=5)

data = along_track.geographic_nearest_neighbor(
    latitude=latitude,
    longitude=longitude,
    date=date,
    missions=['al']
)

print(data)
