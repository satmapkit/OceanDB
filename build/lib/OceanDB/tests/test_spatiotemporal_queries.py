import numpy as np
from datetime import datetime
from OceanDB.AlongTrack import AlongTrack

along_track = AlongTrack()


"""
TEST single point spatiotemporal query
"""

latitude =  -69
longitude = 4.4
date = datetime(year=2022, month=3, day=23, hour=23)

sla_geographic = along_track.geographic_points_in_r_dt(
    latitudes=np.array([latitude]),
    longitudes=np.array([longitude]),
    dates=[date]
)

