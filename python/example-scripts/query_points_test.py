from OceanDB.AlongTrack import AlongTrack
import matplotlib.pyplot as plt
import numpy as np
import datetime

atdb = AlongTrack(db_name='ocean')

# data = atdb.geographic_points_in_spatialtemporal_window(11.375, -79.17, 500000, '2002-08-01', '2002-09-01')
# data = atdb.geographic_points_in_spatialtemporal_window(11., -80, 800000, '1992-05-15', '1994-05-25', should_basin_mask=1)
# data = atdb.geographic_points_in_spatialtemporal_window(11, -80, 800000, '2002-05-15', '2002-05-25', should_basin_mask=0)
# data = atdb.geographic_points_in_spatialtemporal_window(11, 150, 800000, '2002-05-15', '2002-05-25', should_basin_mask=0)
missions = None #['s3b','s6a']

latitude = 78
longitude = -1.5

# latitude = 60
# longitude = -85

data = atdb.geographic_nearest_neighbor(latitude, longitude, datetime.datetime(year=2021, month=5, day=15, hour=3), missions=missions)

data = atdb.geographic_points_in_spatialtemporal_window(latitude, longitude, datetime.datetime(year=2021, month=5, day=15, hour=3), missions=missions)

plt.figure()
plt.scatter(data["longitude"], data["latitude"], c=data["sla_filtered"])
plt.show()


[x, y, sla, t] = atdb.projected_points_in_spatialtemporal_window(latitude, longitude, datetime.datetime(year=2021, month=5, day=15, hour=3))
plt.figure()
plt.scatter(x, y, c=sla)
plt.show()

# print(data)