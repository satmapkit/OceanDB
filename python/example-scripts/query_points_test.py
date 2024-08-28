from OceanDB.AlongTrack import AlongTrack
import matplotlib.pyplot as plt
import numpy as np
import datetime

atdb = AlongTrack(db_name='ocean')

# data = atdb.geographic_points_in_spatialtemporal_window(11.375, -79.17, 500000, '2002-08-01', '2002-09-01')
# data = atdb.geographic_points_in_spatialtemporal_window(11., -80, 800000, '1992-05-15', '1994-05-25', should_basin_mask=1)
# data = atdb.geographic_points_in_spatialtemporal_window(11, -80, 800000, '2002-05-15', '2002-05-25', should_basin_mask=0)
# data = atdb.geographic_points_in_spatialtemporal_window(11, 150, 800000, '2002-05-15', '2002-05-25', should_basin_mask=0)

data = atdb.geographic_points_in_spatialtemporal_window(11, 150, datetime.datetime(2022,5,15))

plt.figure()
plt.scatter(data["longitude"], data["latitude"], c=data["sla_filtered"])
plt.show()


[x, y, sla, t] = atdb.projected_points_in_spatialtemporal_window(11, 150, datetime.datetime(2022,5,15))
plt.figure()
plt.scatter(x, y, c=sla)
plt.show()

# print(data)