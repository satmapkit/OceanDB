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

aa = np.array(data)

x = np.array([data_i[0] for data_i in data])
y = np.array([data_i[1] for data_i in data])
sla = np.array([data_i[2] for data_i in data])
t = np.array([data_i[3] for data_i in data])

plt.figure()
plt.scatter(x, y, c=sla)
plt.show()


[x, y, sla, t] = atdb.projected_points_in_spatialtemporal_window(11, 150, datetime.datetime(2022,5,15))
plt.figure()
plt.scatter(x, y, c=sla)
plt.show()

# print(data)