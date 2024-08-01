from OceanDB.AlongTrack import AlongTrack
import matplotlib.pyplot as plt

atdb = AlongTrack(db_name='ocean')

# data = atdb.geographic_points_in_spatialtemporal_window(11.375, -79.17, 500000, '2002-08-01', '2002-09-01')
data = atdb.geographic_points_in_spatialtemporal_window(11., -80, 800000, '2002-05-15', '2005-05-25', should_basin_mask=1)

x = [data_i[0] for data_i in data]
y = [data_i[1] for data_i in data]
sla = [data_i[2] for data_i in data]

plt.figure()
plt.scatter(x, y, c=sla)
plt.show()

import numpy as np

x = np.array(x)
y = np.array(y)
sla = np.array(sla)

np.stack((x, y), axis=1)

# print(data)