import netCDF4 as nc
from OceanDB.AlongTrack import AlongTrack
import numpy as np
import matplotlib.pyplot as plt
import psycopg as pg
from psycopg import sql

atdb = AlongTrack()

lon = np.arange(-85, -75, .1)
lat = np.arange(4., 16., .1)
lon = np.arange(-85, -75, .1)
lat = np.arange(22., 37., .1)
lons, lats = np.meshgrid(lon, lat)

basin_mask = atdb.basin_mask(lats, lons)

plt.figure()
plt.scatter(lons, lats, c=basin_mask,  vmin=0, vmax=100)
plt.show()

basin_connection_map = atdb.basin_connection_map

print('done')