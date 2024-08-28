from OceanDB.AlongTrack import AlongTrack
import psycopg as pg
from psycopg import sql
import matplotlib.pyplot as plt
import numpy as np
import datetime
import time
from datetime import timedelta
# import asyncio

atdb = AlongTrack(db_name='ocean')

# gen = atdb.map_points_in_spatialtemporal_window([140], [20], datetime.datetime(year=2021, month=5, day=15, hour=3), should_basin_mask=0)

date = datetime.datetime(year=2021, month=5, day=15, hour=3)
lon = np.arange(320.-180., 340.-180., 1.)
lat = np.arange(20., 40., 1.)
lons, lats = np.meshgrid(lon, lat)
missions = ['s3b','s6a-lr']

start = time.time()
for delta_x, delta_y, delta_t, sla in atdb.projected_geographic_points_in_spatialtemporal_windows(lats.reshape(-1), lons.reshape(-1), date, should_basin_mask=1, missions=missions):
    a = delta_x+delta_y
end = time.time()
print(f"Script end. Total time: {end - start}")
