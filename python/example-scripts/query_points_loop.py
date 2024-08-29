from OceanDB.AlongTrack import AlongTrack
import numpy as np
import datetime
import time

atdb = AlongTrack(db_name='ocean')
# lat: -70 to 70
# lon: -90 to 0 takes 84 sec, compared to 24 for all other 90 degree increments
# lon: -90 to -45 takes 70 sec, -45 to 0 takes 1.8
# lon: -90 to -67.5 takes 55 sec, -67.5 to -45 takes 14
# lat: 0 to 70, lon -90 to -67.5 takes 18 sec, but -70 to 0 only takes 0.9
date = datetime.datetime(year=2021, month=5, day=15, hour=3)
lon = np.arange(-180., 179, 1.)
# lon = np.arange(-67.5, -45., 1.)
lat = np.arange(-70., 70., 1.)
lons, lats = np.meshgrid(lon, lat)
missions = ['s3b','s6a-lr']
missions = None

# print("spatial temporal query...")
# start = time.time()
# for data in atdb.geographic_points_in_spatialtemporal_windows(lats.reshape(-1), lons.reshape(-1), date, missions=missions):
#     a = data["sla_filtered"]
# end = time.time()
# print(f"total time: {end - start}")

print("nearest neighbor query...")
start = time.time()
for data in atdb.geographic_nearest_neighbors(lats.reshape(-1), lons.reshape(-1), date, missions=missions):
    a = data["sla_filtered"]
end = time.time()
print(f"total time: {end - start}")
