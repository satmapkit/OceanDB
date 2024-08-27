import netCDF4 as nc
from OceanDB.AlongTrack import AlongTrack
import numpy as np
import matplotlib.pyplot as plt

atdb = AlongTrack()
file_path = atdb.data_file_path_with_name('basin_masks/new_basin_mask.nc')

ds = nc.Dataset(file_path, 'r')
ds.set_auto_mask(False)
basin_lat = ds.variables['lat']
basin_lon = ds.variables['lon']
basin_mask = ds.variables['basinmask'][:]

onesixth = 1/6

lon = np.arange(-85, -75, .1)
lat = np.arange(4., 16., .1)
lon = np.arange(-85, -75, .1)
lat = np.arange(22., 37., .1)
lons, lats = np.meshgrid(lon, lat)

i = np.floor((lats+90)/onesixth).astype(int)
j = np.floor((lons%360) /onesixth).astype(int)

m = basin_mask[i,j]

blons, blats = np.meshgrid(basin_lon, basin_lat)

plt.figure()
# plt.scatter(blons, blats, c=basin_mask)
plt.scatter(lons, lats, c=m,  vmin=0, vmax=100)
plt.show()

print('done')