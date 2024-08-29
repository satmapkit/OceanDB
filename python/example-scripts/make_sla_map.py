from OceanDB.AlongTrack import AlongTrack
import numpy as np
import datetime
import time
import xarray as xr
import pandas as pd
import matplotlib as mpl
import matplotlib.pyplot as plt

def gaussian_kernel(x2, sigma):
    return np.exp(-0.5 * x2 / (sigma*sigma)) / (sigma * np.sqrt(2 * np.pi))

def gaussian_kernel_smoother(x2, data, kernel_width):
    weights = gaussian_kernel(x2, kernel_width)

    # Normalize weights
    weights /= weights.sum()

    # Apply the weights to the data
    smoothed_data = np.sum(weights * data)
    return smoothed_data

atdb = AlongTrack()

should_write_to_file = 0
date = datetime.datetime(year=2021, month=5, day=15, hour=3)
resolution = 1.0
dpc = 1/resolution
filename = 'sla_mapped_'+date.strftime('%Y-%m-%d_%H-%M')+'_res_1_'+str(round(dpc))+'.nc'
lon_dim = np.arange(-120., -70., resolution) + resolution
lat_dim = np.arange(-10., 40., resolution) + resolution
lon_dim = np.arange(-180, 180-resolution, resolution) + resolution/2
lat_dim = np.arange(-70, 70-resolution, resolution) + resolution/2
lon_grid, lat_grid = np.meshgrid(lon_dim, lat_dim)
lat_world = lat_grid.reshape(-1)
lon_world = lon_grid.reshape(-1)

basin_mask = atdb.basin_mask(lat_world, lon_world)
ocean_indices = (basin_mask > 0) & (basin_mask < 1000)
lat_ocean = lat_world[ocean_indices]
lon_ocean = lon_world[ocean_indices]



missions = None
# missions = ['s3b','s6a']

print(f"Building nearest-neighbor map with resolution {resolution}...")
sla_world_nn = np.empty_like(lon_world)
sla_world_nn[:] = np.nan
sla_ocean = sla_world_nn[ocean_indices]
start = time.time()
i = 0
for data in atdb.geographic_nearest_neighbors(lat_ocean, lon_ocean, date, missions=missions):
    sla_ocean[i] = data["sla_filtered"][0]
    i = i + 1
end = time.time()
print(f"Mapping complete. Total time: {end - start}")
sla_world_nn[ocean_indices] = sla_ocean
sla_world_nn = sla_world_nn.reshape(lon_grid.shape)
sla_map_nn = xr.DataArray(sla_world_nn, coords={'latitude': lat_dim, 'longitude': lon_dim},
                                  dims=["latitude", "longitude"])

print(f"Building gaussian kernel map with resolution {resolution}...")
sla_world_gk = np.empty_like(lon_world)
sla_world_gk[:] = np.nan
sla_ocean = sla_world_gk[ocean_indices]
L = (50e3 + 250e3 * (900 / (lat_ocean ** 2 + 900))) / 3.34
# search radius may be too small and leave white stripes
radius = L * np.sqrt(-np.log(0.0001))
start = time.time()
i = 0
for data in atdb.projected_geographic_points_in_spatialtemporal_windows(lat_ocean, lon_ocean, date, distance=radius, missions=missions):
    # out_of_bounds = data["sla_filtered"] > 1.0
    # data["delta_x"] = data["delta_x"][~out_of_bounds]
    # data["delta_y"] = data["delta_y"][~out_of_bounds]
    # data["sla_filtered"] = data["sla_filtered"][~out_of_bounds]
    x2 = data["delta_x"]**2 + data["delta_y"]**2
    sla_ocean[i] = gaussian_kernel_smoother(x2, data["sla_filtered"], L[i])
    i = i + 1
end = time.time()
print(f"Mapping complete. Total time: {end - start}")
sla_world_gk[ocean_indices] = sla_ocean
sla_world_gk = sla_world_gk.reshape(lon_grid.shape)
sla_map_gk = xr.DataArray(sla_world_gk, coords={'latitude': lat_dim, 'longitude': lon_dim},
                                  dims=["latitude", "longitude"])
if should_write_to_file:
    sla_map_gk.to_dataset(name="sla").to_netcdf(filename, "w", group="gaussian_kernel", format="NETCDF4")
    sla_map_nn.to_dataset(name="sla").to_netcdf(filename, "a", group="nearest_neighbor", format="NETCDF4")

# plt.figure()
# plt.scatter(lon_ocean, lat_ocean, c=sla_ocean,  vmin=-0.5, vmax=0.5)
# plt.show()

vmin = -.5
vmax = .5
norm = mpl.colors.Normalize(vmin=vmin,vmax=vmax)

plt.figure()
# ax = sla_map.plot.contourf(levels=100, norm=norm, cmap='RdBu_r')
ax = sla_map_nn.plot.pcolormesh(norm=norm, cmap='RdBu_r')
_ = plt.title('Nearest neighbor map')
plt.show()

plt.figure()
# ax = sla_map.plot.contourf(levels=100, norm=norm, cmap='RdBu_r')
ax = sla_map_gk.plot.pcolormesh(norm=norm, cmap='RdBu_r')
_ = plt.title('Gaussian kernel map')
plt.show()

print("done")