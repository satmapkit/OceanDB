import netCDF4 as nc
import os
from pathlib import Path
import numpy as np

from OceanDB.config import Config
from copy_netcdfs import paths

# load datasets to copy
copy_ds = {}
copy_offsets = {}
for in_path, out in paths.items():
    copy_ds[out[0]] = nc.Dataset(Path(in_path))
    copy_offsets[out[0]] = out[2]


# find overlapping lat/lons
ref_filename = "tests/data/eddy/cyclonic.nc"
reference = copy_ds[ref_filename]
ref_slice = slice(copy_offsets[ref_filename], copy_offsets[ref_filename] + 100)
ref_mask = reference["track"][ref_slice] == 1
ref_lat = np.mean(reference["latitude"][ref_slice][ref_mask])
ref_lon = np.mean(reference["longitude"][ref_slice][ref_mask])

print("finding along track points near (lat,lon):", ref_lat, ref_lon)

for fname, ds in copy_ds.items():
    row = np.where(
        (ds["latitude"][:] - ref_lat) ** 2 + (ds["longitude"][:] - ref_lon) ** 2
        < 1.5**2
    )
    print("for", fname)
    if row[0].size:
        print("first", np.min(row), row)
    else:
        print("no rows found")


# load default datasets
config = Config()

eddy_directory = config.eddy_data_directory
alongtrack_directory = config.along_track_data_directory

# eddy file
eddy_fname = f"{eddy_directory}/META3.2_DT_allsat_Cyclonic_long_19930101_20220209.nc"

# along track files
along_track_prefix = f"{alongtrack_directory}/SEALEVEL_GLO_PHY_L3_MY_008_062/"
along_track_fnames = []
for next_part in os.listdir(Path(along_track_prefix)):
    fname = Path(along_track_prefix, next_part)
    while 1:
        fname = Path(fname, os.listdir(fname)[0])
        if os.path.isfile(fname):
            break
    along_track_fnames.append(fname)


eddy_ds = nc.Dataset(Path(eddy_fname))
along_track_dss = [
    nc.Dataset(Path(along_track_fname)) for along_track_fname in along_track_fnames
]

along_track_missions = [ds.source for ds in along_track_dss]
