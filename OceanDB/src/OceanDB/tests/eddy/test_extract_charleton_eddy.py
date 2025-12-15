import netCDF4 as nc
cyclonic_filepath = "data/eddy/META3.2_DT_allsat_Cyclonic_long_19930101_20220209.nc"

ds = nc.Dataset(cyclonic_filepath, 'r')
date_time = ds.variables['time']  # Extract dates from the dataset and convert them to standard datetime

time_data = nc.num2date(date_time[:], date_time.units, only_use_cftime_datetimes=False,
                        only_use_python_datetimes=False)

eddy_data = {
    'amplitude': ds.variables['amplitude'][:],
    # 'cyclonic_type': ds.variables['cyclonic_type'][:],
    'latitude': ds.variables['latitude'][:],
    'longitude': ds.variables['longitude'][:],
    'observation_flag': ds.variables['observation_flag'][:],
    'observation_number': ds.variables['observation_number'][:],
    'speed_average': ds.variables['speed_average'][:],
    'speed_radius': ds.variables['speed_radius'][:],
    'date_time': time_data,
    'track': ds.variables['track'][:],
}