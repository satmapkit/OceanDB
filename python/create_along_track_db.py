from OceanDB.AlongTrack import AlongTrack
# import OceanDB
import os
import yaml

with open('along_params.yaml', 'r') as param_file:
    along_params = yaml.full_load(param_file)
print(along_params)
# Database access
host = along_params['db_connect']['host']
username = along_params['db_connect']['username']
password = along_params['db_connect']['password']
port = along_params['db_connect']['port']

# Path to directory with Ocean basin files to be uploaded.
directory_basins = os.path.abspath(os.getcwd())  # Default to current active script directory
# Path to Along Track NetCDF files
directory_nc = along_params['copernicus_marine']['nc_files_path']

# atdb = AlongTrackDatabase(host, username, password, port)
atdb = AlongTrack(host, username, password, port, db_name='ocean')

# atdb.drop_database()

# Build Database
# atdb.create_database()
#
# atdb.create_along_track_table()
# atdb.create_along_track_indices()
# atdb.create_along_track_metadata_table()

# atdb.create_basin_table()
# atdb.insert_basin_from_csv()

# atdb.create_basin_connection_table()
# atdb.insert_basin_connection_from_csv()

# Database build complete. Now load data
#
# # Ocean Basins -- These relations do not currently check to see if they already exist.
# # The basins query will throw an error when duplicate data is entered.
# # The basin connections relation will continue to load duplicate data.
# atdb.insert_basin_from_csv(directory_basins)
# atdb.insert_ocean_basin_connections_from_csv(directory_basins)
#
# # Load Along Track NetCDF files from an existing directory of files
# # atdb.create_along_track_table_partitions('monthly') Create partitions is done in real time as data is loaded.
# # Add a partition size parameter to the insert data from NetCDF function?
print(directory_nc)
atdb.insert_along_track_data_from_netcdf(directory_nc)
# atdb.insert_along_track_data_from_netcdf('/Volumes/MoreStorage/along-track-data/SEALEVEL_GLO_PHY_L3_MY_008_062/cmems_obs-sl_glo_phy-ssh_my_j1-l3-duacs_PT1S_202112/2002/05')