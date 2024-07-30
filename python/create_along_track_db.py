from OceanDB.AlongTrack import AlongTrack
# import OceanDB
import os

# Database access
host = 'localhost'
username = 'postgres'
password = 'eJ^n$+%Ghwq3#oFW'
port = 5432

# Path to directory with Ocean basin files to be uploaded.
directory_basins = os.path.abspath(os.getcwd())  # Default to current active script directory
# Path to Along Track NetCDF files
directory_nc = 'path/to/netcdf'
# directory_nc = '/Users/briancurtis/Documents/Eddy/Along_files2'

# atdb = AlongTrackDatabase(host, username, password, port)
atdb = AlongTrack(host, username, password, port, db_name='along_track4')

# Build Database
atdb.create_database()
atdb.create_along_track_table()
atdb.create_along_track_indices()
atdb.create_along_track_metadata_table()
atdb.create_ocean_basin_tables()
atdb.create_ocean_basin_connection_tables()

# Database build complete. Now load data
#
# # Ocean Basins -- These relations do not currently check to see if they already exist.
# # The basins query will throw an error when duplicate data is entered.
# # The basin connections relation will continue to load duplicate data.
# atdb.insert_ocean_basins_from_csv(directory_basins)
# atdb.insert_ocean_basin_connections_from_csv(directory_basins)
#
# # Load Along Track NetCDF files from an existing directory of files
# # atdb.create_along_track_table_partitions('monthly') Create partitions is done in real time as data is loaded.
# # Add a partition size parameter to the insert data from NetCDF function?
# # atdb.insert_along_track_data_from_netcdf('/Users/briancurtis/Documents/Eddy/along_test_ncs')
# atdb.insert_along_track_data_from_netcdf('/Volumes/MoreStorage/along-track-data/SEALEVEL_GLO_PHY_L3_MY_008_062/cmems_obs-sl_glo_phy-ssh_my_j1-l3-duacs_PT1S_202112/2002/05')
atdb.insert_along_track_data_from_netcdf_with_pandas(directory_nc)