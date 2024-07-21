from AlongTrack import AlongTrackDatabase
import os

# Database access
host = 'localhost'
username = 'postgres'
password = 'eJ^n$+%Ghwq3#oFW'
port = 5432

# Path to directory with files to be uploaded.
directory = os.path.abspath(os.getcwd()) # Default to current active script directory

atdb = AlongTrackDatabase(host, username, password, port, directory)
# Build Database
atdb.create_database()
atdb.create_along_track_table()
atdb.create_along_track_indices()
atdb.create_ocean_basin_tables()
atdb.create_ocean_basin_connection_tables()
# Database build complete. Now load data
atdb.insert_ocean_basins_from_csv()
atdb.insert_ocean_basin_connections_from_csv()
# atdb.create_along_track_table_partitions('monthly')
# atdb.insert_along_track_data_from_netcdf('path/to/file.nc')