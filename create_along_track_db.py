from AlongTrack import AlongTrackDatabase

atdb = AlongTrackDatabase("localhost", "postgres")
atdb.create_database()
atdb.create_along_track_table()
# atdb.create_ocean_basin_connection_tables()
# atdb.create_along_track_table_partitions('monthly')
# atdb.insert_along_track_data_from_netcdf('path/to/file.nc')