from OceanDB.AlongTrack import AlongTrack
# import OceanDB

atdb = AlongTrack()

# atdb.drop_database()

# Build Database
atdb.create_database()

atdb.create_along_track_table()
atdb.create_along_track_metadata_table()

atdb.create_basin_table()
atdb.insert_basin_from_csv()

atdb.create_basin_connection_table()
atdb.insert_basin_connection_from_csv()
