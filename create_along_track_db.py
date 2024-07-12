from AlongTrack import AlongTrackDatabase

atdb = AlongTrackDatabase("localhost", "postgres")
atdb.create_database()
atdb.create_along_track_table()
