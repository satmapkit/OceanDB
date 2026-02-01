# from OceanDB.data_access import AlongTrack
# from OceanDB.data_access.project_compiler import ProjectionCompiler
# from OceanDB.data_access.schema.along_track_schema import along_track_schema
#
# along_track = AlongTrack()
# query_string = along_track.load_sql_file("queries/along_track/geographic_points_in_spatialtemporal_window.sql")
#
# compiler = ProjectionCompiler(schema=along_track_schema)
#
# query = compiler.compile(
#     sql_template=query_string,
#     fields=['latitude'],
# )
# along_track.execute_query(
#     query=query,
#     params=
# )