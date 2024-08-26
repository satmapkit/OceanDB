from OceanDB.AlongTrack import AlongTrack
import psycopg as pg
from psycopg import sql
import matplotlib.pyplot as plt
import numpy as np
import datetime
import time
from datetime import timedelta
# import asyncio

atdb = AlongTrack(db_name='ocean')

lon = np.arange(320.-180., 330.-180., 1.)
lat = np.arange(20., 25., 1.)
lons, lats = np.meshgrid(lon, lat)

latitudes = lats.reshape(-1)
longitudes = lons.reshape(-1)
# date = datetime.datetime(year=2021, month=5, day=15, hour=3)
# distance=500000
# time_window=timedelta(seconds=856710)
# start = time.time()
# atdb.map_points_in_spatialtemporal_window(lats.reshape(-1),lons.reshape(-1),datetime.datetime(year=2021,month=5,day=15,hour=3))
# end = time.time()
# print(f"Script end. Total time: {end - start}")

query_string_a = """
    SELECT longitude, latitude, sla_filtered, EXTRACT(EPOCH FROM ('2021-05-15 03:00:00'::timestamp - date_time)) AS time_difference_secs
    FROM along_track alt
    LEFT JOIN basin on ST_Intersects(basin.basin_geog, alt.along_track_point)
    WHERE ST_DWithin(ST_MakePoint(%(longitude)s, %(latitude)s),along_track_point, 500000)
    AND date_time BETWEEN '2021-05-15 03:00:00'::timestamp - '4 days 22:59:15'::interval
    AND '2021-05-15 03:00:00'::timestamp + '4 days 22:59:15'::interval
    AND basin.id IN (
        SELECT UNNEST(ARRAY[pbc.connected_id, basin_id])
        FROM basin pb
        LEFT JOIN basin_connection pbc on basin_id = pb.id
        WHERE ST_Intersects(pb.basin_geog, ST_MakePoint(%(longitude)s, %(latitude)s))
    )"""
query_string_b = """
    SELECT longitude, latitude, sla_filtered, EXTRACT(EPOCH FROM ('2021-05-15 03:00:00'::timestamp - date_time)) AS time_difference_secs
    FROM along_track alt
    WHERE ST_DWithin(ST_MakePoint(%(longitude)s, %(latitude)s),along_track_point, 500000)
    AND date_time BETWEEN '2021-05-15 03:00:00'::timestamp - '4 days 22:59:15'::interval
    AND '2021-05-15 03:00:00'::timestamp + '4 days 22:59:15'::interval
    """

latlons = [{"latitude": latitudes, "longitude": longitudes} for latitudes, longitudes in zip(latitudes, longitudes)]

start = time.time()
with pg.connect(atdb.connect_string()) as connection:
    with connection.cursor() as cursor:
        cursor.executemany(query_string_a, latlons, returning=True)
        # cursor.executemany("select (%(latitude)s,%(longitude)s)", latlons, returning=True)
        while True:
            data = cursor.fetchall()
            if not cursor.nextset():
                break
        # for i in range(0, len(latlons)):
        #     cursor.execute(query, latlons[i], prepare=True)
        # data = cursor.fetchall()

end = time.time()
print(f"Script end. Total time: {end - start}")