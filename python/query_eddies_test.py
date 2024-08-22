import json

from OceanDB.Eddy import Eddy
import folium
from folium import GeoJson
import webbrowser
import os
import psycopg as pg
from psycopg import sql

eddy_db = Eddy(db_name='ocean')

eddy_track = 41
cyclonic_type = -1

speed_radii = eddy_db.eddy_speed_radii_json(eddy_track, cyclonic_type)
max_latitude = speed_radii[0][1]
min_latitude = speed_radii[0][2]
max_longitude = speed_radii[0][3]
min_longitude = speed_radii[0][4]
sw_corner = [min_longitude, min_latitude-360]
ne_corner = [max_longitude, max_latitude-360]

map = folium.Map(
    width="100%",
    height="100%",
    max_bounds=True,
    no_wrap = True,
)
print(sw_corner)
map.fit_bounds([ne_corner, sw_corner])
GeoJson(speed_radii[0][0]).add_to(map)

map.save('test.html')
webbrowser.open('file://'+ os.getcwd() + '/test.html', new=2)