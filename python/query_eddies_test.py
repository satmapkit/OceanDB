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
print(speed_radii[0][1])
print(speed_radii[0][2])
print(speed_radii[0][3])
print(speed_radii[0][4])

map = folium.Map(
width="%100",
height="%100",
zoom_start=10)
GeoJson(speed_radii[0][0]).add_to(map)

map.save('test.html')
webbrowser.open('file://'+ os.getcwd() + '/test.html', new=2)