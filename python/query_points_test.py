from OceanDB.AlongTrack import AlongTrack
import yaml

with open('along_params.yaml', 'r') as param_file:
    along_params = yaml.full_load(param_file)

# Database access
host = along_params['db_connect']['host']
username = along_params['db_connect']['username']
password = along_params['db_connect']['password']
port = along_params['db_connect']['port']

# atdb = AlongTrackDatabase(host, username, password, port)
atdb = AlongTrack(host, username, password, port, db_name='ocean')

data = atdb.geographic_points_in_spatialtemporal_window(11.375, -79.17, 100000, '2002-08-01', '2002-09-01')

print(data)