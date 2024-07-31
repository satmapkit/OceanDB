from OceanDB.AlongTrack import AlongTrack
import  matplotlib.pyplot as plt
import yaml

with open('along_params_jje.yaml', 'r') as param_file:
    along_params = yaml.full_load(param_file)

# Database access
host = along_params['db_connect']['host']
username = along_params['db_connect']['username']
password = along_params['db_connect']['password']
port = along_params['db_connect']['port']

# atdb = AlongTrackDatabase(host, username, password, port)
atdb = AlongTrack(host, username, password, port, db_name='ocean')

# data = atdb.geographic_points_in_spatialtemporal_window(11.375, -79.17, 500000, '2002-08-01', '2002-09-01')
data = atdb.geographic_points_in_spatialtemporal_window(-65.93, 24.987, 100000, '2002-04-01', '2003-01-01')

x = [data_i[0] for data_i in data]
y = [data_i[1] for data_i in data]
sla = [data_i[2] for data_i in data]

plt.figure()
plt.scatter(x, y, c=sla)
plt.show()

# print(data)