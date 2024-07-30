from OceanDB.AlongTrack import AlongTrack
import os
import yaml

with open('along_params.yaml', 'r') as param_file:
    along_params = yaml.full_load(param_file)

# Database access
host = along_params.get('host')
username = along_params.get('username')
password = along_params.get('password')
port = along_params.get('port')

# atdb = AlongTrackDatabase(host, username, password, port)
atdb = AlongTrack(host, username, password, port, db_name='along_track')

# atdb.drop_database()
# atdb.drop_along_track_metadata_table()
# atdb.create_along_track_metadata_table()

# Load Along Track NetCDF files from an existing directory of files
# atdb.create_along_track_table_partitions('monthly') Create partitions is done in real time as data is loaded.
# Add a partition size parameter to the insert data from NetCDF function?
# atdb.insert_along_track_data_from_netcdf('/Users/briancurtis/Documents/Eddy/along_test_ncs')
# atdb.insert_along_track_data_from_netcdf('/Volumes/MoreStorage/along-track-data/SEALEVEL_GLO_PHY_L3_MY_008_062/cmems_obs-sl_glo_phy-ssh_my_j1-l3-duacs_PT1S_202112/2002/04')
# atdb.drop_table('along_track')
# atdb.create_along_track_table()
# atdb.truncate_along_track_metadata_table()
# atdb.insert_along_track_data_from_netcdf_with_tuples('/Users/jearly/Documents/Data/along-track-data/SEALEVEL_GLO_PHY_L3_MY_008_062/cmems_obs-sl_glo_phy-ssh_my_j1-l3-duacs_PT1S_202112/2002/04')
# atdb.insert_along_track_data_from_netcdf('/Users/jearly/Documents/Data/along-track-data/SEALEVEL_GLO_PHY_L3_MY_008_062/cmems_obs-sl_glo_phy-ssh_my_j1-l3-duacs_PT1S_202112/2002/04')
# atdb.insert_along_track_data_from_netcdf('/Users/briancurtis/Documents/Eddy/Along_files2')
atdb.create_along_track_indices()
