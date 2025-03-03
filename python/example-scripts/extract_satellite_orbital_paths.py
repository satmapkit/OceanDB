from OceanDB.AlongTrack import AlongTrack
import xarray as xr

atdb = AlongTrack(db_name='ocean')
missions = ['al', 'alg', 'c2', 'c2n', 'e1g', 'e1', 'e2', 'en', 'enn', 'g2', 'h2a', 'h2b', 'j1g', 'j1', 'j1n', 'j2g',
                'j2', 'j2n', 'j3', 'j3n', 's3a', 's3b', 's6a', 'tp', 'tpn']
atdb.write_orbital_path_and_cycles_for_missions("orbital_paths.nc",missions)
# atdb.write_orbital_path_and_cycles_for_missions("orbital_paths.nc",['j1','e1'])

# [cycle, cycle_encoding] = atdb.orbital_cycles_for_mission_as_xarray('j1n')
# cycle = cycle.rename({'index': 'cycle_index'})
# cycle.to_netcdf("orbital_cycle.nc", "w", group="j1n", encoding=cycle_encoding, format="NETCDF4")
#
# first_full_cycle = cycle['cycle'].min().item() + 1
# [along_track, along_encoding] = atdb.orbital_path_for_mission_as_xarray('j1n',first_full_cycle)
# along_track = along_track.rename({'index': 'along_track_index'})
# comp = {'zlib': True, 'complevel': 9}
# comp_vars = {"latitude":comp,"longitude":comp}
# along_track.to_netcdf("orbital_cycle.nc", "a", group="j1n", encoding=comp_vars, format="NETCDF4")
