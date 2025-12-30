from OceanDB.OceanDB_ETL import AlongTrackMetaData

import netCDF4 as nc


filepath = "/Users/mddarr/delat/azath/ocean/OceanDB/data/copernicus/SEALEVEL_GLO_PHY_L3_MY_008_062/cmems_obs-sl_glo_phy-ssh_my_al-l3-duacs_PT1S_202411/2013/03/dt_global_al_phy_l3_1hz_20130314_20240205.nc"
AlongTrackMetaData.from_netcdf(
    ds=nc.Dataset(filepath),
    file_name='yippy_skippy'
)