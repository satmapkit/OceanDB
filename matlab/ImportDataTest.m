
ncfile = '/Volumes/MoreStorage/along-track-data/SEALEVEL_GLO_PHY_L3_MY_008_062/cmems_obs-sl_glo_phy-ssh_my_j1-l3-duacs_PT1S_202112/2002/04/dt_global_j1_phy_l3_20020424_20210603.nc';

atdb = AlongTrack(username='postgres',db_name='along_track3');
atdb.dropAlongTrackTable();
atdb.createAlongTrackTable();
% atdb.insertAlongTrackDataFromCopernicusNetCDF(ncfile);

ncfolder = dir('/Volumes/MoreStorage/along-track-data/SEALEVEL_GLO_PHY_L3_MY_008_062/cmems_obs-sl_glo_phy-ssh_my_j1-l3-duacs_PT1S_202112/2002/04/*.nc');
for iFile=1:length(ncfolder)
    tic
    atdb.insertAlongTrackDataFromCopernicusNetCDF(fullfile(ncfolder(iFile).folder,ncfolder(iFile).name));
    fprintf('Time to load: %f seconds.\n',toc);
end