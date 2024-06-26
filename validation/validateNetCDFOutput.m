filename = 'along_track_data_for_eddy_id_76543.nc';

fieldnames = cell(1,1);
fieldnames{1} = "index";
fieldnames{end+1} = "along_file_name";
fieldnames{end+1} = "track";
fieldnames{end+1} = "cycle";
fieldnames{end+1} = "latitude";
fieldnames{end+1} = "longitude";
fieldnames{end+1} = "sla_unfiltered";
fieldnames{end+1} = "sla_filtered";
fieldnames{end+1} = "time";
fieldnames{end+1} = "dac";


% eddy.alongTrackFilename = ncread(filename,"along_file_name");
for i=1:length(fieldnames)
    eddy.(fieldnames{i}) = ncread(filename,fieldnames{i});
end

%%
unique_along_files = unique(eddy.along_file_name);
iAlongFile = 1;
along_path = join(['cmems_original',eddy.along_file_name(iAlongFile)],'/');
indicesForFile = eddy.along_file_name == eddy.along_file_name(iAlongFile);
t = ncread(along_path,'time');

%%
figure
scatter(eddy.longitude,eddy.latitude)

% https://data.marine.copernicus.eu/product/SEALEVEL_GLO_PHY_L3_MY_008_062/files?subdataset=cmems_obs-sl_glo_phy-ssh_my_j3-l3-duacs_PT1S_202112&path=SEALEVEL_GLO_PHY_L3_MY_008_062%2Fcmems_obs-sl_glo_phy-ssh_my_j3-l3-duacs_PT1S_202112%2F2016%2F06%2F