filename = '../python/eddy_+527413.nc';
% filename = 'eddy_-41.nc';

eddy_time = days(ncread(filename,'eddy/time'))+datetime(1950,01,01);
eddy_lat = ncread(filename,'eddy/latitude');
eddy_lon = ncread(filename,'eddy/longitude');
eddy_amp = ncread(filename,'eddy/amplitude');

figure
scatter(eddy_lon,eddy_lat,6^2,eddy_amp)

at_time = days(ncread(filename,'alongtrack/time'))+datetime(1950,01,01);
at_lat = ncread(filename,'alongtrack/latitude');
at_lon = ncread(filename,'alongtrack/longitude');
at_sla = ncread(filename,'alongtrack/sla_filtered');

hold on
scatter(at_lon,at_lat,3^2,at_sla)