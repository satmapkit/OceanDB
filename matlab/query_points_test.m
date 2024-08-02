atdb = AlongTrack();

results = atdb.geographicPointsInSpatialtemporalWindow(11, -150, 800000, '2002-05-15', '2002-05-25');

figure
scatter(results.longitude,results.latitude,5^2,results.sla_filtered,"filled");