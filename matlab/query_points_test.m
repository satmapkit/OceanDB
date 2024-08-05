atdb = AlongTrack();

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%
% Geographic Points
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

results = atdb.geographicPointsInSpatialtemporalWindow(11, -80, datetime(2002,5,15),distance=1000e3);

figure(Name="Geographic points")
scatter(results.longitude,results.latitude,5^2,results.sla_filtered,"filled");

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%
% Projected Points
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

Lx=2000e3;
Ly=2000e3;

results = atdb.projectedPointsInSpatialtemporalWindow(50, -138, datetime(2002,5,15),Lx=Lx,Ly=Ly);

% results = atdb.projectedPointsInSpatialtemporalWindow(11, -80, datetime(2002,5,15),Lx=Lx,Ly=Ly);

figure(Name="Projected points")
tiledlayout(2,1)
nexttile
scatter(results.longitude,results.latitude,5^2,results.sla_filtered,"filled");
nexttile
scatter(results.x/1e3,results.y/1e3,5^2,results.sla_filtered,"filled");
axis equal
xlim([0 Lx/1e3])
ylim([0 Ly/1e3])

d = sqrt((results.x-Lx/2).^2 + (results.y-Ly/2).^2);
figure, scatter(results.distance,results.distance-d,'filled')