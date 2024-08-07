classdef AlongTrack < handle
    %UNTITLED Summary of this class goes here
    %   Detailed explanation goes here

    properties
        host = 'localhost'
        username = 'postgres'
        password = ''
        port = 5432
        db_name = 'ocean'
        partitions_created = []
    end

    properties (Constant)
        alongTrackTableName = 'along_track'
        alongTrackMetadataTableName = 'along_track_metadata'
        oceanBasinTableName = 'basin'
        oceanBasinConnectionTableName = 'basin_connection'
    end

    methods
        %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
        %
        % Initialization
        %
        %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

        function self = AlongTrack(options)
            arguments
                options.username
                options.password
                options.db_name
                options.host
                options.port
            end
            if exist('config.yaml','file')
                config = yamlread('config.yaml');
                if isfield(config,'db_connect')
                    db_connect = config.db_connect;
                    if isfield(db_connect,'host')
                        self.host = db_connect.host;
                    end
                    if isfield(db_connect,'password')
                        self.password = db_connect.password;
                    end
                    if isfield(db_connect,'db_name')
                        self.db_name = db_connect.db_name;
                    end
                    if isfield(db_connect,'host')
                        self.host = db_connect.host;
                    end
                    if isfield(db_connect,'port')
                        self.port = db_connect.port;
                    end
                end
            end
            if isfield(options,'host')
                self.host = options.host;
            end
            if isfield(options,'password')
                self.password = options.password;
            end
            if isfield(options,'db_name')
                self.db_name = options.db_name;
            end
            if isfield(options,'host')
                self.host = options.host;
            end
            if isfield(options,'port')
                self.port = options.port;
            end
        end

        %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
        %
        % Database creation/destruction
        %
        %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

        function createDatabase(self)
            pg_conn = postgresql(self.username,self.password,Server=self.host,PortNumber=self.port);
            query = "SELECT 1 FROM pg_database WHERE datname = '%s'";
            results = pg_conn.fetch(sprintf(query,self.db_name),DataReturnFormat="structure");
            
            if isempty(results)
                query = "CREATE DATABASE %s";
                pg_conn.AutoCommit = "on";
                pg_conn.execute(sprintf(query,self.db_name));

                atdb_conn = self.connection();
                atdb_conn.execute("CREATE EXTENSION IF NOT EXISTS plpgsql;");
                atdb_conn.execute("CREATE EXTENSION IF NOT EXISTS postgis;");
                atdb_conn.execute("CREATE EXTENSION IF NOT EXISTS btree_gist;");
                atdb_conn.commit();
                atdb_conn.close();

                fprintf(join(["Database " self.db_name " created successfully.\n"]))
            else
                fprintf(join(["Database " self.db_name " already exists.\n"]))
            end

            pg_conn.close();
        end

        function dropDatabase(self)
            pg_conn = postgresql(self.username,self.password,Server=self.host,PortNumber=self.port);
            query = "DROP DATABASE %s WITH (FORCE)";
            pg_conn.AutoCommit = "on";
            pg_conn.execute(sprintf(query,self.db_name));
            pg_conn.close();

            fprintf(join(["Database " self.db_name " dropped.\n"]));
        end

        %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
        %
        % Generic database actions
        %
        %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

        function connection = connection(self)
            connection = postgresql(self.username,self.password,Server=self.host,DatabaseName=self.db_name,PortNumber=self.port);
        end

        function executeQuery(self,query)
            arguments
                self AlongTrack
            end
            arguments (Repeating)
                query string
            end

            atdb_conn = self.connection();
            for iQuery=1:length(query)
                atdb_conn.execute(query{iQuery});
            end
            atdb_conn.commit();
            atdb_conn.close();
        end

        function dropTable(self,tableName)
            arguments
                self AlongTrack
                tableName string
            end

            tokenizedQuery = "DROP TABLE IF EXISTS public.{table_name}";
            query = regexprep(tokenizedQuery,"{table_name}",tableName);

            self.executeQuery(query)
            fprintf(join(["Table " tableName " dropped from database " self.db_name ".\n"]));
        end

        function truncateTable(self,tableName)
            arguments
                self AlongTrack
                tableName string
            end

            tokenizedQuery = "TRUNCATE public.{table_name}";
            query = regexprep(tokenizedQuery,"{table_name}",tableName);

            self.executeQuery(query)
            fprintf(join(["All data removed from table " tableName " in database " self.db_name ".\n"]));
        end

        function query = sqlQueryWithName(self,name)
            arguments
                self 
                name 
            end
            [filepath,~,~] = fileparts(mfilename('fullpath'));
            path = fullfile(filepath,'..', 'sql',name);
            query = join(readlines(path));
        end

        function path = dataFilePathWithName(self,name)
            [filepath,~,~] = fileparts(mfilename('fullpath'));
            path = fullfile(filepath,'..', 'data',name);
        end

        function query = queryWithParameters(self,tokenizedQuery,parameters)
            query = tokenizedQuery;
            for k = keys(parameters)
                token = join(["%\(" k{1} "\)s"],"");
                value = parameters(k{1});
                if isa(value,'double')
                    query = regexprep(query,token,string(value));
                elseif isa(value,'duration')
                    query = regexprep(query,token,sprintf("interval '%d seconds'",seconds(value)));
                elseif isa(value,'datetime')
                    query = regexprep(query,token,sprintf("TIMESTAMP '%s'",string(value,"yyyy-MM-dd HH:mm:ss")));
                else
                    error('Type not recognized.')
                end
            end
        end

        %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
        %
        % along_track table creation/destruction
        %
        %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

        function createAlongTrackTable(self)
            tokenizedQuery = self.sqlQueryWithName("create_along_track_table.sql");
            query = regexprep(tokenizedQuery,"{table_name}",self.alongTrackTableName);

            self.executeQuery(query);

            fprintf(join(["Table " self.alongTrackTableName " added to database " self.db_name " (if it did not previously exist).\n"]));
        end

        function dropAlongTrackTable(self)
            self.dropTable(self.alongTrackTableName);
        end

        function truncateAlongTrackTable(self)
            self.truncateTable(self.alongTrackTableName);
        end

        function createAlongTrackIndices(self)
            tokenizedQuery = self.sqlQueryWithName("create_along_track_index_point.sql");
            queryPointIndex = regexprep(tokenizedQuery,"{table_name}",self.alongTrackTableName);

            tokenizedQuery = self.sqlQueryWithName("create_along_track_index_date.sql");
            queryDateIndex = regexprep(tokenizedQuery,"{table_name}",self.alongTrackTableName);

            tokenizedQuery = self.sqlQueryWithName("create_along_track_index_point_date.sql");
            queryPointDateIndex = regexprep(tokenizedQuery,"{table_name}",self.alongTrackTableName);

            tokenizedQuery = self.sqlQueryWithName("create_along_track_index_filename.sql");
            queryFilenameIndex = regexprep(tokenizedQuery,"{table_name}",self.alongTrackTableName);

            self.executeQuery(queryPointIndex,queryDateIndex,queryFilenameIndex);

            fprintf(join(["Indices added to table " self.alongTrackTableName " in database " self.db_name ".\n"]));
        end

        function dropAlongTrackIndices(self)
            tokenizedQuery = self.sqlQueryWithName("drop_along_track_index_point.sql");
            queryPointIndex = regexprep(tokenizedQuery,"{table_name}",self.alongTrackTableName);

            tokenizedQuery = self.sqlQueryWithName("drop_along_track_index_date.sql");
            queryDateIndex = regexprep(tokenizedQuery,"{table_name}",self.alongTrackTableName);

            tokenizedQuery = self.sqlQueryWithName("drop_along_track_index_point_date.sql");
            queryPointDateIndex = regexprep(tokenizedQuery,"{table_name}",self.alongTrackTableName);

            tokenizedQuery = self.sqlQueryWithName("drop_along_track_index_filename.sql");
            queryFilenameIndex = regexprep(tokenizedQuery,"{table_name}",self.alongTrackTableName);

            self.executeQuery(queryPointIndex,queryDateIndex,queryFilenameIndex);

            fprintf(join(["Indices dropped from table " self.alongTrackTableName " in database " self.db_name ".\n"]));
        end

        function insertAlongTrackDataFromCopernicusNetCDF(self,file)
           ncid = netcdf.open(file);

           time = netcdf.getVar(ncid,netcdf.inqVarID(ncid,'time'));
           lat = netcdf.getVar(ncid,netcdf.inqVarID(ncid,'latitude'));
           lon = netcdf.getVar(ncid,netcdf.inqVarID(ncid,'longitude'));
           cycle = netcdf.getVar(ncid,netcdf.inqVarID(ncid,'cycle'));
           track = netcdf.getVar(ncid,netcdf.inqVarID(ncid,'track'));
           sla_unfiltered = netcdf.getVar(ncid,netcdf.inqVarID(ncid,'sla_unfiltered'));
           sla_filtered = netcdf.getVar(ncid,netcdf.inqVarID(ncid,'sla_filtered'));
           dac = netcdf.getVar(ncid,netcdf.inqVarID(ncid,'dac'));
           ocean_tide = netcdf.getVar(ncid,netcdf.inqVarID(ncid,'ocean_tide'));
           internal_tide = netcdf.getVar(ncid,netcdf.inqVarID(ncid,'internal_tide'));
           lwe = netcdf.getVar(ncid,netcdf.inqVarID(ncid,'lwe'));
           mdt = netcdf.getVar(ncid,netcdf.inqVarID(ncid,'mdt'));
           % tpa_correction = ncread(file,'tpa_correction');

           netcdf.close(ncid);

           date_time = datetime(1950,01,01) + days(time);
           atData = table(date_time,lat,lon,cycle,track,sla_unfiltered,sla_filtered,dac,ocean_tide,internal_tide,lwe,mdt);

           self.createAlongTrackTablePartitionsForDates(date_time);

           atdb_conn = self.connection();
           sqlwrite(atdb_conn,self.alongTrackTableName,atData);
           atdb_conn.commit();
           atdb_conn.close();
        end

        function createAlongTrackTablePartitionsForDates(self,dates,options)
            arguments
                self AlongTrack
                dates datetime
                options.partitionDuration char {mustBeMember(options.partitionDuration,["monthly","yearly"])} = "yearly"
            end
            
            if strcmp(options.partitionDuration,"yearly")
                for aYear = year(min(dates)):1:year(max(dates))
                    partitionName = sprintf('%s_%d',self.alongTrackTableName,aYear);
                    partitionMinDate = sprintf('%d-01-01',aYear);
                    partitionMaxDate = sprintf('%d-01-01',aYear+1);
                    tokenizedQuery = self.sqlQueryWithName("create_along_track_table_partition.sql");
                    query = regexprep(tokenizedQuery,"{table_name}",self.alongTrackTableName);
                    query = regexprep(query,"{partition_name}",partitionName);
                    query = regexprep(query,"{min_partition_date}",partitionMinDate);
                    query = regexprep(query,"{max_partition_date}",partitionMaxDate);

                    self.executeQuery(query);
                end
            else
                error('Not yet implemented.');
            end

        end

        %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
        %
        % along_track_metadata table creation/destruction
        %
        %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

        function createAlongTrackMetadataTable(self)
            tokenizedQuery = self.sqlQueryWithName("create_along_track_metadata_table.sql");
            query = regexprep(tokenizedQuery,"{table_name}",self.alongTrackMetadataTableName);

            self.executeQuery(query);

            fprintf(join(["Table " self.alongTrackMetadataTableName " added to database " self.db_name " (if it did not previously exist).\n"]));
        end

        function dropAlongTrackMetadataTable(self)
            self.dropTable(self.alongTrackMetadataTableName);
        end

        function truncateAlongTrackMetadataTable(self)
            self.truncateTable(self.alongTrackMetadataTableName);
        end

        %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
        %
        % OceanBasin table creation/destruction
        %
        %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

        function createBasinTable(self)
            tokenizedQuery = self.sqlQueryWithName("create_basin_table.sql");
            queryCreateTable = regexprep(tokenizedQuery,"{table_name}",self.oceanBasinTableName);

            tokenizedQuery = self.sqlQueryWithName("create_basin_index_geom.sql");
            queryCreateIndex = regexprep(tokenizedQuery,"{table_name}",self.oceanBasinTableName);

            self.executeQuery(queryCreateTable,queryCreateIndex);

            fprintf(join(["Table " self.oceanBasinTableName " added to database " self.db_name " (if it did not previously exist).\n"]));
        end

        function dropBasinTable(self)
            self.dropTable(self.oceanBasinTableName);
        end

        function insertBasinDataFromCSV(self)
            % Note that this works with so few steps because the data in
            % the csv file has header names that exactly match the column
            % names in the table.
            basinData = readtable(self.dataFilePathWithName("ocean_basins.csv"));
            atdb_conn = self.connection();
            sqlwrite(atdb_conn,self.oceanBasinTableName,basinData);
            atdb_conn.commit();
            atdb_conn.close();
        end

        %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
        %
        % OceanBasinConnection table creation/destruction
        %
        %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

        function createOceanBasinConnectionTable(self)
            tokenizedQuery = self.sqlQueryWithName("create_basin_connection_table.sql");
            queryCreateTable = regexprep(tokenizedQuery,"{table_name}",self.oceanBasinConnectionTableName);

            tokenizedQuery = self.sqlQueryWithName("create_basin_connection_index_basin_id.sql");
            queryCreateIndex = regexprep(tokenizedQuery,"{table_name}",self.oceanBasinConnectionTableName);

            self.executeQuery(queryCreateTable,queryCreateIndex);

            fprintf(join(["Table " self.oceanBasinConnectionTableName " added to database " self.db_name " (if it did not previously exist).\n"]));
        end

        function dropOceanBasinConnectionTable(self)
            self.dropTable(self.oceanBasinConnectionTableName);
        end

        function insertOceanBasinConnectionDataFromCSV(self)
            % Note that this works with so few steps because the data in
            % the csv file has header names that exactly match the column
            % names in the table.
            basinConnection = readtable(self.dataFilePathWithName("ocean_basin_connections.csv"));
            basinConnection.Properties.VariableNames(1) = "basin_id";
            basinConnection.Properties.VariableNames(2) = "connected_id";
            atdb_conn = self.connection();
            sqlwrite(atdb_conn,self.oceanBasinConnectionTableName,basinConnection);
            atdb_conn.commit();
            atdb_conn.close();
        end

        %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
        %
        % simple queries
        %
        %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

        function results = geographicPointsInSpatialtemporalWindow(self,latitude,longitude,date,options)
            arguments
                self 
                latitude double
                longitude double
                date datetime
                options.distance double = 500000
                options.time_window duration = seconds(856710)
                options.should_basin_mask = 1
            end
            if options.should_basin_mask == 1
                tokenizedQuery = self.sqlQueryWithName("geographic_points_in_spatialtemporal_window.sql");
            else
                tokenizedQuery = self.sqlQueryWithName("geographic_points_in_spatialtemporal_window_nomask.sql");
            end

            query = regexprep(tokenizedQuery,"%\(latitude\)s",string(latitude));
            query = regexprep(query,"%\(longitude\)s",string(longitude));
            query = regexprep(query,"%\(distance\)s",string(options.distance));
            query = regexprep(query,"%\(central_date_time\)s",sprintf("TIMESTAMP '%s'",string(date,"yyyy-MM-dd HH:mm:ss")));
            query = regexprep(query,"%\(time_delta\)s",sprintf("INTERVAL '%d seconds'",seconds(options.time_window/2)));

            atdb_conn = self.connection();
            results = fetch(atdb_conn,query);
            atdb_conn.close();
        end

        function results = projectedPointsInSpatialtemporalWindow(self,latitude,longitude,date,options)
            arguments
                self
                latitude double
                longitude double
                date datetime
                options.Lx double = 1000e3
                options.Ly double = 500e3
                options.time_window = seconds(856710)
                options.should_basin_mask = 1
            end
            
            [x0,y0, minLat,minLon,maxLat,maxLon] = AlongTrack.latLonBoundsForTransverseMercatorBox(latitude,longitude,options.Lx,options.Ly);

            if options.should_basin_mask == 1
                tokenizedQuery = self.sqlQueryWithName("geographic_points_in_spatialtemporal_projected_window.sql");
            else
                tokenizedQuery = self.sqlQueryWithName("geographic_points_in_spatialtemporal_projected_window_nomask.sql");
            end
            params = containers.Map();
            params('latitude') = latitude;
            params('longitude') = longitude;
            params('xmin') = minLon;
            params('ymin') = minLat;
            params('xmax') = maxLon;
            params('ymax') = maxLat;
            params('ymax') = maxLat;
            params('central_date_time') = date;
            params('time_delta') = options.time_window/2;

            query = self.queryWithParameters(tokenizedQuery,params);

            atdb_conn = self.connection();
            results = fetch(atdb_conn,query);
            if minLon < -180 || maxLon > 180
                
            end
            atdb_conn.close();

            [results.x,results.y] = AlongTrack.LatitudeLongitudeToTransverseMercator(results.latitude,results.longitude,lon0=longitude);
            outOfBounds = results.x < x0-options.Lx/2 | results.x > x0+options.Lx/2 | results.y < y0-options.Ly/2 | results.y > y0+options.Ly/2;
            results(outOfBounds,:) = [];
            results.x = results.x - (x0-options.Lx/2);
            results.y = results.y - (y0-options.Ly/2);


        end
    end

    methods (Static)
        function [x0, y0, minLat,minLon,maxLat,maxLon] = latLonBoundsForTransverseMercatorBox(latitude, longitude, Lx, Ly)
            lat0 = latitude;
            lon0 = longitude;
            [x0,y0] = AlongTrack.LatitudeLongitudeToTransverseMercator(lat0,lon0,lon0=lon0);
            x = zeros(4,1);
            y = zeros(4,1);

            x(1) = x0 - Lx/2;
            y(1) = y0 - Ly/2;

            x(2) = x0 - Lx/2;
            y(2) = y0 + Ly/2;

            x(3) = x0;
            y(3) = y0 + Ly/2;

            x(4) = x0;
            y(4) = y0 - Ly/2;

            x(5) = x0 + Lx/2;
            y(5) = y0 - Ly/2;

            x(6) = x0 + Lx/2;
            y(6) = y0 + Ly/2;

            [lats,lons] = AlongTrack.TransverseMercatorToLatitudeLongitude(x,y,lon0=lon0);
            minLat = min(lats);
            maxLat = max(lats);
            minLon = min(lons);
            maxLon = max(lons);
        end

        % These are SPHERICAL version of the transverse mercator
        % projection. In my tests, at 1000km x 1000km, they produces
        % distances errors of O(1500m) compared to O(500m) with the
        % elliptical geometry. Playing around, I think it is pretty clear
        % that this approximation is perfectly fine. In 2000 km boxes, the
        % error gets worse at the equator, but only 2x more.
        % d = sqrt((results.x-Lx/2).^2 + (results.y-Ly/2).^2);
        % figure, scatter(results.distance,results.distance-d,'filled')
        function [x,y] = LatitudeLongitudeToTransverseMercator(lat, lon, options)
            arguments
                lat (:,1) double {mustBeNumeric,mustBeReal}
                lon (:,1) double {mustBeNumeric,mustBeReal}
                options.lon0 (1,1) double {mustBeNumeric,mustBeReal}
            end
            k0 = 0.9996;
            WGS84a=6378137;
            R = k0*WGS84a;

            phi = lat*pi/180;
            deltaLambda = (lon - options.lon0)*pi/180;
            sinLambdaCosPhi = sin(deltaLambda).*cos(phi);
            x = (R/2)*log((1+sinLambdaCosPhi)./(1-sinLambdaCosPhi));
            y = R*atan(tan(phi)./cos(deltaLambda));
        end

        function [lat,lon] = TransverseMercatorToLatitudeLongitude(x, y, options)
            arguments
                x (:,1) double {mustBeNumeric,mustBeReal}
                y (:,1) double {mustBeNumeric,mustBeReal}
                options.lon0 (1,1) double {mustBeNumeric,mustBeReal}
            end
            k0 = 0.9996;
            WGS84a=6378137;
            R = k0*WGS84a;

            lon = (180/pi)*atan(sinh(x/R).*sec(y/R)) + options.lon0;
            lat = (180/pi)*asin(sech(x/R).*sin(y/R));
        end
    end
end