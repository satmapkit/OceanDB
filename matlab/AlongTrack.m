classdef AlongTrack < handle
    %UNTITLED Summary of this class goes here
    %   Detailed explanation goes here

    properties
        host
        username
        password
        port
        partitions_created = []

        db_name
    end

    properties (Constant)
        alongTrackTableName = 'along_track'
        alongTrackMetadataTableName = 'along_track_metadata'
        oceanBasinTableName = 'basins'
        oceanBasinConnectionTableName = 'basin_connections'
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
                options.password = ''
                options.db_name = 'along_track'
                options.host = 'localhost'
                options.port = 5432
            end
            self.username = options.username;
            self.password = options.password;
            self.db_name = options.db_name;
            self.port = options.port;
            self.host = options.host;
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
            [filepath,~,~] = fileparts(mfilename('fullpath'));
            path = fullfile(filepath,'..', 'sql',name);
            query = join(readlines(path));
        end

        function path = dataFilePathWithName(self,name)
            [filepath,~,~] = fileparts(mfilename('fullpath'));
            path = fullfile(filepath,'..', 'data',name);
        end

        %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
        %
        % AlongTrack table creation/destruction
        %
        %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

        function createAlongTrackTable(self)
            tokenizedQuery = self.sqlQueryWithName("createAlongTrackTable.sql");
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
            tokenizedQuery = self.sqlQueryWithName("createAlongTrackIndexPoint.sql");
            queryPointIndex = regexprep(tokenizedQuery,"{table_name}",self.alongTrackTableName);

            tokenizedQuery = self.sqlQueryWithName("createAlongTrackIndexDate.sql");
            queryDateIndex = regexprep(tokenizedQuery,"{table_name}",self.alongTrackTableName);

            tokenizedQuery = self.sqlQueryWithName("createAlongTrackIndexPointDate.sql");
            queryPointDateIndex = regexprep(tokenizedQuery,"{table_name}",self.alongTrackTableName);

            tokenizedQuery = self.sqlQueryWithName("createAlongTrackIndexFilename.sql");
            queryFilenameIndex = regexprep(tokenizedQuery,"{table_name}",self.alongTrackTableName);

            self.executeQuery(queryPointIndex,queryDateIndex,queryFilenameIndex);

            fprintf(join(["Indices added to table " self.alongTrackTableName " in database " self.db_name ".\n"]));
        end

        function dropAlongTrackIndices(self)
            tokenizedQuery = self.sqlQueryWithName("dropAlongTrackIndexPoint.sql");
            queryPointIndex = regexprep(tokenizedQuery,"{table_name}",self.alongTrackTableName);

            tokenizedQuery = self.sqlQueryWithName("dropAlongTrackIndexDate.sql");
            queryDateIndex = regexprep(tokenizedQuery,"{table_name}",self.alongTrackTableName);

            tokenizedQuery = self.sqlQueryWithName("dropAlongTrackIndexPointDate.sql");
            queryPointDateIndex = regexprep(tokenizedQuery,"{table_name}",self.alongTrackTableName);

            tokenizedQuery = self.sqlQueryWithName("dropAlongTrackIndexFilename.sql");
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
                    tokenizedQuery = self.sqlQueryWithName("createAlongTrackTablePartition.sql");
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
        % AlongTrackMetadata table creation/destruction
        %
        %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

        function createAlongTrackMetadataTable(self)
            tokenizedQuery = self.sqlQueryWithName("createAlongTrackMetadataTable.sql");
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

        function createOceanBasinTable(self)
            tokenizedQuery = self.sqlQueryWithName("createOceanBasinTable.sql");
            queryCreateTable = regexprep(tokenizedQuery,"{table_name}",self.oceanBasinTableName);

            tokenizedQuery = self.sqlQueryWithName("createOceanBasinIndexGeom.sql");
            queryCreateIndex = regexprep(tokenizedQuery,"{table_name}",self.oceanBasinTableName);

            self.executeQuery(queryCreateTable,queryCreateIndex);

            fprintf(join(["Table " self.oceanBasinTableName " added to database " self.db_name " (if it did not previously exist).\n"]));
        end

        function dropOceanBasinTable(self)
            self.dropTable(self.oceanBasinTableName);
        end

        function insertOceanBasinDataFromCSV(self)
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
            tokenizedQuery = self.sqlQueryWithName("createOceanBasinConnectionTable.sql");
            queryCreateTable = regexprep(tokenizedQuery,"{table_name}",self.oceanBasinConnectionTableName);

            tokenizedQuery = self.sqlQueryWithName("createOceanBasinConnectionIndexBasinID.sql");
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

    end
end