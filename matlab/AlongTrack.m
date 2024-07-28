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

        %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
        %
        % AlongTrack table creation/destruction
        %
        %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

        function createAlongTrackTable(self)
            tokenizedQuery = join(readlines("../sql/createAlongTrackTable.sql"));
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
            tokenizedQuery = join(readlines("../sql/createAlongTrackIndexPoint.sql"));
            queryPointIndex = regexprep(tokenizedQuery,"{table_name}",self.alongTrackTableName);

            tokenizedQuery = join(readlines("../sql/createAlongTrackIndexDate.sql"));
            queryDateIndex = regexprep(tokenizedQuery,"{table_name}",self.alongTrackTableName);

            tokenizedQuery = join(readlines("../sql/createAlongTrackIndexPointDate.sql"));
            queryPointDateIndex = regexprep(tokenizedQuery,"{table_name}",self.alongTrackTableName);

            tokenizedQuery = join(readlines("../sql/createAlongTrackIndexFilename.sql"));
            queryFilenameIndex = regexprep(tokenizedQuery,"{table_name}",self.alongTrackTableName);

            self.executeQuery(queryPointIndex,queryDateIndex,queryFilenameIndex);

            fprintf(join(["Indices added to table " self.alongTrackTableName " in database " self.db_name ".\n"]));
        end

        function dropAlongTrackIndices(self)
            tokenizedQuery = join(readlines("../sql/dropAlongTrackIndexPoint.sql"));
            queryPointIndex = regexprep(tokenizedQuery,"{table_name}",self.alongTrackTableName);

            tokenizedQuery = join(readlines("../sql/dropAlongTrackIndexDate.sql"));
            queryDateIndex = regexprep(tokenizedQuery,"{table_name}",self.alongTrackTableName);

            tokenizedQuery = join(readlines("../sql/dropAlongTrackIndexPointDate.sql"));
            queryPointDateIndex = regexprep(tokenizedQuery,"{table_name}",self.alongTrackTableName);

            tokenizedQuery = join(readlines("../sql/dropAlongTrackIndexFilename.sql"));
            queryFilenameIndex = regexprep(tokenizedQuery,"{table_name}",self.alongTrackTableName);

            self.executeQuery(queryPointIndex,queryDateIndex,queryFilenameIndex);

            fprintf(join(["Indices dropped from table " self.alongTrackTableName " in database " self.db_name ".\n"]));
        end

        %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
        %
        % AlongTrackMetadata table creation/destruction
        %
        %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

        function createAlongTrackMetadataTable(self)
            tokenizedQuery = join(readlines("../sql/createAlongTrackMetadataTable.sql"));
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
            tokenizedQuery = join(readlines("../sql/createOceanBasinTable.sql"));
            queryCreateTable = regexprep(tokenizedQuery,"{table_name}",self.oceanBasinTableName);

            tokenizedQuery = join(readlines("../sql/createOceanBasinIndexGeom.sql"));
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
            basinData = readtable("../data/ocean_basins.csv");
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
            tokenizedQuery = join(readlines("../sql/createOceanBasinConnectionTable.sql"));
            queryCreateTable = regexprep(tokenizedQuery,"{table_name}",self.oceanBasinConnectionTableName);

            tokenizedQuery = join(readlines("../sql/createOceanBasinConnectionIndexBasinID.sql"));
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
            basinConnection = readtable("../data/basin_connections_for_load.csv");
            basinConnection.Properties.VariableNames(1) = "basin_id";
            basinConnection.Properties.VariableNames(2) = "connected_id";
            atdb_conn = self.connection();
            sqlwrite(atdb_conn,self.oceanBasinConnectionTableName,basinConnection);
            atdb_conn.commit();
            atdb_conn.close();
        end

    end
end