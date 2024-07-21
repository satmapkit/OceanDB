import netCDF4 as nc
import psycopg as pg
from psycopg import sql
import glob

class AlongTrackDatabase:
    dbName: str = 'along_track'
    alongTrackTableName: str = 'along_track'
    oceanBasinsTableName: str = 'basins'
    oceanBasinsConnectionsTableName: str = 'basin_connections'

    def __init__(self, host, username, password, port=5432, directory=''):
        self.host = host
        self.username = username
        self.password = password
        self.port = port
        self.directory = directory
    def connect_string(self):
        conn_str = f'''
            host={self.host} 
            dbname={self.dbName}
            port=5432 
            user={self.username} 
            password={self.password}'''
        return conn_str

    def create_database(self):
        with pg.connect(f'host={self.host} port=5432 user={self.username} password={self.password}')  as pg_conn:
            pg_conn.autocommit = True  # Enable autocommit to execute CREATE DATABASE command

            # SQL module usage: https://www.psycopg.org/docs/sql.html#module-psycopg2.sql
            with pg_conn.cursor() as cur:
                cur.execute(sql.SQL("SELECT 1 FROM pg_database WHERE datname = %s"), (self.dbName,))
                exists = cur.fetchone()
                if exists is not None:
                    print(f"Database '{self.dbName}' already exists.")
                else:
                    # Create the new database
                    cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(self.dbName)))

                    # Now create a connection to the new db, and add postgis (autocommit set to true!)
                    with pg.connect(self.connect_string()) as atdb_conn:
                        with atdb_conn.cursor() as atdb_cur:
                            atdb_cur.execute(sql.SQL("CREATE EXTENSION IF NOT EXISTS plpgsql;"))
                            atdb_cur.execute(sql.SQL("CREATE EXTENSION IF NOT EXISTS postgis;"))
                            atdb_cur.execute(sql.SQL("CREATE EXTENSION IF NOT EXISTS btree_gist;"))
                            atdb_conn.commit()
                    print(f"Database '{self.dbName}' created successfully.")



    def drop_database(self):
        with pg.connect(self.connect_string()) as conn:
            conn.autocommit = True  # Enable autocommit to execute CREATE DATABASE command
            # Create a cursor object
            with conn.cursor() as cur:
                # Create the new database
                cur.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(self.dbName)))

        print(f"Database '{self.dbName}' dropped.")

    def create_along_track_table(self):
        query = sql.SQL("""
        CREATE TABLE IF NOT EXISTS public.{table_name}
        (
            idx bigint NOT NULL GENERATED ALWAYS AS IDENTITY ( INCREMENT 1 START 1 MINVALUE 1 MAXVALUE 9223372036854775807 CACHE 1 ),
            nme text COLLATE pg_catalog."default",
            track smallint,
            cycle smallint,
            lat double precision,
            lon double precision,
            sla_unfiltered smallint,
            sla_filtered smallint,
            date_time timestamp without time zone,
            dac smallint,
            ocean_tide smallint,
            internal_tide smallint,
            lwe smallint,
            mdt smallint,
            tpa_correction smallint,
            cat_point geometry(Point,4326) GENERATED ALWAYS AS (st_setsrid(st_makepoint(lon, lat), 4326)) STORED,
            CONSTRAINT cop_along_pkey PRIMARY KEY (date_time, idx)
        ) PARTITION BY RANGE (date_time)
        """).format(table_name=sql.Identifier(self.alongTrackTableName))

        with pg.connect(self.connect_string()) as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                conn.commit()
        print(f"Table '{self.alongTrackTableName} added to database (if it did not previously exist) '{self.dbName}'.")

    def create_along_track_indices(self):
        query_create_point_index = sql.SQL("""
            CREATE INDEX IF NOT EXISTS cat_pt_idx
 	        ON public.{table_name} USING gist
 	        (cat_point)
 	        WITH (buffering=auto);
        """).format(
            table_name=sql.Identifier(self.alongTrackTableName)
        )

        query_create_date_index = sql.SQL("""
            CREATE INDEX IF NOT EXISTS date_idx
            ON public.{table_name} USING btree
        	((date_time::date) ASC NULLS LAST)
         	WITH (deduplicate_items=True);
        """).format(
            table_name=sql.Identifier(self.alongTrackTableName)
        )

        query_create_filename_index = sql.SQL("""
            CREATE INDEX IF NOT EXISTS nme_alng_idx
         	ON public.{table_name} USING btree
         	(nme COLLATE pg_catalog."default" ASC NULLS LAST)
         	WITH (deduplicate_items=True);
        """).format(
            table_name=sql.Identifier(self.alongTrackTableName)
        )

        # Brian says this may require some additional initialization.
        query_create_combined_point_date_index = sql.SQL("""
            CREATE INDEX IF NOT EXISTS cat_pt_date_idx
         	ON public.{table_name} USING gist
         	(cat_point, (date_time::date))
         	WITH (buffering=auto);"""
        ).format(
            table_name=sql.Identifier(self.alongTrackTableName)
        )


        with pg.connect(self.connect_string()) as conn:
            with conn.cursor() as cur:
                cur.execute(query_create_point_index)
                cur.execute(query_create_date_index)
                cur.execute(query_create_filename_index)
                conn.commit()

    def create_ocean_basin_tables(self):
        query_create_ocean_basins_table = sql.SQL("""
            CREATE TABLE IF NOT EXISTS public.{table_name}
            (
                id SERIAL,
                geom geometry(MultiPolygon,4326),
                feature_id integer,
                name character varying(28) COLLATE pg_catalog."default",
                wikidataid character varying(8) COLLATE pg_catalog."default",
                ne_id bigint,
                area double precision,
                CONSTRAINT basins_pkey PRIMARY KEY (id)
            )"""
        ).format(
            table_name = sql.Identifier(self.oceanBasinsTableName)
        )

        query_create_ocean_basins_index = sql.SQL("""
            CREATE INDEX IF NOT EXISTS sidx_basins_geom
            ON public.{table_name} USING gist
            (geom)
            TABLESPACE pg_default;
        """).format(
            table_name=sql.Identifier(self.oceanBasinsTableName)
        )


        with pg.connect(self.connect_string()) as conn:
            with conn.cursor() as cur:
                cur.execute(query_create_ocean_basins_table)
                cur.execute(query_create_ocean_basins_index)
                conn.commit()

        print(f"Table '{self.oceanBasinsTableName}' added to database (if they did not previously exist) '{self.dbName}'.")

    def create_ocean_basin_connection_tables(self):
        query_create_ocean_basins_connections_table = sql.SQL("""
            CREATE TABLE IF NOT EXISTS public.{table_name}
            (
                pid smallint NOT NULL GENERATED ALWAYS AS IDENTITY ( INCREMENT 1 START 1 MINVALUE 1 MAXVALUE 32767 CACHE 1 ),
                connected_id smallint NOT NULL,
                basin_id smallint NOT NULL,
                CONSTRAINT basin_connections_pkey PRIMARY KEY (pid)
            )"""
        ).format(
            table_name=sql.Identifier(self.oceanBasinsConnectionsTableName)
        )

        query_create_ocean_basins_connections_index = sql.SQL("""
           CREATE INDEX IF NOT EXISTS basin_id_idx
            ON public.{table_name} USING btree
            (basin_id ASC NULLS LAST, connected_id ASC NULLS LAST)
            WITH (deduplicate_items=True)
            TABLESPACE pg_default;
        """).format(
            table_name=sql.Identifier(self.oceanBasinsConnectionsTableName)
        )


        with pg.connect(self.connect_string()) as conn:
            with conn.cursor() as cur:
                cur.execute(query_create_ocean_basins_connections_table)
                cur.execute(query_create_ocean_basins_connections_index)
                conn.commit()

        print(f"Table '{self.oceanBasinsConnectionsTableName}' added to database '{self.dbName}'.")


    def insert_ocean_basins_from_csv(self):
        with pg.connect(self.connect_string()) as conn:
            query = sql.SQL("COPY {table} ({fields}) FROM STDIN WITH (FORMAT CSV, HEADER)").format(
                fields=sql.SQL(',').join([
                    sql.Identifier('id'),
                    sql.Identifier('geom'),
                    sql.Identifier('feature_id'),
                    sql.Identifier('name'),
                    sql.Identifier('wikidataid'),
                    sql.Identifier('ne_id'),
                    sql.Identifier('area'),
                ]),
                table=sql.Identifier(self.oceanBasinsTableName),
            )
            try:
                for filename in glob.glob(f'{self.directory}/ocean_basins.csv'):
                    # for filename in glob.glob(loadfile):
                    with conn.cursor() as cur:
                        with open(filename, 'r') as f:
                            # f = f.read()
                            with cur.copy(query) as copy:
                                while data := f.read(100):
                                    copy.write(data)
                        conn.commit()
            except FileNotFoundError:
                print("File 'ocean_basins.csv' not found")
            else:
                print('Ocean basins loaded')

    def insert_ocean_basin_connections_from_csv(self):
        with pg.connect(self.connect_string()) as conn:
            query = sql.SQL("COPY {table} ({fields}) FROM STDIN WITH (FORMAT CSV, HEADER)").format(
                fields=sql.SQL(',').join([
                    sql.Identifier('basin_id'),
                    sql.Identifier('connected_id'),
                ]),
                table=sql.Identifier(self.oceanBasinsConnectionsTableName),
            )
            try:
                for filename in glob.glob(f'{self.directory}/basin_connections_for_load.csv'):
                    with conn.cursor() as cur:
                        with open(filename, 'r') as f:
                            with cur.copy(query) as copy:
                                while data := f.read(100):
                                    copy.write(data)
                        conn.commit()
            except FileNotFoundError:
                print("File 'basin_connections_for_load.csv' not found")
            else:
                print('Ocean basin connections loaded')
