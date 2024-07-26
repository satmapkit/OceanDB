import netCDF4 as nc
import psycopg as pg
from psycopg import sql
import glob
import struct
from io import BytesIO
import time
from dateutil.relativedelta import relativedelta
import os


class AlongTrackDatabase:
    db_name: str = 'along_track'
    alongTrackTableName: str = 'along_track'
    alongTrackMetadataTableName: str = 'along_track_metadata'
    oceanBasinsTableName: str = 'basins'
    oceanBasinsConnectionsTableName: str = 'basin_connections'

    def __init__(self, host, username, password, port=5432, db_name='along_track'):
        self.host = host
        self.username = username
        self.password = password
        self.port = port
        self.db_name = db_name
        self.__partitions_created = []

    def connect_string(self):
        conn_str = f'''
            host={self.host} 
            dbname={self.db_name}
            port={self.port} 
            user={self.username} 
            password={self.password}'''
        return conn_str

    def create_database(self):
        with pg.connect(f'host={self.host} port={self.port} user={self.username} password={self.password}') as pg_conn:
            pg_conn.autocommit = True  # Enable autocommit to execute CREATE DATABASE command

            # SQL module usage: https://www.psycopg.org/docs/sql.html#module-psycopg2.sql
            with pg_conn.cursor() as cur:
                cur.execute(sql.SQL("SELECT 1 FROM pg_database WHERE datname = %s"), (self.db_name,))
                exists = cur.fetchone()
                if exists is not None:
                    print(f"Database '{self.db_name}' already exists.")
                else:
                    # Create the new database
                    cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(self.db_name)))

                    # Now create a connection to the new db, and add postgis (autocommit set to true!)
                    with pg.connect(self.connect_string()) as atdb_conn:
                        with atdb_conn.cursor() as atdb_cur:
                            atdb_cur.execute(sql.SQL("CREATE EXTENSION IF NOT EXISTS plpgsql;"))
                            atdb_cur.execute(sql.SQL("CREATE EXTENSION IF NOT EXISTS postgis;"))
                            atdb_cur.execute(sql.SQL("CREATE EXTENSION IF NOT EXISTS btree_gist;"))
                            atdb_conn.commit()
                    print(f"Database '{self.db_name}' created successfully.")

    def drop_database(self):
        with pg.connect(f'host={self.host} port={self.port} user={self.username} password={self.password}') as conn:
            conn.autocommit = True  # Enable autocommit to execute CREATE DATABASE command
            # Create a cursor object
            with conn.cursor() as cur:
                # Create the new database
                cur.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(self.db_name)))

        print(f"Database '{self.db_name}' dropped.")

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
        print(f"Table '{self.alongTrackTableName} added to database (if it did not previously exist) '{self.db_name}'.")

    def drop_along_track_table(self):
        query_drop_table = sql.SQL("""
                    DROP TABLE IF EXISTS public.{table_name}
                    """).format(
            table_name=sql.Identifier(self.alongTrackTableName)
        )

        with pg.connect(self.connect_string()) as conn:
            with conn.cursor() as cur:
                cur.execute(query_drop_table)
                conn.commit()

        print(f"Table '{self.alongTrackTableName} dropped from database.'{self.db_name}'.")

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

        query_create_combined_point_date_index = sql.SQL("""
            CREATE INDEX IF NOT EXISTS cat_pt_date_idx
            ON public.{table_name} USING gist
            (cat_point, (date_time::date))
            WITH (buffering=auto);
            """).format(
            table_name=sql.Identifier(self.alongTrackTableName)
        )

        with pg.connect(self.connect_string()) as conn:
            with conn.cursor() as cur:
                cur.execute(query_create_point_index)
                cur.execute(query_create_date_index)
                cur.execute(query_create_filename_index)
                conn.commit()

    def drop_along_track_indices(self):
        query_drop_point_index = sql.SQL("""
            DROP INDEX IF EXISTS cat_pt_idx
            ON public.{table_name}
            """).format(
            table_name=sql.Identifier(self.alongTrackTableName)
        )

        query_drop_date_index = sql.SQL("""
            DROP INDEX IF EXISTS date_idx
            ON public.{table_name}
        """).format(
            table_name=sql.Identifier(self.alongTrackTableName)
        )

        query_drop_filename_index = sql.SQL("""
            DROP INDEX IF EXISTS nme_alng_idx
            ON public.{table_name}
        """).format(
            table_name=sql.Identifier(self.alongTrackTableName)
        )

        query_drop_combined_point_date_index = sql.SQL("""
            DROP INDEX IF EXISTS cat_pt_date_idx
            ON public.{table_name}
            """).format(
            table_name=sql.Identifier(self.alongTrackTableName)
        )

        with pg.connect(self.connect_string()) as conn:
            with conn.cursor() as cur:
                cur.execute(query_drop_point_index)
                cur.execute(query_drop_date_index)
                cur.execute(query_drop_filename_index)
                conn.commit()

    def create_along_track_metadata_table(self):
        create_table_query = sql.SQL("""
        CREATE TABLE IF NOT EXISTS public.{table_name} (
            nme text NOT NULL,
            conventions text NULL,
            metadata_conventions text NULL,
            cdm_data_type text NULL,
            comment text NULL,
            contact text NULL,
            creator_email text NULL,
            creator_name text NULL,
            creator_url text NULL,
            date_created timestamp with time zone NULL,
            date_issued timestamp with time zone NULL,
            date_modified timestamp with time zone NULL,
            history text NULL,
            institution text NULL,
            keywords text NULL,
            license text NULL,
            platform text NULL,
            processing_level text NULL,
            product_version text NULL,
            project text NULL,
            "references" text NULL,
            software_version text NULL,
            source text NULL,
            ssalto_duacs_comment text NULL,
            summary text NULL,
            title text NULL,
            CONSTRAINT cop_meta_pkey PRIMARY KEY (nme)
          );
          """).format(table_name=sql.Identifier(self.alongTrackMetadataTableName))

        with pg.connect(self.connect_string()) as conn:
            with conn.cursor() as cur:
                cur.execute(create_table_query)
                conn.commit()
        print(f"Table {self.alongTrackMetadataTableName} added to database {self.db_name} (if it did not previously exist).")

    def drop_along_track_metadata_table(self):
        query_drop_table = sql.SQL("""
                    DROP TABLE IF EXISTS public.{table_name}
                    """).format(
            table_name=sql.Identifier(self.alongTrackMetadataTableName)
        )

        with pg.connect(self.connect_string()) as conn:
            with conn.cursor() as cur:
                cur.execute(query_drop_table)
                conn.commit()

        print(f"Table {self.alongTrackMetadataTableName} dropped from database {self.db_name}.")

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
            table_name=sql.Identifier(self.oceanBasinsTableName)
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

        print(f"Table '{self.oceanBasinsTableName}' added to database (if they did not previously exist) '{self.db_name}'.")

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

        print(f"Table '{self.oceanBasinsConnectionsTableName}' added to database '{self.db_name}'.")

    def insert_ocean_basins_from_csv(self, directory):
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
                for filename in glob.glob(f'{directory}/ocean_basins.csv'):
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

    def insert_ocean_basin_connections_from_csv(self, directory):
        with pg.connect(self.connect_string()) as conn:
            query = sql.SQL("COPY {table} ({fields}) FROM STDIN WITH (FORMAT CSV, HEADER)").format(
                fields=sql.SQL(',').join([
                    sql.Identifier('basin_id'),
                    sql.Identifier('connected_id'),
                ]),
                table=sql.Identifier(self.oceanBasinsConnectionsTableName),
            )
            try:
                for filename in glob.glob(f'{directory}/basin_connections_for_load.csv'):
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

    def import_metadata_to_psql(self, ds, fname):
        query = sql.SQL(
            "INSERT INTO {table} ({fields}) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s,%s,%s,%s,%s,%s,%s, %s,%s,%s,%s,%s,%s,%s,%s)").format(
            fields=sql.SQL(',').join([
                sql.Identifier('nme'),
                sql.Identifier('conventions'),
                sql.Identifier('metadata_conventions'),
                sql.Identifier('cdm_data_type'),
                sql.Identifier('comment'),
                sql.Identifier('contact'),
                sql.Identifier('creator_email'),
                sql.Identifier('creator_name'),
                sql.Identifier('creator_url'),
                sql.Identifier('date_created'),
                sql.Identifier('date_issued'),
                sql.Identifier('date_modified'),
                sql.Identifier('history'),
                sql.Identifier('institution'),
                sql.Identifier('keywords'),
                sql.Identifier('license'),
                sql.Identifier('platform'),
                sql.Identifier('processing_level'),
                sql.Identifier('product_version'),
                sql.Identifier('project'),
                sql.Identifier('references'),
                sql.Identifier('software_version'),
                sql.Identifier('source'),
                sql.Identifier('ssalto_duacs_comment'),
                sql.Identifier('summary'),
                sql.Identifier('title'),
            ]),
            table=sql.Identifier(self.alongTrackMetadataTableName),
        )
        with pg.connect(self.connect_string()) as conn:
            with conn.cursor() as cur:
                data = (fname, ds.Conventions, ds.Metadata_Conventions, ds.cdm_data_type, ds.comment, ds.contact,
                        ds.creator_email,
                        ds.creator_name, ds.creator_url, ds.date_created, ds.date_issued, ds.date_modified, ds.history,
                        ds.institution,
                        ds.keywords, ds.license, ds.platform, ds.processing_level, ds.product_version, ds.project,
                        ds.references,
                        ds.software_version, ds.source, ds.ssalto_duacs_comment, ds.summary, ds.title)

                cur.execute(query, data)
                conn.commit()

    def extract_data_from_netcdf(self, file_path, fname):
        # Open the NetCDF file
        try:
            ds = nc.Dataset(file_path, 'r')

            self.import_metadata_to_psql(ds, fname)

            # Don't scale most variables, so they are stored more efficiently in db
            ds.variables['sla_unfiltered'].set_auto_maskandscale(False)
            ds.variables['sla_filtered'].set_auto_maskandscale(False)
            ds.variables['ocean_tide'].set_auto_maskandscale(False)
            ds.variables['internal_tide'].set_auto_maskandscale(False)
            ds.variables['lwe'].set_auto_maskandscale(False)
            ds.variables['mdt'].set_auto_maskandscale(False)
            ds.variables['dac'].set_auto_maskandscale(False)
            ds.variables['tpa_correction'].set_auto_maskandscale(False)

            time_data = ds.variables['time']  # Extract dates from the dataset and convert them to standard datetime
            time_min = time_data[:].min()
            time_max = time_data[:].max()
            time_min = nc.num2date(time_min, time_data.units, only_use_cftime_datetimes=False,
                                   only_use_python_datetimes=False)
            time_max = nc.num2date(time_max, time_data.units, only_use_cftime_datetimes=False,
                                   only_use_python_datetimes=False)

            self.create_partitions(time_min, time_max,
                                   1)  # If necessary, create partitions now in preparation for data upload. 1 for monthly partitions, 12 for yearly partitions

            time_data = nc.num2date(time_data[:], time_data.units, only_use_cftime_datetimes=False,
                                    only_use_python_datetimes=False)
            time_data = nc.date2num(time_data[:],
                                    "microseconds since 2000-01-01 00:00:00")  # Convert the standard date back to the 8-byte integer PSQL uses
            lat_data = ds.variables['latitude'][:]
            lon_data = ds.variables['longitude'][:]
            cycle_data = ds.variables['cycle'][:]
            track_data = ds.variables['track'][:]
            sla_un_data = ds.variables['sla_unfiltered'][:]
            sla_f_data = ds.variables['sla_filtered'][:]
            dac_data = ds.variables['dac'][:]
            o_tide_data = ds.variables['ocean_tide'][:]
            i_tide_data = ds.variables['internal_tide'][:]
            lwe_data = ds.variables['lwe'][:]
            mdt_data = ds.variables['mdt'][:]
            tpa_corr_data = ds.variables['tpa_correction'][:]

            ds.close()

            return time_data, lat_data, lon_data, cycle_data, track_data, sla_un_data, sla_f_data, dac_data, o_tide_data, i_tide_data, lwe_data, mdt_data, tpa_corr_data
        except FileNotFoundError:
            print("File '{}' not found".format(file_path))

    # Define the function to import data into PostgreSQL using copy_from in binary format
    def import_data_to_postgresql(self, fname, time_data, lat_data, lon_data, cycle_data, track_data, sla_un_data,
                                  sla_f_data,
                                  dac_data, o_tide_data, i_tide_data, lwe_data, mdt_data, tpa_corr_data):
        with pg.connect(self.connect_string()) as conn:
            with conn.cursor() as cur:
                output = BytesIO()
                output.write(
                    struct.pack('!11sii', b'PGCOPY\n\377\r\n\0', 0, 0))  # Write the PostgreSQL COPY binary header
                fname_binary = str.encode(fname)
                str_len = len(fname_binary)

                # See the Struct docs for explanations of the letter codes used to pack the data: https://docs.python.org/3/library/struct.html
                for i in range(len(time_data)):
                    output.write(struct.pack('!h', 14))  # Number of fields
                    output.write(struct.pack('!i' + str(str_len) + 's', str_len, fname_binary))
                    output.write(struct.pack('!ih', 2, track_data[i]))
                    output.write(struct.pack('!ih', 2, cycle_data[i]))
                    output.write(struct.pack('!id', 8, lat_data[i]))
                    output.write(struct.pack('!id', 8, lon_data[i]))
                    output.write(struct.pack('!ih', 2, sla_un_data[i]))
                    output.write(struct.pack('!ih', 2, sla_f_data[i]))
                    output.write(struct.pack('!iq', 8, time_data[i]))  # PSQL stores time as an 8-byte integer
                    output.write(struct.pack('!ih', 2, dac_data[i]))
                    output.write(struct.pack('!ih', 2, o_tide_data[i]))
                    output.write(struct.pack('!ih', 2, i_tide_data[i]))
                    output.write(struct.pack('!ih', 2, lwe_data[i]))
                    output.write(struct.pack('!ih', 2, mdt_data[i]))
                    output.write(struct.pack('!ih', 2, tpa_corr_data))

                output.write(struct.pack('!h', -1))  # Write the COPY binary trailer
                output.seek(0)
                # Connect to the PostgreSQL database
                copy_query = sql.SQL(
                    "COPY {along_tbl_nme} ( nme, track, cycle, lat, lon, sla_unfiltered, sla_filtered, date_time, dac, ocean_tide, internal_tide, lwe, mdt, tpa_correction) FROM STDIN WITH (FORMAT binary);").format(
                    along_tbl_nme=sql.Identifier(self.alongTrackTableName))
                with cur.copy(copy_query) as copy:
                    while data := output.read(100):
                        copy.write(data)
                conn.commit()

    # End import_data_to_postgresql

    def create_partitions(self, min_date, max_date, partition_duration=12):
        if partition_duration != 12 and partition_duration != 1:
            raise ValueError('Partitions must be either yearly (12) or monthly(1)')
        dates = [min_date, max_date]
        with pg.connect(self.connect_string()) as conn:
            with conn.cursor() as cur:
                for date in dates:
                    year = date.year
                    month = 1
                    month_str = '01'
                    month_partition_str = ''
                    this_year = date.replace(day=1, month=1, hour=0, minute=0, second=0, microsecond=0)
                    next_year = date + relativedelta(years=1)
                    next_year = next_year.replace(day=1, month=1, hour=0, minute=0, second=0, microsecond=0)
                    this_month = date.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
                    next_month = date + relativedelta(months=1)
                    next_month = next_month.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
                    to_partition = next_year
                    min_partition = this_year

                    if partition_duration == 1:
                        month = date.month
                        month_str = f"{month}"
                        if month <= 9:
                            month_str = f"{0}{month}"
                        month_partition_str = month_str
                        to_partition = next_month
                        min_partition = this_month

                    partition_name = f"{self.alongTrackTableName}_{str(year)}{month_partition_str}"  # Monthly partions are named after the table name eg: along_track_YYYYMM and yearly are named along_track_YYYY
                    min_partition_date = f"{min_partition}"
                    to_partition_date = f"{to_partition}"

                    query = sql.SQL(
                        "CREATE TABLE IF NOT EXISTS {partition_nm} PARTITION OF {table_name} FOR VALUES FROM ('{min_partition_date}') TO ('{to_partition}');").format(
                        table_name=sql.Identifier(self.alongTrackTableName),
                        partition_nm=sql.Identifier(partition_name),
                        min_partition_date=sql.Identifier(min_partition_date),
                        to_partition=sql.Identifier(to_partition_date),
                    )
                    # 		print(query)
                    if partition_name not in self.__partitions_created:
                        try:
                            cur.execute(query)
                        except Exception as err:
                            print(f"Unable to create partition: {err}")
                        else:
                            self.__partitions_created.append(partition_name)
                            conn.commit()

    def insert_along_track_data_from_netcdf(self, directory):
        start = time.time()
        for file_path in glob.glob(directory + '/*.nc'):
            names = [os.path.basename(x) for x in glob.glob(file_path)]
            fname = names[0]  # filename will be used to link data to metadata
            time_data, lat_data, lon_data, cycle_data, track_data, sla_un_data, sla_f_data, dac_data, o_tide_data, i_tide_data, lwe_data, mdt_data, tpa_corr_data = self.extract_data_from_netcdf(
                file_path, fname)
            import_start = time.time()
            self.import_data_to_postgresql(fname, time_data, lat_data, lon_data, cycle_data, track_data, sla_un_data,
                                           sla_f_data, dac_data, o_tide_data, i_tide_data, lwe_data, mdt_data,
                                           tpa_corr_data)
            import_end = time.time()
            print(f"{fname} import time: {import_end - import_start}")
        end = time.time()
        print(f"Script end. Total time: {end - start}")
