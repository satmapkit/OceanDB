import netCDF4 as nc
import pandas as pd
import psycopg as pg
from psycopg import sql
import glob
import struct
from io import BytesIO
import time
from dateutil.relativedelta import relativedelta
from datetime import timedelta
import os
import yaml
import numpy as np


class AlongTrack:
    db_name: str
    along_track_table_name: str = 'along_track'
    along_track_metadata_table_name: str = 'along_track_metadata'
    ocean_basin_table_name: str = 'basin'
    ocean_basins_connections_table_name: str = 'basin_connection'

    ######################################################
    #
    # Initialization
    #
    ######################################################

    def __init__(self, host="", username="", password="", port=5432, db_name='ocean', nc_files_path=""):
        with open(os.path.join(os.path.dirname(__file__), '../config.yaml'), 'r') as param_file:
            along_params = yaml.full_load(param_file)
            if 'db_connect' in along_params:
                if 'host' in along_params['db_connect']:
                    self.host = along_params['db_connect']['host']
                if 'username' in along_params['db_connect']:
                    self.username = along_params['db_connect']['username']
                if 'password' in along_params['db_connect']:
                    self.password = along_params['db_connect']['password']
                if 'port' in along_params['db_connect']:
                    self.port = along_params['db_connect']['port']
                if 'db_name' in along_params['db_connect']:
                    self.db_name = along_params['db_connect']['db_name']
            if 'copernicus_marine' in along_params:
                if 'nc_files_path' in along_params['copernicus_marine']:
                    self.nc_files_path = along_params['copernicus_marine']['nc_files_path']


        # locally defined variable override the settings in the config file
        if host:
            self.host = host
        if username:
            self.username = username
        if password:
            self.password = password
        if port:
            self.port = port
        if db_name:
            self.db_name = db_name
        if nc_files_path:
            self.nc_files_path = nc_files_path

        self.__partitions_created = []

    ######################################################
    #
    # Database creation/destruction
    #
    ######################################################

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
            with conn.cursor() as cur:
                cur.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(self.db_name)))

        print(f"Database '{self.db_name}' dropped.")

    ######################################################
    #
    # Generic database actions
    #
    ######################################################

    def connect_string(self):
        conn_str = f'''
            host={self.host} 
            dbname={self.db_name}
            port={self.port} 
            user={self.username} 
            password={self.password}'''
        return conn_str

    def drop_table(self, name):
        query_drop_table = sql.SQL("""DROP TABLE IF EXISTS public.{table_name}""").format(
            table_name=sql.Identifier(name)
        )

        with pg.connect(self.connect_string()) as conn:
            with conn.cursor() as cur:
                cur.execute(query_drop_table)
                conn.commit()

        print(f"Table '{name} dropped from database.'{self.db_name}'.")

    def truncate_table(self, name):
        query_truncate_table = sql.SQL("""TRUNCATE public.{table_name}""").format(
            table_name=sql.Identifier(name)
        )

        with pg.connect(self.connect_string()) as conn:
            with conn.cursor() as cur:
                cur.execute(query_truncate_table)
                conn.commit()

        print(f"All data removed from table '{name} in database.'{self.db_name}'.")

    def sql_query_with_name(self, name):
        sql_folder_path = os.path.join(os.path.dirname(__file__), '../../sql/')
        with open(os.path.join(sql_folder_path, name), 'r') as file:
            tokenized_query = file.read()
        return tokenized_query

    def data_file_path_with_name(self, name):
        sql_folder_path = os.path.join(os.path.dirname(__file__), '../../data/')
        path = os.path.join(sql_folder_path, name)
        return path

    ######################################################
    #
    # along_track table creation/destruction
    #
    ######################################################

    def create_along_track_table(self):
        tokenized_query = self.sql_query_with_name('create_along_track_table.sql')
        query = sql.SQL(tokenized_query).format(table_name=sql.Identifier(self.along_track_table_name))

        with pg.connect(self.connect_string()) as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                conn.commit()
        print(f"Table '{self.along_track_table_name} added to database (if it did not previously exist) '{self.db_name}'.")

    def drop_along_track_table(self):
        self.drop_table(self.along_track_table_name)

    def truncate_along_track_table(self):
        self.truncate_table(self.along_track_table_name)

    def create_along_track_indices(self):
        tokenized_query = self.sql_query_with_name('create_along_track_index_point.sql')
        query_create_point_index = sql.SQL(tokenized_query).format(table_name=sql.Identifier(self.along_track_table_name))

        tokenized_query = self.sql_query_with_name('create_along_track_index_point_geom.sql')
        query_create_point_geom_index = sql.SQL(tokenized_query).format(
            table_name=sql.Identifier(self.along_track_table_name))

        tokenized_query = self.sql_query_with_name('create_along_track_index_date.sql')
        query_create_date_index = sql.SQL(tokenized_query).format(table_name=sql.Identifier(self.along_track_table_name))

        tokenized_query = self.sql_query_with_name('create_along_track_index_filename.sql')
        query_create_filename_index = sql.SQL(tokenized_query).format(table_name=sql.Identifier(self.along_track_table_name))

        tokenized_query = self.sql_query_with_name('create_along_track_index_time.sql')
        create_along_track_index_time = sql.SQL(tokenized_query).format(
            table_name=sql.Identifier(self.along_track_table_name))

        tokenized_query = self.sql_query_with_name('create_along_track_index_mission.sql')
        create_along_track_index_mission = sql.SQL(tokenized_query).format(
            table_name=sql.Identifier(self.along_track_table_name))

        tokenized_query = self.sql_query_with_name('create_along_track_index_point_date.sql')
        query_create_combined_point_date_index = sql.SQL(tokenized_query).format(table_name=sql.Identifier(self.along_track_table_name))


        with pg.connect(self.connect_string()) as conn:
            with conn.cursor() as cur:
                cur.execute(query_create_point_index)
                cur.execute(query_create_point_geom_index)
                cur.execute(query_create_date_index)
                cur.execute(query_create_filename_index)
                cur.execute(create_along_track_index_time)
                cur.execute(create_along_track_index_mission)
                conn.commit()

    def drop_along_track_indices(self):
        tokenized_query = self.sql_query_with_name('drop_along_track_index_point.sql')
        query_drop_point_index = sql.SQL(tokenized_query).format(table_name=sql.Identifier(self.along_track_table_name))

        tokenized_query = self.sql_query_with_name('drop_along_track_index_point_geom.sql')
        query_drop_point_geom_index = sql.SQL(tokenized_query).format(table_name=sql.Identifier(self.along_track_table_name))

        tokenized_query = self.sql_query_with_name('drop_along_track_index_date.sql')
        query_drop_date_index = sql.SQL(tokenized_query).format(table_name=sql.Identifier(self.along_track_table_name))

        tokenized_query = self.sql_query_with_name('drop_along_track_index_filename.sql')
        query_drop_filename_index = sql.SQL(tokenized_query).format(table_name=sql.Identifier(self.along_track_table_name))

        tokenized_query = self.sql_query_with_name('drop_along_track_index_time.sql')
        drop_along_track_index_time = sql.SQL(tokenized_query).format(
            table_name=sql.Identifier(self.along_track_table_name))

        tokenized_query = self.sql_query_with_name('drop_along_track_index_mission.sql')
        drop_along_track_index_mission = sql.SQL(tokenized_query).format(
            table_name=sql.Identifier(self.along_track_table_name))

        tokenized_query = self.sql_query_with_name('drop_along_track_index_point_date.sql')
        query_drop_combined_point_date_index = sql.SQL(tokenized_query).format(table_name=sql.Identifier(self.along_track_table_name))

        with pg.connect(self.connect_string()) as conn:
            with conn.cursor() as cur:
                cur.execute(query_drop_point_index)
                cur.execute(query_drop_point_geom_index)
                cur.execute(query_drop_date_index)
                cur.execute(query_drop_filename_index)
                cur.execute(drop_along_track_index_time)
                cur.execute(drop_along_track_index_mission)
                conn.commit()

    ######################################################
    #
    # along_track_metadata table creation/destruction
    #
    ######################################################

    def create_along_track_metadata_table(self):
        tokenized_query = self.sql_query_with_name('create_along_track_metadata_table.sql')
        create_table_query = sql.SQL(tokenized_query).format(table_name=sql.Identifier(self.along_track_metadata_table_name))

        with pg.connect(self.connect_string()) as conn:
            with conn.cursor() as cur:
                cur.execute(create_table_query)
                conn.commit()
        print(f"Table {self.along_track_metadata_table_name} added to database {self.db_name} (if it did not previously exist).")

    def drop_along_track_metadata_table(self):
        self.drop_table(self.along_track_metadata_table_name)

    def truncate_along_track_metadata_table(self):
        self.truncate_table(self.along_track_metadata_table_name)

    ######################################################
    #
    # basin table creation/destruction
    #
    ######################################################

    def create_basin_table(self):
        tokenized_query = self.sql_query_with_name('create_basin_table.sql')
        query_create_ocean_basins_table = sql.SQL(tokenized_query).format(
            table_name=sql.Identifier(self.ocean_basin_table_name)
        )

        tokenized_query = self.sql_query_with_name('create_basin_index_geom.sql')
        query_create_ocean_basins_index = sql.SQL(tokenized_query).format(
            table_name=sql.Identifier(self.ocean_basin_table_name)
        )

        with pg.connect(self.connect_string()) as conn:
            with conn.cursor() as cur:
                cur.execute(query_create_ocean_basins_table)
                cur.execute(query_create_ocean_basins_index)
                conn.commit()

        print(f"Table '{self.ocean_basin_table_name}' added to database (if it did not previously exist) '{self.db_name}'.")

    def drop_basin_table(self):
        self.drop_table(self.ocean_basin_table_name)

    def insert_basin_from_csv(self):
        query = sql.SQL("COPY {table} ({fields}) FROM STDIN WITH (FORMAT CSV, HEADER)").format(
            fields=sql.SQL(',').join([
                sql.Identifier('id'),
                sql.Identifier('basin_geog'),
                sql.Identifier('feature_id'),
                sql.Identifier('name'),
                sql.Identifier('wikidataid'),
                sql.Identifier('ne_id'),
                sql.Identifier('area'),
            ]),
            table=sql.Identifier(self.ocean_basin_table_name),
        )

        try:
            with open(self.data_file_path_with_name('ocean_basins.csv'), 'r') as f:
                with pg.connect(self.connect_string()) as conn:
                    with conn.cursor() as cursor:
                        with cursor.copy(query) as copy:
                            copy.write(f.read())
                        conn.commit()
        except FileNotFoundError:
            print("File 'ocean_basins.csv' not found")
        else:
            print('Ocean basins loaded')


    ######################################################
    #
    # basin_connection table creation/destruction
    #
    ######################################################

    def create_basin_connection_table(self):
        tokenized_query = self.sql_query_with_name('create_basin_connection_table.sql')
        query_create_ocean_basins_connections_table = sql.SQL(tokenized_query).format(
            table_name=sql.Identifier(self.ocean_basins_connections_table_name)
        )

        tokenized_query = self.sql_query_with_name('create_basin_connection_index_basin_id.sql')
        query_create_ocean_basins_connections_index = sql.SQL(tokenized_query).format(
            table_name=sql.Identifier(self.ocean_basins_connections_table_name)
        )

        with pg.connect(self.connect_string()) as conn:
            with conn.cursor() as cur:
                cur.execute(query_create_ocean_basins_connections_table)
                cur.execute(query_create_ocean_basins_connections_index)
                conn.commit()

        print(f"Table '{self.ocean_basins_connections_table_name}' added to database '{self.db_name}'.")

    def drop_basin_connection_table(self):
        self.drop_table(self.ocean_basins_connections_table_name)

    def insert_basin_connection_from_csv(self):
        query = sql.SQL("COPY {table} ({fields}) FROM STDIN WITH (FORMAT CSV, HEADER)").format(
            fields=sql.SQL(',').join([
                sql.Identifier('basin_id'),
                sql.Identifier('connected_id'),
            ]),
            table=sql.Identifier(self.ocean_basins_connections_table_name),
        )

        try:
            with open(self.data_file_path_with_name('ocean_basin_connections.csv'), 'r') as filename:
                with pg.connect(self.connect_string()) as conn:
                    with conn.cursor() as cursor:
                        with cursor.copy(query) as copy:
                            while data := filename.read(100):
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
                sql.Identifier('file_name'),
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
            table=sql.Identifier(self.along_track_metadata_table_name),
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

    def remove_previously_imported_files(self, file_paths):
        query = sql.SQL("SELECT EXISTS(SELECT 1 FROM {table} WHERE file_name=%s)").format(table=sql.Identifier(self.along_track_metadata_table_name))
        filenames = [[os.path.basename(x)] for x in file_paths]
        pruned_file_paths = []
        with pg.connect(self.connect_string()) as connection:
            with connection.cursor() as cursor:
                # for i in range(len(filenames)):
                #     cursor.execute(query, filenames[i])
                #     if not cursor.fetchone()[0]:
                #         pruned_file_paths.append(file_paths[i])
                cursor.executemany(query, filenames, returning=True)
                i=0
                while True:
                    if not cursor.fetchone()[0]:
                        pruned_file_paths.append(file_paths[i])
                    i=i+1
                    if not cursor.nextset():
                        break

        return pruned_file_paths


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

    def extract_data_tuple_from_netcdf(self, file_path, fname):
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
            # time_data = nc.date2num(time_data[:],
            #                         "microseconds since 2000-01-01 00:00:00")  # Convert the standard date back to the 8-byte integer PSQL uses

            data = {
                'time': time_data,
                'latitude': ds.variables['latitude'][:],
                'longitude': ds.variables['longitude'][:],
                'cycle': ds.variables['cycle'][:],
                'track': ds.variables['track'][:],
                'sla_unfiltered': ds.variables['sla_unfiltered'][:],
                'sla_filtered': ds.variables['sla_filtered'][:],
                'dac': ds.variables['dac'][:],
                'ocean_tide': ds.variables['ocean_tide'][:],
                'internal_tide': ds.variables['internal_tide'][:],
                'lwe': ds.variables['lwe'][:],
                'mdt': ds.variables['mdt'][:],
                'tpa_correction': ds.variables['tpa_correction'][:]
            }

            data['longitude'][:] = (data['longitude'][:] + 180) % 360 - 180

            ds.close()

            return data
        except FileNotFoundError:
            print("File '{}' not found".format(file_path))

    def import_data_tuple_to_postgresql(self, data, filename):
        copy_query = sql.SQL(
            "COPY {along_tbl_nme} ( file_name, track, cycle, latitude, longitude, sla_unfiltered, sla_filtered, date_time, dac, ocean_tide, internal_tide, lwe, mdt, tpa_correction) FROM STDIN").format(
            along_tbl_nme=sql.Identifier(self.along_track_table_name))
        copy_query = sql.SQL(
            "COPY {along_tbl_nme} (  file_name, track, cycle, latitude, longitude, sla_unfiltered, sla_filtered, date_time, dac, ocean_tide, internal_tide, lwe, mdt) FROM STDIN").format(
            along_tbl_nme=sql.Identifier(self.along_track_table_name))

        with pg.connect(self.connect_string()) as connection:
            with connection.cursor() as cursor:
                with cursor.copy(copy_query) as copy:
                    for i in range(len(data['time'])):
                        copy.write_row([
                            filename,
                            data['track'][i],
                            data['cycle'][i],
                            data['latitude'][i],
                            data['longitude'][i],
                            data['sla_unfiltered'][i],
                            data['sla_filtered'][i],
                            data['time'][i],
                            data['dac'][i],
                            data['ocean_tide'][i],
                            data['internal_tide'][i],
                            data['lwe'][i],
                            data['mdt'][i]
                        ])


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
                    "COPY {along_tbl_nme} ( file_name, track, cycle, latitude, longitude, sla_unfiltered, sla_filtered, date_time, dac, ocean_tide, internal_tide, lwe, mdt, tpa_correction) FROM STDIN WITH (FORMAT binary);").format(
                    along_tbl_nme=sql.Identifier(self.along_track_table_name))
                with cur.copy(copy_query) as copy:
                    while data := output.read(8192):
                        copy.write(data)
                # conn.commit()

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

                    partition_name = f"{self.along_track_table_name}_{str(year)}{month_partition_str}"  # Monthly partions are named after the table name eg: along_track_YYYYMM and yearly are named along_track_YYYY
                    min_partition_date = f"{min_partition}"
                    to_partition_date = f"{to_partition}"

                    tokenized_query = self.sql_query_with_name('create_along_track_table_partition.sql')
                    query = sql.SQL(tokenized_query).format(
                        table_name=sql.Identifier(self.along_track_table_name),
                        partition_name=sql.Identifier(partition_name),
                        min_partition_date=sql.Identifier(min_partition_date),
                        max_partition_date=sql.Identifier(to_partition_date),
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

    # Cim's version... specify missions get directory from config.yaml
    def insert_along_track_data_from_netcdf_with_tuples(self, missions):
        start = time.time()
        directory = self.nc_files_path
        # copied code from satmapkit_utilities/open_cmems_local to get list of filenamess.
        file_paths = [fn for fn in glob.glob(os.path.join(directory,'**/*.nc'),recursive=True) if any('_'+m+'-l3' in fn for m in missions)]
        print(f"Found %d files." % len(file_paths))
        file_paths = self.remove_previously_imported_files(file_paths)
        print(f"Pruning files already imported. Will import %d files into the database." % len(file_paths))
        for file_path in file_paths:
            filename = [os.path.basename(x) for x in glob.glob(file_path)]
            data = self.extract_data_tuple_from_netcdf(file_path, filename)
            import_start = time.time()
            self.import_data_tuple_to_postgresql(data, filename)
            import_end = time.time()
            print(f"{filename} import time: {import_end - import_start}")
        end = time.time()
        print(f"Script end. Total time: {end - start}")    

    ######################################################
    #
    # simple queries
    #
    ######################################################

    def geographic_points_in_spatialtemporal_window(self, latitude, longitude, date, distance=500000, time_window=timedelta(seconds=856710), should_basin_mask=1):
        tokenized_query = self.sql_query_with_name('geographic_points_in_spatialtemporal_window.sql')
        values = {"longitude": longitude,
                  "latitude": latitude,
                  "central_date_time": date,
                  "distance": distance,
                  "time_delta": time_window/2}

        with pg.connect(self.connect_string()) as connection:
            with connection.cursor() as cursor:
                cursor.execute(tokenized_query,values)
                data = cursor.fetchall()

        return data

    def projected_points_in_spatialtemporal_window(self, latitude: float, longitude: float, date, Lx: float = 500000., Ly: float = 500000., time_window=timedelta(seconds=856710), should_basin_mask=1):
        if should_basin_mask == 1:
            tokenized_query = self.sql_query_with_name("geographic_points_in_spatialtemporal_projected_window.sql")
        else:
            tokenized_query = self.sql_query_with_name("geographic_points_in_spatialtemporal_projected_window_nomask.sql")

        [x0, y0, minLat, minLon, maxLat, maxLon] = AlongTrack.latitude_longitude_bounds_for_transverse_mercator_box(latitude, longitude, 2*Lx, 2*Ly)

        values = {"longitude": longitude,
                  "latitude": latitude,
                  "xmin": minLon,
                  "ymin": minLat,
                  "xmax": maxLon,
                  "ymax": maxLat,
                  "central_date_time": date,
                  "time_delta": time_window / 2}

        with pg.connect(self.connect_string()) as connection:
            with connection.cursor() as cursor:
                cursor.execute(tokenized_query, values)
                data = cursor.fetchall()

        lon = np.array([data_i[0] for data_i in data])
        lat = np.array([data_i[1] for data_i in data])
        sla = np.array([data_i[2] for data_i in data])
        t = np.array([data_i[3] for data_i in data])

        [x, y] = AlongTrack.latitude_longitude_to_spherical_transverse_mercator(lat, lon, longitude)
        out_of_bounds = (x < x0 - Lx) | (x > x0 + Lx) | (y < y0 - Ly) | (y > y0 + Ly)
        x = x[~out_of_bounds]
        y = y[~out_of_bounds]
        sla = sla[~out_of_bounds]
        t = t[~out_of_bounds]

        x = x - x0
        y = y - y0

        return x, y, sla, t

    @staticmethod
    def latitude_longitude_bounds_for_transverse_mercator_box(lat0: float, lon0: float, Lx: float, Ly: float):
        [x0, y0] = AlongTrack.latitude_longitude_to_spherical_transverse_mercator(lat0, lon0, lon0=lon0)
        x = np.zeros(6)
        y = np.zeros(6)

        x[1] = x0 - Lx / 2
        y[1] = y0 - Ly / 2

        x[2] = x0 - Lx / 2
        y[2] = y0 + Ly / 2

        x[3] = x0
        y[3] = y0 + Ly / 2

        x[4] = x0
        y[4] = y0 - Ly / 2

        x[5] = x0 + Lx / 2
        y[5] = y0 - Ly / 2

        x[0] = x0 + Lx / 2
        y[0] = y0 + Ly / 2

        [lats, lons] = AlongTrack.spherical_transverse_mercator_to_latitude_longitude(x, y, lon0)
        minLat = min(lats)
        maxLat = max(lats)
        minLon = min(lons)
        maxLon = max(lons)

        return x0, y0, minLat, minLon, maxLat, maxLon

    @staticmethod
    def latitude_longitude_to_spherical_transverse_mercator(lat: float, lon: float, lon0: float):
        k0 = 0.9996
        WGS84a = 6378137.
        R = k0 * WGS84a

        phi = np.array(lat) * np.pi / 180
        deltaLambda = (np.array(lon) - np.array(lon0)) * np.pi / 180
        sinLambdaCosPhi = np.sin(deltaLambda) * np.cos(phi)
        x = (R / 2) * np.log((1 + sinLambdaCosPhi) / (1 - sinLambdaCosPhi))
        y = R * np.arctan(np.tan(phi) / np.cos(deltaLambda))

        return x, y

    @staticmethod
    def spherical_transverse_mercator_to_latitude_longitude(x: float, y: float, lon0: float):
        k0 = 0.9996
        WGS84a = 6378137
        R = k0 * WGS84a

        lon = (180 / np.pi) * np.arctan(np.sinh(x / R) / np.cos(y / R)) + lon0
        lat = (180 / np.pi) * np.arcsin(np.sin(y / R) / np.cosh(x / R))

        return lat, lon