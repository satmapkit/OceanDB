import psycopg as pg
from psycopg import sql
import os
import yaml
import pandas as pd


class OceanDB:
    db_name: str

    ######################################################
    #
    # Initialization
    #
    ######################################################

    def __init__(self, host="", username="", password="", port=5432, db_name='ocean'):
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
    # Convert to xarray
    #
    ######################################################
    def data_as_xarray(self, data, header_array, row_metadata):
        meta = dict()
        encodings = dict()

        try:
            df = pd.DataFrame(data, columns=header_array)
        except Exception as err:
            print(err)

        # We need to either assign the metadata to the 'attrs' of the variable, or the encoding. If we do the encoding,
        # then xarray will actively transform the data. For the moment, I am only transforming the datatype.
        # The other notable piece is time. Time data already has some default encoding associated with it, so we just
        # need to override it.

        # encoding_keys = ['dtype', 'scale_factor', 'add_offset', '_FillValue']
        encoding_keys = ['dtype']
        disallowed_time_keys = ['dtype', 'units', 'calendar', 'scale_factor', 'add_offset']

        xrdata = df.to_xarray()
        for record in row_metadata:
            if record['var_name'] in header_array:
                # Remove empty items from dictionary. Xarray will throw an error is an item is None
                if record['var_name'] == 'time':
                    xrdata[record['var_name']].attrs = {k: v for k, v in record.items() if
                                                        v is not None and k not in disallowed_time_keys}
                    xrdata.time.encoding['dtype'] = 'float64'
                    xrdata.time.encoding['units'] = 'days since 1950-01-01 00:00:00'
                    xrdata.time.encoding['calendar'] = 'gregorian'
                else:
                    xrdata[record['var_name']].attrs = {k: v for k, v in record.items() if
                                                  v is not None and k not in encoding_keys}
                    encodings[record['var_name']] = {k: v for k, v in record.items() if
                                                     v is not None and k in encoding_keys}
        return xrdata, encodings