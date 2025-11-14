from pathlib import Path
import psycopg as pg
from psycopg import sql
import os
import yaml
from importlib import resources
import time
import pandas as pd

from OceanDB.logging import get_logger


# from OceanDB.old.logger import get_logger


class MOceanDB:
    def __init__(self, host="", username="", password="", port=5432, db_name='ocean'):
        self.sql_pkg = "OceanDB.sql"
        self.data_pkg = "OceanDB.data"
        self.logger = get_logger()

        try:
            base_dir = os.path.dirname(__file__)
        except NameError:
            # __file__ not defined (e.g., in notebook)
            base_dir = os.getcwd()

        config_path = os.path.join(base_dir, ".", "config.yaml")

        self.host = host
        self.username = username
        self.password = password
        self.port = port
        self.db_name = db_name

        # Load YAML config if it exists
        if os.path.exists(config_path):
            with open(config_path, "r") as param_file:
                params = yaml.safe_load(param_file) or {}
                db_cfg = params.get("db_connect", {})
                self.host = host or db_cfg.get("host", self.host)
                self.username = username or db_cfg.get("username", self.username)
                self.password = password or db_cfg.get("password", self.password)
                self.port = port or db_cfg.get("port", self.port)
                self.db_name = db_name or db_cfg.get("db_name", self.db_name)
        else:
            print(f"config.yaml not found at {config_path}, using defaults or env vars")

    ######################################################
    #
    # Database creation/destruction
    #
    ######################################################
    # def create_database(self):
    #     with pg.connect(f"host={self.host} port={self.port} user={self.username} password={self.password}") as conn:
    #         conn.autocommit = True
    #         with conn.cursor() as cur:
    #             cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (self.db_name,))
    #             if cur.fetchone():
    #                 print(f"Database '{self.db_name}' already exists.")
    #                 return
    #             cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(self.db_name)))
    #             print(f"✅ Database '{self.db_name}' created successfully.")

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

    def truncate_table(self, name):
        query_truncate_table = sql.SQL("""TRUNCATE public.{table_name}""").format(
            table_name=sql.Identifier(name)
        )

        with pg.connect(self.connect_string()) as conn:
            with conn.cursor() as cur:
                cur.execute(query_truncate_table)
                conn.commit()

        print(f"All data removed from table '{name} in database.'{self.db_name}'.")

    def load_sql(self, filename: str) -> str:
        with resources.files(self.sql_pkg).joinpath(filename).open("r", encoding="utf-8") as f:
            tokenized_query = f.read()
        return tokenized_query

    def data_file_path_with_name(self, name: str) -> Path:
        """Return a pathlib.Path-like object pointing to a packaged CSV file."""
        path = resources.files(self.data_pkg) / name
        if not path.is_file():
            raise FileNotFoundError(f"Data file not found: {name}")
        return path

    def load_csv_df(self, name: str):
        """Optional helper to load a CSV file directly into pandas."""
        import pandas as pd
        path = self.data_file_path_with_name(name)
        with path.open("r", encoding="utf-8") as f:
            return pd.read_csv(f)

    def vacuum_analyze(self):
        print(f"Starting VACUUM ANALYZE...")
        start = time.time()
        with pg.connect(self.connect_string()) as conn:
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute("VACUUM ANALYZE")
        end = time.time()
        print(f"Finished. Total time: {end - start}")

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
            raise err

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
                if 'time' in record['var_name']:
                    xrdata[record['var_name']].attrs = {k: v for k, v in record.items() if
                                                        v is not None and k not in disallowed_time_keys}
                    xrdata[record['var_name']].encoding['dtype'] = 'float64'
                    xrdata[record['var_name']].encoding['units'] = 'days since 1950-01-01 00:00:00'
                    xrdata[record['var_name']].encoding['calendar'] = 'gregorian'
                else:
                    xrdata[record['var_name']].attrs = {k: v for k, v in record.items() if
                                                  v is not None and k not in encoding_keys}
                    encodings[record['var_name']] = {k: v for k, v in record.items() if
                                                     v is not None and k in encoding_keys}
        return xrdata, encodings


# ocean_db = OceanDB()
# ocean_db.load_sql('create_along_track_index_basin.sql')
# ocean_db.data_file_path_with_name("ocean_basins.csv")
