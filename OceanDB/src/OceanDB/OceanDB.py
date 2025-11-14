from functools import cached_property
from io import BytesIO
import netCDF4 as nc
import psycopg as pg
from psycopg import sql
import os
import yaml
from importlib import resources
import time
import pandas as pd
from typing import IO
import psycopg as pg
from psycopg.rows import dict_row
from typing import Any, List, Dict, Optional
import numpy as np

from sqlalchemy import create_engine

from OceanDB.utils.logging import get_logger

class OceanDB:
    def __init__(self, host="", username="", password="", port=5432, db_name='ocean', config_path='/app/config.yaml'):
        self.sql_pkg = "OceanDB.sql"
        self.data_pkg = "OceanDB.data"
        self.logger = get_logger()

        self.host = host
        self.username = username
        self.password = password
        self.port = port
        self.db_name = db_name

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


    def connect_string(self):
        conn_str = f'''
            host={self.host}
            dbname={self.db_name}
            port={self.port}
            user={self.username}
            password={self.password}'''
        return conn_str


    def load_module_file(self, module, filename, encoding="utf-8", mode="rb") -> IO:
        """
        Open a resource file bundled within a Python package.

        Handles both text ('r') and binary ('rb') modes safely.
        Automatically omits encoding when opening in binary mode.
        """
        file_path = resources.files(module).joinpath(filename)

        # encoding is only valid for text mode
        if "b" in mode:
            return file_path.open(mode)
        return file_path.open(mode, encoding=encoding)

    def load_sql_file(self, filename):
        with self.load_module_file(module="OceanDB.sql",
                                   filename=filename,
                                   mode="r",
                                   encoding="utf-8") as f:
            query = f.read()
            return query

    def select_query(self, table: str, query: str) -> Optional[List[Dict[str, Any]]]:
        """
        Execute a SQL query and optionally return rows if the query produces results.

        Parameters
        ----------
        table : str
            Logical table name (for logging/debugging)
        query : str
            SQL query string (can be SELECT, INSERT, UPDATE, etc.)

        Returns
        -------
        Optional[List[Dict[str, Any]]]
            List of dictionaries (rows), or None if no results.
        """
        try:
            with pg.connect(self.connect_string(), row_factory=dict_row) as conn:
                with conn.cursor() as cur:
                    cur.execute(query)

                    # Detect if the query returns rows (e.g. SELECT)
                    if cur.description:
                        rows = cur.fetchall()
                        self.logger.info(f"✅ Retrieved {len(rows)} rows from {table}")
                        return rows
                    else:
                        conn.commit()
                        self.logger.info(f"✅ Executed non-returning query on {table}")
                        return None

        except Exception as ex:
            self.logger.info(f"❌ Error executing {table}")
            self.logger.info(f"❌ Error while Executing Query: {ex}")
            return None

    def execute_query(self, table, query):
        try:
            with pg.connect(self.connect_string()) as conn:
                 with conn.cursor() as cur:
                     cur.execute(query)
                     conn.commit()
        except Exception as ex:
            self.logger.info(f"Error executing {table}")
            self.logger.info(f"Error while Executing Query {ex}")

    def get_engine(self, echo: bool = False):
        """Return a SQLAlchemy engine connected to the OceanDB Postgres database."""
        host = os.getenv("DB_HOST", "postgres")
        port = os.getenv("DB_PORT", "5432")
        user = os.getenv("DB_USER", "postgres")
        password = os.getenv("DB_PASSWORD", "postgres")
        db = os.getenv("DB_NAME", "ocean")

        url = f"postgresql+psycopg://{user}:{password}@{host}:{port}/{db}"
        engine = create_engine(url, echo=echo)
        return engine

    def vacuum_analyze(self):
        print(f"Starting VACUUM ANALYZE...")
        start = time.time()
        with pg.connect(self.connect_string()) as conn:
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute("VACUUM ANALYZE")
        end = time.time()
        print(f"Finished. Total time: {end - start}")


    def drop_database(self):
        with pg.connect(f'host={self.host} port={self.port} user={self.username} password={self.password}') as conn:
            conn.autocommit = True  # Enable autocommit to execute CREATE DATABASE command
            with conn.cursor() as cur:
                cur.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(self.db_name)))

        print(f"Database '{self.db_name}' dropped.")

    def truncate_table(self, name):
        query_truncate_table = sql.SQL("""TRUNCATE public.{table_name}""").format(
            table_name=sql.Identifier(name)
        )

        with pg.connect(self.connect_string()) as conn:
            with conn.cursor() as cur:
                cur.execute(query_truncate_table)
                conn.commit()

        print(f"All data removed from table '{name} in database.'{self.db_name}'.")

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

    @cached_property
    def basin_mask_data(self):
        """
        Load the basin mask NetCDF file packaged with the module.
        Returns the 'basinmask' variable as a NumPy array.
        """
        # Open resource file via importlib.resources
        with self.load_module_file("OceanDB.data", "basin_masks/new_basin_mask.nc", mode="rb") as f:
            ds = nc.Dataset("inmemory.nc", memory=f.read())  # load from memory buffer
            ds.set_auto_mask(False)
            basin_mask = ds.variables["basinmask"][:]
            ds.close()
            return basin_mask


    def basin_mask(self, latitude, longitude):
        """
        Get basin_id from lat & lng
        """
        onesixth = 1 / 6
        i = np.floor((latitude + 90) / onesixth).astype(int)
        j = np.floor((longitude % 360) / onesixth).astype(int)
        mask_data = self.basin_mask_data
        basin_mask = mask_data[i, j]
        return basin_mask

    @cached_property
    def basin_connection_map(self) -> dict:
        with pg.connect(self.connect_string()) as connection:
            with connection.cursor() as cursor:
                cursor.execute("""SELECT DISTINCT basin_id FROM basin_connections ORDER BY basin_id""")
                unique_ids = cursor.fetchall()
        uid = [data_i[0] for data_i in unique_ids]
        basin_id_dict = [{"basin_id": basin_id} for basin_id in uid]

        query = """SELECT array_agg(connected_id) as connected_basin_id
        		FROM basin_connections
        		WHERE basin_id = %(basin_id)s
        		GROUP BY basin_id"""

        basin_id_connection_dict = {}
        with pg.connect(self.connect_string()) as connection:
            with connection.cursor() as cursor:
                cursor.executemany(query, basin_id_dict, returning=True)
                i = 0
                while True:
                    data = cursor.fetchall()
                    basin_id_connection_dict[uid[i]] = data[0][0]
                    basin_id_connection_dict[uid[i]].insert(0, uid[i])
                    i = i + 1
                    if not cursor.nextset():
                        break
        return basin_id_connection_dict


# ocean_db = OceanDB()
# ocean_db.basin_connection_map