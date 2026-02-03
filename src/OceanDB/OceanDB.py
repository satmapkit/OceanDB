from functools import cached_property
import netCDF4 as nc
from psycopg import sql
import psycopg as pg
from importlib import resources
import time
from typing import IO
import numpy as np
from sqlalchemy import create_engine

from OceanDB.config import Config
from OceanDB.utils.logging import get_logger


class OceanDB:
    """
    Base class for all classes that interface with the Postgres database

    This class expects a .env file at the project root with database credentials.  See instructions in the README
    """

    def __init__(
        self,
    ):
        self.config = Config()
        self.connection_string = self.config.postgres_dsn
        self.host = self.config.postgres_host
        self.username = self.config.postgres_username
        self.password = self.config.postgres_password
        self.port = self.config.postgres_port
        self.db_name = self.config.postgres_database

        self.sql_pkg = "OceanDB.sql"
        self.data_pkg = "OceanDB.data"
        self.logger = get_logger()

    def load_module_file(
        self, module: str, filename: str, encoding="utf-8", mode="rb"
    ) -> IO:
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

    def load_sql_file(self, filename: str):
        """
        Load the contents of a SQL file
        """
        with self.load_module_file(
            module="OceanDB.sql", filename=filename, mode="r", encoding="utf-8"
        ) as f:
            query = f.read()
            return query

    def get_engine(self, echo: bool = False):
        """Return a SQLAlchemy engine connected to the OceanDB Postgres database."""

        host = self.config.postgres_host
        port = self.config.postgres_port
        user = self.config.postgres_username
        password = self.config.postgres_password
        db = self.config.postgres_database
        url = f"postgresql+psycopg://{user}:{password}@{host}:{port}/{db}"
        engine = create_engine(url, echo=echo)
        return engine

    def vacuum_analyze(self):
        print(f"Starting VACUUM ANALYZE...")
        start = time.time()
        with pg.connect(self.connection_string) as conn:
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute("VACUUM ANALYZE")
        end = time.time()
        print(f"Finished. Total time: {end - start}")

    def drop_database(self):
        with pg.connect(
            f"host={self.host} port={self.port} user={self.username} password={self.password}"
        ) as conn:
            conn.autocommit = (
                True  # Enable autocommit to execute CREATE DATABASE command
            )
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL("DROP DATABASE {} WITH (FORCE)").format(
                        sql.Identifier(self.db_name)
                    )
                )

        print(f"Database '{self.db_name}' dropped.")

    def truncate_table(self, name):
        query_truncate_table = sql.SQL("""TRUNCATE public.{table_name}""").format(
            table_name=sql.Identifier(name)
        )

        with pg.connect(self.connection_string) as conn:
            with conn.cursor() as cur:
                cur.execute(query_truncate_table)
                conn.commit()
        print(f"All data removed from table '{name} in database.'{self.db_name}'.")


    @cached_property
    def basin_mask_data(self):
        """
        Load the basin mask NetCDF file packaged with the module.
        Returns the 'basinmask' variable as a NumPy array.
        """
        # Open resource file via importlib.resources
        with self.load_module_file(
            "OceanDB.data", "basin_masks/new_basin_mask.nc", mode="rb"
        ) as f:
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
        with pg.connect(self.connection_string) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """SELECT DISTINCT basin_id FROM basin_connections ORDER BY basin_id"""
                )
                unique_ids = cursor.fetchall()

        uid = [data_i[0] for data_i in unique_ids]
        basin_id_dict = [{"basin_id": basin_id} for basin_id in uid]

        query = """SELECT array_agg(connected_id) as connected_basin_id
        		FROM basin_connections
        		WHERE basin_id = %(basin_id)s
        		GROUP BY basin_id"""

        basin_id_connection_dict = {}
        with pg.connect(self.connection_string) as connection:
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
