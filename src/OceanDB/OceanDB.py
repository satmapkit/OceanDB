from functools import cached_property
import netCDF4 as nc

from psycopg import sql
import psycopg as pg
from psycopg.types import TypeInfo
from psycopg.types.shapely import register_shapely
from psycopg.rows import RowFactory, tuple_row

from importlib import resources
import time
from typing import (
    IO,
    LiteralString,
    Literal,
    Callable,
    TypeVar,
    ParamSpec,
    Concatenate,
    TypeVar,
    Any,
)
import numpy as np
from sqlalchemy import create_engine

from OceanDB.config import Config
from OceanDB.utils.logging import get_logger

C = TypeVar("C", bound="OceanDB", covariant=True)
P = ParamSpec("P")
R = TypeVar("R")
Row = TypeVar("Row")


def connect_to_db(
    conn_string: Callable[[C], str],
    autocommit: bool = False,
    commit: bool = False,
    use_geometry: bool = False,
    row_factory: RowFactory[Row] = tuple_row,
) -> Callable[
    [Callable[Concatenate[C, pg.Cursor[Row], P], R]], Callable[Concatenate[C, P], R]
]:
    # TODO: figure out if we need both commit and autocommit
    def decorator(
        func: Callable[Concatenate[C, pg.Cursor[Row], P], R],
    ) -> Callable[Concatenate[C, P], R]:
        def wrapper(self: C, *args: P.args, **kwargs: P.kwargs) -> R:
            with pg.connect(conn_string(self)) as conn:
                conn.autocommit = autocommit
                if use_geometry:
                    info = TypeInfo.fetch(conn, "geometry")
                    if info is None:
                        raise ValueError(
                            "Failed to fetch geometry in database. Is the correct plugin installed?"
                        )
                    register_shapely(info, conn)

                with conn.cursor(row_factory=row_factory) as cur:
                    out = func(self, cur, *args, **kwargs)

                if commit:
                    conn.commit()
            return out

        return wrapper

    return decorator


class OceanDB:
    """
    Base class for all classes that interface with the Postgres database

    This class expects a .env file at the project root with database credentials.  See instructions in the README
    """

    def __init__(self, config: Config | None = None):
        self.config = Config() if config is None else config
        self.host = self.config.postgres_host
        self.username = self.config.postgres_username
        self.password = self.config.postgres_password
        self.port = self.config.postgres_port
        self.db_name = self.config.postgres_database

        self.sql_pkg = "OceanDB.sql"
        self.data_pkg = "OceanDB.data"
        self.logger = get_logger()

    def connection_string(self):
        """
        :returns:
            Connection string to connect to the database, and read from tables
        """
        # TODO: should this be computed here, or in config (as it is presently)?
        return self.config.postgres_dsn

    def connection_string_admin(self):
        """
        :returns:
            String to connect to the postgres instance, but not the database.
            This is used to create or destroy the database.
        """
        # TODO: should this be computed here, or in config (as it is presently)?
        return self.config.postgres_dsn_admin

    def load_module_file(
        self,
        module: str,
        filename: str,
        encoding="utf-8",
        mode: Literal["r", "rb"] = "rb",
    ) -> IO:
        """
        Open a resource file bundled within a Python package.

        Handles both text ('r') and binary ('rb') modes safely.
        Automatically omits encoding when opening in binary mode.
        """
        file_path = resources.files(module).joinpath(filename)

        # encoding is only valid for text mode
        if mode == "rb":
            return file_path.open(mode)
        return file_path.open(mode, encoding=encoding)

    def load_sql_file(self, filename: str) -> LiteralString:
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

    @connect_to_db(connection_string)
    def vacuum_analyze(self, cur):
        print(f"Starting VACUUM ANALYZE...")
        start = time.time()
        cur.execute("VACUUM ANALYZE")
        end = time.time()
        print(f"Finished. Total time: {end - start}")

    @connect_to_db(connection_string_admin, autocommit=True)
    def drop_database(self, cur):
        cur.execute(
            sql.SQL("DROP DATABASE {} WITH (FORCE)").format(
                sql.Identifier(self.db_name)
            )
        )

        print(f"Database '{self.db_name}' dropped.")

    @connect_to_db(connection_string, commit=True)
    def truncate_table(self, cur, name):
        query_truncate_table = sql.SQL("""TRUNCATE public.{table_name}""").format(
            table_name=sql.Identifier(name)
        )

        cur.execute(query_truncate_table)
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

    @connect_to_db(connection_string)
    def _get_unique_basin_ids(self, cursor: pg.Cursor) -> list[tuple[int]]:
        cursor.execute(
            """SELECT DISTINCT basin_id FROM basin_connections ORDER BY basin_id"""
        )
        return cursor.fetchall()

    @connect_to_db(connection_string)
    def _get_connected_basins(
        self,
        cursor: pg.Cursor,
        uid: list[int],
    ):
        basin_id_dict = [{"basin_id": basin_id} for basin_id in uid]

        query = """SELECT array_agg(connected_id) as connected_basin_id
        		FROM basin_connections
        		WHERE basin_id = %(basin_id)s
        		GROUP BY basin_id"""

        cursor.executemany(query, basin_id_dict, returning=True)
        basin_id_connection_dict = {}
        i = 0
        while True:
            data = cursor.fetchall()
            basin_id_connection_dict[uid[i]] = data[0][0]
            basin_id_connection_dict[uid[i]].insert(0, uid[i])
            i = i + 1
            if not cursor.nextset():
                break
        return basin_id_connection_dict

    @cached_property
    def basin_connection_map(self) -> dict:
        unique_ids = self._get_unique_basin_ids()
        uid = [data_i[0] for data_i in unique_ids]
        basin_id_connection_dict = self._get_connected_basins(uid)
        return basin_id_connection_dict
