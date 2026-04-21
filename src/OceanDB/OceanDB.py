from contextlib import contextmanager
from importlib import resources
import time
from typing import IO, LiteralString, Literal

import psycopg as pg
from psycopg import sql
from psycopg.rows import tuple_row
from psycopg.types import TypeInfo
from psycopg.types.shapely import register_shapely
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
        config: Config | None = None,
    ):
        self.config = Config() if config is None else config
        self.connection_string = self.config.postgres_dsn
        self.host = self.config.postgres_host
        self.username = self.config.postgres_username
        self.password = self.config.postgres_password
        self.port = self.config.postgres_port
        self.db_name = self.config.postgres_database

        self.sql_pkg = "OceanDB.sql"
        self.data_pkg = "OceanDB.data"
        self.logger = get_logger()

    @contextmanager
    def cursor(
        self,
        *,
        autocommit: bool = False,
        commit: bool = False,
        use_geometry: bool = False,
        row_factory=tuple_row,
        connection_string: str | None = None,
    ):
        """
        Managed PostgreSQL cursor for all OceanDB database access.
        """
        if autocommit and commit:
            raise ValueError("autocommit and commit cannot both be True")

        dsn = self.connection_string if connection_string is None else connection_string

        with pg.connect(dsn) as conn:
            conn.autocommit = autocommit

            if use_geometry:
                info = TypeInfo.fetch(conn, "geometry")
                if info is None:
                    raise ValueError("Failed to fetch geometry")
                register_shapely(info, conn)

            try:
                with conn.cursor(row_factory=row_factory) as cur:
                    yield cur
            except Exception:
                conn.rollback()
                raise
            else:
                if commit:
                    conn.commit()

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

    def vacuum_analyze(self):
        print(f"Starting VACUUM ANALYZE...")
        start = time.time()
        with self.cursor(autocommit=True) as cur:
            cur.execute("VACUUM ANALYZE")
        end = time.time()
        print(f"Finished. Total time: {end - start}")

    def drop_database(self):
        with self.cursor(
            autocommit=True,
            connection_string=(
                f"host={self.host} port={self.port} "
                f"user={self.username} password={self.password}"
            ),
        ) as cur:
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

        with self.cursor(commit=True) as cur:
            cur.execute(query_truncate_table)
        print(f"All data removed from table '{name} in database.'{self.db_name}'.")
