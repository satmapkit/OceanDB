from datetime import datetime

from dateutil.relativedelta import relativedelta
from psycopg import sql
from sqlalchemy import text

from OceanDB.base_write_query import BaseWriteQuery
from OceanDB.cli_utils import format_status_line
from OceanDB.query_spec import RawSpec

table_definitions = [
    {
        "name": "basin",
        "filepath": "tables/basin/create_basin_table.sql",
        "params": {"table_name": "basin"},
    },
    {
        "name": "basin_connections",
        "filepath": "tables/basin/create_basin_connection_table.sql",
        "params": {"table_name": "basin_connection"},
    },
    {
        "name": "along_track_metadata",
        "filepath": "tables/along_track/create_along_track_metadata_table.sql",
        "params": {"table_name": "along_track_metadata"},
    },
    {
        "name": "along_track",
        "filepath": "tables/along_track/create_along_track_table.sql",
        "params": {"table_name": "along_track"},
    },
]


eddy_tables = [
    {
        "name": "eddy",
        "filepath": "tables/eddy/create_eddy_table.sql",
        "params": {"table_name": "eddy"},
    },
    {
        "name": "chelton_eddy",
        "filepath": "tables/eddy/create_chelton_eddy_table.sql",
        "params": {"table_name": "chelton_eddy"},
    },
]


sql_index_files = [
    {
        "name": "along_track_index_basin",
        "filepath": "indices/along_track/create_along_track_index_basin.sql",
        "params": {"index_name": "along_track_index_basin"},
        "table_name": "along_track",
        "index_name": "along_track_basin_idx",
    },
    {
        "name": "along_track_index_date",
        "filepath": "indices/along_track/create_along_track_index_date.sql",
        "params": {"index_name": "along_track_index_date"},
        "table_name": "along_track",
        "index_name": "along_track_date_idx",
    },
    {
        "name": "along_track_index_filename",
        "filepath": "indices/along_track/create_along_track_index_filename.sql",
        "params": {"index_name": "along_track_index_filename"},
        "table_name": "along_track",
        "index_name": "along_track_file_name_idx",
    },
    {
        "name": "along_track_index_mission",
        "filepath": "indices/along_track/create_along_track_index_mission.sql",
        "params": {"index_name": "along_track_index_mission"},
        "table_name": "along_track",
        "index_name": "along_track_mission_idx",
    },
    {
        "name": "along_track_index_point",
        "filepath": "indices/along_track/create_along_track_index_point.sql",
        "params": {"index_name": "along_track_index_point"},
        "table_name": "along_track",
        "index_name": "along_track_point_idx",
    },
    {
        "name": "along_track_index_point_date",
        "filepath": "indices/along_track/create_along_track_index_point_date.sql",
        "params": {"index_name": "along_track_index_point_date"},
        "table_name": "along_track",
        "index_name": "along_track_point_date_idx",
    },
    {
        "name": "along_track_index_point_date_mission",
        "filepath": "indices/along_track/create_along_track_index_point_date_mission.sql",
        "params": {"index_name": "along_track_index_point_date_mission"},
        "table_name": "along_track",
        "index_name": "along_track_point_date_mission_idx",
    },
    {
        "name": "along_track_index_point_date_mission_basin",
        "filepath": "indices/along_track/create_along_track_index_point_date_mission_basin.sql",
        "params": {"index_name": "along_track_index_point_date_mission_basin"},
        "table_name": "along_track",
        "index_name": "along_track_point_date_mission_basin_idx",
    },
    {
        "name": "along_track_index_point_geom",
        "filepath": "indices/along_track/create_along_track_index_point_geom.sql",
        "params": {"index_name": "along_track_index_point_geom"},
        "table_name": "along_track",
        "index_name": "along_track_point_geom_idx",
    },
    {
        "name": "along_track_index_time",
        "filepath": "indices/along_track/create_along_track_index_time.sql",
        "params": {"index_name": "along_track_index_time"},
        "table_name": "along_track",
        "index_name": "along_track_time_idx",
    },
    {
        "name": "basin_connection_index_basin_id",
        "filepath": "indices/basin/create_basin_connection_index_basin_id.sql",
        "params": {"index_name": "basin_connection_index_basin_id"},
        "table_name": "basin_connections",
        "index_name": "basin_id_idx",
    },
    {
        "name": "basin_index_geom",
        "filepath": "indices/basin/create_basin_index_geom.sql",
        "params": {"index_name": "basin_index_geom"},
        "table_name": "basin",
        "index_name": "basin_geog_idx",
    },
    # {
    #     "name": "chelton_eddy_index_point",
    #     "filepath": "indices/create_chelton_eddy_index_point.sql",
    #     "params": {"index_name": "chelton_eddy_index_point"},
    # },
    # {
    #     "name": "chelton_eddy_index_track_cyclonic_type",
    #     "filepath": "indices/create_chelton_eddy_index_track_cyclonic_type.sql",
    #     "params": {"index_name": "chelton_eddy_index_track_cyclonic_type"},
    # },
    # {
    #     "name": "eddy_index_point",
    #     "filepath": "indices/create_eddy_index_point.sql",
    #     "params": {"index_name": "eddy_index_point"},
    # },
    # {
    #     "name": "eddy_index_track_cyclonic_type",
    #     "filepath": "indices/create_eddy_index_track_cyclonic_type.sql",
    #     "params": {"index_name": "eddy_index_track_cyclonic_type"},
    # },
    # {
    #     "name": "eddy_index_track_cyclonic_type",
    #     "filepath": "indices/create_eddy_index_track_cyclonic_type.sql",
    #     "params": {"index_name": "eddy_index_track_cyclonic_type"},
    # },
    # {
    #     "name": "eddy_index_track_cyclonic_type",
    #     "filepath": "indices/create_eddy_index_track_cyclonic_type.sql",
    #     "params": {"index_name": "eddy_index_track_cyclonic_type"},
    # },
    # {
    #     "name": "eddy_index_track_cyclonic_type",
    #     "filepath": "indices/create_eddy_index_track_cyclonic_type.sql",
    #     "params": {"index_name": "eddy_index_track_cyclonic_type"},
    # }
]

eddy_index_files = [
    {
        "name": "eddy_index_point",
        "filepath": "indices/eddy/create_eddy_index_point.sql",
        "params": {"index_name": "eddy_index_point"},
        "table_name": "eddy",
        "index_name": "eddy_point_idx",
    },
    {
        "name": "eddy_index_track_cyclonic_type",
        "filepath": "indices/eddy/create_eddy_index_track_cyclonic_type.sql",
        "params": {"index_name": "eddy_index_track_cyclonic_type"},
        "table_name": "eddy",
        "index_name": "track_times_cyclonic_type_idx",
    },
]


EXPECTED_TABLE_INDEXES = {
    "along_track": {
        "along_track_basin_idx",
        "along_track_date_idx",
        "along_track_file_name_idx",
        "along_track_mission_idx",
        "along_track_point_idx",
        "along_track_point_date_idx",
        "along_track_point_date_mission_idx",
        "along_track_point_date_mission_basin_idx",
        "along_track_point_geom_idx",
        "along_track_time_idx",
    },
    "basin": {
        "basin_geog_idx",
    },
    "basin_connections": {
        "basin_id_idx",
    },
    "chelton_eddy": {
        "chelton_eddy_point_idx",
        "chelton_track_times_cyclonic_type_idx",
    },
    "eddy": {
        "eddy_point_idx",
        "track_times_cyclonic_type_idx",
    },
}


class OceanDBInit(BaseWriteQuery):
    def _log_index_status(
        self,
        label: str,
        message: str,
        *,
        label_color: str,
    ) -> None:
        self.logger.info(format_status_line(label, message, label_color=label_color))

    def index_exists(self, *, table_name: str, index_name: str) -> bool:
        with self.cursor() as cur:
            cur.execute(
                """
                SELECT EXISTS (
                    SELECT 1
                    FROM pg_indexes
                    WHERE schemaname = 'public'
                      AND tablename = %s
                      AND indexname = %s
                )
                """,
                (table_name, index_name),
            )
            return bool(cur.fetchone()[0])

    def _create_index_group(self, indexes, *, group_name: str) -> None:
        total = len(indexes)
        self._log_index_status(
            "CHECK",
            f"Checking {total} {group_name} index definition(s).",
            label_color="blue",
        )

        for position, index in enumerate(indexes, start=1):
            index_name = index["index_name"]
            table_name = index["table_name"]
            progress = f"[{position}/{total}]"

            if self.index_exists(table_name=table_name, index_name=index_name):
                self._log_index_status(
                    "SKIP",
                    f"{progress} {index_name} on {table_name}: already present.",
                    label_color="yellow",
                )
                continue

            self._log_index_status(
                "START",
                f"{progress} {index_name} on {table_name}.",
                label_color="magenta",
            )
            query = self.parametrize_sql_statements(index)
            self.execute_write_query(query)
            self._log_index_status(
                "DONE",
                f"{progress} {index_name} on {table_name}.",
                label_color="green",
            )

    def table_exists(self, table: str) -> bool:
        with self.cursor() as cur:
            cur.execute(
                "SELECT EXISTS ( SELECT FROM pg_tables WHERE schemaname = 'public' AND tablename  = %(tablename)s);",
                {"tablename": table},
            )
            res = cur.fetchone()
            print("for", table, "result is", res)
            if not res:
                return False
            exists = res[0]
            if exists:
                return True
        return False

    def create_database(self):
        # Create the Database
        created_database = False
        with self.cursor(
            autocommit=True, connection_string=self.config.postgres_dsn_admin
        ) as cur:
            cur.execute(
                "SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = %s)",
                (self.db_name,),
            )
            exists = cur.fetchone()[0]
            if exists:
                print(f"Database '{self.db_name}' already exists.")
            else:
                cur.execute(
                    sql.SQL("CREATE DATABASE {}").format(sql.Identifier(self.db_name))
                )
                created_database = True
                print(f"Database '{self.db_name}' created successfully.")

        ## Enable POSTGIS extensions
        with self.cursor(commit=True) as cur:
            cur.execute(sql.SQL("CREATE EXTENSION IF NOT EXISTS plpgsql;"))
            cur.execute(sql.SQL("CREATE EXTENSION IF NOT EXISTS postgis;"))
            cur.execute(sql.SQL("CREATE EXTENSION IF NOT EXISTS btree_gist;"))
        print(f"Database '{self.db_name}' POSTGIS enabled.")
        return created_database

    def create_tables(self):
        for table in table_definitions:
            try:
                table_name = table["name"]
                query = self.parametrize_sql_statements(table)
                self.execute_write_query(query)
                self.logger.info(f"Executing {table_name}")
            except Exception as ex:
                self.logger.info(f"{table_name}")
                self.logger.info(ex)

    def create_eddy_tables(self):
        for table in eddy_tables:
            try:
                table_name = table["name"]
                query = self.parametrize_sql_statements(table)
                self.execute_write_query(query)
                self.logger.info(f"Executing {table_name}")
            except Exception as ex:
                self.logger.info(f"{table}")
                self.logger.info(ex)

    def create_indices(self):
        self._create_index_group(sql_index_files, group_name="standard")

    def create_eddy_indices(self):
        self._create_index_group(eddy_index_files, group_name="eddy")

    def create_partitions(self, min_date, max_date):
        """
        Create a partition for each month between min_date & max_date
        Args:
        min_date (str | datetime): start date, e.g. "2020-01-01"
        max_date (str | datetime): end date, e.g. "2020-06-01"
        """
        if isinstance(min_date, str):
            min_date = datetime.strptime(min_date, "%Y-%m-%d")
        if isinstance(max_date, str):
            max_date = datetime.strptime(max_date, "%Y-%m-%d")

        query_filepath = "tables/along_track/create_along_track_table_partition.sql"

        table_name = "along_track"
        current = min_date.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

        sql_statement = self.load_sql(query_filepath)

        while current < max_date:
            next_month = (current + relativedelta(months=1)).replace(day=1)
            partition_name = f"{table_name}_{current.year}_{current.month:02d}"

            safe_params = {
                "partition_name": sql.Identifier(partition_name),
                "table_name": sql.Identifier(table_name),
                "min_partition_date": sql.Literal(current.strftime("%Y-%m-%d")),
                "max_partition_date": sql.Literal(next_month.strftime("%Y-%m-%d")),
            }
            # Safely construct SQL
            query = RawSpec(sql.SQL(sql_statement).format(**safe_params))
            self.execute_write_query(query)

            print(f"Created partition {partition_name}")
            current = next_month

    def parametrize_sql_statements(self, table):
        """
        Some of the SQL statements are parameterized
        Substitute parameters
        {
            "name": "along_track_partition",
            "filepath": "tables/create_along_track_table_partition.sql",
            "params": {
                "table_name": "along_track",
                "partition_name": "along_track_2025_10",
                "min_partition_date": "2025-10-01",
                "max_partition_date": "2025-11-01",
            },
        }
        """
        query_filepath = table["filepath"]
        query_params = table["params"]
        sql_statement = self.load_sql(query_filepath)
        safe_params = {}
        for key, value in query_params.items():
            if "name" in key:
                safe_params[key] = sql.Identifier(value)
            elif "date" in key:
                safe_params[key] = sql.SQL(value)  # not sql.Literal
            else:
                safe_params[key] = sql.Literal(value)
        # Render query safely
        query = sql.SQL(sql_statement).format(**safe_params)
        return RawSpec(query)

    def load_sql(self, filename: str) -> str:
        # with resources.files(self.sql_pkg).joinpath(filename).open("r", encoding="utf-8") as f:
        #     tokenized_query = f.read()
        with self.load_module_file(
            "OceanDB.sql", filename, mode="r", encoding="utf-8"
        ) as f:
            tokenized_query = f.read()
            return tokenized_query

    def validate_schema(self):
        """
        Validates that all the expected tables & indices  have been created
        """
        engine = self.get_engine()
        print("VALIDDATING SCHEMA")
        for table_name, expected_indices in EXPECTED_TABLE_INDEXES.items():
            schema_name = "public"  # change if you use another schema

            with engine.connect() as conn:
                rows = conn.execute(
                    text("""
                        SELECT indexname, indexdef
                        FROM pg_indexes
                        WHERE schemaname = :schema AND tablename = :table
                        ORDER BY indexname
                    """),
                    {"schema": schema_name, "table": table_name},
                ).fetchall()
                actual_indexes = {row[0] for row in rows}
                missing = expected_indices - actual_indexes

                print(f"MISSING INDICES for table {table_name}:  {missing}")
                assert len(missing) == 0
