import re
from datetime import datetime
from typing import LiteralString, Mapping, Sequence, cast

from dateutil.relativedelta import relativedelta
from psycopg import sql
from sqlalchemy import text

from OceanDB.base_write_query import BaseWriteQuery
from OceanDB.managed_index_oceandb import (
    PARTITIONED_ALONG_TRACK_INDEX_PATTERN, eddy_index_files, sql_index_files)
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


drop_index_files = [
    {
        "name": "along_track_index_basin",
        "filepath": "drop/drop_along_track_index_basin.sql",
    },
    {
        "name": "along_track_index_date",
        "filepath": "drop/drop_along_track_index_date.sql",
    },
    {
        "name": "along_track_index_filename",
        "filepath": "drop/drop_along_track_index_filename.sql",
    },
    {
        "name": "along_track_index_mission",
        "filepath": "drop/drop_along_track_index_mission.sql",
    },
    {
        "name": "along_track_index_point",
        "filepath": "drop/drop_along_track_index_point.sql",
    },
    {
        "name": "along_track_index_point_date",
        "filepath": "drop/drop_along_track_index_point_date.sql",
    },
    {
        "name": "along_track_index_point_date_mission",
        "filepath": "drop/drop_along_track_index_point_date_mission.sql",
    },
    {
        "name": "along_track_index_point_date_mission_basin",
        "filepath": "drop/drop_along_track_index_point_date_mission_basin.sql",
    },
    {
        "name": "along_track_index_point_geom",
        "filepath": "drop/drop_along_track_index_point_geom.sql",
    },
    {
        "name": "along_track_index_time",
        "filepath": "drop/drop_along_track_index_time.sql",
    },
    {
        "name": "basin_connection_index_basin_id",
        "filepath": "drop/drop_basin_connection_index_basin_id.sql",
    },
    {"name": "basin_index_geom", "filepath": "drop/drop_basin_index_geom.sql"},
]


drop_eddy_index_files = [
    {"name": "eddy_index_point", "filepath": "drop/drop_eddy_index_point.sql"},
    {
        "name": "eddy_index_track_cyclonic_type",
        "filepath": "drop/drop_eddy_index_track_cyclonic_type.sql",
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
    def _normalize_month_start(self, value: datetime) -> datetime:
        return value.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

    def _iter_partition_months(
        self, start_date: datetime, end_date: datetime
    ) -> list[datetime]:
        start_month = self._normalize_month_start(start_date)
        end_month = self._normalize_month_start(end_date)
        if end_month < start_month:
            raise ValueError("end_date must be greater than or equal to start_date")

        months = []
        current = start_month
        while current <= end_month:
            months.append(current)
            current = (current + relativedelta(months=1)).replace(day=1)
        return months

    def _along_track_partition_name(self, value: datetime) -> str:
        return f"along_track_{value.year}_{value.month:02d}"

    def _parse_along_track_partition_name(self, partition_name: str) -> datetime:
        match = re.fullmatch(r"along_track_(\d{4})_(\d{2})", partition_name)
        if not match:
            raise ValueError(f"Invalid along-track partition name '{partition_name}'")
        year = int(match.group(1))
        month = int(match.group(2))
        return datetime(year, month, 1)

    def _is_managed_index_name(self, index_name: str) -> bool:
        managed_names = self.managed_index_names()
        if index_name in managed_names:
            return True

        for managed_name in managed_names:
            if index_name.startswith(f"{managed_name}_"):
                return True

        return False

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
            out = cur.fetchone()
            if out is None:
                raise ValueError("Bad result from database when looking for database")
            exists = out[0]
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
            table_name = table["name"]
            try:
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

    def _create_index_group(
        self, indices: Sequence[Mapping[str, str | dict[str, str]]]
    ) -> None:
        for index in indices:
            table_name = index["name"]
            self.logger.info(f"Starting index creation for {table_name}")
            query = self.parametrize_sql_statements(index)
            self.execute_write_query(query)
            self.logger.info(f"Executing {table_name}")

    def create_indices(self):
        self._create_index_group(sql_index_files)

    def create_default_indices(self):
        self._create_index_group(self.default_index_files())

    def create_eddy_indices(self):
        self._create_index_group(eddy_index_files)

    def list_partitionable_along_track_indices(self) -> list[str]:
        return [
            index["logical_name"]
            for index in self.partitionable_along_track_index_definitions()
        ]

    def list_along_track_partitions(self) -> list[str]:
        engine = self.get_engine()

        with engine.connect() as conn:
            rows = conn.execute(text("""
                    SELECT child.relname
                    FROM pg_inherits
                    JOIN pg_class parent ON pg_inherits.inhparent = parent.oid
                    JOIN pg_class child ON pg_inherits.inhrelid = child.oid
                    JOIN pg_namespace ns ON child.relnamespace = ns.oid
                    WHERE parent.relname = 'along_track'
                      AND ns.nspname = 'public'
                    ORDER BY child.relname
                """)).fetchall()

        return [row[0] for row in rows]

    def create_along_track_index_by_partition(
        self,
        logical_name: str,
        start_date: datetime,
        end_date: datetime,
    ) -> dict[str, list[str] | str]:
        index_info = self.partitionable_along_track_index_definition(logical_name)
        sql_statement = self.load_sql_file(index_info["filepath"])
        existing_partitions = set(self.list_along_track_partitions())

        created_partitions = []
        missing_partitions = []

        for month in self._iter_partition_months(start_date, end_date):
            partition_name = self._along_track_partition_name(month)
            if partition_name not in existing_partitions:
                missing_partitions.append(partition_name)
                continue

            partition_index_name = (
                f"{index_info['base_index_name']}_{month.year}_{month.month:02d}"
            )
            partition_sql = PARTITIONED_ALONG_TRACK_INDEX_PATTERN.sub(
                (
                    f"CREATE INDEX IF NOT EXISTS {partition_index_name} "
                    f"ON public.{partition_name}"
                ),
                sql_statement,
                count=1,
            )
            self.logger.info(
                f"Starting index creation for {partition_index_name} on partition {partition_name}"
            )
            partition_statement = cast(LiteralString, partition_sql)
            partition_query = cast(sql.Composed, sql.SQL(partition_statement))
            self.execute_write_query(RawSpec(partition_query))
            self.logger.info(f"Executing {logical_name} for partition {partition_name}")
            created_partitions.append(partition_name)

        return {
            "logical_name": logical_name,
            "base_index_name": index_info["base_index_name"],
            "created_partitions": created_partitions,
            "missing_partitions": missing_partitions,
        }

    def _execute_raw_sql_file(self, filepath: str):
        sql_statement = self.load_sql_file(filepath)
        raw_query = cast(sql.Composed, sql.SQL(sql_statement))
        query = RawSpec(raw_query)
        self.execute_write_query(query)

    def drop_indices(self):
        for index in drop_index_files:
            self._execute_raw_sql_file(index["filepath"])
            self.logger.info(f"Dropping {index['name']}")

    def drop_eddy_indices(self):
        for index in drop_eddy_index_files:
            self._execute_raw_sql_file(index["filepath"])
            self.logger.info(f"Dropping {index['name']}")

    def list_indices(
        self, schema_name: str = "public", managed_only: bool = True
    ) -> list[dict[str, str]]:
        engine = self.get_engine()

        with engine.connect() as conn:
            rows = conn.execute(
                text("""
                    SELECT schemaname, tablename, indexname, indexdef
                    FROM pg_indexes
                    WHERE schemaname = :schema
                    ORDER BY tablename, indexname
                """),
                {"schema": schema_name},
            ).fetchall()

        index_rows = [
            {
                "schema_name": row[0],
                "table_name": row[1],
                "index_name": row[2],
                "index_definition": row[3],
            }
            for row in rows
        ]

        if not managed_only:
            return index_rows

        return [
            row for row in index_rows if self._is_managed_index_name(row["index_name"])
        ]

    def list_defined_indices(self) -> list[dict[str, str]]:
        return self.managed_index_definitions()

    def show_index_definitions(
        self, identifier: str | None = None
    ) -> list[dict[str, str]]:
        index_rows = self.list_defined_indices()
        if identifier is None:
            return index_rows

        matching_rows = [
            row
            for row in index_rows
            if row["logical_name"] == identifier or row["index_name"] == identifier
        ]
        if not matching_rows:
            raise ValueError(f"Unknown index '{identifier}'")
        return matching_rows

    def show_partitioned_index_ranges(
        self, logical_name: str | None = None
    ) -> list[dict[str, str | int | None]]:
        logical_names = (
            [logical_name]
            if logical_name is not None
            else self.list_partitionable_along_track_indices()
        )

        managed_rows = self.list_indices(managed_only=True)
        rows_by_logical_name = []

        for current_logical_name in logical_names:
            index_info = self.partitionable_along_track_index_definition(
                current_logical_name
            )
            base_index_name = index_info["base_index_name"]
            matching_rows = [
                row
                for row in managed_rows
                if row["table_name"].startswith("along_track_")
                and row["index_name"].startswith(f"{base_index_name}_")
            ]

            partition_names = sorted(row["table_name"] for row in matching_rows)
            if not partition_names:
                rows_by_logical_name.append(
                    {
                        "logical_name": current_logical_name,
                        "base_index_name": base_index_name,
                        "range_number": 0,
                        "partition_count": 0,
                        "start_partition": None,
                        "end_partition": None,
                    }
                )
                continue

            current_range = [partition_names[0]]
            current_previous_date = self._parse_along_track_partition_name(
                partition_names[0]
            )

            ranges = []
            for partition_name in partition_names[1:]:
                current_date = self._parse_along_track_partition_name(partition_name)
                next_expected_date = (
                    current_previous_date + relativedelta(months=1)
                ).replace(day=1)

                if current_date == next_expected_date:
                    current_range.append(partition_name)
                else:
                    ranges.append(current_range)
                    current_range = [partition_name]

                current_previous_date = current_date

            ranges.append(current_range)

            for range_number, partition_range in enumerate(ranges, start=1):
                rows_by_logical_name.append(
                    {
                        "logical_name": current_logical_name,
                        "base_index_name": base_index_name,
                        "range_number": range_number,
                        "partition_count": len(partition_range),
                        "start_partition": partition_range[0],
                        "end_partition": partition_range[-1],
                    }
                )

        return rows_by_logical_name

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

        sql_statement = self.load_sql_file(query_filepath)

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
        sql_statement = self.load_sql_file(query_filepath)
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
