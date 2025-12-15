from io import BytesIO
from pathlib import Path
import psycopg as pg
from psycopg import sql
import os
import yaml
from importlib import resources
import time
import pandas as pd
from typing import IO
from datetime import datetime
from dateutil.relativedelta import relativedelta

from sqlalchemy import text

from OceanDB.OceanDB import OceanDB

table_definitions = [
    # {
    #     "name": "basin",
    #     "filepath": "tables/create_basin_table.sql",
    #     "params": {"table_name": "basin"},
    # },
    # {
    #     "name": "along_track_metadata",
    #     "filepath": "tables/create_along_track_metadata_table.sql",
    #     "params": {"table_name": "along_track_metadata"},
    # },
    # {
    #     "name": "along_track",
    #     "filepath": "tables/create_along_track_table.sql",
    #     "params": {"table_name": "along_track"},
    # },
    # {
    #     "name": "basin_connection",
    #     "filepath": "tables/create_basin_connection_table.sql",
    #     "params": {"table_name": "basin_connection"},
    # }
    #

    {
        "name": "eddy",
        "filepath": "tables/create_eddy_table.sql",
        "params": {"table_name": "eddy"},
    },
    {
        "name": "chelton_eddy",
        "filepath": "tables/create_chelton_eddy_table.sql",
        "params": {"table_name": "chelton_eddy"},
    },

]

sql_index_files = [
    {
        "name": "along_track_index_basin",
        "filepath": "indices/create_along_track_index_basin.sql",
        "params": {"index_name": "along_track_index_basin"},
    },
    {
        "name": "along_track_index_date",
        "filepath": "indices/create_along_track_index_date.sql",
        "params": {"index_name": "along_track_index_date"},
    },
    {
        "name": "along_track_index_filename",
        "filepath": "indices/create_along_track_index_filename.sql",
        "params": {"index_name": "along_track_index_filename"},
    },
    {
        "name": "along_track_index_mission",
        "filepath": "indices/create_along_track_index_mission.sql",
        "params": {"index_name": "along_track_index_mission"},
    },
    {
        "name": "along_track_index_point",
        "filepath": "indices/create_along_track_index_point.sql",
        "params": {"index_name": "along_track_index_point"},
    },
    {
        "name": "along_track_index_point_date",
        "filepath": "indices/create_along_track_index_point_date.sql",
        "params": {"index_name": "along_track_index_point_date"},
    },
    {
        "name": "along_track_index_point_date_mission",
        "filepath": "indices/create_along_track_index_point_date_mission.sql",
        "params": {"index_name": "along_track_index_point_date_mission"},
    },
    {
        "name": "along_track_index_point_date_mission_basin",
        "filepath": "indices/create_along_track_index_point_date_mission_basin.sql",
        "params": {"index_name": "along_track_index_point_date_mission_basin"},
    },
    {
        "name": "along_track_index_point_geom",
        "filepath": "indices/create_along_track_index_point_geom.sql",
        "params": {"index_name": "along_track_index_point_geom"},
    },
    {
        "name": "along_track_index_time",
        "filepath": "indices/create_along_track_index_time.sql",
        "params": {"index_name": "along_track_index_time"},
    },
    {
        "name": "basin_connection_index_basin_id",
        "filepath": "indices/create_basin_connection_index_basin_id.sql",
        "params": {"index_name": "basin_connection_index_basin_id"},
    },
    {
        "name": "basin_index_geom",
        "filepath": "indices/create_basin_index_geom.sql",
        "params": {"index_name": "basin_index_geom"},
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



EXPECTED_TABLE_INDEXES = {
    # "along_track": {
    #     "along_track_basin_idx",
    #     "along_track_date_idx",
    #     "along_track_file_name_idx",
    #     "along_track_mission_idx",
    #     "along_track_point_idx",
    #     "along_track_point_date_idx",
    #     "along_track_point_date_mission_idx",
    #     "along_track_point_date_mission_basin_idx",
    #     "along_track_point_geom_idx",
    #     "along_track_time_idx",
    # },
    # "basin": {
    #     "basin_geog_idx",
    # },
    # "basin_connections": {
    #     "basin_id_idx",
    # },
    "chelton_eddy": {
        "chelton_eddy_point_idx",
        "chelton_track_times_cyclonic_type_idx",
    },
    "eddy": {
        "eddy_point_idx",
        "track_times_cyclonic_type_idx",
    },
}


class OceanDBInit(OceanDB):
    def __init__(self):
        super().__init__()

    def create_database(self):
        # Create the Database
        with pg.connect(self.config.postgres_dsn_admin) as conn:
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (self.db_name,))
                if cur.fetchone():
                    print(f"Database '{self.db_name}' already exists.")
                    return
                cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(self.db_name)))
                print(f"Database '{self.db_name}' created successfully.")

        ## Enable POSTGIS extensions
        with pg.connect(self.config.postgres_dsn) as atdb_conn:
            with atdb_conn.cursor() as atdb_cur:
                atdb_cur.execute(sql.SQL("CREATE EXTENSION IF NOT EXISTS plpgsql;"))
                atdb_cur.execute(sql.SQL("CREATE EXTENSION IF NOT EXISTS postgis;"))
                atdb_cur.execute(sql.SQL("CREATE EXTENSION IF NOT EXISTS btree_gist;"))
                atdb_conn.commit()
        print(f"Database '{self.db_name}' POSTGIS enabled.")

    def create_tables(self):
        for table in table_definitions:
            try:
                table_name = table['name']
                query = self.parametrize_sql_statements(table)
                self.execute_query(table, query)
                self.logger.info(f"Executing {table_name}")
            except Exception as ex:
                self.logger.info(f"{table_name}")
                self.logger.info(ex)
    #
    def create_indices(self):
        for index in sql_index_files:
            table_name = index['name']
            query = self.parametrize_sql_statements(index)
            self.execute_query(index, query)
            self.logger.info(f"Executing {table_name}")

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

        query_filepath = "tables/create_along_track_table_partition.sql"

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
            query = sql.SQL(sql_statement).format(**safe_params)
            self.execute_query(query=query, table=partition_name)

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
        query_filepath = table['filepath']
        query_params = table['params']
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
        return query

    def load_sql(self, filename: str) -> str:
        # with resources.files(self.sql_pkg).joinpath(filename).open("r", encoding="utf-8") as f:
        #     tokenized_query = f.read()
        with self.load_module_file( "OceanDB.sql", filename, mode="r", encoding="utf-8") as f:
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
