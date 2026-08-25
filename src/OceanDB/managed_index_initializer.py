from collections.abc import Sequence
from datetime import datetime
from typing import LiteralString, cast

from dateutil.relativedelta import relativedelta
from psycopg import sql

from OceanDB.managed_index_oceandb import ManagedIndexOceanDB
from OceanDB.managed_indices import (PARTITIONED_ALONG_TRACK_INDEX_PATTERN,
                                     IndexDefinition)
from OceanDB.query_spec import RawSpec


class ManagedIndexInitializer(ManagedIndexOceanDB):
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

    def _create_index_group(self, indices: Sequence[IndexDefinition]) -> None:
        for index in indices:
            self.logger.info(f"Starting index creation for {index.name}")
            query = index.to_spec()
            self.execute_write_query(query)
            self.logger.info(f"Executing {index.name}")

    def create_indices(self):
        self._create_index_group(
            self.managed_indices.definitions_for_tables(
                "along_track", "basin", "basin_connections"
            )
        )

    def create_default_indices(self):
        self._create_index_group(self.managed_indices.default_definitions())

    def create_eddy_indices(self):
        self._create_index_group(self.managed_indices.definitions_for_tables("eddy"))

    def create_along_track_index_by_partition(
        self,
        logical_name: str,
        start_date: datetime,
        end_date: datetime,
    ) -> dict[str, list[str] | str]:
        index_info = self.managed_indices.partitionable_along_track_index_definition(
            logical_name
        )
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
        for index in self.managed_indices.drop_index_files:
            self._execute_raw_sql_file(index.filepath)
            self.logger.info(f"Dropping {index.name}")

    def drop_eddy_indices(self):
        for index in self.managed_indices.drop_eddy_index_files:
            self._execute_raw_sql_file(index.filepath)
            self.logger.info(f"Dropping {index.name}")
