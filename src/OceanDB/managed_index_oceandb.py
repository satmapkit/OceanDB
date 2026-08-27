import re
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from functools import cached_property

from dateutil.relativedelta import relativedelta
from psycopg import sql
from psycopg.rows import class_row
from sqlalchemy import text

from OceanDB.base_write_query import BaseWriteQuery
from OceanDB.managed_indices import (DEFAULT_INDEX_NAMES, INDEX_RESOURCES,
                                     INDEX_SQL_PATTERN,
                                     PARTITIONED_ALONG_TRACK_INDEX_PATTERN,
                                     IndexDefinition, ManagedIndices,
                                     normalize_sql)
from OceanDB.query_spec import RawSpec

__all__ = [
    "DEFAULT_INDEX_NAMES",
    "INDEX_RESOURCES",
    "INDEX_SQL_PATTERN",
    "PARTITIONED_ALONG_TRACK_INDEX_PATTERN",
    "DatabaseIndex",
    "IndexDefinition",
    "ManagedIndexOceanDB",
    "ManagedIndices",
    "normalize_sql",
]


@dataclass(frozen=True)
class DatabaseIndex:
    schema_name: str
    table_name: str
    index_name: str
    index_definition: str
    access_method: str
    index_kind: str
    table_kind: str
    is_unique: bool
    is_primary: bool
    is_valid: bool
    is_ready: bool
    constraint_name: str | None
    constraint_type: str | None
    parent_index_name: str | None
    parent_table_name: str | None

    @property
    def is_constraint_owned(self) -> bool:
        return self.constraint_name is not None

    @property
    def is_partitioned_parent(self) -> bool:
        return self.index_kind == "I"

    @property
    def is_attached_partition_index(self) -> bool:
        return self.parent_index_name is not None

    @property
    def is_standalone_partition_index(self) -> bool:
        return self.parent_table_name is not None and self.parent_index_name is None


class ManagedIndexOceanDB(BaseWriteQuery):
    def __init__(self, config=None, managed_indices: ManagedIndices | None = None):
        super().__init__(config=config)
        self.managed_indices = managed_indices or ManagedIndices()

    def create_indexes(self, definitions: Sequence[IndexDefinition]) -> None:
        for definition in definitions:
            self.logger.info(f"Starting index creation for {definition.name}")
            self.execute_write_query(definition.create_spec())
            self.logger.info(f"Executing {definition.name}")

        self.__dict__.pop("partition_index_name_map", None)

    def drop_indexes(self, schema_name: str = "public") -> None:
        indexes = (
            index
            for index in self.inventory_indexes(schema_name)
            if self._is_managed_index_name(index.index_name)
            and not index.is_constraint_owned
            and not index.is_attached_partition_index
        )
        for index in sorted(indexes, key=lambda index: index.is_partitioned_parent):
            query = sql.SQL("DROP INDEX IF EXISTS {}").format(
                sql.Identifier(schema_name, index.index_name)
            )
            self.execute_write_query(RawSpec(query))

        self.__dict__.pop("partition_index_name_map", None)

    @cached_property
    def partition_index_name_map(self) -> dict[str, str]:
        return self._load_partition_index_name_map()

    def _load_partition_index_name_map(self) -> dict[str, str]:
        return {
            index.index_name: index.parent_index_name
            for index in self.inventory_indexes()
            if index.parent_index_name in self.managed_indices.managed_index_names
        }

    def inventory_indexes(
        self, schema_name: str = "public"
    ) -> tuple[DatabaseIndex, ...]:
        with self.cursor(row_factory=class_row(DatabaseIndex)) as cur:
            cur.execute(
                """
                SELECT
                    namespace.nspname AS schema_name,
                    indexed_table.relname AS table_name,
                    index_relation.relname AS index_name,
                    pg_get_indexdef(index_relation.oid) AS index_definition,
                    access_method.amname AS access_method,
                    index_relation.relkind::text AS index_kind,
                    indexed_table.relkind::text AS table_kind,
                    index_metadata.indisunique AS is_unique,
                    index_metadata.indisprimary AS is_primary,
                    index_metadata.indisvalid AS is_valid,
                    index_metadata.indisready AS is_ready,
                    owning_constraint.conname AS constraint_name,
                    owning_constraint.contype::text AS constraint_type,
                    parent_index.relname AS parent_index_name,
                    parent_table.relname AS parent_table_name
                FROM pg_index index_metadata
                JOIN pg_class index_relation
                    ON index_relation.oid = index_metadata.indexrelid
                JOIN pg_class indexed_table
                    ON indexed_table.oid = index_metadata.indrelid
                JOIN pg_namespace namespace
                    ON namespace.oid = indexed_table.relnamespace
                JOIN pg_am access_method
                    ON access_method.oid = index_relation.relam
                LEFT JOIN pg_constraint owning_constraint
                    ON owning_constraint.conindid = index_relation.oid
                LEFT JOIN pg_inherits index_inheritance
                    ON index_inheritance.inhrelid = index_relation.oid
                LEFT JOIN pg_class parent_index
                    ON parent_index.oid = index_inheritance.inhparent
                LEFT JOIN pg_inherits table_inheritance
                    ON table_inheritance.inhrelid = indexed_table.oid
                LEFT JOIN pg_class parent_table
                    ON parent_table.oid = table_inheritance.inhparent
                WHERE namespace.nspname = %(schema_name)s
                ORDER BY indexed_table.relname, index_relation.relname
                """,
                {"schema_name": schema_name},
            )
            return tuple(cur.fetchall())

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

    def _is_managed_index_name(self, index_name: str) -> bool:
        managed_names = self.managed_indices.managed_index_names
        if index_name in managed_names:
            return True

        for managed_name in managed_names:
            if index_name.startswith(f"{managed_name}_"):
                return True

        return False

    def list_indices(
        self, schema_name: str = "public", managed_only: bool = True
    ) -> list[dict[str, str]]:
        index_rows = [
            {
                "schema_name": index.schema_name,
                "table_name": index.table_name,
                "index_name": index.index_name,
                "index_definition": index.index_definition,
            }
            for index in self.inventory_indexes(schema_name=schema_name)
        ]

        if not managed_only:
            return index_rows

        return [
            row for row in index_rows if self._is_managed_index_name(row["index_name"])
        ]

    def show_index_definitions(
        self, identifier: str | None = None
    ) -> list[dict[str, str]]:
        index_rows = self.managed_indices.definitions
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

    def _parse_along_track_partition_name(self, partition_name: str) -> datetime:
        match = re.fullmatch(r"along_track_(\d{4})_(\d{2})", partition_name)
        if not match:
            raise ValueError(f"Invalid along-track partition name '{partition_name}'")
        year = int(match.group(1))
        month = int(match.group(2))
        return datetime(year, month, 1)

    def show_partitioned_index_ranges(
        self, logical_name: str | None = None
    ) -> list[dict[str, str | int | None]]:
        logical_names = (
            [logical_name]
            if logical_name is not None
            else self.managed_indices.list_partitionable_along_track_indices()
        )

        managed_rows = self.list_indices(managed_only=True)
        rows_by_logical_name: list[dict[str, str | int | None]] = []

        for current_logical_name in logical_names:
            index_info = (
                self.managed_indices.partitionable_along_track_index_definition(
                    current_logical_name
                )
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

            ranges: list[list[str]] = []
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
