import re
from datetime import datetime
from functools import cached_property

from dateutil.relativedelta import relativedelta
from sqlalchemy import text

from OceanDB.OceanDB import OceanDB

along_track_index_files = [
    {
        "name": "along_track_index_basin",
        "filepath": "indices/along_track/create_along_track_index_basin.sql",
        "params": {"index_name": "along_track_index_basin"},
    },
    {
        "name": "along_track_index_date",
        "filepath": "indices/along_track/create_along_track_index_date.sql",
        "params": {"index_name": "along_track_index_date"},
    },
    {
        "name": "along_track_index_filename",
        "filepath": "indices/along_track/create_along_track_index_filename.sql",
        "params": {"index_name": "along_track_index_filename"},
    },
    {
        "name": "along_track_index_mission",
        "filepath": "indices/along_track/create_along_track_index_mission.sql",
        "params": {"index_name": "along_track_index_mission"},
    },
    {
        "name": "along_track_index_point",
        "filepath": "indices/along_track/create_along_track_index_point.sql",
        "params": {"index_name": "along_track_index_point"},
    },
    {
        "name": "along_track_index_point_date",
        "filepath": "indices/along_track/create_along_track_index_point_date.sql",
        "params": {"index_name": "along_track_index_point_date"},
    },
    {
        "name": "along_track_index_point_date_mission",
        "filepath": "indices/along_track/create_along_track_index_point_date_mission.sql",
        "params": {"index_name": "along_track_index_point_date_mission"},
    },
    {
        "name": "along_track_index_point_date_mission_basin",
        "filepath": "indices/along_track/create_along_track_index_point_date_mission_basin.sql",
        "params": {"index_name": "along_track_index_point_date_mission_basin"},
    },
    {
        "name": "along_track_index_point_geom",
        "filepath": "indices/along_track/create_along_track_index_point_geom.sql",
        "params": {"index_name": "along_track_index_point_geom"},
    },
    {
        "name": "along_track_index_time",
        "filepath": "indices/along_track/create_along_track_index_time.sql",
        "params": {"index_name": "along_track_index_time"},
    },
]

basin_index_files = [
    {
        "name": "basin_connection_index_basin_id",
        "filepath": "indices/basin/create_basin_connection_index_basin_id.sql",
        "params": {"index_name": "basin_connection_index_basin_id"},
    },
    {
        "name": "basin_index_geom",
        "filepath": "indices/basin/create_basin_index_geom.sql",
        "params": {"index_name": "basin_index_geom"},
    },
]

sql_index_files = along_track_index_files + basin_index_files

eddy_index_files = [
    {
        "name": "eddy_index_point",
        "filepath": "indices/eddy/create_eddy_index_point.sql",
        "params": {"index_name": "eddy_index_point"},
    },
    {
        "name": "eddy_index_track_cyclonic_type",
        "filepath": "indices/eddy/create_eddy_index_track_cyclonic_type.sql",
        "params": {"index_name": "eddy_index_track_cyclonic_type"},
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

DEFAULT_INDEX_LOGICAL_NAMES = {
    "eddy_index_track_cyclonic_type",
    "along_track_index_point_date",
    "along_track_index_point_date_mission_basin",
    "basin_index_geom",
    "basin_connection_index_basin_id",
}

PARTITIONED_ALONG_TRACK_INDEX_PATTERN = re.compile(
    r"CREATE\s+INDEX\s+IF\s+NOT\s+EXISTS\s+(?P<index_name>\S+)\s+ON\s+"
    r"(?P<table_name>(?:public\.)?along_track)",
    flags=re.IGNORECASE,
)

INDEX_SQL_PATTERN = re.compile(
    r"CREATE\s+INDEX\s+IF\s+NOT\s+EXISTS\s+(?P<index_name>\S+)\s+ON\s+"
    r"(?P<table_name>(?:public\.)?[A-Za-z_][A-Za-z0-9_]*)",
    flags=re.IGNORECASE,
)


def normalize_sql(sql_statement: str) -> str:
    return " ".join(sql_statement.split())


def load_index_metadata(oceandb: OceanDB, index: dict[str, str]) -> dict[str, str]:
    sql_statement = oceandb.load_sql_file(index["filepath"])
    match = INDEX_SQL_PATTERN.search(sql_statement)
    if not match:
        raise ValueError(f"Unable to parse index SQL for '{index['name']}'")

    return {
        **index,
        "index_name": match.group("index_name").replace("public.", ""),
        "table_name": match.group("table_name").replace("public.", ""),
    }


class ManagedIndexOceanDB(OceanDB):
    def default_index_files(self) -> list[dict[str, str | dict[str, str]]]:
        return [
            index
            for index in sql_index_files + eddy_index_files
            if index["name"] in DEFAULT_INDEX_LOGICAL_NAMES
        ]

    def default_index_definitions(self) -> list[dict[str, str]]:
        return [
            index
            for index in self.managed_index_definitions
            if index["logical_name"] in DEFAULT_INDEX_LOGICAL_NAMES
        ]

    @cached_property
    def managed_index_definitions(self) -> list[dict[str, str]]:
        defined_rows = []
        for index in sql_index_files + eddy_index_files:
            metadata = load_index_metadata(self, index)
            raw_sql = self.load_sql_file(index["filepath"]).strip()
            defined_rows.append(
                {
                    "logical_name": index["name"],
                    "table_name": metadata["table_name"],
                    "index_name": metadata["index_name"],
                    "index_definition": normalize_sql(raw_sql),
                    "index_definition_multiline": raw_sql,
                    "filepath": index["filepath"],
                }
            )

        return sorted(
            defined_rows,
            key=lambda row: (row["table_name"], row["index_name"]),
        )

    def partitionable_along_track_index_definitions(self) -> list[dict[str, str]]:
        partitionable_indices = []

        for index in self.managed_index_definitions:
            if not index["filepath"].startswith("indices/along_track/"):
                continue

            match = PARTITIONED_ALONG_TRACK_INDEX_PATTERN.search(
                index["index_definition_multiline"]
            )
            if not match:
                raise ValueError(
                    f"Unable to parse partitioned index SQL for '{index['logical_name']}'"
                )

            partitionable_indices.append(
                {
                    "logical_name": index["logical_name"],
                    "filepath": index["filepath"],
                    "base_index_name": match.group("index_name"),
                }
            )

        return partitionable_indices

    def partitionable_along_track_index_definition(
        self, logical_name: str
    ) -> dict[str, str]:
        for index in self.partitionable_along_track_index_definitions():
            if index["logical_name"] == logical_name:
                return index

        if any(index["name"] == logical_name for index in sql_index_files):
            raise ValueError(
                f"Index '{logical_name}' is not available for partitioned creation"
            )

        raise ValueError(f"Unknown index '{logical_name}'")

    @cached_property
    def managed_index_names(self) -> set[str]:
        return {index["index_name"] for index in self.managed_index_definitions}

    @cached_property
    def partition_index_name_map(self) -> dict[str, str]:
        return self._load_partition_index_name_map()

    def _load_partition_index_name_map(self) -> dict[str, str]:
        with self.cursor() as cur:
            cur.execute("""
                SELECT
                    child_idx.relname AS child_index_name,
                    parent_idx.relname AS parent_index_name
                FROM pg_inherits inh
                JOIN pg_class child_idx
                    ON child_idx.oid = inh.inhrelid
                JOIN pg_class parent_idx
                    ON parent_idx.oid = inh.inhparent
                """)
            rows = cur.fetchall()

        return {
            child_index_name: parent_index_name
            for child_index_name, parent_index_name in rows
            if parent_index_name in self.managed_index_names
        }

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

    def _is_managed_index_name(self, index_name: str) -> bool:
        managed_names = self.managed_index_names
        if index_name in managed_names:
            return True

        for managed_name in managed_names:
            if index_name.startswith(f"{managed_name}_"):
                return True

        return False

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

    def show_index_definitions(
        self, identifier: str | None = None
    ) -> list[dict[str, str]]:
        index_rows = self.managed_index_definitions
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
