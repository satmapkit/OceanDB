import re
from collections.abc import Collection, Sequence
from dataclasses import dataclass
from datetime import datetime
from functools import cache, cached_property
from typing import LiteralString

from dateutil.relativedelta import relativedelta
from sqlalchemy import text

from OceanDB.OceanDB import OceanDB
from OceanDB.resource_loader import ResourceLoader


@dataclass(frozen=True)
class IndexDefinition:
    name: str
    table: str
    create_sql: LiteralString


@dataclass(frozen=True)
class DropIndexFile:
    name: str
    filepath: str


INDEX_RESOURCES = (
    "indices/along_track/create_along_track_index_basin.sql",
    "indices/along_track/create_along_track_index_date.sql",
    "indices/along_track/create_along_track_index_filename.sql",
    "indices/along_track/create_along_track_index_mission.sql",
    "indices/along_track/create_along_track_index_point.sql",
    "indices/along_track/create_along_track_index_point_date.sql",
    "indices/along_track/create_along_track_index_point_date_mission.sql",
    "indices/along_track/create_along_track_index_point_date_mission_basin.sql",
    "indices/along_track/create_along_track_index_point_geom.sql",
    "indices/along_track/create_along_track_index_time.sql",
    "indices/basin/create_basin_connection_index_basin_id.sql",
    "indices/basin/create_basin_index_geom.sql",
    "indices/eddy/create_eddy_index_point.sql",
    "indices/eddy/create_eddy_index_track_cyclonic_type.sql",
)

DROP_INDEX_FILES: list[DropIndexFile] = [
    DropIndexFile(
        name="along_track_basin_idx",
        filepath="drop/drop_along_track_index_basin.sql",
    ),
    DropIndexFile(
        name="along_track_date_idx",
        filepath="drop/drop_along_track_index_date.sql",
    ),
    DropIndexFile(
        name="along_track_file_name_idx",
        filepath="drop/drop_along_track_index_filename.sql",
    ),
    DropIndexFile(
        name="along_track_mission_idx",
        filepath="drop/drop_along_track_index_mission.sql",
    ),
    DropIndexFile(
        name="along_track_point_idx",
        filepath="drop/drop_along_track_index_point.sql",
    ),
    DropIndexFile(
        name="along_track_point_date_idx",
        filepath="drop/drop_along_track_index_point_date.sql",
    ),
    DropIndexFile(
        name="along_track_point_date_mission_idx",
        filepath="drop/drop_along_track_index_point_date_mission.sql",
    ),
    DropIndexFile(
        name="along_track_point_date_mission_basin_idx",
        filepath="drop/drop_along_track_index_point_date_mission_basin.sql",
    ),
    DropIndexFile(
        name="along_track_point_geom_idx",
        filepath="drop/drop_along_track_index_point_geom.sql",
    ),
    DropIndexFile(
        name="along_track_time_idx",
        filepath="drop/drop_along_track_index_time.sql",
    ),
    DropIndexFile(
        name="basin_id_idx",
        filepath="drop/drop_basin_connection_index_basin_id.sql",
    ),
    DropIndexFile(
        name="basin_geog_idx",
        filepath="drop/drop_basin_index_geom.sql",
    ),
]

DROP_EDDY_INDEX_FILES: list[DropIndexFile] = [
    DropIndexFile(
        name="eddy_point_idx",
        filepath="drop/drop_eddy_index_point.sql",
    ),
    DropIndexFile(
        name="track_times_cyclonic_type_idx",
        filepath="drop/drop_eddy_index_track_cyclonic_type.sql",
    ),
]

DEFAULT_INDEX_NAMES = frozenset(
    {
        "track_times_cyclonic_type_idx",
        "along_track_point_date_idx",
        "along_track_point_date_mission_basin_idx",
        "basin_geog_idx",
        "basin_id_idx",
    }
)

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


class ManagedIndices(ResourceLoader):
    """
    Helper for loading and describing OceanDB-managed index definitions.
    """

    def __init__(
        self,
        index_resources: Sequence[str] | None = None,
        drop_index_files: list[DropIndexFile] | None = None,
        drop_eddy_index_files: list[DropIndexFile] | None = None,
        default_indices: Collection[str] | None = None,
    ):
        self.index_resources = tuple(index_resources or INDEX_RESOURCES)
        self.drop_index_files = drop_index_files or DROP_INDEX_FILES
        self.drop_eddy_index_files = drop_eddy_index_files or DROP_EDDY_INDEX_FILES
        self.default_indices = default_indices or DEFAULT_INDEX_NAMES

    @cached_property
    def index_definitions(self) -> tuple[IndexDefinition, ...]:
        definitions = tuple(
            self._load_index_definition(filepath) for filepath in self.index_resources
        )
        return tuple(
            sorted(
                definitions,
                key=lambda definition: (definition.table, definition.name),
            )
        )

    def _load_index_definition(self, filepath: str) -> IndexDefinition:
        create_sql = self.load_sql_file(filepath)
        match = INDEX_SQL_PATTERN.search(create_sql)
        if not match:
            raise ValueError(f"Unable to parse index SQL resource '{filepath}'")

        return IndexDefinition(
            name=match.group("index_name").replace("public.", ""),
            table=match.group("table_name").replace("public.", ""),
            create_sql=create_sql,
        )

    def definitions_for_tables(self, *tables: str) -> tuple[IndexDefinition, ...]:
        return tuple(
            definition
            for definition in self.index_definitions
            if definition.table in tables
        )

    @cache
    def default_definitions(self) -> tuple[IndexDefinition, ...]:
        return tuple(
            definition
            for definition in self.index_definitions
            if definition.name in self.default_indices
        )

    @cache
    def default_index_definitions(self) -> list[dict[str, str]]:
        return [
            index
            for index in self.definitions
            if index["logical_name"] in self.default_indices
        ]

    @cached_property
    def definitions(self) -> list[dict[str, str]]:
        resource_by_name = {
            self._load_index_definition(filepath).name: filepath
            for filepath in self.index_resources
        }
        return [
            {
                "logical_name": definition.name,
                "table_name": definition.table,
                "index_name": definition.name,
                "index_definition": normalize_sql(definition.create_sql),
                "index_definition_multiline": definition.create_sql.strip(),
                "filepath": resource_by_name.get(definition.name, ""),
            }
            for definition in self.index_definitions
        ]

    def partitionable_along_track_index_definitions(self) -> list[dict[str, str]]:
        partitionable_indices = []

        for definition in self.definitions:
            if definition["table_name"] != "along_track":
                continue

            match = PARTITIONED_ALONG_TRACK_INDEX_PATTERN.search(
                definition["index_definition_multiline"]
            )
            if not match:
                raise ValueError(
                    f"Unable to parse partitioned index SQL for "
                    f"'{definition['logical_name']}'"
                )

            partitionable_indices.append(
                {
                    "logical_name": definition["logical_name"],
                    "filepath": definition["filepath"],
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

        if any(
            definition.name == logical_name for definition in self.index_definitions
        ):
            raise ValueError(
                f"Index '{logical_name}' is not available for partitioned creation"
            )

        raise ValueError(f"Unknown index '{logical_name}'")

    @cached_property
    def managed_index_names(self) -> set[str]:
        return {definition.name for definition in self.index_definitions}

    def list_partitionable_along_track_indices(self) -> list[str]:
        return [
            index["logical_name"]
            for index in self.partitionable_along_track_index_definitions()
        ]


class ManagedIndexOceanDB(OceanDB):
    def __init__(self, config=None, managed_indices: ManagedIndices | None = None):
        super().__init__(config=config)
        self.managed_indices = managed_indices or ManagedIndices()

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
            if parent_index_name in self.managed_indices.managed_index_names
        }

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
        rows_by_logical_name = []

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
