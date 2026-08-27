import re
from collections.abc import Collection, Sequence
from dataclasses import dataclass
from functools import cache, cached_property
from typing import LiteralString

from psycopg import sql

from OceanDB.query_spec import RawSpec
from OceanDB.resource_loader import ResourceLoader


@dataclass(frozen=True)
class IndexDefinition:
    name: str
    table: str
    create_sql: LiteralString

    def create_spec(self):
        return RawSpec(sql.SQL(self.create_sql).format())


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
        index_resources: Sequence[str] = INDEX_RESOURCES,
        default_indices: Collection[str] = DEFAULT_INDEX_NAMES,
    ):
        self.index_resources = tuple(index_resources)
        self.default_indices = tuple(default_indices)

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
