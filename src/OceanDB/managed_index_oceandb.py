import re

from OceanDB.OceanDB import OceanDB

sql_index_files = [
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

        for index in self.managed_index_definitions():
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

    def managed_index_names(self) -> set[str]:
        return {index["index_name"] for index in self.managed_index_definitions()}
