from __future__ import annotations

from datetime import datetime

from OceanDB.cli_utils import format_status_line
from OceanDB.managed_index_oceandb import ManagedIndexOceanDB, ManagedIndices
from OceanDB.utils.logging import get_logger

logger = get_logger()


def create_basins_etl():
    from OceanDB.etl.basins_etl import BasinsETL

    return BasinsETL()


def create_along_track_etl(debug: bool = False):
    from OceanDB.etl.along_track_etl import AlongTrackETL

    return AlongTrackETL(debug=debug)


def create_eddy_etl():
    from OceanDB.etl.eddy_etl import EddyETL

    return EddyETL()


def create_copernicus_marine_client():
    from OceanDB.etl.copernicus_marine import OceanDBCopernicusMarine

    return OceanDBCopernicusMarine()


def create_managed_index_oceandb():
    return ManagedIndexOceanDB()


def create_managed_indices():
    return ManagedIndices()


def render_ingest_mode(mode: str) -> str:
    return format_status_line(
        "MODE",
        f"Using {mode.upper()} ingest mode.",
        label_color="magenta",
    )


def partitioned_index_choices() -> list[str]:
    return [
        index["logical_name"]
        for index in create_managed_indices().partitionable_along_track_index_definitions()
    ]


def defined_index_choices() -> list[str]:
    return [index["logical_name"] for index in create_managed_indices().definitions]


def format_duration(seconds: float) -> str:
    minutes, seconds = divmod(seconds, 60)
    hours, minutes = divmod(int(minutes), 60)
    if hours:
        return f"{hours}h {minutes}m {seconds:.1f}s"
    if minutes:
        return f"{minutes}m {seconds:.1f}s"
    return f"{seconds:.1f}s"


def format_timestamp(value: datetime | None) -> str:
    if value is None:
        return "-"
    return value.strftime("%Y-%m-%d %H:%M:%S")


def timestamp_now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")
