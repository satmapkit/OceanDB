from __future__ import annotations

from collections.abc import Callable
from datetime import datetime
from typing import Any, Literal

from OceanDB.OceanDB_Initializer import OceanDBInit

ProgressCallback = Callable[[dict[str, Any]], None]


def _create_eddy_etl():
    from OceanDB.etl.eddy_etl import EddyETL

    return EddyETL()


def ingest_eddy(
    only_ingest: Literal["both", "cyclonic", "anticyclonic"] = "both",
    offset_cyclonic: int = 0,
    offset_anticyclonic: int = 0,
    on_progress: ProgressCallback | None = None,
    init_database_if_not_exists: bool = False,
) -> dict[str, list[dict[str, Any]]]:
    return _create_eddy_etl().ingest(
        only_ingest=only_ingest,
        offset_cyclonic=offset_cyclonic,
        offset_anticyclonic=offset_anticyclonic,
        on_progress=on_progress,
        init_database_if_not_exists=init_database_if_not_exists,
    )


def create_all_indices() -> None:
    ocean_db_init = OceanDBInit()
    ocean_db_init.create_indices()
    ocean_db_init.create_eddy_indices()


def create_default_indices() -> None:
    ocean_db_init = OceanDBInit()
    ocean_db_init.create_default_indices()


def create_partitioned_along_track_index(
    index_name: str,
    start_date: datetime,
    end_date: datetime,
) -> dict[str, list[str] | str]:
    ocean_db_init = OceanDBInit()
    return ocean_db_init.create_along_track_index_by_partition(
        index_name,
        start_date,
        end_date,
    )


def drop_all_indices() -> None:
    ocean_db_init = OceanDBInit()
    ocean_db_init.drop_indices()
    ocean_db_init.drop_eddy_indices()
