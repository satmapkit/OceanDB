from __future__ import annotations

from datetime import datetime

from OceanDB.OceanDB_Initializer import OceanDBInit


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
