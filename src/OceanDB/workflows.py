from __future__ import annotations

from collections.abc import Callable
from datetime import datetime
from pathlib import Path
from typing import Any, Literal

from OceanDB.commands.shared import AVISO_EDDY_FILENAMES
from OceanDB.OceanDB_Initializer import OceanDBInit

ProgressCallback = Callable[[dict[str, Any]], None]


def _emit(on_progress: ProgressCallback | None, event: dict[str, Any]) -> None:
    if on_progress is not None:
        on_progress(event)


def _create_along_track_etl(debug: bool = False):
    from OceanDB.etl.along_track_etl import AlongTrackETL

    return AlongTrackETL(debug=debug)


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
    ocean_db_init = OceanDBInit()
    database_initialized = (
        ocean_db_init.database_exists() and ocean_db_init.table_exists("eddy")
    )

    if not database_initialized:
        if not init_database_if_not_exists:
            raise RuntimeError(
                f"Database '{ocean_db_init.db_name}' is not initialized. "
                "Run database initialization first or set "
                "init_database_if_not_exists=True."
            )
        ocean_db_init.initialize_database()

    oceandb_etl = _create_eddy_etl()
    eddy_directory = Path(oceandb_etl.config.eddy_data_directory)
    processed_files: list[dict[str, Any]] = []

    ingest_specs = [
        ("cyclonic", AVISO_EDDY_FILENAMES[0], -1, offset_cyclonic),
        ("anticyclonic", AVISO_EDDY_FILENAMES[1], 1, offset_anticyclonic),
    ]
    for kind, filename, cyclonic_type, offset in ingest_specs:
        if only_ingest not in ("both", kind):
            continue

        filepath = eddy_directory / filename
        _emit(
            on_progress,
            {
                "type": "eddy_file_start",
                "kind": kind,
                "filename": filename,
                "filepath": filepath,
                "offset": offset,
            },
        )
        oceandb_etl.ingest_eddy_data_file(
            filepath,
            cyclonic_type=cyclonic_type,
            offset=offset,
        )
        processed_files.append(
            {"kind": kind, "filename": filename, "filepath": filepath, "offset": offset}
        )

    return {"processed_files": processed_files}


def ingest_along_track(
    missions: list[str] | tuple[str, ...],
    start_date: datetime | None = None,
    end_date: datetime | None = None,
    workers: int = 6,
    debug: bool = False,
    on_progress: ProgressCallback | None = None,
    init_database_if_not_exists: bool = False,
) -> dict[str, Any]:
    return _create_along_track_etl(debug=debug).ingest(
        missions=missions,
        start_date=start_date,
        end_date=end_date,
        workers=workers,
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
