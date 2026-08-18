from __future__ import annotations

import time
from collections.abc import Callable
from datetime import datetime
from multiprocessing import Pool, TimeoutError
from pathlib import Path
from typing import Any, Literal

from OceanDB.commands.shared import AVISO_EDDY_FILENAMES
from OceanDB.OceanDB_Initializer import OceanDBInit

ProgressCallback = Callable[[dict[str, Any]], None]
EARLIEST_DATE = datetime(1990, 1, 1)


def _emit(on_progress: ProgressCallback | None, event: dict[str, Any]) -> None:
    if on_progress is not None:
        on_progress(event)


def _create_along_track_etl(debug: bool = False):
    from OceanDB.etl.along_track_etl import AlongTrackETL

    return AlongTrackETL(debug=debug)


def _create_eddy_etl():
    from OceanDB.etl.eddy_etl import EddyETL

    return EddyETL()


def initialize_database(
    partition_start: str = "1990-01-01",
    partition_end: str = "2025-11-01",
) -> dict[str, bool]:
    return OceanDBInit().initialize_database(partition_start, partition_end)


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
        initialize_database()

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


def _iter_year_months(start: datetime | None, end: datetime | None):
    if start is None:
        start = EARLIEST_DATE
    if end is None:
        end = datetime.now()
    if end < start:
        return

    year, month = start.year, start.month
    end_year, end_month = end.year, end.month
    while (year < end_year) or (year == end_year and month <= end_month):
        yield year, month
        month += 1
        if month == 13:
            month = 1
            year += 1


def discover_along_track_files(
    missions: list[str] | tuple[str, ...],
    start_date: datetime | None = None,
    end_date: datetime | None = None,
) -> dict[str, Any]:
    start_date = start_date.replace(tzinfo=None) if start_date is not None else None
    end_date = end_date.replace(tzinfo=None) if end_date is not None else None

    oceandb_etl = _create_along_track_etl()
    selected_missions = list(missions)

    if not selected_missions or (
        len(selected_missions) == 1 and selected_missions[0] == "all"
    ):
        selected_missions = list(oceandb_etl.missions)

    invalid_missions = [
        mission for mission in selected_missions if mission not in oceandb_etl.missions
    ]
    if invalid_missions:
        raise ValueError(
            f"received invalid arguments {invalid_missions}. "
            f"Received missions must be from the following list {oceandb_etl.missions}"
        )

    if start_date and end_date and end_date < start_date:
        raise ValueError("end_date must be >= start_date")

    year_months = (
        None
        if start_date is None and end_date is None
        else list(_iter_year_months(start_date, end_date))
    )
    prefix = "SEALEVEL_GLO_PHY_L3_MY_008_062"
    files: list[Path] = []

    for mission in selected_missions:
        file_structures = [
            f"cmems_obs-sl_glo_phy-ssh_my_{mission}-l3-duacs_PT1S_202411",
            f"cmems_obs-sl_glo_phy-ssh_my_{mission}-lr-l3-duacs_PT1S_202411",
        ]
        for structure in file_structures:
            ingest_directory = (
                Path(oceandb_etl.config.along_track_data_directory) / prefix / structure
            )
            if not ingest_directory.exists():
                continue
            if year_months is None:
                files.extend(ingest_directory.rglob("*.nc"))
                continue
            for year, month in year_months:
                month_dir = ingest_directory / f"{year:04d}" / f"{month:02d}"
                if month_dir.exists():
                    files.extend(month_dir.rglob("*.nc"))

    return {"missions": selected_missions, "files": files}


def ingest_along_track(
    missions: list[str] | tuple[str, ...],
    start_date: datetime | None = None,
    end_date: datetime | None = None,
    workers: int = 6,
    debug: bool = False,
    on_progress: ProgressCallback | None = None,
    init_database_if_not_exists: bool = False,
) -> dict[str, Any]:
    ocean_db_init = OceanDBInit()
    database_initialized = (
        ocean_db_init.database_exists() and ocean_db_init.table_exists("along_track")
    )

    if not database_initialized:
        if not init_database_if_not_exists:
            raise RuntimeError(
                f"Database '{ocean_db_init.db_name}' is not initialized. "
                "Run database initialization first or set "
                "init_database_if_not_exists=True."
            )
        initialize_database()

    discovery = discover_along_track_files(missions, start_date, end_date)
    nc_files: list[Path] = discovery["files"]
    if not nc_files:
        return {
            "missions": discovery["missions"],
            "matched_count": 0,
            "skipped_count": 0,
            "ingested_count": 0,
            "results": [],
            "duration_seconds": 0.0,
        }

    oceandb_etl = _create_along_track_etl(debug=debug)
    metadata_filenames = oceandb_etl.query_metadata()
    along_track_files = [
        file for file in nc_files if file.name not in metadata_filenames
    ]
    skipped_count = len(nc_files) - len(along_track_files)
    if not along_track_files:
        return {
            "missions": discovery["missions"],
            "matched_count": len(nc_files),
            "skipped_count": skipped_count,
            "ingested_count": 0,
            "results": [],
            "duration_seconds": 0.0,
        }

    _emit(
        on_progress,
        {
            "type": "along_track_start",
            "matched_count": len(nc_files),
            "skipped_count": skipped_count,
            "ingest_count": len(along_track_files),
            "ingest_mode": oceandb_etl.config.ingest_mode,
        },
    )

    start_ingest_time = time.perf_counter()
    completed = skipped_count
    total = len(nc_files)
    results_out: list[dict[str, Any]] = []
    multiprocessing_pool = Pool(workers)
    results = multiprocessing_pool.imap_unordered(
        oceandb_etl.process_along_track_file,
        along_track_files,
    )

    try:
        heartbeat_seconds = 30
        completed_new_files = 0
        total_new_files = len(along_track_files)

        while completed_new_files < total_new_files:
            try:
                result = results.next(timeout=heartbeat_seconds)
            except TimeoutError:
                _emit(
                    on_progress,
                    {
                        "type": "along_track_wait",
                        "completed": completed,
                        "total": total,
                        "completed_new_files": completed_new_files,
                        "total_new_files": total_new_files,
                        "active_workers": min(
                            workers, total_new_files - completed_new_files
                        ),
                    },
                )
                continue

            completed += 1
            completed_new_files += 1
            results_out.append(result)
            _emit(
                on_progress,
                {
                    "type": "along_track_file_complete",
                    "completed": completed,
                    "total": total,
                    "remaining": total - completed,
                    "result": result,
                },
            )
    finally:
        if multiprocessing_pool is not None:
            multiprocessing_pool.close()
            multiprocessing_pool.join()

    duration_seconds = time.perf_counter() - start_ingest_time
    return {
        "missions": discovery["missions"],
        "matched_count": len(nc_files),
        "skipped_count": skipped_count,
        "ingested_count": len(along_track_files),
        "results": results_out,
        "duration_seconds": duration_seconds,
    }


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
