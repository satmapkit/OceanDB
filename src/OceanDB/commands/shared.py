from __future__ import annotations

from datetime import datetime
from pathlib import Path

import click

from OceanDB.OceanDB_Initializer import eddy_index_files, sql_index_files
from OceanDB.cli_utils import format_key_value, format_status_line
from OceanDB.utils.logging import get_logger

logger = get_logger()

AVISO_HOST = "ftp-access.aviso.altimetry.fr"
AVISO_PORT = 2221
AVISO_EDDY_REMOTE_DIRECTORY = (
    "value-added/eddy-trajectory/delayed-time/META3.2_DT_allsat"
)
AVISO_EDDY_FILENAMES = [
    "META3.2_DT_allsat_Cyclonic_long_19930101_20220209.nc",
    "META3.2_DT_allsat_Anticyclonic_long_19930101_20220209.nc",
]
EARLIEST_DATE = datetime(1990, 1, 1)


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


def render_ingest_mode(mode: str) -> str:
    return format_status_line(
        "MODE",
        f"Using {mode.upper()} ingest mode.",
        label_color="magenta",
    )


def partitioned_index_choices() -> list[str]:
    return [
        index["name"]
        for index in sql_index_files
        if index["filepath"].startswith("indices/along_track/")
    ]


def defined_index_choices() -> list[str]:
    return [index["name"] for index in sql_index_files] + [
        index["name"] for index in eddy_index_files
    ]


def to_naive(dt: datetime | None) -> datetime | None:
    if dt is None:
        return None
    return dt.replace(tzinfo=None)


def iter_year_months(start: datetime | None, end: datetime | None):
    """Yield (year, month) between start and end (inclusive)."""
    start = to_naive(start)
    end = to_naive(end)

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


def get_netcdf4_files(
    missions: list[str],
    start_date: datetime | None = None,
    end_date: datetime | None = None,
) -> list[Path]:
    """Generate along-track NetCDF paths for the selected missions and dates."""
    oceandb_etl = create_along_track_etl()
    missions = list(missions)

    if not missions or (len(missions) == 1 and missions[0] == "all"):
        missions = oceandb_etl.missions

    invalid_missions = [
        mission for mission in missions if mission not in oceandb_etl.missions
    ]
    if invalid_missions:
        raise ValueError(
            f"received invalid arguments {invalid_missions}. "
            f"Received missions must be from the following list {oceandb_etl.missions}"
        )

    click.echo(
        format_key_value(
            "Ingesting missions:",
            ", ".join(missions),
            label_color="blue",
        )
    )

    prefix = "SEALEVEL_GLO_PHY_L3_MY_008_062"
    all_netcdf_files: list[Path] = []

    if start_date is None and end_date is None:
        year_months = None
    else:
        if start_date and end_date and end_date < start_date:
            raise ValueError("end_date must be >= start_date")
        year_months = list(iter_year_months(start_date, end_date))

    for mission in missions:
        file_structure = f"cmems_obs-sl_glo_phy-ssh_my_{mission}-l3-duacs_PT1S_202411"
        second_file_structure = (
            f"cmems_obs-sl_glo_phy-ssh_my_{mission}-lr-l3-duacs_PT1S_202411"
        )

        for structure in [file_structure, second_file_structure]:
            ingest_directory = (
                Path(oceandb_etl.config.along_track_data_directory)
                / prefix
                / structure
            )

            if not ingest_directory.exists():
                continue

            if year_months is None:
                nc_files = list(ingest_directory.rglob("*.nc"))
            else:
                nc_files = []
                for year, month in year_months:
                    month_dir = ingest_directory / f"{year:04d}" / f"{month:02d}"
                    if month_dir.exists():
                        nc_files.extend(month_dir.rglob("*.nc"))

            all_netcdf_files.extend(nc_files)

    click.echo(
        format_status_line(
            "MATCHED",
            f"{len(all_netcdf_files)} file(s) for ingestion.",
            label_color="green",
        )
    )
    return all_netcdf_files


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
