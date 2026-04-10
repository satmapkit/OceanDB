import time
from datetime import datetime
from multiprocessing import Pool
from pathlib import Path

import click

from OceanDB.OceanDB_Initializer import OceanDBInit
from OceanDB.config import Config
from OceanDB.utils.logging import get_logger

logger = get_logger()


def _create_base_etl():
    from OceanDB.etl.base_etl import BaseETL

    return BaseETL()


def _create_along_track_etl():
    from OceanDB.etl.along_track_etl import AlongTrackETL

    return AlongTrackETL()


def _create_eddy_etl():
    from OceanDB.etl.eddy_etl import EddyETL

    return EddyETL()


def _create_copernicus_marine_client():
    from OceanDB.etl.copernicus_marine import OceanDBCopernicusMarine

    return OceanDBCopernicusMarine()


@click.group()
def cli():
    pass


@cli.command()
def process():
    logger.info("OceanDB")


@cli.command()
def init():
    ocean_db_init = OceanDBInit()
    ocean_db_init.create_database()
    ocean_db_init.create_tables()
    ocean_db_init.create_eddy_tables()
    ocean_db_init.create_partitions("1990-01-01", "2025-11-01")
    oceandb_etl = _create_base_etl()
    oceandb_etl.insert_basins_data()
    oceandb_etl.insert_basin_connections_data()


@cli.command("create-indices")
def create_indices():
    ocean_db_init = OceanDBInit()
    ocean_db_init.create_indices()
    ocean_db_init.create_eddy_indices()


@cli.command()
def ingest_eddy():
    """
    Ingest eddy detection datasets into OceanDB.

    Each file is parsed in batches and the extracted eddy observations are inserted
    into the PostgreSQL ``eddy`` table.

    The data source consists of two long-term global datasets:
    - Cyclonic eddies (``cyclonic_type = 1``)
    - Anticyclonic eddies (``cyclonic_type = 0``)

    The input directory is resolved from the OceanDB configuration
    (``eddy_data_directory``).

    Notes
    -----
    - This command performs a full historical ingest.
    - Inserts use strict PostgreSQL typing (INSERT, not COPY).
    - Intended to be run once per database or during reinitialization.
    """
    oceandb_etl = _create_eddy_etl()
    eddy_directory = oceandb_etl.config.eddy_data_directory

    print("Processing Ingesting META3.2_DT_allsat_Cyclonic_long_19930101_20220209.nc")
    cyclonic_filepath = Path(
        f"{eddy_directory}/META3.2_DT_allsat_Cyclonic_long_19930101_20220209.nc"
    )
    oceandb_etl.ingest_eddy_data_file(cyclonic_filepath, cyclonic_type=-1)

    print(
        "Processing Ingesting META3.2_DT_allsat_Anticyclonic_long_19930101_20220209.nc"
    )
    anticyclonic_filepath = Path(
        f"{eddy_directory}/META3.2_DT_allsat_AntiCyclonic_long_19930101_20220209.nc"
    )
    oceandb_etl.ingest_eddy_data_file(anticyclonic_filepath, cyclonic_type=1)


@cli.command
def download():
    config = Config()

    along_track_directory = Path(config.along_track_data_directory)

    # 2️⃣ Check contents
    files = list(along_track_directory.glob("*"))

    if not along_track_directory.exists():
        click.echo(
            f"Directory does not exist: {along_track_directory}\n"
            f"Please create it or update ALONG_TRACK_DATA_DIRECTORY in your .env "
        )
        return

    if not files:
        click.echo(f"Found {len(files)} file(s) in directory. No need to download.")
        click.echo("✔ Data already exists.")

        # 3️⃣ Directory is empty — warn user
        click.echo(
            "\n⚠️  No data found in the directory.\n"
            f"The directory '{along_track_directory}' is empty.\n"
            "Downloading the full dataset requires more than **20 GB** of storage.\n"
        )

        # 4️⃣ Ask the user if they want to continue
        proceed = click.confirm(
            "Do you want to proceed with downloading 20+ GB of data?", default=False
        )

        if not proceed:
            click.echo("Download canceled.")
            return

    oceandb_cm = _create_copernicus_marine_client()
    # click.echo("\n⬇️  Starting download... (this may take hours)")
    #
    oceandb_cm.sync_copernicus_along_track_data()
    # click.echo("✔ Download complete.")
EARLIEST_DATE = datetime(1990, 1, 1)


def _to_naive(dt: datetime | None) -> datetime | None:
    if dt is None:
        return None
    return dt.replace(tzinfo=None)


def iter_year_months(
    start: datetime | None,
    end: datetime | None,
):
    """
    Yield (year, month) between start and end (inclusive).

    Semantics:
    - start=None → 1990-01
    - end=None   → current month
    """

    # 🔒 normalize FIRST
    start = _to_naive(start)
    end = _to_naive(end)

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
    missions: list, start_date: datetime = None, end_date: datetime = None
) -> list[Path]:
    """
    Generate a list of NetCDF along-track files based on missions and optional date filtering.
    If start_date and end_date are both None → return ALL files for those missions.
    """

    oceandb_etl = _create_along_track_etl()
    missions = list(missions)

    # -----------------------
    # Mission handling
    # -----------------------
    if not missions or (len(missions) == 1 and missions[0] == "all"):
        missions = oceandb_etl.missions

    invalid_missions = []
    for mission in missions:

        if mission not in oceandb_etl.missions:
            invalid_missions.append(mission)
    if invalid_missions:

        raise Exception(
            f"received invalid arguments {invalid_missions}.  Received missions must be from the following list {oceandb_etl.missions}"
        )

    click.echo(f"Ingesting missions: {missions}")
    click.echo(f"N Missions {len(missions)}")
    prefix = "SEALEVEL_GLO_PHY_L3_MY_008_062"
    all_netcdf_files = []

    if start_date is None and end_date is None:
        # ingest EVERYTHING
        year_months = None

    else:
        # at least one bound is provided
        if start_date and end_date and end_date < start_date:
            raise ValueError("end_date must be >= start_date")

        year_months = list(iter_year_months(start_date, end_date))

    # Collect files
    # For whatever reason the s6a satellite data directory is different than all the others.

    for mission in missions:
        file_structure = f"cmems_obs-sl_glo_phy-ssh_my_{mission}-l3-duacs_PT1S_202411"
        second_file_structure = (
            f"cmems_obs-sl_glo_phy-ssh_my_{mission}-lr-l3-duacs_PT1S_202411"
        )

        for structure in [file_structure, second_file_structure]:
            ingest_directory = (
                Path(oceandb_etl.config.along_track_data_directory) / prefix / structure
            )

            if not ingest_directory.exists():
                # Not all missions have both structures
                continue

            if year_months is None:
                # Ingest ALL .nc files recursively
                nc_files = list(ingest_directory.rglob("*.nc"))
            else:
                # Ingest date-filtered months only
                nc_files = []
                for year, month in year_months:
                    month_dir = ingest_directory / f"{year:04d}" / f"{month:02d}"
                    if month_dir.exists():
                        nc_files.extend(month_dir.rglob("*.nc"))

            all_netcdf_files.extend(nc_files)

    return all_netcdf_files


def _format_duration(seconds: float) -> str:
    minutes, seconds = divmod(seconds, 60)
    hours, minutes = divmod(int(minutes), 60)
    if hours:
        return f"{hours}h {minutes}m {seconds:.1f}s"
    if minutes:
        return f"{minutes}m {seconds:.1f}s"
    return f"{seconds:.1f}s"


@cli.command()
@click.argument("missions", nargs=-1)
@click.option(
    "--start-date",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    required=False,
)
@click.option(
    "--end-date",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    required=False,
)
def ingest_along_track(missions, start_date, end_date):
    """
    Ingest along-track altimetry data for one or more missions.

    This command  parses, and ingests along-track NetCDF files
    into the OceanDB PostgreSQL database. Data are streamed into Postgres
    using a bulk COPY operation for efficiency.

    Parameters
    ----------
    missions : tuple[str]
        One or more mission identifiers (e.g. ``j3``, ``al``, ``s3a``).
        If no missions are provided, all supported missions are ingested.

    start_date : datetime, optional
        The beginning of the date range (inclusive). Must be provided in the
        form ``YYYY-MM-DD``. If both ``start_date`` and ``end_date`` are omitted,
        the command ingests **all** files for the selected missions.

    end_date : datetime, optional
        The end of the date range (inclusive). Must be provided in the form
        ``YYYY-MM-DD``. If only one of ``start_date`` or ``end_date`` is provided,
        the command will raise an error.

    Behavior
    --------
    - If no dates are provided → ingest **all** available files.
    - If both dates are provided → ingest only files belonging to year/month
      folders within the given range.
    - The command asks for confirmation before running ingestion, as the
      operation may take several hours depending on the number and size of
      files.

    Examples
    --------
    Ingest all along-track data for the Jason-3 mission::

        oceandb ingest-along-track j3

    Ingest data for multiple missions within a date range::

        oceandb ingest-along-track j3 al s3a \\
            --start-date 2023-01-01 \\
            --end-date 2023-03-31
    """

    missions = list(missions)
    nc_files = get_netcdf4_files(
        missions=missions, start_date=start_date, end_date=end_date
    )

    if not click.confirm(
        f"Ingesting {len(nc_files)} files This may take many hours. Continue?"
    ):
        return

    # Query the ingested metadata so that we can skip processing files that have already been processed
    oceandb_etl = _create_along_track_etl()
    metadata_filenames = oceandb_etl.query_metadata()

    start_ingest_time = time.perf_counter()

    along_track_files = [
        file for file in nc_files if file.name not in metadata_filenames
    ]

    if not along_track_files:
        click.echo("All matching files have already been ingested. Nothing to do.")
        return

    click.echo(
        f"Processing {len(along_track_files)} new file(s); "
        f"skipping {len(nc_files) - len(along_track_files)} already ingested file(s)."
    )

    process_count = 6
    with Pool(process_count) as multiprocessing_pool:
        completed = 0
        total = len(along_track_files)
        for result in multiprocessing_pool.imap_unordered(
            oceandb_etl.process_along_track_file, along_track_files
        ):
            completed += 1
            remaining = total - completed
            click.echo(
                f"[{completed}/{total}] {result['file_name']} | "
                f"{result['size_mb']:.2f} MB | "
                f"{_format_duration(result['duration_seconds'])} | "
                f"{remaining} remaining"
            )

    full_ingest_duration = time.perf_counter() - start_ingest_time
    click.echo(
        f"Finished ingesting {len(along_track_files)} file(s) in "
        f"{_format_duration(full_ingest_duration)}."
    )
