from datetime import datetime
import click
from pathlib import Path
from OceanDB.OceanDB_ETL import OceanDBETl, AlongTrackData, AlongTrackMetaData
from OceanDB.OceanDB_Initializer import OceanDBInit
from OceanDB.config import Config
from OceanDB.utils.logging import get_logger

import netCDF4 as nc
import time

logger = get_logger()


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
    ocean_db_init.create_indices()
    ocean_db_init.create_partitions("1990-01-01", "2025-11-01")
    # ocean_db_init.validate_schema()
    oceandb_etl = OceanDBETl()
    oceandb_etl.insert_basins_data()
    oceandb_etl.insert_basin_connections_data()


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
            "Do you want to proceed with downloading 20+ GB of data?",
            default=False
        )

        if not proceed:
            click.echo("Download canceled.")
            return

    from OceanDB.OceanDB_Copernicus import OceanDBCopernicusMarine
    oceandb_cm = OceanDBCopernicusMarine()
    # click.echo("\n⬇️  Starting download... (this may take hours)")
    #
    oceandb_cm.sync_copernicus_along_track_data()
    # click.echo("✔ Download complete.")


def parse_date(ctx, param, value) -> datetime:
    """Parse date strings into datetime objects."""
    if value is None:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        raise click.BadParameter(f"Invalid date format: {value}. Use YYYY-MM-DD.")

from datetime import datetime

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


# def iter_year_months(
#     start: datetime | None,
#     end: datetime | None,
# ):
#     """
#     Yield (year, month) between start and end (inclusive).
#
#     Semantics:
#     - start=None → 1990-01
#     - end=None   → current month
#     """
#
#     EARLIEST_DATE = datetime(1990, 1, 1)
#
#     if start is None:
#         start = EARLIEST_DATE
#
#     if end is None:
#         end = datetime.now(tz=start.tzinfo or timezone.utc)
#
#     if end < start:
#         return
#
#     year, month = start.year, start.month
#     end_year, end_month = end.year, end.month
#
#     while (year < end_year) or (year == end_year and month <= end_month):
#         yield year, month
#         month += 1
#         if month == 13:
#             month = 1
#             year += 1


# def iter_year_months(
#     start: datetime | None,
#     end: datetime | None,
# ):
#     """
#     Yield (year, month) between start and end (inclusive).
#
#     - start=None → earliest available data
#     - end=None   → current month
#     """
#     if start is None and end is None:
#         raise ValueError("At least one of start or end must be provided")
#
#     if end is None:
#         end = datetime.now(tz=start.tzinfo if start else timezone.utc)
#
#     if start is None:
#         start = end.replace(day=1)
#
#     year, month = start.year, start.month
#     end_year, end_month = end.year, end.month
#
#     while (year < end_year) or (year == end_year and month <= end_month):
#         yield year, month
#         month += 1
#         if month == 13:
#             month = 1
#             year += 1



def get_netcdf4_files(
    missions: list,
    start_date: datetime = None,
    end_date: datetime = None
    ) -> list[Path]:
    """
    Generate a list of NetCDF along-track files based on missions and optional date filtering.
    If start_date and end_date are both None → return ALL files for those missions.
    """

    oceandb_etl = OceanDBETl()
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

        raise Exception(f"received invalid arguments {invalid_missions}.  Received missions must be from the following list {oceandb_etl.missions}")


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
    #For whatever reason the s6a satellite data directory is different than all the others.

    for mission in missions:
        file_structure = f"cmems_obs-sl_glo_phy-ssh_my_{mission}-l3-duacs_PT1S_202411"
        second_file_structure = f"cmems_obs-sl_glo_phy-ssh_my_{mission}-lr-l3-duacs_PT1S_202411"

        for structure in [file_structure, second_file_structure]:
            ingest_directory = (
                    Path(oceandb_etl.config.along_track_data_directory)
                    / prefix
                    / structure
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
    nc_files = get_netcdf4_files(missions=missions, start_date=start_date, end_date=end_date)

    if not click.confirm(f"Ingesting {len(nc_files)} files This may take many hours. Continue?"):
        return

    # Query the ingested metadata so that we can skip processing files that have already been processed
    oceandb_etl = OceanDBETl()
    metadata_filenames = oceandb_etl.query_metadata()

    for file in nc_files:
        file_name = file.name
        if file_name in metadata_filenames:
            print(f"{file_name} already processed, skipping")
            continue

        print(f"Processing {file_name}")
        start = time.perf_counter()

        along_track_data, along_track_metadata = oceandb_etl.extract_along_track_file(file=file)
        oceandb_etl.ingest_along_track_file( along_track_data=along_track_data, along_track_metadata=along_track_metadata)

        size_mb = file.stat().st_size / (1024 * 1024)
        duration = time.perf_counter() - start
        print(f"✅ {file.name} | {size_mb:.2f} MB | {duration:.2f} seconds")



# @cli.command
# @cli.argument("missions", nargs=-1)
# @cli.option(
#     "--start-date",
#     type=click.DateTime(formats=["%Y-%m-%d"]),
#     required=False,
#     help="Start date (YYYY-MM-DD)",
# )
# @cli.option(
#     "--end-date",
#     type=click.DateTime(formats=["%Y-%m-%d"]),
#     required=False,
#     help="End date (YYYY-MM-DD, inclusive)",
# )
# def ingest_along_track(missions=None, start_date=None, end_date=None):
#     """
#     Ingest along-track NetCDF4 files into the OceanDB database.
#
#     This command loads one or more missions and optionally filters the data
#     by a start and end date. Files are discovered on disk based on the mission
#     directory structure and passed to the OceanDBETL ingestion pipeline.
#
#     Parameters
#     ----------
#     missions : tuple[str], optional
#         One or more mission codes to ingest. Mission values are provided using
#         repeated flags (e.g. ``-m j3 -m al``). If omitted or if the value
#         ``"all"`` is supplied, all supported missions will be ingested.
#
#         Examples:
#             - ``-m al``
#             - ``-m j3 -m s3a``
#             - ``--missions al``
#
    # start_date : datetime, optional
    #     The beginning of the date range (inclusive). Must be provided in the
    #     form ``YYYY-MM-DD``. If both ``start_date`` and ``end_date`` are omitted,
    #     the command ingests **all** files for the selected missions.
    #
    # end_date : datetime, optional
    #     The end of the date range (inclusive). Must be provided in the form
    #     ``YYYY-MM-DD``. If only one of ``start_date`` or ``end_date`` is provided,
    #     the command will raise an error.
    #
    # Behavior
    # --------
    # - If no dates are provided → ingest **all** available files.
    # - If both dates are provided → ingest only files belonging to year/month
    #   folders within the given range.
    # - The command asks for confirmation before running ingestion, as the
    #   operation may take several hours depending on the number and size of
    #   files.
#
#     Examples
#     --------
#     Ingest the 'al' mission:
#
#         oceandb ingest -m al
#
#     Ingest multiple missions:
#
#         oceandb ingest -m j3 -m s3a
#
#     Ingest all missions between March and May 2013:
#
#         oceandb ingest --start-date 2013-03-01 --end-date 2013-05-31
#
#     Returns
#     -------
#     None
#         Processes each file and writes results to the database.
#     """
#     print(missions)
