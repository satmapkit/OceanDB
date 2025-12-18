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


from datetime import date

def iter_year_months(start: datetime, end: datetime):
    """Yield (year, month) for all months between start and end (inclusive)."""
    year, month = start.year, start.month
    end_year, end_month = end.year, end.month

    while (year < end_year) or (year == end_year and month <= end_month):
        yield year, month
        # increment month
        month += 1
        if month == 13:
            month = 1
            year += 1


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

    click.echo(f"Ingesting missions: {missions}")

    prefix = "SEALEVEL_GLO_PHY_L3_MY_008_062"
    all_netcdf_files = []

    # -----------------------
    # Determine year/months
    # -----------------------
    if start_date is None and end_date is None:
        year_months = None  # means: ingest EVERYTHING
    elif start_date is None or end_date is None:
        raise ValueError("start_date and end_date must both be provided or both be None.")
    else:
        year_months = list(iter_year_months(start_date, end_date))

    # -----------------------
    # Collect files
    # -----------------------
    for mission in missions:
        file_structure = f"cmems_obs-sl_glo_phy-ssh_my_{mission}-l3-duacs_PT1S_202411"
        ingest_directory = (
            Path(oceandb_etl.config.along_track_data_directory)
            / prefix
            / file_structure
        )

        if year_months is None:
            # Ingest ALL .nc files recursively
            nc_files = list(ingest_directory.rglob("*.nc"))
        else:
            # Ingest date-filtered months only
            nc_files = [
                nc_file
                for year, month in year_months
                for nc_file in (ingest_directory / f"{year:04d}" / f"{month:02d}").rglob("*.nc")
                if (ingest_directory / f"{year:04d}" / f"{month:02d}").exists()
            ]

        all_netcdf_files.extend(nc_files)

    return all_netcdf_files




@cli.command
@click.option(
    "--missions",
    "-m",
    multiple=True,
    help="One or more missions to ingest (e.g. -m j3 -m al -m s3a).",
    required=False
)
@click.option(
    "--start-date",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    required=False,
    help="Start date (YYYY-MM-DD)",
)
@click.option(
    "--end-date",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    required=False,
    help="End date (YYYY-MM-DD, inclusive)",
)
def ingest(missions=None, start_date=None, end_date=None):
    """
    Ingest along-track NetCDF4 files into the OceanDB database.

    This command loads one or more missions and optionally filters the data
    by a start and end date. Files are discovered on disk based on the mission
    directory structure and passed to the OceanDBETL ingestion pipeline.

    Parameters
    ----------
    missions : tuple[str], optional
        One or more mission codes to ingest. Mission values are provided using
        repeated flags (e.g. ``-m j3 -m al``). If omitted or if the value
        ``"all"`` is supplied, all supported missions will be ingested.

        Examples:
            - ``-m al``
            - ``-m j3 -m s3a``
            - ``--missions al``

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
    Ingest the 'al' mission:

        oceandb ingest -m al

    Ingest multiple missions:

        oceandb ingest -m j3 -m s3a

    Ingest all missions between March and May 2013:

        oceandb ingest --start-date 2013-03-01 --end-date 2013-05-31

    Returns
    -------
    None
        Processes each file and writes results to the database.
    """
    oceandb_etl = OceanDBETl()
    nc_files = get_netcdf4_files(missions=missions, start_date=start_date, end_date=end_date)

    if not click.confirm(f"Ingesting {len(nc_files)} files This may take many hours. Continue?"):
        return

    # Query the ingested metadata so that we can skip processing files that have already been processed
    metadata_filenames = oceandb_etl.query_metadata()

    for file in nc_files:
        file_name = file.name
        if file_name in metadata_filenames:
            continue

        print(f"Processing {file_name}")
        start = time.perf_counter()

        along_track_data, along_track_metadata = oceandb_etl.extract_along_track_file(file=file)
        oceandb_etl.ingest_along_track_file( along_track_data=along_track_data, along_track_metadata=along_track_metadata)

        size_mb = file.stat().st_size / (1024 * 1024)
        duration = time.perf_counter() - start
        print(f"✅ {file.name} | {size_mb:.2f} MB | {duration:.2f} seconds")
