import time
import webbrowser
from datetime import datetime
from multiprocessing import Pool
from pathlib import Path

import click
import paramiko
import psycopg as pg

from OceanDB.cli_utils import (format_key_value, format_status_line,
                               render_table, style_value)
from OceanDB.config import Config
from OceanDB.OceanDB_Initializer import OceanDBInit
from OceanDB.utils.basin_visualization import write_basin_map
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


def _create_basins_etl():
    from OceanDB.etl.basins_etl import BasinsETL

    return BasinsETL()


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


@cli.group("summary")
def summary_group():
    """Summary commands."""
    pass


@cli.group("along_track")
def along_track_group():
    """Along-track data commands."""
    pass


@cli.command()
def process():
    logger.info("OceanDB")


@cli.command()
def init():
    ocean_db_init = OceanDBInit()
    database_created = ocean_db_init.create_database()
    if not database_created:
        return
    ocean_db_init.create_tables()
    ocean_db_init.create_eddy_tables()
    ocean_db_init.create_partitions("1990-01-01", "2025-11-01")
    oceandb_etl = _create_basins_etl()
    oceandb_etl.insert_basins_data()
    oceandb_etl.insert_basin_connections_data()


@cli.command("create-indices")
def create_indices():
    ocean_db_init = OceanDBInit()
    ocean_db_init.create_indices()
    ocean_db_init.create_eddy_indices()


@cli.command()
@click.option(
    "--output",
    type=click.Path(path_type=Path, dir_okay=False, writable=True),
    default=Path("artifacts/basin_map.html"),
    show_default=True,
    help="HTML output path for the basin visualization.",
)
def visualize_basins(output: Path):
    """
    Render a quick basin polygon map with basin IDs into a standalone HTML file.
    """
    output_path = write_basin_map(output)
    click.echo(f"Wrote basin visualization to {output_path}")
    opened = webbrowser.open(output_path.resolve().as_uri())
    if opened:
        click.echo("Opened basin visualization in your default browser.")
    else:
        click.echo(
            "Could not automatically open the browser. "
            f"Open {output_path.resolve()} manually."
        )


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
@click.option(
    "--dataset-type",
    default="my",
    show_default=True,
    help="Copernicus dataset type to download.",
)
@click.option(
    "--dataset-version",
    default="202411",
    show_default=True,
    help="Copernicus dataset version to download.",
)
@click.option(
    "--dry-run",
    is_flag=True,
    help="List matching Copernicus files without downloading them.",
)
@click.option(
    "--yes",
    is_flag=True,
    help="Skip the confirmation prompt.",
)
@click.option(
    "--overwrite",
    is_flag=True,
    help="Overwrite files that already exist locally.",
)
def download(
    missions,
    start_date,
    end_date,
    dataset_type,
    dataset_version,
    dry_run,
    yes,
    overwrite,
):
    """
    Download Copernicus Marine along-track data.

    If no missions are provided, all OceanDB-supported along-track missions are
    selected. Date bounds are month-granular because Copernicus stores these
    files under year/month directories.
    """
    config = Config()

    along_track_directory = Path(config.along_track_data_directory)

    if not config.along_track_data_directory:
        raise click.ClickException(
            "ALONG_TRACK_DATA_DIRECTORY is not set. Add it to your .env file."
        )
    if not along_track_directory.exists():
        along_track_directory.mkdir(parents=True)

    selected_missions = list(missions) or ["all"]
    dry_run_prefix = "Checking" if dry_run else "Downloading"
    click.echo(
        format_status_line(
            dry_run_prefix.upper(),
            "Copernicus along-track data for missions: "
            f"{', '.join(selected_missions)}",
            label_color="blue",
        )
    )
    click.echo(
        format_key_value(
            "Output directory:",
            str(along_track_directory),
            label_color="yellow",
        )
    )

    oceandb_cm = _create_copernicus_marine_client()

    preview_results = oceandb_cm.sync_copernicus_along_track_data(
        missions=selected_missions,
        output_directory=along_track_directory,
        start_date=start_date,
        end_date=end_date,
        dataset_type=dataset_type,
        version=dataset_version,
        dry_run=True,
        overwrite=overwrite,
    )
    summary = oceandb_cm.summarize_get_results(preview_results)
    click.echo(
        "\n"
        + format_status_line(
            "PREVIEW",
            f"{summary.file_count} file(s), "
            f"{oceandb_cm.format_size(summary.total_size_mb)}, "
            f"{summary.request_count} request(s).",
            label_color="magenta",
        )
    )

    if dry_run:
        return

    if not yes:
        click.echo(
            "\n"
            + format_status_line(
                "WARNING",
                "Copernicus along-track downloads can require tens of GB of storage.",
                label_color="yellow",
            )
        )
        proceed = click.confirm(
            "Do you want to proceed with downloading data?", default=False
        )
        if not proceed:
            click.echo("Download canceled.")
            return

    results = oceandb_cm.sync_copernicus_along_track_data(
        missions=selected_missions,
        output_directory=along_track_directory,
        start_date=start_date,
        end_date=end_date,
        dataset_type=dataset_type,
        version=dataset_version,
        dry_run=False,
        overwrite=overwrite,
    )
    click.echo(
        format_status_line(
            "DONE",
            f"Finished {len(results)} Copernicus download request(s).",
            label_color="green",
        )
    )


@cli.command("download-eddy")
@click.option(
    "--dry-run",
    is_flag=True,
    help="List Eddy AVISO files without downloading them.",
)
@click.option(
    "--yes",
    is_flag=True,
    help="Skip the confirmation prompt.",
)
@click.option(
    "--overwrite",
    is_flag=True,
    help="Overwrite files that already exist locally.",
)
def download_eddy(dry_run: bool, yes: bool, overwrite: bool):
    """Download Eddy AVISO NetCDF files over SFTP."""
    config = Config()
    output_dir = Path(config.eddy_data_directory)
    username = config.aviso_username
    password = config.aviso_password

    if not config.eddy_data_directory:
        raise click.ClickException(
            "EDDY_DATA_DIRECTORY is not set. Add it to your .env file."
        )
    if not username or not password:
        raise click.ClickException(
            "AVISO credentials are missing. Set AVISO_USERNAME and "
            "AVISO_PASSWORD in your .env file."
        )

    output_dir.mkdir(parents=True, exist_ok=True)

    dry_run_prefix = "Checking" if dry_run else "Downloading"
    click.echo(
        format_status_line(
            dry_run_prefix.upper(),
            "AVISO eddy data files",
            label_color="blue",
        )
    )
    click.echo(
        format_key_value(
            "Output directory:",
            str(output_dir),
            label_color="yellow",
        )
    )

    existing_files = [
        filename
        for filename in AVISO_EDDY_FILENAMES
        if (output_dir / filename).exists()
    ]
    files_to_download = (
        AVISO_EDDY_FILENAMES
        if overwrite
        else [
            filename
            for filename in AVISO_EDDY_FILENAMES
            if filename not in existing_files
        ]
    )
    click.echo(
        "\n"
        + format_status_line(
            "PREVIEW",
            f"{len(files_to_download)} file(s) to download, "
            f"{len(existing_files)} existing file(s) found.",
            label_color="magenta",
        )
    )

    if dry_run:
        return

    if not yes and files_to_download:
        click.echo(
            "\n"
            + format_status_line(
                "WARNING",
                "AVISO eddy downloads may overwrite local files when requested.",
                label_color="yellow",
            )
        )
        proceed = click.confirm(
            "Do you want to proceed with downloading data?", default=False
        )
        if not proceed:
            click.echo("Download canceled.")
            return

    if not files_to_download:
        click.echo(
            format_status_line(
                "DONE",
                "All AVISO eddy files already exist locally.",
                label_color="green",
            )
        )
        return

    with paramiko.SSHClient() as ssh:
        ssh.load_system_host_keys()
        ssh.connect(
            hostname=AVISO_HOST,
            port=AVISO_PORT,
            username=username,
            password=password,
        )
        with ssh.open_sftp() as sftp:
            for filename in files_to_download:
                remote_path = f"{AVISO_EDDY_REMOTE_DIRECTORY}/{filename}"
                local_path = output_dir / filename
                sftp.get(remote_path, str(local_path))

    click.echo(
        format_status_line(
            "DONE",
            f"Finished downloading {len(files_to_download)} AVISO eddy file(s).",
            label_color="green",
        )
    )


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
    missions: list, start_date: datetime | None = None, end_date: datetime | None = None
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

    click.echo(
        format_status_line(
            "MATCHED",
            f"{len(all_netcdf_files)} file(s) for ingestion.",
            label_color="green",
        )
    )
    return all_netcdf_files


def _format_duration(seconds: float) -> str:
    minutes, seconds = divmod(seconds, 60)
    hours, minutes = divmod(int(minutes), 60)
    if hours:
        return f"{hours}h {minutes}m {seconds:.1f}s"
    if minutes:
        return f"{minutes}m {seconds:.1f}s"
    return f"{seconds:.1f}s"


def _format_timestamp(value: datetime | None) -> str:
    if value is None:
        return "-"
    return value.strftime("%Y-%m-%d %H:%M:%S")


def _render_along_track_summary() -> None:
    try:
        oceandb_etl = _create_along_track_etl()
        summary_rows = oceandb_etl.summarize_ingested_missions()
    except pg.OperationalError as e:
        raise click.ClickException(f"Unable to access database: {e}") from e

    if not summary_rows:
        click.echo(
            format_status_line(
                "EMPTY",
                "No along-track data has been ingested yet.",
                label_color="yellow",
            )
        )
        return

    headers = [
        ("Mission", "mission"),
        ("Start", "start_date"),
        ("End", "end_date"),
        ("Files", "file_count"),
        ("Observations", "observation_count"),
    ]

    formatted_rows = []
    for row in summary_rows:
        formatted_rows.append(
            {
                "mission": str(row["mission"]),
                "start_date": _format_timestamp(row["start_date"]),
                "end_date": _format_timestamp(row["end_date"]),
                "file_count": str(row["file_count"]),
                "observation_count": str(row["observation_count"]),
            }
        )

    def _summary_cell_styler(column: str, value: str) -> str:
        if column == "mission":
            return style_value(value, fg="cyan", bold=True)
        if column in {"file_count", "observation_count"}:
            return style_value(value, fg="green")
        return style_value(value, fg="white")

    click.echo(
        format_status_line(
            "SUMMARY",
            f"{len(formatted_rows)} mission(s) with ingested along-track data.",
            label_color="magenta",
        )
    )
    for line in render_table(headers, formatted_rows, cell_styler=_summary_cell_styler):
        click.echo(line)


@along_track_group.command("summary")
def summarize_along_track():
    """
    Summarize ingested along-track missions and their date coverage.
    """
    _render_along_track_summary()


@summary_group.command("alongtrack")
def summarize_alongtrack_from_summary_group():
    """
    Summarize ingested along-track missions and their date coverage.
    """
    _render_along_track_summary()


@cli.command("along-track-summary", hidden=True)
def summarize_along_track_legacy():
    """Backward-compatible alias for along-track summary."""
    _render_along_track_summary()


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

    if not nc_files:
        click.echo(
            format_status_line(
                "EMPTY",
                "No matching along-track files were found. Nothing to ingest.",
                label_color="yellow",
            )
        )
        return

    if not click.confirm(f"Ingest {len(nc_files)} file(s)? This may take many hours."):
        return

    # Query the ingested metadata so that we can skip processing files that have already been processed
    oceandb_etl = _create_along_track_etl()
    metadata_filenames = oceandb_etl.query_metadata()

    start_ingest_time = time.perf_counter()

    along_track_files = [
        file for file in nc_files if file.name not in metadata_filenames
    ]

    if not along_track_files:
        click.echo(
            format_status_line(
                "SKIP",
                "All matching files have already been ingested. Nothing to do.",
                label_color="yellow",
            )
        )
        return

    click.echo(
        format_status_line(
            "INGEST",
            f"Processing {len(along_track_files)} new file(s); "
            f"skipping {len(nc_files) - len(along_track_files)} already ingested file(s).",
            label_color="blue",
        )
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
                format_status_line(
                    f"{completed}/{total}",
                    f"{result['file_name']} | "
                    f"{result['size_mb']:.2f} MB | "
                    f"{_format_duration(result['duration_seconds'])} | "
                    f"{remaining} remaining",
                    label_color="cyan",
                )
            )

    full_ingest_duration = time.perf_counter() - start_ingest_time
    click.echo(
        format_status_line(
            "DONE",
            f"Finished ingesting {len(along_track_files)} file(s) in "
            f"{_format_duration(full_ingest_duration)}.",
            label_color="green",
        )
    )
