from __future__ import annotations

from pathlib import Path

import click

from OceanDB.aviso import (AVISO_EDDY_FILENAMES, AVISO_EDDY_REMOTE_DIRECTORY,
                           AVISO_HOST, AVISO_PORT)
from OceanDB.cli_utils import format_key_value, format_status_line
from OceanDB.commands.shared import create_copernicus_marine_client
from OceanDB.config import Config


@click.command()
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

    oceandb_cm = create_copernicus_marine_client()

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
            "Do you want to proceed with downloading data?",
            default=False,
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


@click.command("download-eddy")
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
    import paramiko

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
            "Do you want to proceed with downloading data?",
            default=False,
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
