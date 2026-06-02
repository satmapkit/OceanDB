from __future__ import annotations

import time
from multiprocessing import Pool, TimeoutError
from pathlib import Path
from typing import Literal

import click

from OceanDB.cli_utils import format_status_line
from OceanDB.commands.shared import (AVISO_EDDY_FILENAMES,
                                     create_along_track_etl, create_eddy_etl,
                                     format_duration, get_netcdf4_files,
                                     render_ingest_mode, timestamp_now)


@click.command("ingest-eddy")
@click.option(
    "--cyclonic-only",
    "only_ingest",
    flag_value="cyclonic",
    help="Only ingest cyclonic data",
    default="both",
)
@click.option(
    "--anticyclonic-only",
    "only_ingest",
    flag_value="anticyclonic",
    help="Only ingest anticyclonic data",
    default="both",
)
@click.option(
    "--offset-cyclonic",
    default="0",
    show_default=True,
    type=int,
    help="Rows to offset the cyclonic eddy ingestion by",
)
@click.option(
    "--offset-anticyclonic",
    default="0",
    show_default=True,
    type=int,
    help="Rows to offset the anticyclonic eddy ingestion by",
)
def ingest_eddy(
    only_ingest: Literal["both", "cyclonic", "anticyclonic"],
    offset_cyclonic: int,
    offset_anticyclonic: int,
):
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
    oceandb_etl = create_eddy_etl()
    eddy_directory = oceandb_etl.config.eddy_data_directory
    cyclonic_file = AVISO_EDDY_FILENAMES[0]
    anticyclonic_file = AVISO_EDDY_FILENAMES[1]

    if only_ingest in ("both", "cyclonic"):
        print(f"Processing ingesting {cyclonic_file}")
        if offset_cyclonic > 0:
            print(f"Starting at {offset_cyclonic}")
        cyclonic_filepath = Path(f"{eddy_directory}/{cyclonic_file}")
        oceandb_etl.ingest_eddy_data_file(
            cyclonic_filepath,
            cyclonic_type=-1,
            offset=offset_cyclonic,
        )

    if only_ingest in ("both", "anticyclonic"):
        if offset_anticyclonic > 0:
            print(f"Starting at {offset_anticyclonic}")
        print(f"Processing ingesting {anticyclonic_file}")
        anticyclonic_filepath = Path(f"{eddy_directory}/{anticyclonic_file}")
        oceandb_etl.ingest_eddy_data_file(
            anticyclonic_filepath,
            cyclonic_type=1,
            offset=offset_anticyclonic,
        )


@click.command("ingest-along-track")
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
    "--workers",
    type=click.IntRange(min=1),
    default=6,
    show_default=True,
    help="Number of worker processes to use for along-track ingestion.",
)
@click.option(
    "--debug",
    is_flag=True,
    help="Emit verbose worker-level progress logs during ingestion.",
)
def ingest_along_track(missions, start_date, end_date, workers, debug):
    """
    Ingest along-track altimetry data for one or more missions.

    This command parses and ingests along-track NetCDF files into the OceanDB
    PostgreSQL database.
    """
    nc_files = get_netcdf4_files(
        missions=list(missions),
        start_date=start_date,
        end_date=end_date,
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

    oceandb_etl = create_along_track_etl(debug=debug)
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

    skipped_count = len(nc_files) - len(along_track_files)
    click.echo(
        format_status_line(
            "INGEST",
            f"Processing {len(along_track_files)} new file(s); "
            f"skipping {skipped_count} already ingested file(s).",
            label_color="blue",
        )
    )
    click.echo(render_ingest_mode(oceandb_etl.config.ingest_mode))
    process_count = workers
    if debug:
        click.echo(
            format_status_line(
                "WORKERS",
                f"Starting {process_count} worker(s). Waiting for the first completed file...",
                label_color="magenta",
            )
        )

    completed = skipped_count
    total = len(nc_files)
    multiprocessing_pool = None

    if process_count == 1:
        results = map(oceandb_etl.process_along_track_file, along_track_files)
    else:
        multiprocessing_pool = Pool(process_count)
        results = multiprocessing_pool.imap_unordered(
            oceandb_etl.process_along_track_file,
            along_track_files,
        )

    try:
        if multiprocessing_pool is None:
            for result in results:
                completed += 1
                remaining = total - completed
                click.echo(
                    format_status_line(
                        f"{completed}/{total}",
                        f"{timestamp_now()} | {result['file_name']} | {result['size_mb']:.2f} MB | "
                        f"{format_duration(result['duration_seconds'])} | {remaining} remaining",
                        label_color="cyan",
                    )
                )
        else:
            heartbeat_seconds = 30
            completed_new_files = 0
            total_new_files = len(along_track_files)

            while completed_new_files < total_new_files:
                try:
                    result = results.next(timeout=heartbeat_seconds)
                except TimeoutError:
                    active_workers = min(
                        process_count, total_new_files - completed_new_files
                    )
                    click.echo(
                        format_status_line(
                            "WAIT",
                            f"{completed}/{total} file(s) complete | "
                            f"{completed_new_files}/{total_new_files} new file(s) finished | "
                            f"{active_workers} worker(s) still running",
                            label_color="yellow",
                        )
                    )
                    continue

                completed += 1
                completed_new_files += 1
                remaining = total - completed
                click.echo(
                    format_status_line(
                        f"{completed}/{total}",
                        f"{timestamp_now()} | {result['file_name']} | {result['size_mb']:.2f} MB | "
                        f"{format_duration(result['duration_seconds'])} | {remaining} remaining",
                        label_color="cyan",
                    )
                )
    finally:
        if multiprocessing_pool is not None:
            multiprocessing_pool.close()
            multiprocessing_pool.join()

    full_ingest_duration = time.perf_counter() - start_ingest_time
    click.echo(
        format_status_line(
            "DONE",
            f"Finished ingesting {len(along_track_files)} file(s) in {format_duration(full_ingest_duration)}.",
            label_color="green",
        )
    )
