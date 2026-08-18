from __future__ import annotations

from typing import Literal

import click

from OceanDB.cli_utils import format_status_line
from OceanDB.commands.shared import (create_along_track_etl, format_duration,
                                     render_ingest_mode, timestamp_now)
from OceanDB.workflows import ingest_along_track as run_ingest_along_track
from OceanDB.workflows import ingest_eddy as run_ingest_eddy


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

    def on_progress(event):
        if event["type"] != "eddy_file_start":
            return
        if event["offset"] > 0:
            print(f"Starting at {event['offset']}")
        print(f"Processing ingesting {event['filename']}")

    run_ingest_eddy(
        only_ingest=only_ingest,
        offset_cyclonic=offset_cyclonic,
        offset_anticyclonic=offset_anticyclonic,
        on_progress=on_progress,
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
    oceandb_etl = create_along_track_etl(debug=debug)
    discovery = oceandb_etl.discover_files(
        missions=list(missions),
        start_date=start_date,
        end_date=end_date,
    )
    nc_files = discovery["files"]
    selected_missions = discovery["missions"]

    click.echo(f"Ingesting missions: {', '.join(selected_missions)}")
    click.echo(
        format_status_line(
            "MATCHED",
            f"{len(nc_files)} file(s) for ingestion.",
            label_color="green",
        )
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

    def on_progress(event):
        if event["type"] == "along_track_start":
            click.echo(
                format_status_line(
                    "INGEST",
                    f"Processing {event['ingest_count']} new file(s); "
                    f"skipping {event['skipped_count']} already ingested file(s).",
                    label_color="blue",
                )
            )
            click.echo(render_ingest_mode(event["ingest_mode"]))
            if debug:
                click.echo(
                    format_status_line(
                        "WORKERS",
                        f"Starting {workers} worker(s). Waiting for the first completed file...",
                        label_color="magenta",
                    )
                )
            return

        if event["type"] == "along_track_wait":
            click.echo(
                format_status_line(
                    "WAIT",
                    f"{event['completed']}/{event['total']} file(s) complete | "
                    f"{event['completed_new_files']}/{event['total_new_files']} new file(s) finished | "
                    f"{event['active_workers']} worker(s) still running",
                    label_color="yellow",
                )
            )
            return

        if event["type"] == "along_track_file_complete":
            result = event["result"]
            click.echo(
                format_status_line(
                    f"{event['completed']}/{event['total']}",
                    f"{timestamp_now()} | {result['file_name']} | {result['size_mb']:.2f} MB | "
                    f"{format_duration(result['duration_seconds'])} | {event['remaining']} remaining",
                    label_color="cyan",
                )
            )

    result = run_ingest_along_track(
        missions=list(missions),
        start_date=start_date,
        end_date=end_date,
        workers=workers,
        debug=debug,
        on_progress=on_progress,
    )

    if result["matched_count"] and result["ingested_count"] == 0:
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
            "DONE",
            f"Finished ingesting {result['ingested_count']} file(s) in {format_duration(result['duration_seconds'])}.",
            label_color="green",
        )
    )
