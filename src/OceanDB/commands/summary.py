from __future__ import annotations

import click
import psycopg as pg

from OceanDB.cli_utils import format_status_line, render_table, style_value
from OceanDB.commands.shared import create_along_track_etl, format_timestamp


@click.group("summary")
def summary_group():
    """Summary commands."""


@click.group("along_track")
def along_track_group():
    """Along-track data commands."""


def _render_along_track_summary() -> None:
    try:
        oceandb_etl = create_along_track_etl()
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

    total_files = sum(int(row["file_count"]) for row in summary_rows)
    total_observations = sum(int(row["observation_count"]) for row in summary_rows)

    formatted_rows = [
        {
            "mission": str(row["mission"]),
            "start_date": format_timestamp(row["start_date"]),
            "end_date": format_timestamp(row["end_date"]),
            "file_count": str(row["file_count"]),
            "observation_count": str(row["observation_count"]),
        }
        for row in summary_rows
    ]

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
    click.echo(
        format_status_line(
            "TOTAL",
            f"{total_files} file(s) | {total_observations} observation(s)",
            label_color="green",
        )
    )
    for line in render_table(headers, formatted_rows, cell_styler=_summary_cell_styler):
        click.echo(line)


@along_track_group.command("summary")
def summarize_along_track():
    """Summarize ingested along-track missions and their date coverage."""
    _render_along_track_summary()


@summary_group.command("alongtrack")
def summarize_alongtrack_from_summary_group():
    """Summarize ingested along-track missions and their date coverage."""
    _render_along_track_summary()


@click.command("along-track-summary", hidden=True)
def summarize_along_track_legacy():
    """Backward-compatible alias for along-track summary."""
    _render_along_track_summary()
