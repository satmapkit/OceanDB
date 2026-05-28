from __future__ import annotations

from datetime import datetime

import click
import psycopg as pg

from OceanDB.OceanDB_Initializer import OceanDBInit
from OceanDB.cli_utils import format_status_line, render_table, style_value
from OceanDB.commands.shared import partitioned_index_choices


@click.group("index")
def index_group():
    """Index management commands."""


def _create_all_indices() -> None:
    ocean_db_init = OceanDBInit()
    ocean_db_init.create_indices()
    ocean_db_init.create_eddy_indices()


def _render_partitioned_index_choices() -> None:
    click.echo(
        format_status_line(
            "AVAILABLE",
            "Along-track indices you can create by partition range:",
            label_color="blue",
        )
    )
    for index_name in partitioned_index_choices():
        click.echo(style_value(f"  - {index_name}", fg="cyan"))


def _drop_all_indices() -> None:
    ocean_db_init = OceanDBInit()
    ocean_db_init.drop_indices()
    ocean_db_init.drop_eddy_indices()


def _render_index_list(show_definition: bool = False, include_all: bool = False) -> None:
    try:
        ocean_db_init = OceanDBInit()
        index_rows = ocean_db_init.list_indices(managed_only=not include_all)
    except pg.OperationalError as e:
        raise click.ClickException(f"Unable to access database: {e}") from e

    if not index_rows:
        click.echo(
            format_status_line(
                "EMPTY",
                "No indices are currently present in the configured schema.",
                label_color="yellow",
            )
        )
        return

    headers = [("Table", "table_name"), ("Index", "index_name")]
    if show_definition:
        headers.append(("Definition", "index_definition"))

    formatted_rows = []
    for row in index_rows:
        formatted_row = {
            "table_name": str(row["table_name"]),
            "index_name": str(row["index_name"]),
        }
        if show_definition:
            formatted_row["index_definition"] = str(row["index_definition"])
        formatted_rows.append(formatted_row)

    def _index_cell_styler(column: str, value: str) -> str:
        if column == "table_name":
            return style_value(value, fg="cyan", bold=True)
        if column == "index_name":
            return style_value(value, fg="green")
        return style_value(value, fg="white")

    click.echo(
        format_status_line(
            "INDICES",
            f"{len(formatted_rows)} index(es) found.",
            label_color="magenta",
        )
    )
    for line in render_table(headers, formatted_rows, cell_styler=_index_cell_styler):
        click.echo(line)


def _render_partitioned_index_ranges(index_name: str | None = None) -> None:
    try:
        ocean_db_init = OceanDBInit()
        range_rows = ocean_db_init.show_partitioned_index_ranges(logical_name=index_name)
    except pg.OperationalError as e:
        raise click.ClickException(f"Unable to access database: {e}") from e
    except ValueError as e:
        raise click.UsageError(str(e)) from e

    if not range_rows:
        click.echo(
            format_status_line(
                "EMPTY",
                "No partitioned managed indices are currently present.",
                label_color="yellow",
            )
        )
        return

    formatted_rows = [
        {
            "logical_name": str(row["logical_name"]),
            "range_number": str(row["range_number"]),
            "partition_count": str(row["partition_count"]),
            "start_partition": str(row["start_partition"] or "-"),
            "end_partition": str(row["end_partition"] or "-"),
        }
        for row in range_rows
    ]

    click.echo(
        format_status_line(
            "RANGES",
            f"{len(formatted_rows)} partitioned index range(s) found.",
            label_color="magenta",
        )
    )
    for line in render_table(
        [
            ("Index", "logical_name"),
            ("Range", "range_number"),
            ("Partitions", "partition_count"),
            ("Start", "start_partition"),
            ("End", "end_partition"),
        ],
        formatted_rows,
        cell_styler=lambda column, value: (
            style_value(value, fg="cyan", bold=True)
            if column == "logical_name"
            else style_value(value, fg="green" if column == "partition_count" else "white")
        ),
    ):
        click.echo(line)


@index_group.command("create")
@click.option(
    "--all",
    "create_all",
    is_flag=True,
    help="Create all OceanDB-managed indices with the legacy full-table flow.",
)
@click.option(
    "--index-name",
    type=click.Choice(partitioned_index_choices(), case_sensitive=False),
    help="Along-track index to create across a partition range.",
)
@click.option(
    "--start-date",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    help="Start date used to select monthly along-track partitions.",
)
@click.option(
    "--end-date",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    help="End date used to select monthly along-track partitions.",
)
def create_index_command(
    create_all: bool,
    index_name: str | None,
    start_date: datetime | None,
    end_date: datetime | None,
):
    """Create OceanDB indices."""
    if create_all:
        if index_name or start_date or end_date:
            raise click.UsageError(
                "Do not combine --all with --index-name, --start-date, or --end-date."
            )
        _create_all_indices()
        click.echo(
            format_status_line(
                "CREATED",
                "All OceanDB-managed indices were created.",
                label_color="green",
            )
        )
        return

    choice_type = click.Choice(partitioned_index_choices(), case_sensitive=False)
    if index_name is None:
        _render_partitioned_index_choices()
    selected_index = index_name or click.prompt("Index name", type=choice_type, show_choices=False)
    selected_start_date = start_date or click.prompt(
        "Start partition date (YYYY-MM-DD)",
        type=click.DateTime(formats=["%Y-%m-%d"]),
    )
    selected_end_date = end_date or click.prompt(
        "End partition date (YYYY-MM-DD)",
        type=click.DateTime(formats=["%Y-%m-%d"]),
    )

    if selected_end_date < selected_start_date:
        raise click.UsageError("--end-date must be greater than or equal to --start-date.")

    try:
        ocean_db_init = OceanDBInit()
        result = ocean_db_init.create_along_track_index_by_partition(
            selected_index,
            selected_start_date,
            selected_end_date,
        )
    except pg.OperationalError as e:
        raise click.ClickException(f"Unable to access database: {e}") from e
    except ValueError as e:
        raise click.UsageError(str(e)) from e

    click.echo(
        format_status_line(
            "CREATED",
            f"{result['logical_name']} on {len(result['created_partitions'])} partition(s).",
            label_color="green",
        )
    )

    created_rows = [{"partition_name": partition_name} for partition_name in result["created_partitions"]]
    if created_rows:
        for line in render_table(
            [("Partition", "partition_name")],
            created_rows,
            cell_styler=lambda _column, value: style_value(value, fg="cyan"),
        ):
            click.echo(line)

    if result["missing_partitions"]:
        click.echo(
            format_status_line(
                "SKIPPED",
                f"{len(result['missing_partitions'])} partition(s) were missing from the database.",
                label_color="yellow",
            )
        )
        for partition_name in result["missing_partitions"]:
            click.echo(style_value(partition_name, fg="yellow"))


@index_group.command("list")
@click.option(
    "--show-definition",
    is_flag=True,
    help="Include each index definition from pg_indexes.",
)
@click.option(
    "--all",
    "include_all",
    is_flag=True,
    help="Include non-managed indexes such as primary-key and unique indexes.",
)
def list_index_command(show_definition: bool, include_all: bool):
    """List currently created OceanDB indices."""
    _render_index_list(show_definition=show_definition, include_all=include_all)


@index_group.command("show")
@click.option(
    "--index-name",
    type=click.Choice(partitioned_index_choices(), case_sensitive=False),
    help="Show the partition range for one along-track managed index.",
)
def show_index_command(index_name: str | None):
    """Show partition coverage for managed along-track indices."""
    _render_partitioned_index_ranges(index_name=index_name)


@index_group.command("drop")
@click.option(
    "--all",
    "drop_all",
    is_flag=True,
    help="Drop all OceanDB-managed indices.",
)
@click.option(
    "--yes",
    is_flag=True,
    help="Skip the confirmation prompt.",
)
def drop_index_command(drop_all: bool, yes: bool):
    """Drop OceanDB indices."""
    if not drop_all:
        raise click.UsageError("Pass --all to confirm you want to drop all indices.")

    if not yes:
        proceed = click.confirm(
            "Drop all OceanDB-managed indices from the configured database?"
        )
        if not proceed:
            click.echo("Index drop canceled.")
            return

    try:
        _drop_all_indices()
    except pg.OperationalError as e:
        raise click.ClickException(f"Unable to access database: {e}") from e

    click.echo(
        format_status_line(
            "DROPPED",
            "All OceanDB-managed indices were dropped.",
            label_color="yellow",
        )
    )
