from __future__ import annotations

from datetime import datetime

import click
import psycopg as pg

from OceanDB.cli_utils import format_status_line, render_table, style_value
from OceanDB.commands.shared import partitioned_index_choices
from OceanDB.OceanDB_Initializer import OceanDBInit


@click.group("index")
def index_group():
    """Index management commands."""


def _create_all_indices() -> None:
    ocean_db_init = OceanDBInit()
    ocean_db_init.create_indices()
    ocean_db_init.create_eddy_indices()


def _create_default_indices() -> None:
    ocean_db_init = OceanDBInit()
    ocean_db_init.create_default_indices()


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


def _render_index_list() -> None:
    ocean_db_init = OceanDBInit()
    try:
        defined_index_rows = ocean_db_init.list_defined_indices()
        built_index_rows = ocean_db_init.list_indices(managed_only=True)
    except pg.OperationalError as e:
        raise click.ClickException(f"Unable to access database: {e}") from e

    if not defined_index_rows and not built_index_rows:
        click.echo(
            format_status_line(
                "EMPTY",
                "No managed index definitions or built managed indexes are currently available.",
                label_color="yellow",
            )
        )
        return

    def _built_index_cell_styler(column: str, value: str) -> str:
        if column == "table_name":
            return style_value(value, fg="cyan", bold=True)
        if column == "index_name":
            return style_value(value, fg="green")
        return style_value(value, fg="white")

    if built_index_rows:
        click.echo(
            format_status_line(
                "BUILT",
                f"{len(built_index_rows)} managed index(es) currently present in Postgres.",
                label_color="blue",
            )
        )
        built_formatted_rows = [
            {
                "table_name": str(row["table_name"]),
                "index_name": str(row["index_name"]),
            }
            for row in built_index_rows
        ]
        for line in render_table(
            [
                ("Table", "table_name"),
                ("Index", "index_name"),
            ],
            built_formatted_rows,
            cell_styler=_built_index_cell_styler,
        ):
            click.echo(line)
    else:
        click.echo(
            format_status_line(
                "BUILT",
                "No managed indexes are currently built in Postgres.",
                label_color="yellow",
            )
        )

    click.echo()

    def _defined_index_cell_styler(column: str, value: str) -> str:
        if column == "logical_name":
            return style_value(value, fg="magenta", bold=True)
        if column == "table_name":
            return style_value(value, fg="cyan", bold=True)
        if column == "index_name":
            return style_value(value, fg="green")
        return style_value(value, fg="white")

    if defined_index_rows:
        click.echo(
            format_status_line(
                "DEFINED",
                f"{len(defined_index_rows)} managed index definition(s).",
                label_color="magenta",
            )
        )
        defined_formatted_rows = [
            {
                "logical_name": str(row["logical_name"]),
                "table_name": str(row["table_name"]),
                "index_name": str(row["index_name"]),
            }
            for row in defined_index_rows
        ]
        for line in render_table(
            [
                ("Logical Name", "logical_name"),
                ("Table", "table_name"),
                ("Index", "index_name"),
            ],
            defined_formatted_rows,
            cell_styler=_defined_index_cell_styler,
        ):
            click.echo(line)
    else:
        click.echo(
            format_status_line(
                "DEFINED",
                "No managed index definitions are currently available.",
                label_color="yellow",
            )
        )


def _render_partitioned_index_ranges(index_name: str | None = None) -> None:
    try:
        ocean_db_init = OceanDBInit()
        range_rows = ocean_db_init.show_partitioned_index_ranges(
            logical_name=index_name
        )
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
            else style_value(
                value, fg="green" if column == "partition_count" else "white"
            )
        ),
    ):
        click.echo(line)


def _render_index_summary(index_name: str | None = None) -> None:
    try:
        ocean_db_init = OceanDBInit()
        built_index_rows = ocean_db_init.list_indices(managed_only=True)
        range_rows = ocean_db_init.show_partitioned_index_ranges(
            logical_name=index_name
        )
    except pg.OperationalError as e:
        raise click.ClickException(f"Unable to access database: {e}") from e
    except ValueError as e:
        raise click.UsageError(str(e)) from e

    if not built_index_rows and not range_rows:
        click.echo(
            format_status_line(
                "EMPTY",
                "No managed indexes or partition coverage information are currently available.",
                label_color="yellow",
            )
        )
        return

    if built_index_rows:
        click.echo(
            format_status_line(
                "BUILT",
                f"{len(built_index_rows)} managed index(es) currently present in Postgres.",
                label_color="blue",
            )
        )
        built_formatted_rows = [
            {
                "table_name": str(row["table_name"]),
                "index_name": str(row["index_name"]),
            }
            for row in built_index_rows
        ]
        for line in render_table(
            [("Table", "table_name"), ("Index", "index_name")],
            built_formatted_rows,
            cell_styler=lambda column, value: (
                style_value(value, fg="cyan", bold=True)
                if column == "table_name"
                else style_value(value, fg="green")
            ),
        ):
            click.echo(line)
    else:
        click.echo(
            format_status_line(
                "BUILT",
                "No managed indexes are currently built in Postgres.",
                label_color="yellow",
            )
        )

    click.echo()

    if range_rows:
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
                "PARTITIONS",
                f"{len(formatted_rows)} partition coverage row(s) found.",
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
                else style_value(
                    value, fg="green" if column == "partition_count" else "white"
                )
            ),
        ):
            click.echo(line)
    else:
        click.echo(
            format_status_line(
                "PARTITIONS",
                "No partition coverage information is currently available.",
                label_color="yellow",
            )
        )


@index_group.command("create")
@click.option(
    "--default",
    "create_default",
    is_flag=True,
    help="Create the default curated set of OceanDB-managed indices.",
)
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
    create_default: bool,
    create_all: bool,
    index_name: str | None,
    start_date: datetime | None,
    end_date: datetime | None,
):
    """Create OceanDB indices."""
    if create_default and create_all:
        raise click.UsageError("Do not combine --default with --all.")

    if create_default:
        if index_name or start_date or end_date:
            raise click.UsageError(
                "Do not combine --default with --index-name, --start-date, or --end-date."
            )
        _create_default_indices()
        click.echo(
            format_status_line(
                "CREATED",
                "Default OceanDB-managed indices were created.",
                label_color="green",
            )
        )
        return

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
    selected_index = index_name or click.prompt(
        "Index name", type=choice_type, show_choices=False
    )
    selected_start_date = start_date or click.prompt(
        "Start partition date (YYYY-MM-DD)",
        type=click.DateTime(formats=["%Y-%m-%d"]),
    )
    selected_end_date = end_date or click.prompt(
        "End partition date (YYYY-MM-DD)",
        type=click.DateTime(formats=["%Y-%m-%d"]),
    )

    if selected_end_date < selected_start_date:
        raise click.UsageError(
            "--end-date must be greater than or equal to --start-date."
        )

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

    created_rows = [
        {"partition_name": partition_name}
        for partition_name in result["created_partitions"]
    ]
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


def _render_index_definitions(index_name: str | None = None) -> None:
    ocean_db_init = OceanDBInit()
    try:
        index_rows = ocean_db_init.show_index_definitions(identifier=index_name)
    except ValueError as e:
        raise click.UsageError(str(e)) from e

    if not index_rows:
        click.echo(
            format_status_line(
                "EMPTY",
                "No matching managed index definitions were found.",
                label_color="yellow",
            )
        )
        return

    click.echo(
        format_status_line(
            "DEFINITIONS",
            f"{len(index_rows)} managed index definition(s) found.",
            label_color="magenta",
        )
    )

    for row in index_rows:
        click.echo(
            format_status_line(
                row["logical_name"],
                f"{row['index_name']} on {row['table_name']}",
                label_color="blue",
            )
        )
        for definition_line in row["index_definition_multiline"].splitlines():
            click.echo(f"  {definition_line}")
        click.echo()


@index_group.command("list")
def list_index_command():
    """List OceanDB-managed index definitions."""
    _render_index_list()


@index_group.command("show")
@click.argument("index_name", required=False)
@click.option(
    "--index-name",
    "index_name_option",
    help="Backward-compatible option form of the index identifier.",
)
def show_index_command(index_name: str | None, index_name_option: str | None):
    """Show CREATE INDEX statements for managed indices."""
    if index_name and index_name_option:
        raise click.UsageError("Pass either INDEX_NAME or --index-name, not both.")

    selected_index_name = index_name or index_name_option
    _render_index_definitions(index_name=selected_index_name)


@index_group.command("summary")
@click.option(
    "--index-name",
    type=click.Choice(partitioned_index_choices(), case_sensitive=False),
    help="Limit partition coverage output to one along-track partitioned index.",
)
def summary_index_command(index_name: str | None):
    """Show built managed indexes and partition coverage for partitioned indexes."""
    _render_index_summary(index_name=index_name)


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
