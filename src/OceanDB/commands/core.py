from __future__ import annotations

import webbrowser
from pathlib import Path

import click

from OceanDB.commands.shared import logger
from OceanDB.utils.basin_visualization import write_basin_map
from OceanDB.workflows import initialize_database


@click.command()
def process():
    logger.info("OceanDB")


@click.command()
def init():
    initialize_database()


@click.command()
@click.option(
    "--output",
    type=click.Path(path_type=Path, dir_okay=False, writable=True),
    default=Path("artifacts/basin_map.html"),
    show_default=True,
    help="HTML output path for the basin visualization.",
)
def visualize_basins(output: Path):
    """Render a quick basin polygon map with basin IDs into a standalone HTML file."""
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
