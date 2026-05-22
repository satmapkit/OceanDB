from __future__ import annotations

import click

from OceanDB.commands.core import create_indices, init, process, visualize_basins
from OceanDB.commands.download import (
    download_along_track_legacy,
    download_eddy_legacy,
    download_group,
)
from OceanDB.commands.index import index_group
from OceanDB.commands.ingest import (
    ingest_along_track_legacy,
    ingest_eddy_legacy,
    ingest_group,
)
from OceanDB.commands.summary import (
    along_track_group,
    summarize_along_track_legacy,
    summary_group,
)


def create_cli() -> click.Group:
    @click.group()
    def cli():
        pass

    cli.add_command(summary_group)
    cli.add_command(index_group)
    cli.add_command(download_group)
    cli.add_command(ingest_group)

    cli.add_command(process)
    cli.add_command(init)
    cli.add_command(create_indices)
    cli.add_command(visualize_basins)

    cli.add_command(download_along_track_legacy)
    cli.add_command(download_eddy_legacy)
    cli.add_command(ingest_along_track_legacy)
    cli.add_command(ingest_eddy_legacy)
    cli.add_command(along_track_group)
    cli.add_command(summarize_along_track_legacy)

    return cli


cli = create_cli()
