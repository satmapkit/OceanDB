import click
from OceanDB.commands.analysis import analyze_queries
from OceanDB.commands.core import init, process, visualize_basins
from OceanDB.commands.download import download, download_eddy
from OceanDB.commands.index import index_group
from OceanDB.commands.ingest import ingest_along_track, ingest_eddy
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
    cli.add_command(along_track_group)
    cli.add_command(index_group)

    cli.add_command(process)
    cli.add_command(init)
    cli.add_command(analyze_queries)
    cli.add_command(visualize_basins)
    cli.add_command(ingest_eddy)
    cli.add_command(download)
    cli.add_command(download_eddy)
    cli.add_command(summarize_along_track_legacy)
    cli.add_command(ingest_along_track)

    return cli


cli = create_cli()
