import click

from OceanDB.logger import get_logger

logger = get_logger()


@click.group()
def cli():
    pass

@cli.command()
def process():
    logger.info("OceanDB")
