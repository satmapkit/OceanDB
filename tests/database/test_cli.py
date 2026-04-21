from click.testing import CliRunner
import psycopg as pg

from OceanDB import cli as cli_module
from OceanDB.etl.along_track_etl import AlongTrackETL

from .fixtures import *


def test_along_track_summary_command(db_with_alongtrack_data):
    runner = CliRunner()
    original_factory = cli_module._create_along_track_etl

    try:
        cli_module._create_along_track_etl = lambda: db_with_alongtrack_data
        result = runner.invoke(cli_module.cli, ["summary", "alongtrack"])
    finally:
        cli_module._create_along_track_etl = original_factory

    assert result.exit_code == 0
    assert "Mission" in result.output
    assert "j2" in result.output
    assert "2012-12-31" in result.output
    assert "2013-01-09" in result.output


def test_along_track_summary_command_with_no_data(db_with_basin_data):
    along_track_etl = AlongTrackETL(db_with_basin_data.config)
    runner = CliRunner()
    original_factory = cli_module._create_along_track_etl

    try:
        cli_module._create_along_track_etl = lambda: along_track_etl
        result = runner.invoke(cli_module.cli, ["summary", "alongtrack"])
    finally:
        cli_module._create_along_track_etl = original_factory

    assert result.exit_code == 0
    assert "No along-track data has been ingested yet." in result.output


def test_along_track_summary_command_with_database_error():
    runner = CliRunner()
    original_factory = cli_module._create_along_track_etl

    class FailingAlongTrackETL:
        def summarize_ingested_missions(self):
            raise pg.OperationalError("connection refused")

    try:
        cli_module._create_along_track_etl = lambda: FailingAlongTrackETL()
        result = runner.invoke(cli_module.cli, ["summary", "alongtrack"])
    finally:
        cli_module._create_along_track_etl = original_factory

    assert result.exit_code != 0
    assert "Unable to access database" in result.output
