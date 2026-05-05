from click.testing import CliRunner
import pytest

from OceanDB import cli as cli_module
from OceanDB.etl.along_track_etl import AlongTrackETL

from tests.database.fixtures import *

pytestmark = pytest.mark.uses_database


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


def test_along_track_summary_command_supports_color(db_with_alongtrack_data):
    runner = CliRunner()
    original_factory = cli_module._create_along_track_etl

    try:
        cli_module._create_along_track_etl = lambda: db_with_alongtrack_data
        result = runner.invoke(cli_module.cli, ["summary", "alongtrack"], color=True)
    finally:
        cli_module._create_along_track_etl = original_factory

    assert result.exit_code == 0
    assert "\x1b[" in result.output
    assert "SUMMARY" in result.output
    assert "Mission" in result.output


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
