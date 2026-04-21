from click.testing import CliRunner
from datetime import datetime
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


def test_along_track_summary_command_formats_stubbed_rows(monkeypatch):
    runner = CliRunner()

    class FakeAlongTrackETL:
        def summarize_ingested_missions(self):
            return [
                {
                    "mission": "j3",
                    "start_date": datetime(2024, 1, 1, 0, 0, 0),
                    "end_date": datetime(2024, 1, 31, 0, 0, 0),
                    "file_count": 12,
                    "observation_count": 345678,
                }
            ]

    monkeypatch.setattr(
        cli_module, "_create_along_track_etl", lambda: FakeAlongTrackETL()
    )

    result = runner.invoke(cli_module.cli, ["summary", "alongtrack"], color=True)

    assert result.exit_code == 0
    assert "\x1b[" in result.output
    assert "SUMMARY" in result.output
    assert "j3" in result.output
    assert "345678" in result.output


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


def test_init_exits_early_when_database_exists(monkeypatch):
    runner = CliRunner()
    calls = []

    class FakeInit:
        def create_database(self):
            calls.append("create_database")
            return False

        def create_tables(self):
            calls.append("create_tables")

        def create_eddy_tables(self):
            calls.append("create_eddy_tables")

        def create_partitions(self, min_date, max_date):
            calls.append(("create_partitions", min_date, max_date))

    class FakeBaseETL:
        def insert_basins_data(self):
            calls.append("insert_basins_data")

        def insert_basin_connections_data(self):
            calls.append("insert_basin_connections_data")

    monkeypatch.setattr(cli_module, "OceanDBInit", FakeInit)
    monkeypatch.setattr(cli_module, "_create_basins_etl", lambda: FakeBaseETL())

    result = runner.invoke(cli_module.cli, ["init"])

    assert result.exit_code == 0
    assert calls == ["create_database"]
