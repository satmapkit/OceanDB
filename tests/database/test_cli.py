from click.testing import CliRunner

from OceanDB import cli as cli_module

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
    assert "2013-01-01 12:00:00" in result.output
    assert "2013-01-09 12:00:00" in result.output


def test_along_track_summary_command_with_no_data(db_with_basin_data):
    runner = CliRunner()
    original_factory = cli_module._create_along_track_etl

    try:
        cli_module._create_along_track_etl = lambda: db_with_basin_data
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
    monkeypatch.setattr(cli_module, "_create_base_etl", lambda: FakeBaseETL())

    result = runner.invoke(cli_module.cli, ["init"])

    assert result.exit_code == 0
    assert calls == ["create_database"]
