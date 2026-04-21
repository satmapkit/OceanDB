from datetime import datetime

import psycopg as pg
from click.testing import CliRunner

from OceanDB import cli as cli_module


class FakeAlongTrackETLReturningRows:
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


class FakeAlongTrackETLReturningEmpty:
    def summarize_ingested_missions(self):
        return []


class FailingAlongTrackETL:
    def summarize_ingested_missions(self):
        raise pg.OperationalError("connection refused")


def test_along_track_summary_command_formats_rows(monkeypatch):
    runner = CliRunner()

    monkeypatch.setattr(
        cli_module, "_create_along_track_etl", lambda: FakeAlongTrackETLReturningRows()
    )

    result = runner.invoke(cli_module.cli, ["summary", "alongtrack"])

    assert result.exit_code == 0
    assert "Mission" in result.output
    assert "j3" in result.output
    assert "2024-01-01 00:00:00" in result.output
    assert "2024-01-31 00:00:00" in result.output
    assert "345678" in result.output


def test_along_track_summary_command_supports_color(monkeypatch):
    runner = CliRunner()

    monkeypatch.setattr(
        cli_module, "_create_along_track_etl", lambda: FakeAlongTrackETLReturningRows()
    )

    result = runner.invoke(cli_module.cli, ["summary", "alongtrack"], color=True)

    assert result.exit_code == 0
    assert "\x1b[" in result.output
    assert "SUMMARY" in result.output
    assert "Mission" in result.output


def test_along_track_summary_command_with_no_data(monkeypatch):
    runner = CliRunner()

    monkeypatch.setattr(
        cli_module, "_create_along_track_etl", lambda: FakeAlongTrackETLReturningEmpty()
    )

    result = runner.invoke(cli_module.cli, ["summary", "alongtrack"])

    assert result.exit_code == 0
    assert "No along-track data has been ingested yet." in result.output


def test_along_track_summary_command_with_database_error(monkeypatch):
    runner = CliRunner()

    monkeypatch.setattr(
        cli_module, "_create_along_track_etl", lambda: FailingAlongTrackETL()
    )

    result = runner.invoke(cli_module.cli, ["summary", "alongtrack"])

    assert result.exit_code != 0
    assert "Unable to access database" in result.output


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

    class FakeBasinsETL:
        def insert_basins_data(self):
            calls.append("insert_basins_data")

        def insert_basin_connections_data(self):
            calls.append("insert_basin_connections_data")

    monkeypatch.setattr(cli_module, "OceanDBInit", FakeInit)
    monkeypatch.setattr(cli_module, "_create_basins_etl", lambda: FakeBasinsETL())

    result = runner.invoke(cli_module.cli, ["init"])

    assert result.exit_code == 0
    assert calls == ["create_database"]
