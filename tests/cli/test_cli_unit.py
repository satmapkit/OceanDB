from datetime import datetime

import psycopg as pg
import pytest
from click.testing import CliRunner

from OceanDB import cli as cli_module

pytestmark = pytest.mark.unit


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


class FakeInitReturningIndices:
    def list_indices(self, managed_only=True):
        return [
            {
                "schema_name": "public",
                "table_name": "along_track",
                "index_name": "along_track_time_idx",
                "index_definition": (
                    "CREATE INDEX along_track_time_idx ON public.along_track USING btree (date_time)"
                ),
            }
        ]


class FakeInitReturningNoIndices:
    def list_indices(self, managed_only=True):
        return []


class FailingInit:
    def list_indices(self, managed_only=True):
        raise pg.OperationalError("connection refused")


class FakeInitReturningAllIndices:
    def list_indices(self, managed_only=True):
        rows = [
            {
                "schema_name": "public",
                "table_name": "along_track",
                "index_name": "along_track_time_idx",
                "index_definition": (
                    "CREATE INDEX along_track_time_idx ON public.along_track USING btree (date_time)"
                ),
            },
            {
                "schema_name": "public",
                "table_name": "along_track_2024_01",
                "index_name": "along_track_2024_01_pkey",
                "index_definition": (
                    "CREATE UNIQUE INDEX along_track_2024_01_pkey ON public.along_track_2024_01 USING btree (date_time, id)"
                ),
            },
        ]
        if managed_only:
            return [rows[0]]
        return rows


class FailingDropInit:
    def drop_indices(self):
        raise pg.OperationalError("connection refused")

    def drop_eddy_indices(self):
        raise AssertionError("should not be called")


class FailingPartitionCreateInit:
    def create_along_track_index_by_partition(self, logical_name, start_date, end_date):
        raise pg.OperationalError("connection refused")


class FakeInitReturningIndexRanges:
    def show_partitioned_index_ranges(self, logical_name=None):
        rows = [
            {
                "logical_name": "along_track_index_point",
                "base_index_name": "along_track_point_idx",
                "partition_count": 2,
                "start_partition": "along_track_2000_01",
                "end_partition": "along_track_2000_02",
                "range_number": 1,
            },
            {
                "logical_name": "along_track_index_point",
                "base_index_name": "along_track_point_idx",
                "partition_count": 2,
                "start_partition": "along_track_2000_05",
                "end_partition": "along_track_2000_06",
                "range_number": 2,
            },
            {
                "logical_name": "along_track_index_time",
                "base_index_name": "along_track_time_idx",
                "partition_count": 1,
                "start_partition": "along_track_2001_01",
                "end_partition": "along_track_2001_01",
                "range_number": 1,
            },
        ]
        if logical_name is None:
            return rows
        return [row for row in rows if row["logical_name"] == logical_name]


class FailingShowInit:
    def show_partitioned_index_ranges(self, logical_name=None):
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


<<<<<<< Updated upstream
def test_ingest_mode_status_line_formats():
    rendered = cli_module._render_ingest_mode("copy")
    assert "MODE" in rendered
    assert "Using COPY ingest mode." in rendered
=======
def test_index_create_all_command_creates_all_indices(monkeypatch):
    runner = CliRunner()
    calls = []

    class FakeInit:
        def create_indices(self):
            calls.append("create_indices")

        def create_eddy_indices(self):
            calls.append("create_eddy_indices")

    monkeypatch.setattr(cli_module, "OceanDBInit", FakeInit)

    result = runner.invoke(cli_module.cli, ["index", "create", "--all"])

    assert result.exit_code == 0
    assert calls == ["create_indices", "create_eddy_indices"]
    assert "All OceanDB-managed indices were created." in result.output


def test_create_indices_alias_creates_all_indices(monkeypatch):
    runner = CliRunner()
    calls = []

    class FakeInit:
        def create_indices(self):
            calls.append("create_indices")

        def create_eddy_indices(self):
            calls.append("create_eddy_indices")

    monkeypatch.setattr(cli_module, "OceanDBInit", FakeInit)

    result = runner.invoke(cli_module.cli, ["create-indices"])

    assert result.exit_code == 0
    assert calls == ["create_indices", "create_eddy_indices"]


def test_index_create_command_prompts_for_partitioned_creation(monkeypatch):
    runner = CliRunner()
    calls = []

    class FakeInit:
        def create_along_track_index_by_partition(
            self, logical_name, start_date, end_date
        ):
            calls.append((logical_name, start_date, end_date))
            return {
                "logical_name": logical_name,
                "base_index_name": "along_track_time_idx",
                "created_partitions": ["along_track_2024_01", "along_track_2024_02"],
                "missing_partitions": ["along_track_2024_03"],
            }

    monkeypatch.setattr(cli_module, "OceanDBInit", FakeInit)

    result = runner.invoke(
        cli_module.cli,
        ["index", "create"],
        input="along_track_index_time\n2024-01-01\n2024-03-01\n",
    )

    assert result.exit_code == 0
    assert calls[0][0] == "along_track_index_time"
    assert calls[0][1].strftime("%Y-%m-%d") == "2024-01-01"
    assert calls[0][2].strftime("%Y-%m-%d") == "2024-03-01"
    assert "along_track_index_time on 2 partition(s)." in result.output
    assert "Starting index creation" not in result.output
    assert "along_track_2024_01" in result.output
    assert "along_track_2024_02" in result.output
    assert "1 partition(s) were missing" in result.output


def test_index_create_command_accepts_explicit_partition_range(monkeypatch):
    runner = CliRunner()
    calls = []

    class FakeInit:
        def create_along_track_index_by_partition(
            self, logical_name, start_date, end_date
        ):
            calls.append((logical_name, start_date, end_date))
            return {
                "logical_name": logical_name,
                "base_index_name": "along_track_point_idx",
                "created_partitions": ["along_track_2024_04"],
                "missing_partitions": [],
            }

    monkeypatch.setattr(cli_module, "OceanDBInit", FakeInit)

    result = runner.invoke(
        cli_module.cli,
        [
            "index",
            "create",
            "--index-name",
            "along_track_index_point",
            "--start-date",
            "2024-04-01",
            "--end-date",
            "2024-04-30",
        ],
    )

    assert result.exit_code == 0
    assert calls[0][0] == "along_track_index_point"
    assert calls[0][1].strftime("%Y-%m-%d") == "2024-04-01"
    assert calls[0][2].strftime("%Y-%m-%d") == "2024-04-30"
    assert "along_track_2024_04" in result.output


def test_index_create_command_rejects_mixed_all_and_partition_flags():
    runner = CliRunner()

    result = runner.invoke(
        cli_module.cli,
        [
            "index",
            "create",
            "--all",
            "--index-name",
            "along_track_index_time",
        ],
    )

    assert result.exit_code != 0
    assert "Do not combine --all" in result.output


def test_index_create_command_rejects_reversed_dates():
    runner = CliRunner()

    result = runner.invoke(
        cli_module.cli,
        [
            "index",
            "create",
            "--index-name",
            "along_track_index_time",
            "--start-date",
            "2024-05-01",
            "--end-date",
            "2024-04-01",
        ],
    )

    assert result.exit_code != 0
    assert "--end-date must be greater than or equal to --start-date." in result.output


def test_index_create_command_with_database_error(monkeypatch):
    runner = CliRunner()

    monkeypatch.setattr(cli_module, "OceanDBInit", FailingPartitionCreateInit)

    result = runner.invoke(
        cli_module.cli,
        [
            "index",
            "create",
            "--index-name",
            "along_track_index_time",
            "--start-date",
            "2024-01-01",
            "--end-date",
            "2024-01-31",
        ],
    )

    assert result.exit_code != 0
    assert "Unable to access database" in result.output


def test_index_list_command_formats_rows(monkeypatch):
    runner = CliRunner()

    monkeypatch.setattr(cli_module, "OceanDBInit", FakeInitReturningIndices)

    result = runner.invoke(cli_module.cli, ["index", "list"])

    assert result.exit_code == 0
    assert "INDICES" in result.output
    assert "Table" in result.output
    assert "Index" in result.output
    assert "along_track" in result.output
    assert "along_track_time_idx" in result.output
    assert "CREATE INDEX" not in result.output


def test_index_list_command_can_show_definitions(monkeypatch):
    runner = CliRunner()

    monkeypatch.setattr(cli_module, "OceanDBInit", FakeInitReturningIndices)

    result = runner.invoke(cli_module.cli, ["index", "list", "--show-definition"])

    assert result.exit_code == 0
    assert "Definition" in result.output
    assert "CREATE INDEX along_track_time_idx" in result.output


def test_index_list_command_defaults_to_managed_indices_only(monkeypatch):
    runner = CliRunner()

    monkeypatch.setattr(cli_module, "OceanDBInit", FakeInitReturningAllIndices)

    result = runner.invoke(cli_module.cli, ["index", "list"])

    assert result.exit_code == 0
    assert "along_track_time_idx" in result.output
    assert "along_track_2024_01_pkey" not in result.output


def test_index_list_command_can_include_all_indices(monkeypatch):
    runner = CliRunner()

    monkeypatch.setattr(cli_module, "OceanDBInit", FakeInitReturningAllIndices)

    result = runner.invoke(cli_module.cli, ["index", "list", "--all"])

    assert result.exit_code == 0
    assert "along_track_time_idx" in result.output
    assert "along_track_2024_01_pkey" in result.output


def test_index_show_command_formats_ranges(monkeypatch):
    runner = CliRunner()

    monkeypatch.setattr(cli_module, "OceanDBInit", FakeInitReturningIndexRanges)

    result = runner.invoke(cli_module.cli, ["index", "show"])

    assert result.exit_code == 0
    assert "RANGES" in result.output
    assert "along_track_index_point" in result.output
    assert "1" in result.output
    assert "along_track_2000_01" in result.output
    assert "along_track_2000_02" in result.output
    assert "along_track_2000_05" in result.output
    assert "along_track_2000_06" in result.output


def test_index_show_command_can_filter_by_index_name(monkeypatch):
    runner = CliRunner()

    monkeypatch.setattr(cli_module, "OceanDBInit", FakeInitReturningIndexRanges)

    result = runner.invoke(
        cli_module.cli,
        ["index", "show", "--index-name", "along_track_index_time"],
    )

    assert result.exit_code == 0
    assert "along_track_index_time" in result.output
    assert "along_track_index_point" not in result.output
    assert "along_track_2001_01" in result.output


def test_index_show_command_supports_multiple_ranges_for_one_index(monkeypatch):
    runner = CliRunner()

    monkeypatch.setattr(cli_module, "OceanDBInit", FakeInitReturningIndexRanges)

    result = runner.invoke(
        cli_module.cli,
        ["index", "show", "--index-name", "along_track_index_point"],
    )

    assert result.exit_code == 0
    assert "along_track_2000_01" in result.output
    assert "along_track_2000_02" in result.output
    assert "along_track_2000_05" in result.output
    assert "along_track_2000_06" in result.output


def test_index_show_command_with_database_error(monkeypatch):
    runner = CliRunner()

    monkeypatch.setattr(cli_module, "OceanDBInit", FailingShowInit)

    result = runner.invoke(cli_module.cli, ["index", "show"])

    assert result.exit_code != 0
    assert "Unable to access database" in result.output


def test_index_list_command_with_no_indices(monkeypatch):
    runner = CliRunner()

    monkeypatch.setattr(cli_module, "OceanDBInit", FakeInitReturningNoIndices)

    result = runner.invoke(cli_module.cli, ["index", "list"])

    assert result.exit_code == 0
    assert "No indices are currently present" in result.output


def test_index_list_command_with_database_error(monkeypatch):
    runner = CliRunner()

    monkeypatch.setattr(cli_module, "OceanDBInit", FailingInit)

    result = runner.invoke(cli_module.cli, ["index", "list"])

    assert result.exit_code != 0
    assert "Unable to access database" in result.output


def test_index_drop_all_command_drops_all_indices(monkeypatch):
    runner = CliRunner()
    calls = []

    class FakeInit:
        def drop_indices(self):
            calls.append("drop_indices")

        def drop_eddy_indices(self):
            calls.append("drop_eddy_indices")

    monkeypatch.setattr(cli_module, "OceanDBInit", FakeInit)

    result = runner.invoke(cli_module.cli, ["index", "drop", "--all", "--yes"])

    assert result.exit_code == 0
    assert calls == ["drop_indices", "drop_eddy_indices"]
    assert "All OceanDB-managed indices were dropped." in result.output


def test_index_drop_all_command_requires_all_flag():
    runner = CliRunner()

    result = runner.invoke(cli_module.cli, ["index", "drop", "--yes"])

    assert result.exit_code != 0
    assert "Pass --all" in result.output


def test_index_drop_all_command_supports_confirmation(monkeypatch):
    runner = CliRunner()
    calls = []

    class FakeInit:
        def drop_indices(self):
            calls.append("drop_indices")

        def drop_eddy_indices(self):
            calls.append("drop_eddy_indices")

    monkeypatch.setattr(cli_module, "OceanDBInit", FakeInit)

    result = runner.invoke(cli_module.cli, ["index", "drop", "--all"], input="y\n")

    assert result.exit_code == 0
    assert calls == ["drop_indices", "drop_eddy_indices"]


def test_index_drop_all_command_can_be_canceled(monkeypatch):
    runner = CliRunner()

    class FakeInit:
        def drop_indices(self):
            raise AssertionError("should not be called")

        def drop_eddy_indices(self):
            raise AssertionError("should not be called")

    monkeypatch.setattr(cli_module, "OceanDBInit", FakeInit)

    result = runner.invoke(cli_module.cli, ["index", "drop", "--all"], input="n\n")

    assert result.exit_code == 0
    assert "Index drop canceled." in result.output


def test_index_drop_all_command_with_database_error(monkeypatch):
    runner = CliRunner()

    monkeypatch.setattr(cli_module, "OceanDBInit", FailingDropInit)

    result = runner.invoke(cli_module.cli, ["index", "drop", "--all", "--yes"])

    assert result.exit_code != 0
    assert "Unable to access database" in result.output
>>>>>>> Stashed changes
