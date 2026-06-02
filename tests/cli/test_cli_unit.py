from datetime import datetime
from functools import cached_property
from types import SimpleNamespace

import psycopg as pg
import pytest
from click.testing import CliRunner

from OceanDB import cli as cli_module
from OceanDB.commands import analysis as analysis_commands
from OceanDB.commands import core as core_commands
from OceanDB.commands import index as index_commands
from OceanDB.commands import shared as shared_commands
from OceanDB.commands import summary as summary_commands

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
    @cached_property
    def managed_index_definitions(self):
        return [
            {
                "logical_name": "along_track_index_time",
                "table_name": "along_track",
                "index_name": "along_track_time_idx",
                "index_definition": (
                    "CREATE INDEX along_track_time_idx ON public.along_track USING btree (date_time)"
                ),
            }
        ]

    def list_indices(self, managed_only=True):
        assert managed_only is True
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
    @cached_property
    def managed_index_definitions(self):
        return []

    def list_indices(self, managed_only=True):
        assert managed_only is True
        return []


class FailingInit:
    @cached_property
    def managed_index_definitions(self):
        return []

    def list_indices(self, managed_only=True):
        raise pg.OperationalError("connection refused")


class FakeInitReturningAllIndices:
    @cached_property
    def managed_index_definitions(self):
        return [
            {
                "logical_name": "along_track_index_time",
                "table_name": "along_track",
                "index_name": "along_track_time_idx",
                "index_definition": (
                    "CREATE INDEX along_track_time_idx ON public.along_track USING btree (date_time)"
                ),
            },
            {
                "logical_name": "eddy_index_point",
                "table_name": "eddy",
                "index_name": "eddy_point_idx",
                "index_definition": (
                    "CREATE INDEX eddy_point_idx ON public.eddy USING gist (point)"
                ),
            },
        ]

    def list_indices(self, managed_only=True):
        assert managed_only is True
        return [
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
                "table_name": "eddy",
                "index_name": "eddy_point_idx",
                "index_definition": (
                    "CREATE INDEX eddy_point_idx ON public.eddy USING gist (point)"
                ),
            },
        ]


class FakeInitReturningDefinedButNoBuiltIndices:
    @cached_property
    def managed_index_definitions(self):
        return [
            {
                "logical_name": "along_track_index_time",
                "table_name": "along_track",
                "index_name": "along_track_time_idx",
                "index_definition": (
                    "CREATE INDEX along_track_time_idx ON public.along_track USING btree (date_time)"
                ),
            }
        ]

    def list_indices(self, managed_only=True):
        assert managed_only is True
        return []


class FailingDropInit:
    def drop_indices(self):
        raise pg.OperationalError("connection refused")

    def drop_eddy_indices(self):
        raise AssertionError("should not be called")


class FailingPartitionCreateInit:
    def create_along_track_index_by_partition(self, logical_name, start_date, end_date):
        raise pg.OperationalError("connection refused")


class FakeInitReturningDefinitions:
    def show_index_definitions(self, identifier=None):
        rows = [
            {
                "logical_name": "along_track_index_point",
                "table_name": "along_track",
                "index_name": "along_track_point_idx",
                "index_definition": (
                    "CREATE INDEX IF NOT EXISTS along_track_point_idx ON public.along_track USING gist (point)"
                ),
                "index_definition_multiline": (
                    "CREATE INDEX IF NOT EXISTS along_track_point_idx\n"
                    "    ON public.along_track USING gist (point)"
                ),
            },
            {
                "logical_name": "along_track_index_time",
                "table_name": "along_track",
                "index_name": "along_track_time_idx",
                "index_definition": (
                    "CREATE INDEX IF NOT EXISTS along_track_time_idx ON public.along_track USING btree (date_time)"
                ),
                "index_definition_multiline": (
                    "CREATE INDEX IF NOT EXISTS along_track_time_idx\n"
                    "    ON public.along_track USING btree (date_time)"
                ),
            },
        ]
        if identifier is None:
            return rows
        return [
            row
            for row in rows
            if row["logical_name"] == identifier or row["index_name"] == identifier
        ]


class FailingShowInit:
    def show_index_definitions(self, identifier=None):
        raise ValueError("Unknown index 'missing_index'")


class FakeInitReturningIndexSummary:
    def list_indices(self, managed_only=True):
        assert managed_only is True
        return [
            {
                "schema_name": "public",
                "table_name": "along_track_2024_01",
                "index_name": "along_track_time_idx_2024_01",
                "index_definition": "",
            },
            {
                "schema_name": "public",
                "table_name": "eddy",
                "index_name": "track_times_cyclonic_type_idx",
                "index_definition": "",
            },
        ]

    def show_partitioned_index_ranges(self, logical_name=None):
        rows = [
            {
                "logical_name": "along_track_index_time",
                "base_index_name": "along_track_time_idx",
                "partition_count": 2,
                "start_partition": "along_track_2024_01",
                "end_partition": "along_track_2024_02",
                "range_number": 1,
            }
        ]
        if logical_name is None:
            return rows
        return [row for row in rows if row["logical_name"] == logical_name]


def test_along_track_summary_command_formats_rows(monkeypatch):
    runner = CliRunner()

    monkeypatch.setattr(
        summary_commands,
        "create_along_track_etl",
        lambda: FakeAlongTrackETLReturningRows(),
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
        summary_commands,
        "create_along_track_etl",
        lambda: FakeAlongTrackETLReturningRows(),
    )

    result = runner.invoke(cli_module.cli, ["summary", "alongtrack"], color=True)

    assert result.exit_code == 0
    assert "\x1b[" in result.output
    assert "SUMMARY" in result.output
    assert "Mission" in result.output


def test_along_track_summary_command_with_no_data(monkeypatch):
    runner = CliRunner()

    monkeypatch.setattr(
        summary_commands,
        "create_along_track_etl",
        lambda: FakeAlongTrackETLReturningEmpty(),
    )

    result = runner.invoke(cli_module.cli, ["summary", "alongtrack"])

    assert result.exit_code == 0
    assert "No along-track data has been ingested yet." in result.output


def test_along_track_summary_command_with_database_error(monkeypatch):
    runner = CliRunner()

    monkeypatch.setattr(
        summary_commands,
        "create_along_track_etl",
        lambda: FailingAlongTrackETL(),
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

    monkeypatch.setattr(core_commands, "OceanDBInit", FakeInit)
    monkeypatch.setattr(core_commands, "create_basins_etl", lambda: FakeBasinsETL())

    result = runner.invoke(cli_module.cli, ["init"])

    assert result.exit_code == 0
    assert calls == ["create_database"]


def test_ingest_mode_status_line_formats():
    rendered = shared_commands.render_ingest_mode("copy")
    assert "MODE" in rendered
    assert "Using COPY ingest mode." in rendered


def test_index_create_all_command_creates_all_indices(monkeypatch):
    runner = CliRunner()
    calls = []

    class FakeInit:
        def create_indices(self):
            calls.append("create_indices")

        def create_eddy_indices(self):
            calls.append("create_eddy_indices")

    monkeypatch.setattr(index_commands, "OceanDBInit", FakeInit)

    result = runner.invoke(cli_module.cli, ["index", "create", "--all"])

    assert result.exit_code == 0
    assert calls == ["create_indices", "create_eddy_indices"]
    assert "All OceanDB-managed indices were created." in result.output


def test_index_create_default_command_creates_default_indices(monkeypatch):
    runner = CliRunner()
    calls = []

    class FakeInit:
        def create_default_indices(self):
            calls.append("create_default_indices")

    monkeypatch.setattr(index_commands, "OceanDBInit", FakeInit)

    result = runner.invoke(cli_module.cli, ["index", "create", "--default"])

    assert result.exit_code == 0
    assert calls == ["create_default_indices"]
    assert "Default OceanDB-managed indices were created." in result.output


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

    monkeypatch.setattr(index_commands, "OceanDBInit", FakeInit)

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

    monkeypatch.setattr(index_commands, "OceanDBInit", FakeInit)

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


def test_index_create_command_rejects_mixed_default_and_all_flags():
    runner = CliRunner()

    result = runner.invoke(
        cli_module.cli,
        [
            "index",
            "create",
            "--default",
            "--all",
        ],
    )

    assert result.exit_code != 0
    assert "Do not combine --default with --all." in result.output


def test_index_create_command_rejects_mixed_default_and_partition_flags():
    runner = CliRunner()

    result = runner.invoke(
        cli_module.cli,
        [
            "index",
            "create",
            "--default",
            "--index-name",
            "along_track_index_time",
        ],
    )

    assert result.exit_code != 0
    assert "Do not combine --default" in result.output


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

    monkeypatch.setattr(index_commands, "OceanDBInit", FailingPartitionCreateInit)

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

    monkeypatch.setattr(index_commands, "OceanDBInit", FakeInitReturningIndices)

    result = runner.invoke(cli_module.cli, ["index", "list"])

    assert result.exit_code == 0
    assert "DEFINED" in result.output
    assert "BUILT" in result.output
    assert "Logical Name" in result.output
    assert "Table" in result.output
    assert "Index" in result.output
    assert "along_track_index_time" in result.output
    assert "along_track" in result.output
    assert "along_track_time_idx" in result.output
    assert "CREATE INDEX" not in result.output


def test_index_list_command_shows_all_defined_indices(monkeypatch):
    runner = CliRunner()

    monkeypatch.setattr(index_commands, "OceanDBInit", FakeInitReturningAllIndices)

    result = runner.invoke(cli_module.cli, ["index", "list"])

    assert result.exit_code == 0
    assert "along_track_time_idx" in result.output
    assert "eddy_point_idx" in result.output


def test_index_list_command_reports_when_no_built_indices_exist(monkeypatch):
    runner = CliRunner()

    monkeypatch.setattr(
        index_commands,
        "OceanDBInit",
        FakeInitReturningDefinedButNoBuiltIndices,
    )

    result = runner.invoke(cli_module.cli, ["index", "list"])

    assert result.exit_code == 0
    assert "No managed indexes are currently built in Postgres" in result.output


def test_index_show_command_formats_definitions(monkeypatch):
    runner = CliRunner()

    monkeypatch.setattr(index_commands, "OceanDBInit", FakeInitReturningDefinitions)

    result = runner.invoke(cli_module.cli, ["index", "show"])

    assert result.exit_code == 0
    assert "DEFINITIONS" in result.output
    assert "along_track_index_point" in result.output
    assert "CREATE INDEX IF NOT EXISTS along_track_point_idx" in result.output
    assert "\n      ON public.along_track USING gist (point)" in result.output


def test_index_show_command_can_filter_by_index_name(monkeypatch):
    runner = CliRunner()

    monkeypatch.setattr(index_commands, "OceanDBInit", FakeInitReturningDefinitions)

    result = runner.invoke(
        cli_module.cli,
        ["index", "show", "--index-name", "along_track_index_time"],
    )

    assert result.exit_code == 0
    assert "along_track_index_time" in result.output
    assert "along_track_index_point" not in result.output
    assert "CREATE INDEX IF NOT EXISTS along_track_time_idx" in result.output
    assert "\n      ON public.along_track USING btree (date_time)" in result.output


def test_index_show_command_accepts_positional_actual_index_name(monkeypatch):
    runner = CliRunner()

    monkeypatch.setattr(index_commands, "OceanDBInit", FakeInitReturningDefinitions)

    result = runner.invoke(
        cli_module.cli,
        ["index", "show", "along_track_time_idx"],
    )

    assert result.exit_code == 0
    assert "along_track_index_time" in result.output
    assert "CREATE INDEX IF NOT EXISTS along_track_time_idx" in result.output


def test_index_show_command_rejects_both_positional_and_option(monkeypatch):
    runner = CliRunner()

    monkeypatch.setattr(index_commands, "OceanDBInit", FakeInitReturningDefinitions)

    result = runner.invoke(
        cli_module.cli,
        [
            "index",
            "show",
            "along_track_time_idx",
            "--index-name",
            "along_track_index_time",
        ],
    )

    assert result.exit_code != 0
    assert "either INDEX_NAME or --index-name" in result.output


def test_index_show_command_with_unknown_index_error(monkeypatch):
    runner = CliRunner()

    monkeypatch.setattr(index_commands, "OceanDBInit", FailingShowInit)

    result = runner.invoke(
        cli_module.cli,
        ["index", "show", "--index-name", "along_track_index_time"],
    )

    assert result.exit_code != 0
    assert "Unknown index" in result.output


def test_index_summary_command_shows_built_indices_and_partition_ranges(monkeypatch):
    runner = CliRunner()

    monkeypatch.setattr(index_commands, "OceanDBInit", FakeInitReturningIndexSummary)

    result = runner.invoke(cli_module.cli, ["index", "summary"])

    assert result.exit_code == 0
    assert "BUILT" in result.output
    assert "PARTITIONS" in result.output
    assert "track_times_cyclonic_type_idx" in result.output
    assert "along_track_index_time" in result.output
    assert "along_track_2024_01" in result.output
    assert "along_track_2024_02" in result.output


def test_index_summary_command_can_filter_partition_ranges(monkeypatch):
    runner = CliRunner()

    monkeypatch.setattr(index_commands, "OceanDBInit", FakeInitReturningIndexSummary)

    result = runner.invoke(
        cli_module.cli,
        ["index", "summary", "--index-name", "along_track_index_time"],
    )

    assert result.exit_code == 0
    assert "along_track_index_time" in result.output
    assert "along_track_2024_01" in result.output


def test_index_list_command_with_no_indices(monkeypatch):
    runner = CliRunner()

    monkeypatch.setattr(index_commands, "OceanDBInit", FakeInitReturningNoIndices)

    result = runner.invoke(cli_module.cli, ["index", "list"])

    assert result.exit_code == 0
    assert (
        "No managed index definitions or built managed indexes are currently available"
        in result.output
    )


def test_index_list_command_with_database_error(monkeypatch):
    runner = CliRunner()

    monkeypatch.setattr(index_commands, "OceanDBInit", FailingInit)

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

    monkeypatch.setattr(index_commands, "OceanDBInit", FakeInit)

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

    monkeypatch.setattr(index_commands, "OceanDBInit", FakeInit)

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

    monkeypatch.setattr(index_commands, "OceanDBInit", FakeInit)

    result = runner.invoke(cli_module.cli, ["index", "drop", "--all"], input="n\n")

    assert result.exit_code == 0
    assert "Index drop canceled." in result.output


def test_index_drop_all_command_with_database_error(monkeypatch):
    runner = CliRunner()

    monkeypatch.setattr(index_commands, "OceanDBInit", FailingDropInit)

    result = runner.invoke(cli_module.cli, ["index", "drop", "--all", "--yes"])

    assert result.exit_code != 0
    assert "Unable to access database" in result.output


def test_analyze_queries_command_prints_metrics_table(monkeypatch):
    runner = CliRunner()

    class FakeQueryAnalysisRunner:
        index_names = {"along_track_point_date_idx", "unused_idx"}

        def analyze_queries(self):
            return [
                SimpleNamespace(
                    scenario_name="AlongTrack.geographic_point_in_r_dt",
                    tables={"along_track"},
                    candidate_indices={"along_track_point_date_idx"},
                    used_indices={"along_track_point_date_idx"},
                    sql="",
                    explain_result_dict=[],
                    explain_result_str="",
                    total_cost=48778.67,
                    total_time=20.984,
                )
            ]

    monkeypatch.setattr(
        analysis_commands,
        "create_query_analysis_runner",
        FakeQueryAnalysisRunner,
    )

    result = runner.invoke(cli_module.cli, ["analyze-queries"])

    assert result.exit_code == 0
    assert "Total Cost" in result.output
    assert "Total Time" in result.output
    assert "48778.670" in result.output
    assert "20.984" in result.output
    assert "along_track_point_date_idx" in result.output
