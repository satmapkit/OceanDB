"""Focused tests; optional PostgreSQL tests own and remove a UUID scratch DB."""

import json
import os
import tempfile
import threading
import time
import unittest
import uuid
import zipfile
from contextlib import ExitStack
from dataclasses import dataclass, replace
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

import psycopg
from psycopg import sql

import benchmark_runtime as runtime
import graph_index_performance as graph
from OceanDB.config import Config
from OceanDB.index_experiment import (
    BenchmarkCleanupError,
    BenchmarkFailure,
    BenchmarkTimeout,
    index_database_is_reusable,
    index_definition,
    index_signature,
    run_index_performance_test_with_timeout,
    setup_index_performance_test,
)
from OceanDB.managed_index_oceandb import DatabaseIndex
from OceanDB.managed_indices import ManagedIndices
from OceanDB.OceanDB_Initializer import OceanDBInit
from OceanDB.query_analysis import BaseQueryScenario, QueryAnalysisRow
from optimize_geo_points_r_dt import build_scenarios, candidate_specs


def row(seconds=1):
    return QueryAnalysisRow(
        "test.sleep", set(), set(), set(), "SELECT 1", [], "", None, None, seconds
    )


def catalog_index(fields=("along_track_point", "date_time"), **overrides):
    definition = index_definition("experiment", fields)
    values = dict(
        schema_name="public",
        table_name="along_track",
        index_name=definition.name,
        index_definition=definition.create_sql,
        access_method="gist",
        index_kind="I",
        table_kind="p",
        is_unique=False,
        is_primary=False,
        is_valid=True,
        is_ready=True,
        constraint_name=None,
        constraint_type=None,
        parent_index_name=None,
        parent_table_name=None,
    )
    return DatabaseIndex(**{**values, **overrides})


def result_node(label, times, status="completed"):
    return {
        "trial_indexes": (
            [
                {
                    "name": "experiment_" + label,
                    "table": "along_track",
                    "create_sql": label,
                }
            ]
            if label
            else []
        ),
        "performance": [
            {
                "total_time": t,
                "scenario_id": f"scenario_{i+1}",
                "scenario_name": "test.sleep",
            }
            for i, t in enumerate(times)
        ],
        "status": status,
        "index_sizes": {"all": 2048 if label else 1024},
        "error": None,
    }


def envelope(nodes, count=6):
    return {
        "format_version": 2,
        "comparison": {
            "workload": {
                "scenarios": [
                    {"id": f"scenario_{i+1}", "method": "test.sleep"}
                    for i in range(count)
                ]
            },
            "dataset": "fixture",
        },
        "results": nodes,
    }


class DefinitionTests(unittest.TestCase):
    def test_candidates_and_grid_preserve_upstream_workload(self):
        included, excluded = candidate_specs()
        self.assertEqual(
            runtime.Counter(len(s["fields"]) for s in included),
            {2: 7, 3: 12, 4: 12, 0: 1},
        )
        self.assertEqual(len(included) + len(excluded), 61)
        self.assertEqual(included[-1]["id"], "baseline")
        self.assertIn("bm", [s["id"] for s in included])
        self.assertIn("mad", [s["id"] for s in included])
        for spec in included[:-1]:
            fields = spec["fields"]
            self.assertNotEqual(fields[0], "date_time")
            if "date_time" in fields:
                self.assertLess(
                    fields.index("along_track_point"), fields.index("date_time")
                )
        scenarios = build_scenarios()
        workload = runtime.workload(scenarios)
        self.assertEqual(len(workload["scenarios"]), 6)
        self.assertTrue(all(s["points"] == 31966 for s in workload["scenarios"]))
        self.assertEqual(
            [s.kwargs.get("max_radius", "radius") for s in scenarios],
            ["radius", None, 500000, "radius", None, 500000],
        )

    def test_definition_order_options_and_child_validity(self):
        desired = index_definition("experiment", ("along_track_point", "date_time"))
        actual = catalog_index()
        self.assertTrue(index_database_is_reusable([], [desired], [actual]))
        for definition in (
            actual.index_definition.replace(
                "along_track_point, date_time", "date_time, along_track_point"
            ),
            actual.index_definition.replace("USING gist", "USING btree"),
            actual.index_definition.replace("buffering=auto", "buffering=off"),
            actual.index_definition.rstrip().rstrip(";") + " WHERE mission = 'x';",
        ):
            with self.subTest(definition=definition):
                self.assertFalse(
                    index_database_is_reusable(
                        [], [desired], [replace(actual, index_definition=definition)]
                    )
                )
        child = replace(
            actual,
            table_name="along_track_2022_10",
            index_name="child",
            parent_index_name=desired.name,
            is_valid=False,
        )
        self.assertFalse(index_database_is_reusable([], [desired], [actual, child]))
        pg = "CREATE INDEX experiment_ad ON ONLY public.along_track USING gist (along_track_point, date_time) WITH (buffering='auto')"
        self.assertEqual(index_signature(pg), index_signature(desired.create_sql))

    def test_setup_never_drops_mismatched_existing_database(self):
        source, target = Mock(), Mock()
        source.inventory_indexes.return_value = []
        target.database_exists.return_value = True
        target.inventory_indexes.return_value = []
        with patch(
            "OceanDB.index_experiment.ocean_db_init_for_test_db", return_value=target
        ):
            with self.assertRaisesRegex(RuntimeError, "refusing to drop"):
                setup_index_performance_test(
                    source, [index_definition("experiment", ("mission",))], "existing"
                )
        target.drop_database.assert_not_called()
        target.create_indexes.assert_not_called()

    def test_source_empty_or_partial_ingestion_rejected(self):
        database = MagicMock()
        cursor = database.cursor.return_value.__enter__.return_value = Mock()
        cursor.fetchall.side_effect = [[], [], []]
        with self.assertRaisesRegex(RuntimeError, "empty"):
            runtime.dataset_facts(database)
        cursor.fetchall.side_effect = [
            [("file", 3, "a", "b", "hash")],
            [("file", 4)],
            [],
        ]
        with self.assertRaisesRegex(RuntimeError, "Incomplete ingestion"):
            runtime.dataset_facts(database)


class ReportingTests(unittest.TestCase):
    def test_ratios_partial_status_and_compatible_merge(self):
        base = result_node("", [10] * 6)
        fast = result_node("ab", [5] * 6)
        timeout = result_node("am", [1] * 6, "timed_out")
        with tempfile.TemporaryDirectory() as tmp:
            first, second = Path(tmp) / "one.json", Path(tmp) / "two.json"
            first.write_text(json.dumps(envelope([base, fast, timeout])))
            second.write_text(
                json.dumps(
                    envelope([result_node("", [20] * 6), result_node("ab", [5] * 6)])
                )
            )
            rows = graph.combine_results((first, second))
            self.assertEqual(len(rows), 2)
            self.assertEqual(rows[0]["mean_time_saved_percent"], 75)
            self.assertAlmostEqual(rows[0]["geometric_mean_speedup"], 4)
            self.assertEqual(rows[0]["source_file"], str(second.resolve()))
            self.assertIsNone(graph.scenario_times(timeout))
            graph.generate_outputs((first,), Path(tmp) / "analysis")
            self.assertIn(
                "timed_out", (Path(tmp) / "analysis/all_configurations.md").read_text()
            )

    def test_incompatible_and_unverified_legacy_runs_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            first, second = Path(tmp) / "one.json", Path(tmp) / "two.json"
            first.write_text(json.dumps(envelope([result_node("", [1] * 6)])))
            second.write_text(json.dumps(envelope([result_node("", [1] * 4)], count=4)))
            with self.assertRaisesRegex(ValueError, "Incompatible"):
                graph.generate_outputs((first, second), Path(tmp) / "analysis")
            second.write_text(json.dumps([result_node("", [1] * 6)]))
            with self.assertRaisesRegex(ValueError, "legacy"):
                graph.combine_results((first, second))
            changed = envelope([result_node("", [1] * 6)])
            changed["comparison"]["dataset"] = "other dataset"
            second.write_text(json.dumps(changed))
            with self.assertRaisesRegex(ValueError, "Incompatible"):
                graph.combine_results((first, second))

    def test_failed_baseline_still_produces_status_report(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "results.json"
            path.write_text(
                json.dumps(
                    envelope(
                        [result_node("", [1], "timed_out"), result_node("ab", [2] * 6)]
                    )
                )
            )
            figure, table = graph.generate_outputs((path,), Path(tmp) / "analysis")
            self.assertIsNone(figure)
            self.assertIn("unavailable", table.read_text())
            self.assertIn(
                "12.000", (Path(tmp) / "analysis/all_configurations.md").read_text()
            )

    def test_success_cannot_have_missing_or_reordered_scenarios(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "results.json"
            content = envelope([result_node("", [1] * 5)])
            path.write_text(json.dumps(content))
            with self.assertRaisesRegex(ValueError, "Incomplete successful"):
                graph.load_results(path)
            content = envelope([result_node("", [1] * 6)])
            content["results"][0]["performance"][0]["scenario_id"] = "scenario_2"
            path.write_text(json.dumps(content))
            with self.assertRaisesRegex(ValueError, "identity/order"):
                graph.load_results(path)


class RunnerTests(unittest.TestCase):
    def fixture(self, directory):
        config = Config()
        run_id = "test"
        specs, excluded = runtime.specs_for_run(run_id)
        comparison = {"code_hashes": {}, "environment": {}, "workload": {"test": 1}}
        manifest = {
            "run_id": run_id,
            "connection": runtime.connection_identity(config),
            "included": specs,
            "excluded": excluded,
            "comparison": comparison,
            "timeout_seconds": 0.5,
            "source": {
                "dataset": {},
                "indexes": [],
                "version": "test",
                "extensions": [],
                "planner_settings": [],
                "data_directory": directory,
                "source_bytes": 1,
            },
        }
        results = [
            {
                **s,
                "status": "pending",
                "performance": [],
                "error": None,
                "exception": None,
            }
            for s in specs
        ]
        env = {
            "comparison": comparison,
            "manifest_sha256": runtime.digest(manifest),
            "results": results,
        }
        runtime.atomic_json(Path(directory) / "manifest.json", manifest)
        runtime.atomic_json(Path(directory) / "results.json", env)
        return manifest, env

    def test_resume_rejects_changed_manifest_and_trial_identity(self):
        with tempfile.TemporaryDirectory() as directory, ExitStack() as stack:
            manifest, env = self.fixture(directory)
            for name, value in (
                ("code_hashes", {}),
                ("environment", {}),
                ("workload", {"test": 1}),
            ):
                stack.enter_context(patch.object(runtime, name, return_value=value))
            runtime.validate_resume(manifest, env, Config(), [None] * 6)
            manifest["timeout_seconds"] = 10
            with self.assertRaisesRegex(RuntimeError, "Manifest changed"):
                runtime.validate_resume(manifest, env, Config(), [None] * 6)
            env["manifest_sha256"] = runtime.digest(manifest)
            env["results"][0]["database_name"] = "unrelated"
            with self.assertRaisesRegex(RuntimeError, "Trial identity"):
                runtime.validate_resume(manifest, env, Config(), [None] * 6)

    def run_fixture(self, directory, outcome, initial=None):
        manifest, env = self.fixture(directory)
        if initial:
            initial(env)
            runtime.atomic_json(Path(directory) / "results.json", env)
        database = Mock()
        stats = {
            "settings": [],
            "columns": [
                (column, True, 1)
                for column in ("mission", "date_time", "basin_id", "along_track_point")
            ],
        }
        with ExitStack() as stack:
            for name, value in (
                ("build_scenarios", [None] * 6),
                ("code_hashes", {}),
                ("environment", {}),
                ("workload", {"test": 1}),
                ("source_facts", manifest["source"]),
                ("check_disk", {}),
                ("statistics", stats),
                ("prepare_trial", database),
            ):
                stack.enter_context(patch.object(runtime, name, return_value=value))
            stack.enter_context(
                patch.object(
                    runtime.shutil,
                    "disk_usage",
                    return_value=Mock(free=1000 * runtime.GIB),
                )
            )
            run_query = stack.enter_context(
                patch.object(
                    runtime,
                    "run_index_performance_test_with_timeout",
                    side_effect=outcome,
                )
            )
            stack.enter_context(patch.object(graph, "generate_outputs"))
            stack.enter_context(patch.object(runtime, "package"))
            try:
                runtime.run(directory)
            finally:
                self.calls = run_query.call_count
        return runtime.read_json(Path(directory) / "results.json")

    def test_timeout_preserves_partial_and_next_trial_continues(self):
        def outcome(*args, on_event, **kwargs):
            on_event("scenario_completed", number=0, result=row())
            raise BenchmarkTimeout("deadline", [row()], 0.5)

        with tempfile.TemporaryDirectory() as directory:
            env = self.run_fixture(directory, outcome)
            self.assertEqual(self.calls, 32)
            self.assertTrue(
                all(
                    r["status"] == "timed_out"
                    and r["error"] is None
                    and len(r["performance"]) == 1
                    for r in env["results"]
                )
            )

    def test_cleanup_failure_aborts_before_next_trial(self):
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaises(BenchmarkCleanupError):
                self.run_fixture(
                    directory, BenchmarkCleanupError("cleanup unavailable")
                )
            self.assertEqual(self.calls, 1)
            env = runtime.read_json(Path(directory) / "results.json")
            self.assertEqual(env["results"][0]["status"], "interrupted")
            self.assertEqual(env["results"][1]["status"], "prepared")
            self.assertEqual(env["status"], "interrupted")

    def test_resume_skips_terminal_trials_and_archives_interrupted_attempt(self):
        def initial(env):
            for number, status in enumerate(
                ("completed", "failed", "timed_out", "running")
            ):
                result = env["results"][number]
                result["status"] = status
                result["performance"] = [runtime.asdict(row(9))] * (
                    6 if number == 0 else 1
                )
            env["results"][0]["error"] = 54

        def outcome(*args, on_event, **kwargs):
            for i in range(6):
                on_event("scenario_completed", number=i, result=row())
            return [row()] * 6

        with tempfile.TemporaryDirectory() as directory:
            env = self.run_fixture(directory, outcome, initial)
            self.assertEqual(self.calls, 29)
            self.assertEqual(env["results"][0]["error"], 54)
            self.assertEqual(env["results"][1]["status"], "failed")
            self.assertEqual(env["results"][2]["status"], "timed_out")
            resumed = env["results"][3]
            self.assertEqual(len(resumed["interrupted_attempts"][0]["performance"]), 1)
            self.assertEqual(len(resumed["performance"]), 6)
            self.assertEqual(resumed["error"], 6)

    def test_zip_preserves_prior_runs_and_has_valid_checksums(self):
        with tempfile.TemporaryDirectory() as parent:
            directory = Path(parent) / "run.1"
            directory.mkdir()
            runtime.atomic_json(directory / "manifest.json", {"run_id": "one"})
            runtime.atomic_json(directory / "results.json", {"status": "timed_out"})
            archive_path = runtime.package(directory)
            self.assertEqual(archive_path.name, "run.1.zip")
            with zipfile.ZipFile(archive_path) as archive:
                self.assertIsNone(archive.testzip())
                for line in archive.read("run.1/SHA256SUMS.txt").decode().splitlines():
                    checksum, name = line.split("  ", 1)
                    self.assertEqual(
                        runtime.hashlib.sha256(
                            archive.read("run.1/" + name)
                        ).hexdigest(),
                        checksum,
                    )
            runtime.atomic_json(directory / "manifest.json", {"run_id": "two"})
            with self.assertRaisesRegex(RuntimeError, "another run"):
                runtime.package(directory)

    def test_atomic_checkpoint_failure_leaves_previous_file_intact(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "checkpoint.json"
            runtime.atomic_json(path, {"complete": True})
            with self.assertRaises(ValueError):
                runtime.atomic_json(path, {"time": float("nan")})
            self.assertEqual(runtime.read_json(path), {"complete": True})


@dataclass(frozen=True)
class SleepScenario(BaseQueryScenario):
    seconds: float
    crash: bool = False

    @property
    def name(self):
        return "test.sleep"

    def run(self, *, config, observer):
        if self.crash:
            raise RuntimeError("intentional scenario failure")
        with psycopg.connect(config.postgres_dsn) as conn:
            query = sql.SQL("SELECT pg_sleep(%(seconds)s)")
            params = {"seconds": self.seconds}
            observer(query, params, query.as_string(conn))
            conn.execute(query, params).fetchall()


@unittest.skipUnless(
    os.environ.get("OCEANDB_BENCHMARK_DB_TESTS") == "1",
    "Opt-in UUID scratch database tests",
)
class PostgreSQLTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.config = Config().model_copy(
            update={"postgres_database": "idx_validation_" + uuid.uuid4().hex[:12]}
        )
        with psycopg.connect(cls.config.postgres_dsn_admin, autocommit=True) as conn:
            conn.execute(
                sql.SQL("CREATE DATABASE {}").format(
                    sql.Identifier(cls.config.postgres_database)
                )
            )
        cls.database = OceanDBInit(
            config=cls.config, managed_indices=ManagedIndices(())
        )
        with psycopg.connect(cls.config.postgres_dsn, autocommit=True) as conn:
            conn.execute("CREATE EXTENSION btree_gist")
            conn.execute(
                "CREATE TABLE along_track (along_track_point int, date_time int, basin_id int, mission text) PARTITION BY RANGE (date_time)"
            )
            conn.execute(
                "CREATE TABLE along_track_1 PARTITION OF along_track FOR VALUES FROM (0) TO (10)"
            )
            conn.execute("INSERT INTO along_track VALUES (1, 1, 1, 'test')")
            conn.execute(
                "ALTER TABLE along_track ADD COLUMN file_name text DEFAULT 'file.nc'"
            )
            conn.execute(
                "CREATE TABLE along_track_metadata (file_name text PRIMARY KEY, observation_count bigint)"
            )
            conn.execute("INSERT INTO along_track_metadata VALUES ('file.nc', 1)")

    @classmethod
    def tearDownClass(cls):
        with psycopg.connect(cls.config.postgres_dsn_admin, autocommit=True) as conn:
            conn.execute(
                sql.SQL("DROP DATABASE {}").format(
                    sql.Identifier(cls.config.postgres_database)
                )
            )

    def test_actual_partition_index_inventory_and_reversed_order(self):
        desired = index_definition("experiment", ("along_track_point", "date_time"))
        source_indexes = self.database.inventory_indexes()
        self.database.create_indexes([desired])
        actual = self.database.inventory_indexes()
        self.assertTrue(
            index_database_is_reusable(source_indexes, [desired], actual), str(actual)
        )
        with psycopg.connect(self.config.postgres_dsn, autocommit=True) as conn:
            conn.execute("DROP INDEX experiment_ad")
            conn.execute(
                "CREATE INDEX experiment_ad ON along_track USING gist (date_time, along_track_point) WITH (buffering=auto)"
            )
        self.assertFalse(
            index_database_is_reusable(
                source_indexes, [desired], self.database.inventory_indexes()
            )
        )
        with psycopg.connect(self.config.postgres_dsn, autocommit=True) as conn:
            conn.execute("DROP INDEX experiment_ad")

    def test_dataset_checksum_detects_changed_values_and_partial_ingestion(self):
        original = runtime.dataset_facts(self.database)
        with psycopg.connect(self.config.postgres_dsn, autocommit=True) as conn:
            try:
                conn.execute("UPDATE along_track SET basin_id=2")
                changed = runtime.dataset_facts(self.database)
                self.assertEqual(original["files"][0][:4], changed["files"][0][:4])
                self.assertNotEqual(original, changed)
                conn.execute("UPDATE along_track_metadata SET observation_count=2")
                with self.assertRaisesRegex(RuntimeError, "Incomplete ingestion"):
                    runtime.dataset_facts(self.database)
            finally:
                conn.execute("UPDATE along_track SET basin_id=1")
                conn.execute("UPDATE along_track_metadata SET observation_count=1")

    def test_foreign_trial_database_is_never_modified(self):
        result = {
            "id": "baseline",
            "fields": [],
            "database_name": self.config.postgres_database,
        }
        with self.assertRaisesRegex(RuntimeError, "not owned"):
            runtime.prepare_trial(
                self.database, {"run_id": "unrelated"}, result, Mock()
            )

    def test_packaged_source_schema_is_accepted_and_wrong_types_rejected(self):
        name = "idx_validation_" + uuid.uuid4().hex[:12]
        config = self.config.model_copy(update={"postgres_database": name})
        database = OceanDBInit(config=config)
        with psycopg.connect(config.postgres_dsn_admin, autocommit=True) as conn:
            conn.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
        try:
            with psycopg.connect(config.postgres_dsn, autocommit=True) as conn:
                conn.execute("CREATE EXTENSION postgis")
                conn.execute(
                    sql.SQL(
                        database.load_sql_file(
                            "tables/along_track/create_along_track_table.sql"
                        )
                    ).format(table_name=sql.Identifier("along_track"))
                )
                conn.execute(
                    "CREATE TABLE along_track_oct PARTITION OF along_track FOR VALUES FROM ('2022-10-01') TO ('2022-11-01')"
                )
                conn.execute(
                    "CREATE TABLE along_track_metadata (file_name text, observation_count bigint)"
                )
                conn.execute("INSERT INTO along_track_metadata VALUES ('file.nc', 1)")
                conn.execute(
                    "INSERT INTO along_track (file_name, mission, date_time, latitude, longitude, basin_id) VALUES ('file.nc', 'test', '2022-10-15', 1, 1, 1)"
                )
            facts = runtime.source_facts(database, check_files=False)
            self.assertEqual(facts["dataset"]["files"][0][1], 1)
            with psycopg.connect(config.postgres_dsn, autocommit=True) as conn:
                conn.execute("ALTER TABLE along_track ALTER COLUMN track TYPE integer")
            with self.assertRaisesRegex(RuntimeError, "column types"):
                runtime.source_facts(database, check_files=False)
        finally:
            with psycopg.connect(config.postgres_dsn_admin, autocommit=True) as conn:
                conn.execute(sql.SQL("DROP DATABASE {}").format(sql.Identifier(name)))

    def test_owned_trial_prepares_and_reuses_same_database(self):
        name = "idx_validation_" + uuid.uuid4().hex[:12]
        result = {"id": "baseline", "fields": [], "database_name": name}
        manifest = {
            "run_id": "scratch",
            "source": {
                "dataset": runtime.dataset_facts(self.database),
                "indexes": [
                    runtime.asdict(i) for i in self.database.inventory_indexes()
                ],
            },
        }
        try:
            with tempfile.TemporaryDirectory() as directory:
                recorder = Mock(directory=Path(directory))
                first = runtime.prepare_trial(self.database, manifest, result, recorder)
                oid = result["database_oid"]
                second = runtime.prepare_trial(
                    self.database, manifest, result, recorder
                )
                self.assertEqual(first.db_name, second.db_name)
                self.assertEqual(result["database_oid"], oid)
                self.assertEqual(len(result["index_sizes"]), 4)
                self.assertTrue(
                    all(size > 0 for size in result["index_sizes"].values())
                )
        finally:
            with psycopg.connect(
                self.config.postgres_dsn_admin, autocommit=True
            ) as conn:
                conn.execute(
                    sql.SQL("DROP DATABASE IF EXISTS {}").format(sql.Identifier(name))
                )

    def test_timeout_cancels_only_worker_and_next_configuration_runs(self):
        unrelated = psycopg.connect(
            self.config.postgres_dsn, autocommit=True, application_name="unrelated_test"
        )
        errors = []

        def unrelated_query():
            try:
                unrelated.execute("SELECT pg_sleep(10)")
            except psycopg.errors.QueryCanceled:
                pass
            except Exception as exc:
                errors.append(exc)

        thread = threading.Thread(target=unrelated_query)
        thread.start()
        application = "idx_validation_" + uuid.uuid4().hex
        try:
            with self.assertRaises(BenchmarkTimeout) as caught:
                run_index_performance_test_with_timeout(
                    self.database,
                    [SleepScenario(0), SleepScenario(5)],
                    0.5,
                    application_name=application,
                )
            self.assertEqual(len(caught.exception.performance), 1)
            self.assertLess(caught.exception.wall_seconds, 1.5)
            with psycopg.connect(self.config.postgres_dsn) as conn:
                self.assertEqual(
                    conn.execute(
                        "SELECT count(*) FROM pg_stat_activity WHERE application_name=%s",
                        (application,),
                    ).fetchone()[0],
                    0,
                )
                self.assertEqual(
                    conn.execute(
                        "SELECT state FROM pg_stat_activity WHERE pid=%s",
                        (unrelated.info.backend_pid,),
                    ).fetchone()[0],
                    "active",
                )
            result = run_index_performance_test_with_timeout(
                self.database, [SleepScenario(0)], 2
            )
            self.assertEqual(len(result), 1)
        finally:
            unrelated.cancel()
            thread.join(timeout=5)
            unrelated.close()
        self.assertFalse(errors)

    def test_scenario_failure_preserves_completed_results(self):
        with self.assertRaises(BenchmarkFailure) as caught:
            run_index_performance_test_with_timeout(
                self.database, [SleepScenario(0), SleepScenario(0, crash=True)], 2
            )
        self.assertNotIsInstance(caught.exception, BenchmarkTimeout)
        self.assertEqual(len(caught.exception.performance), 1)
        self.assertIn("intentional scenario failure", str(caught.exception))

    def test_maintenance_is_outside_query_deadline(self):
        original = self.database.vacuum_analyze

        def slow_maintenance(*args):
            time.sleep(0.4)
            return original(*args)

        with patch.object(
            self.database, "vacuum_analyze", side_effect=slow_maintenance
        ):
            self.assertEqual(
                len(
                    run_index_performance_test_with_timeout(
                        self.database, [SleepScenario(0)], 0.2
                    )
                ),
                1,
            )

    def test_explain_replay_shares_cumulative_deadline(self):
        # The batch itself fits, but its EXPLAIN ANALYZE replay must share the cap.
        with self.assertRaises(BenchmarkTimeout):
            run_index_performance_test_with_timeout(
                self.database, [SleepScenario(0.3)], 0.5
            )


if __name__ == "__main__":
    unittest.main()
