import json
import os
import tempfile
import threading
import unittest
import uuid
from pathlib import Path
from unittest.mock import Mock, patch

import numpy as np
import psycopg
from psycopg import sql

import benchmark_runtime as common
import compare_refactor as comparison
from OceanDB.config import Config
from OceanDB.managed_indices import ManagedIndices
from OceanDB.ocean_data.basins import BasinMask
from OceanDB.OceanDB_Initializer import OceanDBInit
from refactor_comparison_worker import canonical_rows

OLD_ROOT = Path(__file__).resolve().parents[3] / "OceanDB-pre-refactor"


class ComparisonUnitTests(unittest.TestCase):
    def test_schedule_is_balanced_and_warmup_precedes_five_measured_passes(self):
        jobs = comparison.schedule()
        self.assertEqual(len(jobs), 24)
        self.assertTrue(all(j["validation"] for j in jobs[:4]))
        self.assertTrue(all(not j["validation"] for j in jobs[4:]))
        for query in ("radius", "nn"):
            for version in ("old", "new"):
                selected = [
                    j for j in jobs if j["query"] == query and j["version"] == version
                ]
                self.assertEqual([j["round"] for j in selected], list(range(6)))
        self.assertEqual(
            [j["version"] for j in jobs if j["query"] == "radius" and j["round"] == 1],
            ["old", "new"],
        )
        self.assertEqual(
            [j["version"] for j in jobs if j["query"] == "radius" and j["round"] == 2],
            ["new", "old"],
        )

    def test_index_keys_preserve_expression_mission_for_legacy(self):
        old, new = comparison.definitions("old"), comparison.definitions("new")
        self.assertEqual(len(old), 5)
        self.assertEqual([d.name for d in old], [d.name for d in new])
        self.assertIn(
            "basin_id, split_part(file_name, '_'::text, 3), along_track_point",
            old[-1].create_sql,
        )
        self.assertIn("basin_id, mission, along_track_point", new[-1].create_sql)
        self.assertNotIn("split_part", new[-1].create_sql)

    def test_empty_result_normalization_and_unordered_rows(self):
        self.assertEqual(canonical_rows(None).shape, (0, 4))
        self.assertEqual(
            canonical_rows(tuple(np.array([np.nan]) for _ in range(4))).shape, (0, 4)
        )
        a = tuple(np.array(v) for v in ([2, 1], [0, 1], [0.3, 0.4], [4, 5]))
        b = tuple(v[::-1] for v in a)
        np.testing.assert_array_equal(canonical_rows(a), canonical_rows(b))

    def test_failed_passes_and_validation_cannot_produce_speedups(self):
        state = {
            "jobs": comparison.schedule(),
            "validation": {q: {"matched": True} for q in ("radius", "nn")},
        }
        for j in state["jobs"]:
            j.update(status="completed", seconds=10 if j["version"] == "old" else 5)
        self.assertEqual(comparison.summary_rows(state)[0]["speedup"], 2)
        state["jobs"][4].update(status="timed_out", seconds=None)
        row = comparison.summary_rows(state)[0]
        self.assertIsNone(row["speedup"])
        self.assertIsNone(row["old_median"])
        state["validation"]["nn"]["matched"] = False
        self.assertIsNone(comparison.summary_rows(state)[1]["speedup"])


@unittest.skipUnless(
    os.environ.get("OCEANDB_BENCHMARK_DB_TESTS") == "1",
    "Opt-in scratch database checks",
)
class ComparisonDatabaseTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.root = tempfile.TemporaryDirectory()
        cls.directory = Path(cls.root.name).resolve()
        cls.run_id = "validation_" + uuid.uuid4().hex[:10]
        cls.names = {
            key: "cmp_" + cls.run_id + "_" + key for key in ("source", "old", "new")
        }
        cls.config = Config()
        cls.addClassCleanup(cls.cleanup)
        cls.source = OceanDBInit(
            config=cls.config.model_copy(
                update={"postgres_database": cls.names["source"]}
            ),
            managed_indices=ManagedIndices(()),
        )
        with psycopg.connect(cls.config.postgres_dsn_admin, autocommit=True) as conn:
            conn.execute(
                sql.SQL("CREATE DATABASE {}").format(sql.Identifier(cls.source.db_name))
            )
        mask = BasinMask()
        cls.basin = int(mask.lookup(-30.0, -30.0))
        with psycopg.connect(cls.source.config.postgres_dsn, autocommit=True) as conn:
            conn.execute("CREATE EXTENSION postgis")
            conn.execute("CREATE EXTENSION btree_gist")
            conn.execute(
                sql.SQL(
                    cls.source.load_sql_file(
                        "tables/along_track/create_along_track_table.sql"
                    )
                ).format(table_name=sql.Identifier("along_track"))
            )
            conn.execute(
                "CREATE TABLE along_track_oct PARTITION OF along_track FOR VALUES FROM ('2022-10-01') TO ('2022-11-01')"
            )
            conn.execute(
                "CREATE TABLE along_track_metadata (file_name text PRIMARY KEY, observation_count bigint)"
            )
            file_name = "dt_global_j3n_phy_l3_1hz_20221015_test.nc"
            for lat, lon, date, sla in (
                (-30, -30, "2022-10-15", 10),
                (-30.01, -30, "2022-10-24", 20),
                (-30.02, -30, "2022-10-06", 30),
                (-30.03, -30, "2022-10-25", 40),
                (-35, -30, "2022-10-15", 50),
            ):
                conn.execute(
                    "INSERT INTO along_track (file_name, mission, latitude, longitude, date_time, sla_filtered, basin_id) VALUES (%s,'j3n',%s,%s,%s,%s,%s)",
                    (file_name, lat, lon, date, sla, cls.basin),
                )
            conn.execute(
                "INSERT INTO along_track_metadata VALUES (%s, 5)", (file_name,)
            )
        snapshot = common.statistics(cls.source)
        facts = {
            "dataset": common.dataset_facts(cls.source),
            "planner_settings": snapshot["settings"],
        }
        manifest = {
            "run_id": cls.run_id,
            "databases": cls.names,
            "data_directory": str(cls.directory),
        }
        recorder = Mock(directory=cls.directory)
        cls.databases = {
            v: comparison.clone(cls.source, cls.config, manifest, v, facts, recorder)
            for v in ("old", "new")
        }
        lats, lons = [-30.0, -30.1, 30.0], [-30.0, -30.0, -150.0]
        basins = {int(mask.lookup(lat, lon)) for lat, lon in zip(lats, lons)}
        inputs = {
            "latitudes": lats,
            "longitudes": lons,
            "radius_m": 50000,
            "central_date": "2022-10-15T00:00:00",
            "basin_map": {str(b): [b] for b in basins},
        }
        common.atomic_json(cls.directory / "inputs.json", inputs)

    @classmethod
    def cleanup(cls):
        with psycopg.connect(cls.config.postgres_dsn_admin, autocommit=True) as conn:
            for name in cls.names.values():
                conn.execute(
                    sql.SQL("DROP DATABASE IF EXISTS {}").format(sql.Identifier(name))
                )
        cls.root.cleanup()

    def test_real_legacy_and_current_apis_return_matching_radius_and_nn_results(self):
        for query in ("radius", "nn"):
            paths = {}
            for version in ("old", "new"):
                directory = self.directory / (query + "_" + version)
                job = {
                    "version": version,
                    "query": query,
                    "validation": True,
                    "code_root": str(OLD_ROOT if version == "old" else comparison.ROOT),
                    "inputs_path": str(self.directory / "inputs.json"),
                }
                result = comparison.supervise(
                    job, self.databases[version].config, directory, Mock(), timeout=10
                )
                self.assertEqual(result["status"], "completed", result)
                self.assertGreater(result["seconds"], 0)
                paths[version] = directory / "validation.npz"
                data = common.read_json(directory / "result.json")
                self.assertIn(
                    "<->" if query == "nn" else "ST_DWithin", data["diagnostics"]["sql"]
                )
            self.assertTrue(
                comparison.match_outputs(paths["old"], paths["new"])["matched"]
            )
            with np.load(paths["old"]) as results:
                self.assertEqual(
                    int(results["counts"][0]), 4 if query == "radius" else 3
                )
                self.assertEqual(int(results["counts"][-1]), 0)

    def test_timeout_cleans_only_owned_sessions_and_next_worker_can_run(self):
        fake = self.directory / "sleep_worker.py"
        fake.write_text(
            "import sys,json,psycopg\nprint(json.dumps({'event':'ready'}),flush=True)\nsys.stdin.readline()\nwith psycopg.connect('') as c: c.execute('SELECT pg_sleep(60)')\n"
        )
        config = self.databases["new"].config
        unrelated = psycopg.connect(
            config.postgres_dsn,
            application_name="comparison_unrelated_test",
            autocommit=True,
        )
        errors = []

        def wait_query():
            try:
                unrelated.execute("SELECT pg_sleep(30)")
            except psycopg.errors.QueryCanceled:
                pass
            except Exception as exc:
                errors.append(exc)

        thread = threading.Thread(target=wait_query)
        thread.start()
        messages = []
        try:
            result = comparison.supervise(
                {},
                config,
                self.directory / "timeout",
                lambda *a, **v: messages.append(v),
                timeout=0.3,
                worker_path=fake,
            )
            self.assertEqual(result["status"], "timed_out")
            self.assertIsNone(result["seconds"])
            with psycopg.connect(config.postgres_dsn) as connection:
                count = connection.execute(
                    "SELECT count(*) FROM pg_stat_activity WHERE application_name=%s",
                    (result["application_name"],),
                ).fetchone()[0]
                self.assertEqual(count, 0)
                self.assertEqual(
                    connection.execute(
                        "SELECT state FROM pg_stat_activity WHERE pid=%s",
                        (unrelated.info.backend_pid,),
                    ).fetchone()[0],
                    "active",
                )
            job = {
                "version": "new",
                "query": "nn",
                "validation": False,
                "code_root": str(comparison.ROOT),
                "inputs_path": str(self.directory / "inputs.json"),
            }
            following = comparison.supervise(
                job, config, self.directory / "after_timeout", Mock(), timeout=5
            )
            self.assertEqual(following["status"], "completed", following)
        finally:
            unrelated.cancel()
            thread.join(timeout=5)
            unrelated.close()
        self.assertFalse(errors)

    def test_cleanup_failure_is_fatal(self):
        fake = self.directory / "exit_worker.py"
        fake.write_text(
            "import sys,json\nprint(json.dumps({'event':'ready'}),flush=True)\nsys.stdin.readline()\n"
        )
        with patch.object(
            comparison,
            "_cleanup_index_performance_sessions",
            side_effect=comparison.BenchmarkCleanupError("test cleanup failure"),
        ):
            with self.assertRaises(comparison.BenchmarkCleanupError):
                comparison.supervise(
                    {},
                    self.databases["new"].config,
                    self.directory / "cleanup_failure",
                    Mock(),
                    timeout=2,
                    worker_path=fake,
                )


if __name__ == "__main__":
    unittest.main()
