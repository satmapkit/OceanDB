import hashlib
import json
import os
import subprocess
import sys
import tempfile
import unittest
import uuid
from pathlib import Path
from unittest.mock import Mock, patch

import numpy as np
import psycopg
from psycopg import sql

import compare_native_workflow as native
import compare_refactor as matched
from legacy_workflow_setup import discovery_missions
from OceanDB.config import Config
from refactor_comparison_worker import canonical_rows

OLD_ROOT = native.ROOT.parent / "OceanDB-pre-refactor"


class NativeWorkflowTests(unittest.TestCase):
    def test_native_discovery_uses_full_filename_mission_tags(self):
        files = [
            {"name": f"dt_global_{mission}_phy_l3_1hz_20221001_20240205.nc"}
            for mission in ("h2b", "s6a_lr")
        ]
        labels = discovery_missions(files)
        self.assertEqual(labels, ["h2b", "s6a_lr"])
        self.assertTrue(
            all(
                any("_" + label + "_phy_l3" in item["name"] for label in labels)
                for item in files
            )
        )

    def test_native_builder_has_eight_original_indexes_including_adbm(self):
        definitions = native.legacy_index_sql(OLD_ROOT)
        self.assertEqual(len(definitions), 8)
        self.assertIn(
            "(along_track_point, date_time, basin_id, split_part",
            definitions["create_along_track_index_point_date_mission_basin.sql"],
        )
        self.assertNotIn("create_along_track_index_point_date_mission.sql", definitions)
        self.assertNotIn("create_along_track_index_date.sql", definitions)
        self.assertFalse(any("experiment_bma" in d for d in definitions.values()))

    def test_longitude_conventions_normalize_without_changing_science_values(self):
        first = tuple(
            np.array([v]) for v in (193.945679, -60.012162, 0.069, -441033.491252)
        )
        second = tuple(v.copy() for v in first)
        second[0][0] = (second[0][0] + 180) % 360 - 180
        np.testing.assert_array_equal(
            canonical_rows(first, normalize_longitude=True),
            canonical_rows(second, normalize_longitude=True),
        )
        self.assertEqual(first[0][0], 193.945679)
        self.assertEqual(canonical_rows(first, normalize_longitude=True)[0, 2], 0.069)

    def test_native_nearest_validation_accepts_only_real_boundary_choices(self):
        candidates = [
            (193.0, -60.0, 10, 0, 1),
            (194.0, -60.0, 20, 0, 2),
            (195.0, -60.0, 30, 0, 3),
            (195.0, -60.0, 40, 86400, 3),
        ]
        values = np.asarray(
            [
                [-167.0, -60.0, 0.01, 0],
                [-166.0, -60.0, 0.02, 0],
                [-165.0, -60.0, 0.04, 86400],
            ]
        )
        self.assertTrue(native.native_nearest_matches(values, candidates))
        values[-1, 2] = 0.045
        self.assertFalse(native.native_nearest_matches(values, candidates))

    def test_map_differences_are_reported_but_equal_filter_errors_are_rejected(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            native.common.atomic_json(
                root / "inputs-new.json",
                {
                    "latitudes": [-30.0],
                    "longitudes": [-30.0],
                    "central_date": "2022-10-15T00:00:00",
                    "basin_map": {},
                },
            )
            for query in ("radius", "nn"):
                for version in ("old", "new"):
                    rows = (
                        np.array(
                            [[-30.0, -30.0, 0.01 if version == "old" else 0.02, 0.0]]
                        )
                        if query == "radius"
                        else np.empty((0, 4))
                    )
                    p = root / "jobs" / f"warmup_{query}_{version}"
                    p.mkdir(parents=True)
                    np.savez(
                        p / "validation.npz",
                        rows=rows,
                        counts=[len(rows)],
                        offsets=[0, len(rows)],
                        hashes=[hashlib.sha256(rows.tobytes()).hexdigest()],
                    )
            for changed in (True, False):
                native.common.atomic_json(
                    root / "basin-map-differences.json",
                    [{"location_index": 0}] if changed else [],
                )
                with patch.object(native.psycopg, "connect"):
                    target = Mock()
                    target.load_sql_file.return_value = "SELECT reference_neighbors"
                    result = native.validate(root, target, Mock())
                self.assertEqual(result["radius"]["valid"], changed)
                self.assertFalse(result["radius"]["identical_results"])
                self.assertEqual(
                    result["radius"]["native_map_result_differences"], int(changed)
                )
                self.assertEqual(
                    result["radius"]["unexplained_difference_locations"],
                    [] if changed else [0],
                )


@unittest.skipUnless(
    os.environ.get("OCEANDB_NATIVE_TEST_FILE"),
    "Opt-in native one-file PostgreSQL setup",
)
class NativeDatabaseTests(unittest.TestCase):
    def test_unchanged_native_setup_and_inventory(self):
        config = Config()
        name = "native_test_" + uuid.uuid4().hex[:10]
        raw = Path(os.environ["OCEANDB_NATIVE_TEST_FILE"])
        with tempfile.TemporaryDirectory() as folder:
            directory = Path(folder)
            manifest = {
                "old_root": str(OLD_ROOT),
                "databases": {"old": name},
                "files": [{"path": str(raw), "name": raw.name}],
            }
            (directory / "manifest.json").write_text(json.dumps(manifest))
            try:
                env = matched.pg_environment(config, "native_fixture_" + name)
                env["PGDATABASE"] = "postgres"
                result = subprocess.run(
                    [sys.executable, "-B", str(native.SETUP), str(directory)],
                    env=env,
                    capture_output=True,
                    text=True,
                    timeout=90,
                )
                self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
                target = matched.database(config, name)
                inventory = native.verify_legacy_indexes(target)
                parents = [i for i in inventory if i.table_name == "along_track"]
                self.assertEqual(
                    len(parents), 9
                )  # Eight native indexes plus the primary key.
                with target.cursor() as cur:
                    cur.execute(
                        "SELECT count(*), bool_and(file_name LIKE '{%}') FROM along_track"
                    )
                    count, native_filename_format = cur.fetchone()
                    self.assertGreater(count, 0)
                    self.assertTrue(native_filename_format)
                    cur.execute(
                        "SELECT count(*) FROM information_schema.columns WHERE table_name='along_track' AND column_name='mission'"
                    )
                    self.assertEqual(cur.fetchone()[0], 0)
                facts = native.observed_data(target)
                self.assertEqual(facts[0][0], raw.name)
                self.assertEqual(facts[0][1], count)
                if os.environ.get("OCEANDB_NATIVE_COMPARISON_SOURCE"):
                    source = matched.database(
                        config, os.environ["OCEANDB_NATIVE_COMPARISON_SOURCE"]
                    )
                    source_facts = native.observed_data(source)
                    self.assertEqual(
                        facts[0],
                        next(row for row in source_facts if row[0] == raw.name),
                    )
                self.assertTrue(
                    json.loads((directory / "legacy-setup.json").read_text())[
                        "basin_map"
                    ]
                )
            finally:
                with psycopg.connect(
                    config.postgres_dsn_admin, autocommit=True
                ) as connection:
                    connection.execute(
                        sql.SQL("DROP DATABASE IF EXISTS {}").format(
                            sql.Identifier(name)
                        )
                    )


if __name__ == "__main__":
    unittest.main()
