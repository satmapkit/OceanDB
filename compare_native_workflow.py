"""Compare the unmodified pre-refactor workflow against current queries with BMA."""

import argparse
import ast
import fcntl
import hashlib
import html
import json
import re
import shutil
import statistics
import subprocess
import sys
import time
import traceback
import uuid
import zipfile
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import psycopg
from psycopg import sql

import benchmark_runtime as common
import compare_refactor as matched
from legacy_workflow_setup import discovery_missions
from OceanDB.config import Config
from OceanDB.index_experiment import index_signature
from OceanDB.ocean_data.basins import BasinConnections, BasinMask
from refactor_comparison_worker import canonical_rows

ROOT = Path(__file__).resolve().parent
SETUP = ROOT / "legacy_workflow_setup.py"
LEGACY_KEYS = {
    "along_track_file_name_idx": ("btree", "file_name"),
    "along_track_time_idx": ("btree", "date_time"),
    "along_track_basin_idx": ("btree", "basin_id"),
    "along_track_mission_idx": ("btree", "split_part(file_name, '_'::text, 3)"),
    "along_track_point_idx": ("gist", "along_track_point"),
    "along_track_point_geom_idx": ("gist", "(along_track_point::geometry)"),
    "along_track_point_date_idx": ("gist", "along_track_point|date_time"),
    "along_track_point_date_mission_basin_idx": (
        "gist",
        "along_track_point|date_time|basin_id|split_part(file_name, '_'::text, 3)",
    ),
}


def code_hashes(old_root):
    hashes = matched.code_hashes(old_root)
    for path in (Path(__file__), SETUP):
        hashes["new/" + path.name] = hashlib.sha256(path.read_bytes()).hexdigest()
    return hashes


def legacy_index_sql(old_root):
    tree = ast.parse((old_root / "python/OceanDB/AlongTrack.py").read_text())
    method = next(
        n
        for n in ast.walk(tree)
        if isinstance(n, ast.FunctionDef) and n.name == "create_along_track_indices"
    )
    files = [
        n.args[0].value
        for n in ast.walk(method)
        if isinstance(n, ast.Call)
        and isinstance(n.func, ast.Attribute)
        and n.func.attr == "sql_query_with_name"
    ]
    statements = {name: (old_root / "sql" / name).read_text() for name in files}
    names = {
        re.search(r"CREATE INDEX IF NOT EXISTS (\w+)", statement)[1]
        for statement in statements.values()
    }
    if names != set(LEGACY_KEYS):
        raise RuntimeError(
            "The native index builder differs from the expected eight indexes"
        )
    return statements


def prepare(previous, directory, *, reuse_databases=False):
    previous, directory = previous.resolve(), directory.resolve()
    prior = common.read_json(previous / "manifest.json")
    prior_state = common.read_json(previous / "results.json")
    allowed_statuses = (
        {"failed", "completed", "completed_with_failures"}
        if reuse_databases
        else {"completed"}
    )
    if prior_state["status"] not in allowed_statuses or prior_state[
        "manifest_sha256"
    ] != common.digest(prior):
        raise RuntimeError(
            "Expected a terminal previous comparison with an unchanged manifest"
        )
    if reuse_databases and prior.get("comparison") != "native_workflows":
        raise RuntimeError("Only native workflow databases can be reused here")
    config, old_root = Config(), Path(prior["old_root"])
    if (
        common.connection_identity(config) != prior["connection"]
        or common.environment() != prior["environment"]
    ):
        raise RuntimeError("Environment or PostgreSQL connection changed")
    if subprocess.check_output(
        ["git", "status", "--porcelain", "--untracked-files=no"],
        cwd=old_root,
        text=True,
    ).strip():
        raise RuntimeError("Legacy checkout has tracked changes")
    hashes = code_hashes(old_root)
    for name, value in prior["code_hashes"].items():
        if name.startswith("old/") or name.startswith("new/src/"):
            if hashes.get(name) != value:
                raise RuntimeError(f"Production code/data changed: {name}")
    if (
        shutil.disk_usage(prior["data_directory"]).free
        < (20 if reuse_databases else 40) * common.GIB
    ):
        raise RuntimeError("Insufficient disk reserve for comparison preparation")
    directory.mkdir(parents=True, exist_ok=False)
    run_id = (
        datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ").lower()
        + "_"
        + uuid.uuid4().hex[:6]
    )
    manifest = {
        **prior,
        "format_version": 2,
        "comparison": "native_workflows",
        "run_id": run_id,
        "created_at": common.now(),
        "code_hashes": hashes,
        "new_commit": subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True
        ).strip(),
        "databases": {
            "source": prior["databases"]["source"],
            "old": f"native_{run_id}_old",
            "new": f"native_{run_id}_new",
        },
        "source_owner_run_id": prior.get(
            "source_owner_run_id", prior.get("database_owner_run_id", prior["run_id"])
        ),
        "source_provenance": {
            "directory": str(previous),
            "manifest_sha256": common.digest(prior),
        },
        "old_native_index_sql": legacy_index_sql(old_root),
        "legacy_ingest_mission_labels": discovery_missions(prior["files"]),
        "basin_policy": "Copy current basin_connections rows into legacy basin_connection; retain native map-loading code and report remaining differences",
        "longitude_validation": "Wrap to [-180,180) and round to 1e-10 degrees outside timing",
    }
    for key in ("reuse_preparation", "database_owner_run_id"):
        manifest.pop(key, None)
    manifest["indexes"] = {
        "old": "Unmodified create_along_track_indices(); see old_native_index_sql",
        "new": prior["indexes"]["new"],
    }
    if reuse_databases:
        saved = directory / "reused-preparation"
        saved.mkdir()
        for name in (
            "manifest.json",
            "source-database.json",
            "old-database.json",
            "new-database.json",
            "native-observation-audit.json",
            "legacy-setup.json",
        ):
            shutil.copyfile(previous / name, saved / name)
        manifest["databases"] = prior["databases"]
        manifest["database_owner_run_id"] = prior.get(
            "database_owner_run_id", prior["run_id"]
        )
        manifest["reuse_preparation"] = {
            "previous_directory": str(previous),
            "files": {
                p.name: hashlib.sha256(p.read_bytes()).hexdigest()
                for p in saved.iterdir()
            },
        }
        shutil.copyfile(saved / "legacy-setup.json", directory / "legacy-setup.json")
    for name in ("grid.json", "source-database.json"):
        shutil.copyfile(previous / name, directory / ("previous-" + name))
    common.atomic_json(
        directory / "grid.json", common.read_json(previous / "grid.json")
    )
    common.atomic_json(directory / "manifest.json", manifest)
    state = {
        "status": "prepared",
        "manifest_sha256": common.digest(manifest),
        "jobs": matched.schedule(),
    }
    recorder = common.Recorder(directory, state)
    recorder.save()
    with zipfile.ZipFile(
        directory / "source-code.zip", "x", zipfile.ZIP_DEFLATED
    ) as archive:
        for name in hashes:
            version, relative = name.split("/", 1)
            archive.write((old_root if version == "old" else ROOT) / relative, name)
    recorder.event(
        "prepared",
        databases=manifest["databases"],
        jobs=24,
        points=31966,
        native_legacy_indexes=8,
        current_indexes=5,
    )
    return directory


def verify_legacy_indexes(target):
    with target.cursor() as cur:
        cur.execute(
            "SELECT c.relname, a.amname, ARRAY(SELECT pg_get_indexdef(i.indexrelid,k,true) FROM generate_series(1,i.indnkeyatts) k) "
            "FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid JOIN pg_am a ON a.oid=c.relam "
            "WHERE i.indrelid='along_track'::regclass AND NOT i.indisprimary"
        )
        keys = {
            name: (method, "|".join(fields)) for name, method, fields in cur.fetchall()
        }
    if keys != LEGACY_KEYS:
        raise RuntimeError(f"Native legacy index keys differ: {keys}")
    inventory = target.inventory_indexes()
    relevant = [
        i
        for i in inventory
        if i.table_name == "along_track" or i.parent_table_name == "along_track"
    ]
    parents = {i.index_name: i for i in relevant if i.table_name == "along_track"}
    if set(parents) != set(LEGACY_KEYS) | {"cop_along_pkey"}:
        raise RuntimeError("Unexpected legacy constraint or index")
    if any(not i.is_valid or not i.is_ready for i in relevant):
        raise RuntimeError("Invalid native legacy index")
    for item in relevant:
        if item.parent_index_name:
            if index_signature(item.index_definition) != index_signature(
                parents[item.parent_index_name].index_definition
            ):
                raise RuntimeError("Native child index differs from its parent")
    return inventory


def observed_data(target):
    with target.cursor() as cur:
        cur.execute(target.load_sql_file("queries/benchmark/native_dataset_facts.sql"))
        return json.loads(common.canonical(cur.fetchall()))


def prepare_databases(config, manifest, recorder):
    if manifest.get("reuse_preparation"):
        return reuse_native_databases(config, manifest, recorder)
    directory = recorder.directory
    source = matched.database(config, manifest["databases"]["source"])
    with psycopg.connect(config.postgres_dsn_admin) as connection:
        owner = connection.execute(
            "SELECT shobj_description(oid, 'pg_database') FROM pg_database WHERE datname=%s",
            (source.db_name,),
        ).fetchone()
        if owner != ("OceanDB comparison " + manifest["source_owner_run_id"],):
            raise RuntimeError("Source ownership changed")
        if connection.execute(
            "SELECT 1 FROM pg_database WHERE datname=ANY(%s)",
            ([manifest["databases"][v] for v in ("old", "new")],),
        ).fetchone():
            raise RuntimeError("A native trial database already exists")
    facts = common.source_facts(source)
    if (
        facts["dataset"]
        != common.read_json(directory / "previous-source-database.json")["dataset"]
    ):
        raise RuntimeError("Source observations changed")
    common.atomic_json(directory / "source-database.json", facts)
    recorder.event("legacy_setup_started", log=str(directory / "legacy-setup.log"))
    env = matched.pg_environment(config, "native_setup_" + manifest["run_id"])
    env["PGDATABASE"] = (
        "postgres"  # Native create_database() connects before its target exists.
    )
    with (directory / "legacy-setup.log").open("w") as log:
        subprocess.run(
            [sys.executable, "-B", "-u", str(SETUP), str(directory)],
            env=env,
            stdin=subprocess.DEVNULL,
            stdout=log,
            stderr=subprocess.STDOUT,
            check=True,
        )
    old = matched.database(config, manifest["databases"]["old"])
    with psycopg.connect(config.postgres_dsn_admin, autocommit=True) as connection:
        connection.execute(
            sql.SQL("COMMENT ON DATABASE {} IS {}").format(
                sql.Identifier(old.db_name),
                sql.Literal("OceanDB native comparison " + manifest["run_id"]),
            )
        )
    verify_legacy_indexes(old)
    old_facts = observed_data(old)
    source_values = observed_data(source)
    common.atomic_json(
        directory / "native-observation-audit.json",
        {
            "old": old_facts,
            "current_source": source_values,
            "matched": old_facts == source_values,
        },
    )
    if old_facts != source_values:
        raise RuntimeError(
            "Native ingestion produced different observed values; see native-observation-audit.json"
        )
    if (
        sum(r[1] for r in old_facts) != matched.EXPECTED_ROWS
        or len(old_facts) != matched.EXPECTED_FILES
    ):
        raise RuntimeError("Unexpected native dataset size")
    snapshot = common.statistics(old)
    if common.digest(snapshot["settings"]) != common.digest(facts["planner_settings"]):
        raise RuntimeError("Legacy planner settings differ")
    snapshot["index_sizes"] = {
        name: sum(
            old.get_index_size(i.index_name)
            for i in old.inventory_indexes()
            if i.index_name == name or i.parent_index_name == name
        )
        for name in LEGACY_KEYS
    }
    common.atomic_json(directory / "old-database.json", snapshot)
    recorder.event("legacy_setup_completed", rows=matched.EXPECTED_ROWS, indexes=8)
    new = matched.clone(source, config, manifest, "new", facts, recorder)
    return prepare_corrected_basins(old, new, manifest, recorder)


def copy_corrected_basins(old, new):
    """Copy only connectivity data, transactionally, preserving the legacy schema."""
    with new.cursor() as cur:
        cur.execute("SELECT basin_id, connected_id FROM basin_connections ORDER BY pid")
        rows = cur.fetchall()
    if not rows:
        raise RuntimeError("Current basin connection table is empty")
    with old.cursor(commit=True) as cur:
        cur.execute("SELECT basin_id, connected_id FROM basin_connection ORDER BY pid")
        before = cur.fetchall()
        cur.execute("TRUNCATE basin_connection RESTART IDENTITY")
        cur.executemany(
            "INSERT INTO basin_connection (basin_id, connected_id) VALUES (%s, %s)",
            rows,
        )
        cur.execute("SELECT basin_id, connected_id FROM basin_connection ORDER BY pid")
        after = cur.fetchall()
        if after != rows:
            raise RuntimeError("Corrected basin copy differs from its source")
    return {
        "copied_at": common.now(),
        "source_database": new.db_name,
        "source_table": "basin_connections",
        "target_database": old.db_name,
        "target_table": "basin_connection",
        "before": before,
        "after": after,
        "source_sha256": common.digest(rows),
        "target_sha256": common.digest(after),
        "schema_and_query_code_changed": False,
    }


def prepare_corrected_basins(old, new, manifest, recorder):
    directory = recorder.directory
    audit = copy_corrected_basins(old, new)
    common.atomic_json(directory / "basin-connection-update.json", audit)
    old.vacuum_analyze("basin_connection")
    verify_legacy_indexes(old)
    # Read the map through the original helper in an isolated interpreter. No map
    # override or extra self-links are added to the old implementation.
    script = """import json, sys
from refactor_comparison_worker import make_client
job=json.loads(sys.argv[1])
client,module=make_client(job, {'basin_map': {}})
print(json.dumps({'module': module, 'basin_map': client.basin_connection_map}))
"""
    result = subprocess.check_output(
        [
            sys.executable,
            "-B",
            "-c",
            script,
            json.dumps(
                {
                    "version": "old",
                    "native_workflow": True,
                    "code_root": manifest["old_root"],
                }
            ),
        ],
        env=matched.pg_environment(old.config, "native_basin_preparation"),
        text=True,
    )
    legacy = json.loads(result)
    common.atomic_json(directory / "corrected-legacy-map.json", legacy)
    maps = {
        "old": legacy["basin_map"],
        "new": {
            str(int(k)): list(map(int, v))
            for k, v in BasinConnections(config=new.config).connection_map.items()
        },
    }
    grid = common.read_json(directory / "grid.json")
    for version in ("old", "new"):
        common.atomic_json(
            directory / f"inputs-{version}.json", {**grid, "basin_map": maps[version]}
        )
    mask = BasinMask()
    differences = []
    for i, (lat, lon) in enumerate(
        zip(grid["latitudes"], grid["longitudes"], strict=True)
    ):
        basin = str(int(mask.lookup(lat, lon)))
        if set(maps["old"].get(basin) or []) != set(maps["new"].get(basin) or []):
            differences.append(
                {
                    "location_index": i,
                    "latitude": lat,
                    "longitude": lon,
                    "basin_id": int(basin),
                    "old": maps["old"].get(basin),
                    "new": maps["new"].get(basin),
                }
            )
    common.atomic_json(directory / "basin-map-differences.json", differences)
    recorder.event("native_maps_compared", different_locations=len(differences))
    return {"old": old, "new": new}


def reuse_native_databases(config, manifest, recorder):
    saved = recorder.directory / "reused-preparation"
    for name, expected in manifest["reuse_preparation"]["files"].items():
        if hashlib.sha256((saved / name).read_bytes()).hexdigest() != expected:
            raise RuntimeError("Saved native preparation changed")
    names = manifest["databases"]
    owner = manifest["database_owner_run_id"]
    expected_owners = {
        names["source"]: "OceanDB comparison " + manifest["source_owner_run_id"],
        names["old"]: "OceanDB native comparison " + owner,
        names["new"]: "OceanDB comparison " + owner,
    }
    with psycopg.connect(config.postgres_dsn_admin) as connection:
        owners = dict(
            connection.execute(
                "SELECT datname, shobj_description(oid, 'pg_database') FROM pg_database WHERE datname=ANY(%s)",
                (list(names.values()),),
            ).fetchall()
        )
        if owners != expected_owners:
            raise RuntimeError("Native database ownership changed")
        if connection.execute(
            "SELECT pid FROM pg_stat_activity WHERE datname=ANY(%s) AND backend_type='client backend'",
            (list(names.values()),),
        ).fetchall():
            raise RuntimeError("Native databases still have client sessions")
    recorder.event("native_preparation_reuse_started", databases=names)
    source = matched.database(config, names["source"])
    facts = common.source_facts(source)
    prior = common.read_json(saved / "source-database.json")
    for key in (
        "dataset",
        "indexes",
        "version",
        "extensions",
        "planner_settings",
        "data_directory",
    ):
        if facts[key] != prior[key]:
            raise RuntimeError(f"Source preparation changed: {key}")
    common.atomic_json(recorder.directory / "source-database.json", facts)
    targets = {v: matched.database(config, names[v]) for v in ("old", "new")}
    verify_legacy_indexes(targets["old"])
    values = {}
    for version, target in targets.items():
        snapshot = common.read_json(saved / f"{version}-database.json")
        if common.digest(target.inventory_indexes()) != common.digest(
            snapshot["indexes"]
        ):
            raise RuntimeError(f"Reused {version} index inventory changed")
        values[version] = observed_data(target)
    original_audit = common.read_json(saved / "native-observation-audit.json")
    if (
        not original_audit["matched"]
        or values["old"] != values["new"]
        or values["old"] != original_audit["old"]
    ):
        raise RuntimeError("Reused native observations differ")
    common.atomic_json(
        recorder.directory / "native-observation-audit.json",
        {**values, "matched": True},
    )
    for version, target in targets.items():
        target.vacuum_analyze("along_track")
        snapshot = common.statistics(target)
        if common.digest(snapshot["settings"]) != common.digest(
            facts["planner_settings"]
        ):
            raise RuntimeError("Reused planner settings differ")
        snapshot["index_sizes"] = common.read_json(saved / f"{version}-database.json")[
            "index_sizes"
        ]
        common.atomic_json(recorder.directory / f"{version}-database.json", snapshot)
    recorder.event("native_preparation_reused", rows=matched.EXPECTED_ROWS)
    return prepare_corrected_basins(targets["old"], targets["new"], manifest, recorder)


def native_nearest_matches(rows, candidates):
    if len(rows) != min(3, len(candidates)):
        return False
    if not candidates:
        return True
    distances = np.asarray([r[4] for r in candidates], dtype=float)
    values = np.asarray([r[:4] for r in candidates], dtype=float)
    values[:, 2] *= 0.001

    def counts(array):
        return Counter(
            r.tobytes() for r in canonical_rows(array.T, normalize_longitude=True)
        )

    chosen, allowed = counts(rows), counts(values)
    required = counts(values[distances < distances[-1]])
    return not (chosen - allowed) and not (required - chosen)


def validate(directory, target, recorder):
    inputs = common.read_json(directory / "inputs-new.json")
    changed_map = {
        row["location_index"]
        for row in common.read_json(directory / "basin-map-differences.json")
    }
    mask = BasinMask()
    statement = target.load_sql_file(
        "queries/benchmark/nearest_boundary_validation.sql"
    )
    validation, proofs = {}, []
    with psycopg.connect(
        target.config.postgres_dsn,
        options="-c default_transaction_read_only=on -c statement_timeout=30000",
        application_name="native_tie_validation",
    ) as connection:
        started = time.monotonic()
        for query in ("radius", "nn"):
            versions = {}
            for version in ("old", "new"):
                with np.load(
                    directory / "jobs" / f"warmup_{query}_{version}" / "validation.npz"
                ) as data:
                    versions[version] = {k: data[k] for k in data.files}
            a, b = versions["old"], versions["new"]
            if a["counts"].shape != b["counts"].shape:
                raise RuntimeError("Different location counts")
            different = np.flatnonzero(
                (a["counts"] != b["counts"]) | (a["hashes"] != b["hashes"])
            )
            ties, map_diffs, unexplained, examples = [], [], [], []
            for index in different:
                i = int(index)
                left = a["rows"][a["offsets"][i] : a["offsets"][i + 1]]
                right = b["rows"][b["offsets"][i] : b["offsets"][i + 1]]
                if i in changed_map:
                    map_diffs.append(i)
                    kind = "native_basin_filters_differ"
                elif query == "nn":
                    if time.monotonic() - started > matched.TIMEOUT:
                        raise RuntimeError(
                            "Native output validation exceeded its deadline"
                        )
                    lat, lon = inputs["latitudes"][i], inputs["longitudes"][i]
                    params = {
                        "latitude": lat,
                        "longitude": lon,
                        "central_date_time": datetime.fromisoformat(
                            inputs["central_date"]
                        ),
                        "time_delta": timedelta(days=10),
                        "connected_basin_ids": inputs["basin_map"][
                            str(int(mask.lookup(lat, lon)))
                        ],
                    }
                    candidates = connection.execute(statement, params).fetchall()
                    accepted = native_nearest_matches(
                        left, candidates
                    ) and native_nearest_matches(right, candidates)
                    proofs.append(
                        {
                            "location_index": i,
                            "accepted": accepted,
                            "parameters": params,
                            "candidates": [
                                [float(v) if v is not None else None for v in r]
                                for r in candidates
                            ],
                        }
                    )
                    if accepted:
                        ties.append(i)
                        continue
                    unexplained.append(i)
                    kind = "unexplained"
                else:
                    unexplained.append(i)
                    kind = "unexplained"
                if len(examples) < 20:
                    examples.append(
                        {
                            "location_index": i,
                            "reason": kind,
                            "old_count": len(left),
                            "new_count": len(right),
                            "old_preview": [
                                [float(v) if np.isfinite(v) else None for v in r]
                                for r in left[:3]
                            ],
                            "new_preview": [
                                [float(v) if np.isfinite(v) else None for v in r]
                                for r in right[:3]
                            ],
                        }
                    )
            validation[query] = {
                "valid": not unexplained,
                "identical_results": not len(different),
                "points": len(a["counts"]),
                "old_rows": int(a["counts"].sum()),
                "new_rows": int(b["counts"].sum()),
                "verified_boundary_ties": len(ties),
                "native_map_result_differences": len(map_diffs),
                "native_map_difference_locations": map_diffs,
                "unexplained_difference_locations": unexplained,
                "examples": examples,
            }
            recorder.event(
                "query_validation_completed",
                query=query,
                valid=not unexplained,
                map_differences=len(map_diffs),
                verified_ties=len(ties),
                unexplained=len(unexplained),
            )
    common.atomic_json(directory / "validation.json", validation)
    common.atomic_json(
        directory / "nearest-tie-proofs.json", {"sql": statement, "checks": proofs}
    )
    return validation


def report(directory, manifest, state):
    rows = []
    for query in ("radius", "nn"):
        row = {"query": query, "speedup": None, "saved_percent": None}
        for version in ("old", "new"):
            values = [
                j["seconds"]
                for j in state["jobs"]
                if j["version"] == version
                and j["query"] == query
                and not j["validation"]
                and j["status"] == "completed"
            ]
            row[version + "_completed"] = len(values)
            row[version + "_median"] = (
                statistics.median(values) if len(values) == 5 else None
            )
            row[version + "_range"] = (
                [min(values), max(values)] if len(values) == 5 else None
            )
        if (
            row["old_median"]
            and row["new_median"]
            and state.get("validation", {}).get(query, {}).get("valid")
        ):
            row["speedup"] = row["old_median"] / row["new_median"]
            row["saved_percent"] = 100 * (1 - row["new_median"] / row["old_median"])
        rows.append(row)
    common.atomic_json(directory / "summary.json", rows)
    description = (
        f"Status: {state['status']}. Native pre-refactor code {manifest['old_commit'][:12]} versus current code {manifest['new_commit'][:12]}. "
        "Old: native schema, tuple ingestion, eight-index builder and VACUUM ANALYZE; no BMA was added. "
        "Current: native schema and basin map, BMA plus four singleton GiST indexes. Same 209 source files and 31,966 locations, "
        "October 15, 2022 ±10 days, all missions. Radius 50 km; nearest three without a radius cap. "
        "Medians of five warm-cache API batch passes include fetching and result processing; one warm-up each is excluded. "
        "Version order alternates. Each worker has a 900-second deadline. "
        "The corrected current basin-connection rows are copied into the old database. Native map-loading code is retained, "
        "including differences in handling basins absent from the connection table. Any resulting output differences are reported separately. "
        "Longitude representation is normalized only for untimed validation, to 1e-10 degrees. "
        "See validation.json and basin-map-differences.json for differences; exact distance ties are independently verified. "
        "These results replace the earlier matched-index experiment as the requested workflow comparison."
    )
    headers = [
        "Query",
        "Pre-refactor median (s)",
        "Current median (s)",
        "Workflow speedup",
        "Time saved",
    ]
    body = [
        [
            r["query"],
            *[
                (
                    f"{r[v+'_median']:.3f}"
                    if r[v + "_median"] is not None
                    else "Unavailable"
                )
                for v in ("old", "new")
            ],
            f"{r['speedup']:.3f}×" if r["speedup"] else "—",
            f"{r['saved_percent']:.1f}%" if r["speedup"] else "—",
        ]
        for r in rows
    ]
    lines = [
        "# Native pre-refactor workflow versus current BMA",
        "",
        description,
        "",
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join(["---"] * 5) + " |",
        *["| " + " | ".join(row) + " |" for row in body],
        "",
        "## Validation",
        "",
        "```json",
        json.dumps(state.get("validation", {}), indent=2),
        "```",
        "",
        "## Index definitions",
        "",
    ]
    for version in ("old", "new"):
        path = directory / f"{version}-database.json"
        if path.exists():
            lines += [
                version,
                "",
                "```sql",
                *[
                    i["index_definition"] + ";"
                    for i in common.read_json(path)["indexes"]
                    if i["table_name"] == "along_track"
                ],
                "```",
                "",
            ]
    lines += ["## Every pass", "", "| Pass | Status | Seconds |", "| --- | --- | --- |"]
    for job in state["jobs"]:
        seconds = (
            f"{job['seconds']:.3f}"
            if job["status"] == "completed" and not job["validation"]
            else "—"
        )
        lines.append(f"| {job['id']} | {job['status']} | {seconds} |")
    if state.get("exception"):
        lines += ["", "```", state["exception"], "```"]
    (directory / "summary.md").write_text("\n".join(lines) + "\n")
    table = (
        "<table><tr>"
        + "".join("<th>" + html.escape(h) + "</th>" for h in headers)
        + "</tr>"
    )
    table += (
        "".join(
            "<tr>" + "".join("<td>" + html.escape(c) + "</td>" for c in row) + "</tr>"
            for row in body
        )
        + "</table>"
    )
    (directory / "summary.html").write_text(
        '<!doctype html><html lang="en"><meta charset="utf-8"><title>Native OceanDB comparison</title>'
        "<style>body{font:16px/1.5 system-ui;max-width:1100px;margin:32px auto;padding:0 24px}table{border-collapse:collapse;width:100%}th,td{padding:12px;border:1px solid #ccc}pre{white-space:pre-wrap}</style>"
        "<h1>Native pre-refactor workflow versus current BMA</h1><p>"
        + html.escape(description)
        + "</p>"
        + table
        + "<h2>Validation and index details</h2><pre>"
        + html.escape("\n".join(lines[8:]))
        + "</pre></html>"
    )


def run(directory):
    directory = directory.resolve()
    with (directory / ".runner.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        manifest, state = common.read_json(
            directory / "manifest.json"
        ), common.read_json(directory / "results.json")
        config = Config()
        if state["status"] != "prepared" or state["manifest_sha256"] != common.digest(
            manifest
        ):
            raise RuntimeError("Run already started or manifest changed")
        if (
            manifest["code_hashes"] != code_hashes(Path(manifest["old_root"]))
            or manifest["environment"] != common.environment()
            or manifest["connection"] != common.connection_identity(config)
        ):
            raise RuntimeError("Code, environment or connection changed")
        if (
            common.digest(common.read_json(directory / "grid.json"))
            != manifest["grid_sha256"]
        ):
            raise RuntimeError("Grid changed")
        recorder = common.Recorder(directory, state)
        try:
            for item in manifest["files"]:
                p = Path(item["path"])
                if (
                    p.stat().st_size != item["bytes"]
                    or p.stat().st_mtime_ns != item["mtime_ns"]
                ):
                    raise RuntimeError("Raw source files changed")
            state["status"] = "preparing_databases"
            recorder.save()
            targets = prepare_databases(config, manifest, recorder)
            state["status"] = "validating"
            recorder.save()
            failed = set()
            for job in state["jobs"]:
                if manifest["code_hashes"] != code_hashes(Path(manifest["old_root"])):
                    raise RuntimeError("Code changed during the run")
                if (job["version"], job["query"]) in failed:
                    job.update(
                        status="skipped",
                        exception="Earlier measured pass failed; no retry",
                    )
                    recorder.save()
                    continue
                if not job["validation"] and state["status"] == "validating":
                    state["validation"] = validate(directory, targets["new"], recorder)
                    if not all(v["valid"] for v in state["validation"].values()):
                        raise RuntimeError(
                            "Unexplained output differences; see validation.json"
                        )
                    state["status"] = "measuring"
                    recorder.event("native_validation_passed")
                job.update(status="running", started_at=common.now(), seconds=None)
                recorder.save()
                recorder.event("pass_started", pass_id=job["id"])
                specification = {
                    **job,
                    "native_workflow": True,
                    "code_root": (
                        manifest["old_root"]
                        if job["version"] == "old"
                        else manifest["new_root"]
                    ),
                    "inputs_path": str(directory / f"inputs-{job['version']}.json"),
                }

                def event(kind, **values):
                    if kind == "worker_started":
                        job.update(
                            worker_pid=values["pid"],
                            application_name=values["application_name"],
                        )
                        recorder.save()
                    recorder.event(kind, pass_id=job["id"], **values)

                job.update(
                    matched.supervise(
                        specification,
                        targets[job["version"]].config,
                        directory / "jobs" / job["id"],
                        event,
                    )
                )
                recorder.save()
                recorder.event(
                    "pass_finished",
                    pass_id=job["id"],
                    status=job["status"],
                    seconds=job["seconds"] if not job["validation"] else None,
                )
                if job["status"] != "completed":
                    if job["validation"]:
                        raise RuntimeError("Native warm-up failed")
                    failed.add((job["version"], job["query"]))
            state["status"] = (
                "completed"
                if all(j["status"] == "completed" for j in state["jobs"])
                else "completed_with_failures"
            )
            recorder.event("comparison_finished", status=state["status"])
        except BaseException:
            state.update(status="failed", exception=traceback.format_exc())
            for job in state["jobs"]:
                if job["status"] in {"pending", "running"}:
                    job.update(
                        status=(
                            "interrupted" if job["status"] == "running" else "skipped"
                        ),
                        seconds=None,
                    )
            recorder.event("comparison_failed", exception=state["exception"])
            raise
        finally:
            state["finished_at"] = common.now()
            recorder.save()
            report(directory, manifest, state)
            recorder.event("report_written")
            common.package(directory)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--prepare", type=Path)
    group.add_argument("--run", type=Path)
    parser.add_argument("--previous", type=Path)
    parser.add_argument(
        "--reuse-databases",
        action="store_true",
        help="Reuse verified native preparation but start all warm-ups and timings afresh",
    )
    args = parser.parse_args()
    if args.prepare:
        if not args.previous:
            parser.error("--prepare requires --previous")
        print(
            prepare(args.previous, args.prepare, reuse_databases=args.reuse_databases)
        )
    else:
        run(args.run)
