"""Opt-in real smoke: all writes are to tagged disposable databases."""

import copy
import os
import threading
import time
import uuid
from pathlib import Path

import pytest
from psycopg import sql

from OceanDB.benchmark import database, schedule
from OceanDB.benchmark.config import DEFAULTS
from OceanDB.benchmark.executor import cancel_owned
from OceanDB.benchmark.report import report
from OceanDB.benchmark.runner import run
from OceanDB.benchmark.storage import read

pytestmark = pytest.mark.benchmark_database


@pytest.fixture
def disposable_source(tmp_path):
    if os.environ.get("OCEANDB_BENCHMARK_SMOKE") != "1":
        pytest.skip("Requires an explicitly selected disposable PostgreSQL 18 cluster")
    token = "ob_smoke_" + uuid.uuid4().hex
    name = token[:50]
    created = []
    with database.connect() as c:
        c.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
        c.execute(
            sql.SQL("COMMENT ON DATABASE {} IS {}").format(
                sql.Identifier(name), sql.Literal(token)
            )
        )
    created.append((name, token))
    cfg = copy.deepcopy(DEFAULTS)
    cfg.update(
        source_database=name,
        points=4,
        rows=1,
        columns=2,
        chunk_size=1,
        daily_days=2,
        disk_reserve_gib=0.01,
    )
    try:
        with database.connect(name) as c:
            c.execute("CREATE EXTENSION postgis")
            c.execute("CREATE EXTENSION btree_gist")
            c.execute(
                "CREATE TABLE along_track (id bigint,date_time timestamp,longitude float8,latitude float8,sla_filtered smallint,mission text,basin_id smallint,along_track_point geometry(Point,4326),PRIMARY KEY(date_time,id),UNIQUE(date_time,latitude,longitude)) PARTITION BY RANGE(date_time)"
            )
            c.execute(
                "CREATE TABLE along_track_sample PARTITION OF along_track DEFAULT"
            )
            c.execute(
                "CREATE TABLE along_track_metadata (file_name text PRIMARY KEY,observation_count bigint)"
            )
            c.execute(
                "CREATE TABLE basin_connections (basin_id smallint,connected_id smallint)"
            )
            c.execute("INSERT INTO basin_connections VALUES (1,2)")
            plan = schedule.build(cfg)
            points = plan["samples"]["primary"] + plan["samples"]["local_grid"]
            mask = schedule.BasinMask()
            for i, (lat, lon) in enumerate(points):
                for d, date in enumerate(
                    [
                        "2022-08-15",
                        "2022-10-15",
                        "2022-10-26",
                        "2022-10-27",
                        "2022-11-01",
                        "2022-11-30",
                    ]
                ):
                    c.execute(
                        "INSERT INTO along_track VALUES (%s,%s,%s,%s,%s,%s,%s,ST_SetSRID(ST_MakePoint(%s,%s),4326))",
                        (
                            i * 100 + d,
                            date,
                            lon,
                            lat,
                            100,
                            "s6a" if i % 2 else "j3n",
                            int(mask.lookup(lat, lon)),
                            lon,
                            lat,
                        ),
                    )
            c.execute(
                "INSERT INTO along_track_metadata SELECT %s,count(*) FROM along_track",
                ("synthetic-only",),
            )
        yield cfg, created
    finally:
        # No force, no broad prefix deletion: verify exact names and tokens recorded by this fixture.
        with database.connect() as c:
            for db, owner in reversed(created):
                row = c.execute(
                    "SELECT shobj_description(oid,'pg_database') FROM pg_database WHERE datname=%s",
                    (db,),
                ).fetchone()
                if row == (owner,):
                    c.execute(sql.SQL("DROP DATABASE {}").format(sql.Identifier(db)))


def test_native_pipeline_canary_archive_and_one_use(
    disposable_source, tmp_path, monkeypatch
):
    cfg, created = disposable_source
    # Small fixture schedule exercises all native APIs and an adjacent A/A pair.
    # Production profiles remain tested at their full fixed schedule counts.
    original = database.build

    def small(config):
        p = original(config)
        p["configurations"] = {
            k: v for k, v in p["configurations"].items() if k in ("ad_a", "ad_b", "bad")
        }
        p["episodes"] = [
            e
            for e in p["episodes"]
            if e["trial"] == 0
            and e["configuration"] in p["configurations"]
            and (
                e["workflow"] == "all_record_local_grid"
                and e["scenario"] == "all_record_radius_all"
                or e["workflow"] == "daily_sequence"
                and e["scenario"] == "radius_all"
                or e["workflow"] == "isolated_interior"
                and e["scenario"] == "nn_filtered"
            )
        ]
        p["episode_count"] = len(p["episodes"])
        p["batch_count"] = sum(len(e["jobs"]) for e in p["episodes"])
        return p

    monkeypatch.setattr(database, "build", small)
    output = tmp_path / "synthetic-run"
    try:
        m = database.prepare(cfg, output)
    finally:
        if (output / "manifest.json").exists():
            m = read(output / "manifest.json")
            token = m["owner_token"]
            # Include partially prepared names with exact run id + verified token, never historic DBs.
            with database.connect() as c:
                created.extend(
                    (name, token)
                    for (name,) in c.execute(
                        "SELECT datname FROM pg_database WHERE shobj_description(oid,'pg_database')=%s",
                        (token,),
                    ).fetchall()
                )
    assert read(output / "cache-self-test.json")["status"] == "passed"
    r = run(output)
    assert r["status"] == "completed" and r["statuses"] == {"completed": 15}
    assert all(j["rows"] > 0 for j in r["jobs"])
    assert {j["worker_generation"] for j in r["jobs"]} == {1}
    assert (
        sum(
            read(output / j["directory"] / "ready.json")["initialized_client"]
            for j in r["jobs"]
        )
        == 3
    )
    assert read(output / "verification.json")["unique_ids"] == 15
    assert read(output / "validation.json")["status"] == "passed"
    with pytest.raises(FileExistsError):
        run(output)
    artifact = report(output, tmp_path / "synthetic-report")
    assert Path(artifact["archive"]).is_file()
    assert (
        read(tmp_path / "synthetic-report-archive-verification.json")["status"]
        == "passed"
    )


def test_cancellation_isolation(disposable_source):
    cfg, _ = disposable_source
    name = cfg["source_database"]
    app = "ob_batch_" + uuid.uuid4().hex
    outcome = []
    started = threading.Event()

    def sleeper():
        import psycopg

        try:
            with database.connect(name, app=app) as c:
                started.set()
                c.execute("SELECT pg_sleep(20)")
        except psycopg.errors.QueryCanceled:
            outcome.append("cancelled")

    with database.connect(name, app="unrelated_smoke_canary") as unrelated:
        t = threading.Thread(target=sleeper)
        t.start()
        assert started.wait(5)
        time.sleep(0.1)
        assert cancel_owned(name, app)
        t.join(5)
        assert outcome == ["cancelled"] and unrelated.execute(
            "SELECT 42"
        ).fetchone() == (42,)
