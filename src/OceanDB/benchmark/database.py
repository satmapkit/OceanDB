"""Owned clone preparation. No DDL or maintenance is applied to the source."""

import hashlib
import json
import os
import platform
import re
import shutil
import time
import uuid
from importlib.resources import files
from pathlib import Path

import psycopg
from psycopg import sql

from OceanDB.config import Config

from .schedule import build
from .storage import event, external, now, package_hashes, save


def credentials():
    c = Config()
    return dict(
        host=c.postgres_host,
        port=c.postgres_port,
        user=c.postgres_username,
        password=c.postgres_password,
    )


def connect(
    database="postgres", *, app="oceandb_benchmark_admin", timeout=0, readonly=False
):
    return psycopg.connect(
        dbname=database,
        **credentials(),
        autocommit=True,
        application_name=app,
        options=f'-c statement_timeout={timeout} -c default_transaction_read_only={"on" if readonly else "off"} -c plan_cache_mode=auto',
    )


def resource(name):
    return files("OceanDB.sql.queries").joinpath("benchmark", name + ".sql").read_text()


def hash_value(value):
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, default=str).encode()
    ).hexdigest()


def dataset(c):
    # Metadata describes source files, including coverage/missing input; no hardcoded row counts.
    metadata = c.execute(
        "SELECT to_jsonb(m) FROM public.along_track_metadata m ORDER BY to_jsonb(m)::text"
    ).fetchall()
    basins = c.execute(
        "SELECT basin_id,connected_id FROM public.basin_connections ORDER BY basin_id,connected_id"
    ).fetchall()
    coverage = c.execute(
        "SELECT min(date_time),max(date_time) FROM public.along_track"
    ).fetchone()
    if coverage[0] is None:
        raise ValueError("Source has no observations")
    schema = c.execute(
        "SELECT column_name,data_type,udt_name FROM information_schema.columns WHERE table_schema='public' AND table_name='along_track' ORDER BY ordinal_position"
    ).fetchall()
    if not metadata or not {
        "date_time",
        "along_track_point",
        "mission",
        "basin_id",
        "sla_filtered",
    } <= {r[0] for r in schema}:
        raise ValueError("Expected a populated OceanDB along_track source and metadata")
    return dict(
        metadata=metadata,
        schema=schema,
        basin_connections=basins,
        coverage=[str(x) for x in coverage],
        sha256=hash_value([metadata, schema, basins, coverage]),
    )


def statistics(c):
    rows = c.execute(
        "SELECT row_to_json(s)::text FROM (SELECT * FROM pg_stats WHERE schemaname='public' AND tablename LIKE 'along_track%' ORDER BY tablename,attname,inherited) s"
    ).fetchall()
    return hash_value(rows)


def check(cfg):
    if platform.system() not in ("Darwin", "Linux") or not shutil.which("cc"):
        raise ValueError("Local macOS/Linux and a C compiler are required")
    with connect(cfg["source_database"], readonly=True, timeout=60000) as c:
        if (
            c.info.server_version < 180000
            or not c.execute(
                "SELECT rolsuper FROM pg_roles WHERE rolname=current_user"
            ).fetchone()[0]
        ):
            raise ValueError(
                "Benchmark requires PostgreSQL 18+ and a superuser for scoped buffer eviction"
            )
        if not c.execute(
            "SELECT 1 FROM pg_available_extensions WHERE name='pg_buffercache'"
        ).fetchone():
            raise ValueError("Install the pg_buffercache extension files")
        if c.execute("SHOW server_encoding").fetchone()[0] not in ("UTF8", b"UTF8"):
            raise ValueError("Benchmark requires a UTF8 database")
        version = c.execute("SELECT postgis_full_version()").fetchone()[0]
        server_version = c.info.server_version
        data = Path(c.execute("SHOW data_directory").fetchone()[0]).resolve()
        oid, tablespace = c.execute(
            "SELECT oid,dattablespace FROM pg_database WHERE datname=current_database()"
        ).fetchone()
        default = c.execute(
            "SELECT oid FROM pg_tablespace WHERE spcname='pg_default'"
        ).fetchone()[0]
        if (
            tablespace != default
            or c.execute(
                "SELECT 1 FROM pg_class WHERE NOT relisshared AND reltablespace NOT IN (0,%s) LIMIT 1",
                (default,),
            ).fetchone()
        ):
            raise ValueError("Only the default tablespace is supported")
        pid = int((data / "postmaster.pid").read_text().splitlines()[0])
        os.kill(pid, 0)
        if data.stat().st_uid != os.getuid() or not (data / "base" / str(oid)).is_dir():
            raise ValueError(
                "Run beside PostgreSQL as the OS owner of its data directory"
            )
        # Confirm the endpoint uses the same local cluster, rather than a coincidental path.
        if c.execute("SELECT host(inet_server_addr())").fetchone()[0] not in (
            None,
            "127.0.0.1",
            "::1",
        ):
            raise ValueError("Remote PostgreSQL is unsupported")
        source = dataset(c)
        size = c.execute("SELECT pg_database_size(current_database())").fetchone()[0]
    plan = build(cfg)
    if cfg["exclusions"] and cfg["exclusion_dataset_sha256"] != source["sha256"]:
        raise ValueError("Exclusion policy is bound to a different source inventory")
    free = shutil.disk_usage(data).free
    # Conservative source-size multiplier for template, composites, WAL and sort/build space.
    estimate = int(
        size * (len(plan["configurations"]) + 2) * 2 + cfg["disk_reserve_gib"] * 2**30
    )
    if free < estimate:
        raise ValueError(
            f"Insufficient disk: {free} free bytes, estimated requirement {estimate}"
        )
    return dict(
        status="passed",
        platform=platform.system(),
        postgres_version=server_version,
        postgis=version,
        source=source,
        data_directory=str(data),
        free_bytes=free,
        estimated_required_bytes=estimate,
        episode_count=plan["episode_count"],
        batch_count=plan["batch_count"],
        excluded=plan["excluded"],
    )


def identity(c, inv):
    row = c.execute(
        "SELECT oid,shobj_description(oid,'pg_database'),pg_get_userbyid(datdba) FROM pg_database WHERE datname=%s",
        (inv["database"],),
    ).fetchone()
    if row != (inv["oid"], inv["owner_token"], inv["owner"]):
        raise ValueError("Owned database identity changed: " + inv["database"])
    return row[0]


def inventory(database, token):
    with connect(database, readonly=True) as c:
        oid, owner, comment = c.execute(
            "SELECT oid,pg_get_userbyid(datdba),shobj_description(oid,'pg_database') FROM pg_database WHERE datname=current_database()"
        ).fetchone()
        if comment != token:
            raise ValueError("Ownership token mismatch")
        data = Path(c.execute("SHOW data_directory").fetchone()[0]).resolve()
        relations = c.execute(resource("inventory")).fetchall()
        indexes = c.execute(resource("index_inventory")).fetchall()
        stat = statistics(c)
        meta = dataset(c)
        size = c.execute("SELECT pg_database_size(current_database())").fetchone()[0]
        # Changes in physical heaps or statistics after preparation are fatal at run start.
    out = []
    base = data / "base" / str(oid)
    for rel in relations:
        path = data / rel[3]
        if path.parent != base:
            raise ValueError("Unsupported relation path")
        pattern = re.compile(
            re.escape(path.name) + r"(?:_(?:fsm|vm|init))?(?:\.[0-9]+)?$"
        )
        for f in sorted(base.iterdir()):
            if pattern.fullmatch(f.name):
                s = f.stat()
                if f.is_symlink() or not f.is_file():
                    raise ValueError("Unsafe relation path")
                out.append(
                    dict(
                        path=str(f),
                        bytes=s.st_size,
                        inode=s.st_ino,
                        device=s.st_dev,
                        relfilenode=rel[2],
                    )
                )
    if not out:
        raise ValueError("Empty relation inventory")
    return dict(
        database=database,
        oid=oid,
        owner=owner,
        owner_token=token,
        data_directory=str(data),
        relations=relations,
        files=out,
        indexes=indexes,
        statistics_sha256=stat,
        dataset_sha256=meta["sha256"],
        database_bytes=size,
    )


def verify_indexes(inv, columns):
    expected = {
        ("gist", (x,))
        for x in ("along_track_point", "basin_id", "date_time", "mission")
    }
    if columns:
        expected.add(("gist", tuple(columns)))
    expected |= {
        ("btree", ("date_time", "id")),
        ("btree", ("date_time", "latitude", "longitude")),
    }
    for table in {r[0] for r in inv["indexes"]}:
        rows = [r for r in inv["indexes"] if r[0] == table]
        if (
            len(rows) != len(expected)
            or {(r[2], tuple(r[7])) for r in rows} != expected
            or not all(r[5] and r[6] for r in rows)
        ):
            raise ValueError("Unexpected physical indexes: " + table)


def clone(c, source, target, token):
    while c.execute(
        "SELECT 1 FROM pg_stat_activity WHERE datname=%s", (source,)
    ).fetchone():
        time.sleep(2)
    c.execute(
        sql.SQL("CREATE DATABASE {} TEMPLATE {}").format(
            sql.Identifier(target), sql.Identifier(source)
        )
    )
    c.execute(
        sql.SQL("COMMENT ON DATABASE {} IS {}").format(
            sql.Identifier(target), sql.Literal(token)
        )
    )


def prepare(cfg, output):
    run = external(output)
    if run.exists():
        raise ValueError("Use a new output directory")
    checked = check(cfg)
    plan = build(cfg)
    run.mkdir(parents=True)
    token = "oceandb_benchmark_" + uuid.uuid4().hex
    prefix = "ob_" + uuid.uuid4().hex[:12]
    manifest = dict(
        schema_version=1,
        status="preparing",
        run_id=prefix,
        created_at=now(),
        config=cfg,
        source=checked["source"],
        owner_token=token,
        admin_database=prefix + "_admin",
        configurations={},
        connection={k: v for k, v in credentials().items() if k != "password"},
        environment=checked,
    )
    save(run / "manifest.json", manifest)
    save(run / "plan.json", plan)
    save(run / "code-sha256.json", package_hashes())
    package_root = Path(__file__).resolve().parents[1]
    for relative in package_hashes():
        target = run / "code" / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(package_root / relative, target)
    save(run / "disk.json", dict(time=now(), free_bytes=checked["free_bytes"]))
    try:
        with connect() as c:
            template = prefix + "_template"
            clone(c, cfg["source_database"], template, token)
            c.execute(
                sql.SQL("CREATE DATABASE {}").format(
                    sql.Identifier(manifest["admin_database"])
                )
            )
            c.execute(
                sql.SQL("COMMENT ON DATABASE {} IS {}").format(
                    sql.Identifier(manifest["admin_database"]), sql.Literal(token)
                )
            )
        with connect(manifest["admin_database"]) as c:
            c.execute("CREATE EXTENSION pg_buffercache")
            c.execute(
                "CREATE TABLE cache_canary AS SELECT g,repeat(md5(g::text),100) payload FROM generate_series(1,100) g"
            )
        with connect(template) as c:
            # Drop only top-level, non-constraint along_track indexes on the owned clone.
            indexes = c.execute(
                "SELECT i.relname FROM pg_index x JOIN pg_class i ON i.oid=x.indexrelid WHERE x.indrelid='public.along_track'::regclass AND NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conindid=i.oid)"
            ).fetchall()
            for (name,) in indexes:
                c.execute(
                    sql.SQL("DROP INDEX {}").format(sql.Identifier("public", name))
                )
            for col in ("along_track_point", "basin_id", "date_time", "mission"):
                c.execute(
                    sql.SQL(
                        "CREATE INDEX {} ON public.along_track USING gist ({}) WITH (buffering=auto)"
                    ).format(sql.Identifier("bench_" + col), sql.Identifier(col))
                )
            c.execute("VACUUM ANALYZE")
        ref = inventory(template, token)
        verify_indexes(ref, [])
        if ref["dataset_sha256"] != checked["source"]["sha256"]:
            raise ValueError("Source inventory changed during preparation")
        manifest["template"] = ref
        for label, columns in plan["configurations"].items():
            db = prefix + "_" + label
            with connect() as c:
                clone(
                    c,
                    (
                        manifest["configurations"]["ad_a"]["database"]
                        if label == "ad_b"
                        else template
                    ),
                    db,
                    token,
                )
            if label != "ad_b":
                with connect(db) as c:
                    c.execute(
                        sql.SQL(
                            "CREATE INDEX benchmark_composite ON public.along_track USING gist ({}) WITH (buffering=auto)"
                        ).format(sql.SQL(",").join(map(sql.Identifier, columns)))
                    )
            inv = inventory(db, token)
            verify_indexes(inv, columns)
            if (
                inv["statistics_sha256"] != ref["statistics_sha256"]
                or inv["dataset_sha256"] != ref["dataset_sha256"]
            ):
                raise ValueError("Clone statistics/data inventory differ")
            manifest["configurations"][label] = inv
            save(run / "manifest.json", manifest)
            free = shutil.disk_usage(checked["data_directory"]).free
            save(run / "disk.json", dict(time=now(), free_bytes=free))
            if free < cfg["disk_reserve_gib"] * 2**30:
                raise ValueError("Disk reserve reached; preserving prepared databases")
            event(
                run,
                "configuration_ready",
                configuration=label,
                ready=len(manifest["configurations"]),
                total=len(plan["configurations"]),
            )
        a, b = [manifest["configurations"][k] for k in ("ad_a", "ad_b")]
        if a["indexes"] != b["indexes"] or [r[:3] for r in a["relations"]] != [
            r[:3] for r in b["relations"]
        ]:
            raise ValueError("AD-A/AD-B are not physically identical clones")
        from .cache import Cache

        cache = Cache(run, manifest)
        cache.compile()
        cache.self_test()
        from .storage import digest

        manifest["plan_sha256"] = digest(run / "plan.json")
        manifest["status"] = "ready"
        save(run / "manifest.json", manifest)
        event(
            run, "prepared", episodes=plan["episode_count"], batches=plan["batch_count"]
        )
    except BaseException:
        manifest["status"] = "preparation_failed"
        save(run / "manifest.json", manifest)
        raise
    return manifest
