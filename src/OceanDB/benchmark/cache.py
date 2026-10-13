"""Verified target-only cache preparation, with retry only before measurement."""

import os
import subprocess
import time
from pathlib import Path

from .database import connect, identity
from .storage import event, now, save


class TransientCacheState(RuntimeError):
    """An untimed preparation attempt may be repeated."""


def quiet(c):
    rows = c.execute(
        "SELECT pid,datname,backend_type FROM pg_stat_activity WHERE pid<>pg_backend_pid() AND backend_type IN ('client backend','autovacuum worker','parallel worker')"
    ).fetchall()
    if rows:
        raise TransientCacheState(
            "Waiting for quiet server; no sessions killed: " + str(rows)
        )


def prepare_until_verified(attempt, pause, on_wait):
    n = 0
    while True:
        n += 1
        try:
            return attempt(n)
        except TransientCacheState as exc:
            on_wait(n, str(exc))
            pause(min(5, n))


class Cache:
    def __init__(self, run, manifest):
        self.run = Path(run)
        self.manifest = manifest
        self.binary = self.run / "cache-helper"

    def compile(self):
        subprocess.run(
            [
                "cc",
                "-O2",
                "-Wall",
                "-Wextra",
                str(Path(__file__).with_name("cache_file.c")),
                "-o",
                str(self.binary),
            ],
            check=True,
        )

    def buffers(self, c, inv):
        return c.execute(
            "SELECT bufferid,relfilenode,relforknumber,relblocknumber,isdirty,pinning_backends FROM pg_buffercache WHERE reldatabase=%s AND relfilenode=ANY(%s) ORDER BY bufferid",
            (inv["oid"], [r[2] for r in inv["relations"]]),
        ).fetchall()

    def pages(self, inv, evict=False):
        rows = []
        for f in inv["files"]:
            path = Path(f["path"])
            s = path.stat()
            if (
                path.is_symlink()
                or path.parent != Path(inv["data_directory"]) / "base" / str(inv["oid"])
                or (s.st_ino, s.st_dev, s.st_size)
                != (f["inode"], f["device"], f["bytes"])
            ):
                raise ValueError("Relation file identity/size changed")
            out = subprocess.run(
                [str(self.binary), "evict" if evict else "inspect", str(path)],
                capture_output=True,
                text=True,
                check=True,
            )
            import json

            rows.append(dict(path=str(path), **json.loads(out.stdout)))
        return dict(
            files=rows,
            after_pages=sum(r["after_pages"] for r in rows),
            before_pages=sum(r["before_pages"] for r in rows),
        )

    def attempt(self, label, eid, n):
        inv = self.manifest["configurations"][label]
        record = dict(
            episode=eid, attempt=n, configuration=label, time=now(), status="failed"
        )
        try:
            with connect(self.manifest["admin_database"]) as c:
                oid = identity(c, inv)
                quiet(c)
                before = self.buffers(c, inv)
                record["other_buffer_occupancy"] = c.execute(
                    "SELECT reldatabase,count(*) FROM pg_buffercache WHERE reldatabase IS DISTINCT FROM %s GROUP BY reldatabase ORDER BY reldatabase",
                    (oid,),
                ).fetchall()
                record["load_average"] = os.getloadavg()
                record["memory"] = (
                    subprocess.run(["vm_stat"], capture_output=True, text=True).stdout
                    if os.uname().sysname == "Darwin"
                    else Path("/proc/meminfo").read_text()
                )
                record["postgres_before"] = len(before)
                for buf, node, fork, block, *_ in before:
                    c.execute(
                        "SELECT e.* FROM pg_buffercache b CROSS JOIN LATERAL pg_buffercache_evict(b.bufferid) e WHERE b.bufferid=%s AND b.reldatabase=%s AND b.relfilenode=%s AND b.relforknumber=%s AND b.relblocknumber=%s",
                        (buf, oid, node, fork, block),
                    ).fetchall()
                quiet(c)
                if self.buffers(c, inv):
                    raise TransientCacheState("Target buffers remain cached/pinned")
                record["os_eviction"] = self.pages(inv, True)
                record["os_verified"] = self.pages(inv)
                quiet(c)
                record["postgres_verified"] = len(self.buffers(c, inv))
                if record["os_verified"]["after_pages"] or record["postgres_verified"]:
                    raise TransientCacheState("Target cache remains populated")
            record["status"] = "verified"
        except BaseException as exc:
            record["error"] = str(exc)
            raise
        finally:
            save(self.run / "cache" / f"{eid}__attempt{n:04d}.json", record)
        return record

    def reset(self, label, eid):
        start = time.monotonic()
        event(self.run, "cache_preparation_started", configuration=label, episode=eid)

        def waiting(n, reason):
            event(
                self.run,
                "cache_preparation_waiting",
                configuration=label,
                episode=eid,
                attempt=n,
                reason=reason,
                seconds=time.monotonic() - start,
            )

        record = prepare_until_verified(
            lambda n: self.attempt(label, eid, n), time.sleep, waiting
        )
        record["preparation_seconds"] = time.monotonic() - start
        save(self.run / "cache" / f"{eid}.json", record)
        return record

    def self_test(self):
        label = "ad_a"
        inv = self.manifest["configurations"][label]
        with connect(inv["database"], readonly=True) as c:
            c.execute("SELECT * FROM public.along_track_metadata").fetchall()
        with connect(self.manifest["admin_database"]) as c:
            c.execute("SELECT sum(length(payload)) FROM cache_canary").fetchone()
            canary = c.execute(
                "SELECT bufferid,relfilenode,relblocknumber FROM pg_buffercache WHERE reldatabase=(SELECT oid FROM pg_database WHERE datname=current_database()) AND relfilenode=pg_relation_filenode('cache_canary') ORDER BY bufferid"
            ).fetchall()
            before = len(self.buffers(c, inv))
        pages = self.pages(inv)
        if not before or not pages["after_pages"]:
            raise ValueError("Cache proof requires populated target")
        self.reset(label, "setup-proof-1")
        self.reset(label, "setup-proof-2")
        with connect(self.manifest["admin_database"]) as c:
            after = c.execute(
                "SELECT bufferid,relfilenode,relblocknumber FROM pg_buffercache WHERE reldatabase=(SELECT oid FROM pg_database WHERE datname=current_database()) AND relfilenode=pg_relation_filenode('cache_canary') ORDER BY bufferid"
            ).fetchall()
        if not canary or canary != after:
            raise ValueError("Unrelated canary buffers changed")
        save(
            self.run / "cache-self-test.json",
            dict(
                status="passed",
                canary_buffers_preserved=len(canary),
                populated_target_buffers=before,
                populated_target_pages=pages["after_pages"],
            ),
        )
