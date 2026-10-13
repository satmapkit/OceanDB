"""Persistent native worker: no metadata queries between dates or chunks."""

import json
import os
import sys
import time
import traceback
from pathlib import Path

import psycopg
from psycopg.conninfo import make_conninfo

from OceanDB.config import Config
from OceanDB.data_access.along_track import AlongTrack

from .database import credentials
from .storage import read, save
from .validation import save_rows
from .workloads import execute_batch


def client(database):
    kw = credentials()
    obj = AlongTrack(
        config=Config(
            postgres_host=kw["host"],
            postgres_port=kw["port"],
            postgres_username=kw["user"],
            postgres_password=kw["password"],
            postgres_database=database,
        )
    )
    obj.connection_string = make_conninfo(dbname=database, **kw)
    obj.basin_connections.connection_string = obj.connection_string
    obj.basin_mask_lookup.data
    obj.basin_connections.connection_map
    return obj


def main(run, ready):
    run = Path(run)
    m = read(run / "manifest.json")
    plan = read(run / "plan.json")
    clients = {}
    Path(ready).touch()
    while line := sys.stdin.readline():
        job = json.loads(line)
        p = run / job["directory"]
        label = job["configuration"]
        os.environ["PGAPPNAME"] = job["application_name"]
        os.environ["PGOPTIONS"] = (
            f"-c default_transaction_read_only=on -c statement_timeout={int(job['cap_seconds']*1000)} -c plan_cache_mode=auto"
        )
        try:
            initialized = label not in clients
            if initialized:
                clients[label] = client(m["configurations"][label]["database"])
            save(p / "ready.json", dict(status="ready", initialized_client=initialized))
            if sys.stdin.readline().strip() != "go":
                return
            start = time.perf_counter()
            values = execute_batch(clients[label], job, plan["samples"])
            seconds = time.perf_counter() - start
            save(p / "timing.json", dict(status="completed", seconds=seconds))
            detail = save_rows(values, job, p / "rows.npz")
            save(p / "result.json", dict(status="completed", seconds=seconds, **detail))
            del values
        except BaseException as exc:
            error = traceback.format_exc()
            password = credentials()["password"]
            if password:
                error = error.replace(password, "[redacted]")
            save(
                p / "failure.json",
                dict(
                    status=(
                        "timed_out"
                        if isinstance(exc, psycopg.errors.QueryCanceled)
                        else "failed"
                    ),
                    error_type=type(exc).__name__,
                    traceback=error,
                ),
            )


if __name__ == "__main__":
    main(*sys.argv[1:])
