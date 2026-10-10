"""Isolated legacy/current API worker; select OceanDB before importing either version."""

import argparse
import builtins
import hashlib
import io
import json
import os
import sys
import time
import traceback
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import patch

import numpy as np
import psycopg
from psycopg import sql

FIELDS = ("longitude", "latitude", "sla_filtered", "delta_t")


def emit(event, **values):
    print(json.dumps({"event": event, **values}), flush=True)


def canonical_rows(values, *, normalize_longitude=False):
    if values is None or (
        len(values[0]) == 1 and np.isnan(values[0][0]) and np.isnan(values[1][0])
    ):
        return np.empty((0, 4), dtype="<f8")
    rows = np.column_stack(values).astype("<f8")
    if normalize_longitude:
        # The native legacy importer wraps longitude; current ingestion keeps 0–360.
        # Compare geographic coordinates at 1e-10 degrees, outside query timing.
        rows[:, 0] = np.round((rows[:, 0] + 180) % 360 - 180, 10)
    rows[rows == 0] = 0.0  # normalize negative zero
    rows[np.isnan(rows)] = np.nan
    if len(rows):
        rows = rows[np.lexsort(tuple(rows[:, k] for k in (3, 2, 1, 0)))]
    return np.ascontiguousarray(rows)


def make_client(job, inputs):
    version = job["version"]
    root = Path(job["code_root"])
    sys.path.insert(0, str(root / ("python" if version == "old" else "src")))
    mapping = {
        int(key): list(map(int, value)) for key, value in inputs["basin_map"].items()
    }
    if version == "old":
        from OceanDB.AlongTrack import AlongTrack

        config_path = (root / "python/config.yaml").resolve()
        config = {
            "db_connect": {
                "host": os.environ["PGHOST"],
                "username": os.environ["PGUSER"],
                "password": os.environ.get("PGPASSWORD", ""),
                "port": int(os.environ["PGPORT"]),
                "db_name": os.environ["PGDATABASE"],
            }
        }
        original_open = builtins.open

        def open_config(path, *args, **kwargs):
            if isinstance(path, (str, Path)) and Path(path).resolve() == config_path:
                return io.StringIO(json.dumps(config))
            return original_open(path, *args, **kwargs)

        # Supply the expected configuration in memory; leave the old checkout pristine.
        with patch("builtins.open", side_effect=open_config):
            client = AlongTrack(
                db_name=os.environ["PGDATABASE"], port=int(os.environ["PGPORT"])
            )
        if job.get("native_workflow"):
            client.basin_connection_map
        else:
            client.basin_connection_map = mapping
        client.basin_mask_data  # prime both implementations' identical mask outside timing
    else:
        from OceanDB.config import Config
        from OceanDB.data_access.along_track import AlongTrack

        client = AlongTrack(
            config=Config(
                postgres_host=os.environ["PGHOST"],
                postgres_username=os.environ["PGUSER"],
                postgres_password=os.environ.get("PGPASSWORD", ""),
                postgres_port=int(os.environ["PGPORT"]),
                postgres_database=os.environ["PGDATABASE"],
            )
        )
        if job.get("native_workflow"):
            client.basin_connections.connection_map
        else:
            client.basin_connections.connection_map = mapping
        client.basin_mask_lookup.data
    module = sys.modules[AlongTrack.__module__]
    actual = Path(module.__file__).resolve()
    if not actual.is_relative_to(root):
        raise RuntimeError(f"Wrong version imported: {actual}")
    return client, str(actual)


def execute_batch(client, job, inputs):
    lats = np.asarray(inputs["latitudes"], dtype=float)
    lons = np.asarray(inputs["longitudes"], dtype=float)
    date = datetime.fromisoformat(inputs["central_date"])
    dates = [date] * len(lats)
    started = time.perf_counter()
    if job["version"] == "old":
        if job["query"] == "radius":
            iterator = client.geographic_points_in_spatialtemporal_windows(
                lats,
                lons,
                date,
                distance=inputs["radius_m"],
                time_window=timedelta(days=20),
                missions=None,
            )
        else:
            iterator = client.geographic_nearest_neighbors(
                lats, lons, date, time_window=timedelta(days=20), missions=None
            )
    else:
        kwargs = dict(
            fields=list(FIELDS),
            latitudes=lats,
            longitudes=lons,
            dates=dates,
            time_window=timedelta(days=10),
            missions=None,
        )
        if job["query"] == "radius":
            iterator = client.geographic_point_in_r_dt_batch(
                radius=inputs["radius_m"], **kwargs
            )
        else:
            iterator = client.geographic_nearest_neighbors_batch(
                max_radius=None, **kwargs
            )
    # Access all common arrays, including lazy SLA scaling in the current API.
    results = [
        None if result is None else tuple(result[field] for field in FIELDS)
        for result in iterator
    ]
    seconds = time.perf_counter() - started
    if len(results) != len(lats):
        raise RuntimeError("Batch returned the wrong number of locations")
    return results, seconds


def diagnostics(client, job, inputs):
    latitude, longitude = inputs["latitudes"][0], inputs["longitudes"][0]
    date = datetime.fromisoformat(inputs["central_date"])
    old = job["version"] == "old"
    basin = int(
        client.basin_mask(latitude, longitude)
        if old
        else client.basin_mask_lookup.lookup(latitude, longitude)
    )
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "connected_basin_ids": inputs["basin_map"].get(str(basin)),
    }
    if old:
        filename = (
            "geographic_points_in_spatialtemporal_window.sql"
            if job["query"] == "radius"
            else "geographic_nearest_neighbor.sql"
        )
        statement = sql.SQL(client.sql_query_with_name(filename)).format(
            central_date_time=date,
            time_delta=timedelta(days=10),
            missions=sql.SQL(",").join(client.missions),
        )
    else:
        from OceanDB.query_spec import QuerySpec
        from OceanDB.schemas.along_track_schema import along_track_schema

        filename = (
            "queries/along_track/geographic_points_in_spatialtemporal_window_without_mission.sql"
            if job["query"] == "radius"
            else "queries/along_track/geographic_nearest_neighbor_without_mission.sql"
        )
        statement = QuerySpec(
            sql_template=client.load_sql_file(filename),
            schema=along_track_schema,
            mandatory_fields=[] if job["query"] == "radius" else ["distance"],
        ).sql_projection_compiler(FIELDS)
        params.update(central_date_time=date, time_delta=timedelta(days=10))
    if job["query"] == "radius":
        params["distance"] = inputs["radius_m"]
    with psycopg.connect("") as conn:
        plan = conn.execute(
            sql.SQL("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) ") + statement, params
        ).fetchone()[0]
        return {"sql": statement.as_string(conn), "parameters": params, "explain": plan}


def worker(job_path):
    job = json.loads(Path(job_path).read_text())
    directory = Path(job["directory"])
    inputs = json.loads(Path(job["inputs_path"]).read_text())
    try:
        client, module = make_client(job, inputs)
        emit("ready", module=module)
        if sys.stdin.readline().strip() != "go":
            raise RuntimeError("Missing start signal")
        results, seconds = execute_batch(client, job, inputs)
        output = {
            "status": "completed",
            "seconds": seconds,
            "points": len(results),
            "module": module,
        }
        if job["validation"]:
            arrays = [
                canonical_rows(
                    values, normalize_longitude=job.get("native_workflow", False)
                )
                for values in results
            ]
            counts = np.asarray([len(a) for a in arrays], dtype=np.int64)
            hashes = np.asarray(
                [hashlib.sha256(a.tobytes()).hexdigest() for a in arrays], dtype="S64"
            )
            offsets = np.concatenate(([0], np.cumsum(counts)))
            with (directory / "validation.npz.tmp").open("wb") as stream:
                np.savez_compressed(
                    stream,
                    counts=counts,
                    hashes=hashes,
                    offsets=offsets,
                    rows=np.concatenate(arrays) if arrays else np.empty((0, 4)),
                )
            (directory / "validation.npz.tmp").replace(directory / "validation.npz")
            output["diagnostics"] = diagnostics(client, job, inputs)
            output["rows"] = int(counts.sum())
        # Serialization and EXPLAIN are outside the reported batch time, inside the supervisor's cap.
        temporary = directory / "result.json.tmp"
        temporary.write_text(json.dumps(output, default=str, allow_nan=False))
        temporary.replace(directory / "result.json")
        emit("completed", seconds=seconds)
    except BaseException:
        (directory / "exception.txt").write_text(traceback.format_exc())
        emit("failed")
        raise


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("job", type=Path)
    worker(parser.parse_args().job)
