"""Bounded warm prefix replays, strictly after all timed work."""

import time
from datetime import datetime, timedelta

import psycopg

from OceanDB.query_spec import QuerySpec
from OceanDB.schemas.along_track_schema import along_track_schema

from .database import connect
from .storage import save
from .worker import client


def diagnostics(run, manifest, plan):
    end = time.monotonic() + 180
    out = dict(
        description="Warm diagnostic replays after timing; not plans captured during measured cache starts. 350 ms statements, 180 s budget. Prepared counters describe samples only.",
        cases=[],
    )
    examples = {}
    for e in plan["episodes"]:
        if e["trial"] != 0:
            continue
        for j in e["jobs"]:
            if j["date"] in (
                manifest["config"]["interior_date"],
                manifest["config"]["daily_start"],
                manifest["config"]["boundary_date"],
                None,
            ):
                examples.setdefault((j["configuration"], j["scenario"], j["date"]), j)
    for j in examples.values():
        item = dict(
            configuration=j["configuration"],
            scenario=j["scenario"],
            date=j["date"],
            prefix_completed=0,
            prefix_requested=min(16, j["points"] - 1),
        )
        out["cases"].append(item)
        if time.monotonic() >= end:
            item["status"] = "skipped_budget"
            continue
        try:
            obj = client(manifest["configurations"][j["configuration"]]["database"])
            record = j["temporal_scope"] == "entire_stored_record"
            nn = j["scenario"].startswith("nn")
            name = (
                "geographic_points_in_spatial_window"
                if record
                else (
                    "geographic_nearest_neighbor"
                    if nn
                    else "geographic_points_in_spatialtemporal_window"
                )
            )
            if j["missions"] is None:
                name += "_without_mission"
            query = QuerySpec(
                sql_template=obj.load_sql_file("queries/along_track/" + name + ".sql"),
                schema=along_track_schema,
                mandatory_fields=["distance"] if nn else [],
            ).sql_projection_compiler(j["fields"])
            points = [plan["samples"][j["sample"]][i] for i in j["point_indices"]]

            def parameters(point):
                lat, lon = point
                basin = int(obj.basin_mask_lookup.lookup(lat, lon))
                p = dict(
                    latitude=lat,
                    longitude=lon,
                    connected_basin_ids=obj.basin_connections.connection_map[basin],
                )
                if not record:
                    p.update(
                        central_date_time=datetime.fromisoformat(j["date"]),
                        time_delta=timedelta(days=j["half_window_days"]),
                    )
                if not nn:
                    p["distance"] = j["radius_m"]
                if j["missions"] is not None:
                    p["missions"] = j["missions"]
                return p

            with connect(
                manifest["configurations"][j["configuration"]]["database"],
                timeout=350,
                readonly=True,
            ) as c:
                for point in points[: item["prefix_requested"]]:
                    if time.monotonic() >= end:
                        break
                    c.execute(query, parameters(point), prepare=True).fetchall()
                    item["prefix_completed"] += 1
                item["matched_prefix"] = (
                    item["prefix_completed"] == item["prefix_requested"]
                )
                item["prepared"] = c.execute(
                    "SELECT name,generic_plans,custom_plans FROM pg_prepared_statements"
                ).fetchall()
                if not item["matched_prefix"]:
                    item["status"] = "prefix_incomplete"
                elif time.monotonic() >= end:
                    item["status"] = "skipped_budget"
                else:
                    from psycopg import sql

                    item["plan"] = c.execute(
                        sql.SQL("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) ") + query,
                        parameters(points[item["prefix_requested"]]),
                    ).fetchone()[0]
                    item["status"] = "completed"
        except psycopg.errors.QueryCanceled:
            item["status"] = "diagnostic_timeout"
        except Exception as exc:
            item.update(status="unavailable", error_type=type(exc).__name__)
        save(run / "plans.json", out)
    save(run / "plans.json", out)
