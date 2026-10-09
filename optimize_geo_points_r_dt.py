import itertools
import random
from datetime import datetime, timedelta
from typing import Any

import numpy as np

from OceanDB.data_access.along_track import AlongTrack, Mission
from OceanDB.index_experiment import index_definition
from OceanDB.ocean_data.basins import BasinMask
from OceanDB.query_analysis import BaseQueryScenario, BatchQueryScenario
from OceanDB.schemas.along_track_schema import along_track_schema

TIMEOUT_SECONDS = 900


def random_datetimes(
    rng: random.Random, start: datetime, end: datetime, count: int
) -> list[datetime]:
    span_seconds = int((end - start).total_seconds())
    return [
        start + timedelta(seconds=rng.randrange(span_seconds + 1)) for _ in range(count)
    ]


def random_points(rng: random.Random, count: int) -> tuple[list[float], list[float]]:
    latitudes = [rng.uniform(-80.0, 80.0) for _ in range(count)]
    longitudes = [rng.uniform(-180.0, 180.0) for _ in range(count)]
    return latitudes, longitudes


def batch_scenario_random(
    seed: int,
    *,
    radius: float,
    time_window: timedelta,
    date_start: datetime,
    date_end: datetime,
    n_points: int = 1000,
) -> BatchQueryScenario:
    rng = random.Random(seed)
    latitudes, longitudes = random_points(rng, n_points)
    dates = random_datetimes(rng, date_start, date_end, n_points)
    return BatchQueryScenario(
        query_class=AlongTrack,
        method_name="geographic_point_in_r_dt_batch",
        kwargs={
            "fields": list(along_track_schema.keys()),
            "latitudes": latitudes,
            "longitudes": longitudes,
            "dates": dates,
            "radius": radius,
            "time_window": time_window,
        },
    )


def batch_scenario_grid(
    *,
    method_name: str,
    time_window: timedelta,
    central_date: datetime,
    resolution: float = 1.0,
    missions: list[Mission] | None = None,
    scenario_kwargs: dict[str, Any] | None = None,
) -> BatchQueryScenario:

    latitudes = np.arange(-60, 60, resolution)
    longitudes = np.arange(-180, 180, resolution)

    lons_grid, lats_grid = np.meshgrid(longitudes, latitudes)
    lons = np.reshape(lons_grid, -1)
    lats = np.reshape(lats_grid, -1)

    basin_mask = BasinMask()
    basin_ids = basin_mask.lookup(lats, lons)
    is_ocean = basin_mask.basin_is_ocean(basin_ids)
    lats, lons = lats[is_ocean], lons[is_ocean]

    kwargs = {
        "fields": list(along_track_schema.keys()),
        "latitudes": lats,
        "longitudes": lons,
        "dates": [central_date for _ in range(lons.size)],
        "time_window": time_window,
        **(scenario_kwargs or {}),
    }
    if missions is not None:
        kwargs["missions"] = missions

    return BatchQueryScenario(
        query_class=AlongTrack,
        method_name=method_name,
        kwargs=kwargs,
    )


def build_scenarios() -> list[BaseQueryScenario]:
    central_date = datetime(2022, 10, 15)
    time_window = timedelta(days=10)
    scenarios: list[BaseQueryScenario] = [
        batch_scenario_grid(
            method_name="geographic_point_in_r_dt_batch",
            time_window=time_window,
            central_date=central_date,
            resolution=1,
            scenario_kwargs={"radius": 50_000},
        ),
        batch_scenario_grid(
            method_name="geographic_nearest_neighbors_batch",
            time_window=time_window,
            central_date=central_date,
            resolution=1,
            scenario_kwargs={"max_radius": None},
        ),
        batch_scenario_grid(
            method_name="geographic_nearest_neighbors_batch",
            time_window=time_window,
            central_date=central_date,
            resolution=1,
            scenario_kwargs={"max_radius": 500_000},
        ),
        batch_scenario_grid(
            method_name="geographic_point_in_r_dt_batch",
            time_window=time_window,
            central_date=central_date,
            resolution=1,
            missions=["s6a", "j3n"],
            scenario_kwargs={"radius": 50_000},
        ),
        batch_scenario_grid(
            method_name="geographic_nearest_neighbors_batch",
            time_window=time_window,
            central_date=central_date,
            resolution=1,
            missions=["s6a", "j3n"],
            scenario_kwargs={"max_radius": None},
        ),
        batch_scenario_grid(
            method_name="geographic_nearest_neighbors_batch",
            time_window=time_window,
            central_date=central_date,
            resolution=1,
            missions=["s6a", "j3n"],
            scenario_kwargs={"max_radius": 500_000},
        ),
    ]

    return scenarios


def basic_indexes():
    return [
        index_definition("static", (field,))
        for field in ("mission", "basin_id", "along_track_point", "date_time")
    ]


def candidate_specs():
    """Keep upstream's 7 pairs, 12 triplets, 12 quadruplets and final baseline."""
    included, excluded = [], []
    columns = ("along_track_point", "basin_id", "date_time", "mission")
    for length in (2, 3, 4):
        for fields in itertools.permutations(columns, length):
            reason = None
            if fields[0] == "date_time":
                reason = "date_time leads"
            elif "date_time" in fields and "along_track_point" not in fields:
                reason = "date_time without along_track_point"
            elif "date_time" in fields and fields.index("date_time") < fields.index(
                "along_track_point"
            ):
                reason = "date_time precedes along_track_point"
            item = {"fields": list(fields)}
            if reason:
                excluded.append({**item, "reason": reason})
            else:
                included.append({**item, "id": "".join(field[0] for field in fields)})
    included.append({"id": "baseline", "fields": []})
    return included, excluded


if __name__ == "__main__":
    from benchmark_runtime import main

    main()
