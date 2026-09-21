import itertools
import json
import random
import time
from dataclasses import asdict
from datetime import datetime, timedelta
from typing import Any

import numpy as np

from OceanDB.data_access.along_track import AlongTrack, Mission
from OceanDB.etl import AlongTrackETL
from OceanDB.index_experiment import (IndexNode, index_definition,
                                      run_index_performance_test_with_timeout,
                                      setup_index_performance_test)
from OceanDB.managed_indices import ManagedIndices
from OceanDB.ocean_data.basins import BasinMask
from OceanDB.OceanDB_Initializer import OceanDBInit
from OceanDB.query_analysis import BaseQueryScenario, BatchQueryScenario
from OceanDB.schemas.along_track_schema import along_track_schema

TIMEOUT_SECONDS = 900


def json_default(value: object) -> object:
    if isinstance(value, set):
        return sorted(value)
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


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
    scenario_kwargs: dict[str, Any] = {},
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
        **scenario_kwargs,
    }
    if missions is not None:
        kwargs["missions"] = missions

    return BatchQueryScenario(
        query_class=AlongTrack,
        method_name=method_name,
        kwargs=kwargs,
    )


def main():
    # =======================================
    # setup output
    # =======================================
    json_output = "index_benchmark.json"
    central_date = datetime(2022, 10, 15)
    time_window = timedelta(days=10)
    data_start = central_date - time_window
    data_end = central_date + time_window

    # =======================================
    # create scenarios
    # =======================================
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

    # =======================================
    # define indexes
    # =======================================

    # static indexes always added
    basic_indexes = [
        index_definition("static", fields)
        for fields in (
            ("mission",),
            ("basin_id",),
            ("along_track_point",),
            ("date_time",),
        )
    ]

    trial_index_fields = (
        "along_track_point",
        "basin_id",
        "date_time",
        "mission",
    )
    trial_indexes = [
        [index_definition("experiment", fields)]
        for length in (2, 3, 4)
        for fields in itertools.permutations(trial_index_fields, length)
        if fields[0] != "date_time"
        if "date_time" not in fields
        or fields.index("along_track_point") < fields.index("date_time")
    ]
    trial_indexes.append([])

    all_indexes = tuple(basic_indexes) + tuple(
        index for trial in trial_indexes for index in trial
    )

    # =======================================
    # init oceandb
    # =======================================

    ocean_db_init = OceanDBInit(managed_indices=ManagedIndices(all_indexes))

    # if you want to reset database, run:
    # ocean_db_init.drop_database()

    print("preparing database")
    t1 = time.time()
    if ocean_db_init.database_exists():
        print("database already exists. Assuming prepped")
    else:
        ocean_db_init.initialize_database(
            partition_start="2022-9-01",
            partition_end="2022-11-01",
        )
        print("ingesting")
        AlongTrackETL(config=ocean_db_init.config).ingest(
            missions=["all"],
            start_date=data_start,
            end_date=data_end,
            workers=4,
        )
    print("done in", time.time() - t1, "seconds")

    # =======================================
    # build indexes for each test
    # =======================================
    print("building indexes for each test")
    t1 = time.time()
    nodes = [
        IndexNode(database_name=f"trial_{i}", trial_indexes=trial)
        for i, trial in enumerate(trial_indexes)
    ]
    test_dbs = [
        setup_index_performance_test(
            source_db=ocean_db_init,
            indexes=[*basic_indexes, *node.trial_indexes],
            test_database=node.database_name,
        )
        for node in nodes
    ]
    print("finished building all indexes in", time.time() - t1, "seconds")

    # =======================================
    # search
    # =======================================
    print("searching")

    for node, (test_db, index_sizes) in zip(nodes, test_dbs):
        print("running trial for db", node.database_name, node.pretty_name())
        t1 = time.time()
        node.index_sizes = index_sizes
        try:
            performance = run_index_performance_test_with_timeout(
                test_db,
                scenarios,
                timeout_seconds=TIMEOUT_SECONDS,
            )
            node.performance = performance
            node.error = sum(x.total_time for x in performance)
        except Exception:
            node.error = None

        # save output
        print("done in", time.time() - t1, "seconds.")
        print("saving to", json_output)
        with open(json_output, "w", encoding="utf-8") as output_file:
            json.dump(
                [asdict(node) for node in nodes],
                output_file,
                default=json_default,
                indent=2,
                allow_nan=False,
            )


if __name__ == "__main__":
    main()
