import json
import random
import time
from dataclasses import asdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, get_args

import numpy as np

from OceanDB.data_access.along_track import AlongTrack, Mission
from OceanDB.etl import AlongTrackETL
from OceanDB.index_experiment import (IndexNode, index_definition,
                                      run_index_performance_test_with_timeout,
                                      setup_index_performance_test,
                                      index_definitons_short_name)
from OceanDB.managed_indices import ManagedIndices
from OceanDB.ocean_data.basins import BasinMask
from OceanDB.OceanDB_Initializer import OceanDBInit
from OceanDB.query_analysis import BaseQueryScenario, BatchQueryScenario
from OceanDB.schemas.along_track_schema import along_track_schema
from OceanDB.data_access.query_plan import BasinPlanner, LatitudePlanner

TIMEOUT_SECONDS = 900 * 10
REPEATS = 5
REPEATED_OUTPUT = Path("index_benchmark_repeated.json")


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


def scenario_labels(scenarios: list[BatchQueryScenario]) -> list[dict[str, Any]]:
    return [
        {
            "method": scenario.method_name,
            "planner": type(scenario.kwargs["planner"]).__name__,
            "n_jobs": scenario.kwargs["planner"].n_jobs,
            "chunk_size": scenario.kwargs["planner"].chunk_size,
            "other_kwargs": {key: value for key,value in scenario.kwargs.items() if key not in ["planner"]}
        }
        for scenario in scenarios
    ]


def save_scenario_result(output: Path, results: dict, result: dict) -> None:
    results["results"].append(result)
    temporary = output.with_name(output.name + ".tmp")
    temporary.write_text(
        json.dumps(results, default=json_default, indent=2, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    temporary.replace(output)


def main(repeats: int = REPEATS, json_output: Path = REPEATED_OUTPUT):
    if repeats < 1:
        raise ValueError("repeats must be positive")
    # =======================================
    # setup output
    # =======================================
    central_date = datetime(2022, 10, 15)
    time_window = timedelta(days=10)
    data_start = central_date - time_window
    data_end = central_date + time_window

    all_missions : list[Mission] = list(get_args(Mission))

    # =======================================
    # create scenarios
    # =======================================
    job_chunks = [
            (1, 16),
            (1, 1e6),
            (2, 16),
            (2, 256),
            (8, 16),
            (8, 256),
            (16, 16),
            (16, 256),
            ]
    missions_scenarios = [None, all_missions, ["s6a", "j3n"]]
    method_scenarios = [
            ("geographic_point_in_r_dt_batch", {"radius": 50_000}),
            ("geographic_nearest_neighbors_batch", {"max_radius": None}),
            ("geographic_nearest_neighbors_batch", {"max_radius": 50_000}),
    ]
    planner_scenarios = [
            LatitudePlanner(n_jobs, chunk_size)
            for n_jobs, chunk_size in job_chunks
    ] + [
            BasinPlanner(n_jobs, chunk_size)
            for n_jobs, chunk_size in job_chunks
            ]


    resolution = 1
    scenario_params = [(planner, missions, methods) for planner in planner_scenarios for missions in missions_scenarios for methods in method_scenarios]
    scenarios = [
        batch_scenario_grid(
            method_name=method,
            time_window=time_window,
            central_date=central_date,
            resolution=resolution,
            missions=missions,
            scenario_kwargs={**kwargs, "planner": planner},
        ) for planner, missions, (method, kwargs) in scenario_params
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
    # trial_indexes = [
    #     [index_definition("experiment", fields)]
    #     for length in (2, 3, 4)
    #     for fields in itertools.permutations(trial_index_fields, length)
    #     if fields[0] != "date_time"
    #     if "date_time" not in fields
    #     or (
    #         "along_track_point" in fields
    #         and fields.index("along_track_point") < fields.index("date_time")
    #     )
    # ]
    trial_indexes = [
            [index_definition("experiment", ("basin_id", "along_track_point"))],
            # [index_definition("experiment", ("basin_id", "mission", "along_track_point"))],
            ]
    trial_indexes.insert(0, [])

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
        IndexNode(database_name=index_definitons_short_name(basic_indexes + trial), trial_indexes=trial)
        for i, trial in enumerate(trial_indexes)
    ]
    definitions = [
        {"database_name": node.database_name,
         "trial_indexes": [asdict(index) for index in node.trial_indexes]}
        for node in nodes
    ]
    labels = scenario_labels(scenarios)
    if json_output.exists():
        results = json.loads(json_output.read_text(encoding="utf-8"))
        if (results.get("scenarios") != labels
                or results.get("indexes") != definitions
                or "results" not in results):
            raise ValueError(f"Benchmark configuration differs from {json_output}")
    else:
        results = {"scenarios": labels, "indexes": definitions, "results": []}
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

    for run in range(1, repeats + 1):
        for node, (test_db, index_sizes) in zip(nodes, test_dbs):
            for scenario_index, scenario in enumerate(scenarios):
                if any(
                    item["run"] == run
                    and item["database_name"] == node.database_name
                    and item["scenario_index"] == scenario_index
                    for item in results["results"]
                ):
                    continue
                print("running pass", run, "for db", node.database_name,
                      node.pretty_name(), "scenario", scenario_index)
                t1 = time.time()
                result = {
                    "run": run,
                    "database_name": node.database_name,
                    "index_sizes": index_sizes,
                    "scenario_index": scenario_index,
                    "status": "completed",
                    "performance": None,
                    "error": None,
                }
                try:
                    performance = run_index_performance_test_with_timeout(
                        test_db, [scenario], timeout_seconds=TIMEOUT_SECONDS,
                    )
                    if len(performance) != 1:
                        raise ValueError("Expected one scenario result")
                    result["performance"] = asdict(performance[0])
                except TimeoutError as exc:
                    result["status"] = "timed_out"
                    result["error"] = str(exc)
                except Exception as exc:
                    result["status"] = "failed"
                    result["error"] = str(exc)
                save_scenario_result(json_output, results, result)
                print("done in", time.time() - t1, "seconds; saved to", json_output)


if __name__ == "__main__":
    main(repeats=5)
