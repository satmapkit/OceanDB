import itertools
import json
import random
from dataclasses import asdict
from datetime import datetime, timedelta

import numpy as np

from OceanDB.data_access.along_track import AlongTrack, Mission
from OceanDB.ocean_data.basins import BasinMask
from OceanDB.etl import AlongTrackETL
from OceanDB.index_experiment import (IndexNode, index_definition,
                                      run_index_performance_test,
                                      setup_index_performance_test)
from OceanDB.managed_indices import ManagedIndices
from OceanDB.OceanDB_Initializer import OceanDBInit
from OceanDB.query_analysis import BaseQueryScenario, BatchQueryScenario
from OceanDB.schemas.along_track_schema import along_track_schema


def json_default(value: object) -> object:
    if isinstance(value, set):
        return sorted(value)
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def random_datetimes(
    rng: random.Random, start: datetime, end: datetime, count: int
) -> list[datetime]:
    span_seconds = int((end - start).total_seconds())
    return [
        start + timedelta(seconds=rng.randrange(span_seconds + 1))
        for _ in range(count)
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
    n_points: int = 1000
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
    radius: float,
    time_window: timedelta,
    central_date: datetime,
    resolution: float = 1.0,
    missions: list[Mission]|None = None,
) -> BatchQueryScenario:

    latitudes = np.arange(-60, 60, resolution)
    longitudes = np.arange(-180, 180, resolution)

    lons_grid,lats_grid = np.meshgrid(longitudes, latitudes)
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
        }
    if missions is not None:
        kwargs["missions"] = missions
    if method_name == "geographic_point_in_r_dt_batch":
        kwargs["radius"] = radius

    return BatchQueryScenario(
        query_class=AlongTrack,
        method_name=method_name,
        kwargs=kwargs,
    )




def main():
    # =======================================
    # setup output
    # =======================================
    n_trials = 50
    seed = 1828
    json_output = "no_mission_singleton_indexes.json"
    central_date = datetime(2022, 10, 15)
    time_window = timedelta(days=10)
    data_start = central_date - time_window
    data_end = central_date + time_window




    # =======================================
    # create scenarios
    # =======================================
    # TODO: gridded locations vs random
    # TODO: sorted random vs random
    # TODO: nearest neighbor
    # TODO: improve search to reduce duplicated queries
    # TODO: put results in documentation
    # TODO: add version which selects on mission
    # TODO: nearest neighbor
    # TODO: nearest neighbor with speed
    # TODO: choose date after 2022 with s6a (sentinel 6a)
    # TODO: filesize via something like SELECT schemaname, relname as table_name, indexrelname AS index_name, pg_size_pretty(pg_relation_size(indexrelid)) AS index_size FROM pg_stat_user_indexes ORDER BY pg_relation_size(indexrelid) DESC LIMIT 20;
    scenarios : list[BaseQueryScenario] = [
        batch_scenario_grid(
            method_name="geographic_point_in_r_dt_batch",
            radius=50_000,
            time_window=time_window,
            central_date=central_date,
            resolution=2,
            # all missions
            ),
        batch_scenario_grid(
            method_name="geographic_nearest_neighbors_batch",
            radius=50_000,
            time_window=time_window,
            central_date=central_date,
            resolution=2,
            # all missions
            ),
        batch_scenario_grid(
            method_name="geographic_point_in_r_dt_batch",
            radius=50_000,
            time_window=time_window,
            central_date=central_date,
            resolution=2,
            missions=["s6a", "j3n"]
            ),
        batch_scenario_grid(
            method_name="geographic_nearest_neighbors_batch",
            radius=50_000,
            time_window=time_window,
            central_date=central_date,
            resolution=2,
            missions=["s6a", "j3n"]
            ),
    ]

    # =======================================
    # define indexes
    # =======================================

    # static indexes always added
    basic_indexes = [
        index_definition("static", fields)
        for fields in (("mission", "basin_id"), ("along_track_point",), ("date_time",))
    ]

    trial_index_fields = ["along_track_point", "date_time", "basin_id"]
    trial_indexes = [
        [index_definition("experiment", fields)]
        for fields in itertools.permutations(trial_index_fields)
            ]
    trial_indexes.append([])

    all_indexes = tuple(basic_indexes) + \
                  tuple(index for trial in trial_indexes for index in trial )

    # =======================================
    # init oceandb
    # =======================================

    ocean_db_init = OceanDBInit(managed_indices=ManagedIndices(all_indexes))
    print("preparing database")
    if ocean_db_init.database_exists():
        # ocean_db_init.drop_database()
        print("database already exists. Assuming prepped")
        pass
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
        print("done")


    # =======================================
    # build indexes for each test
    # =======================================
    nodes = [
            IndexNode(
                database_name=f'trial_{i}',
                trial_indexes=trial
            )
            for i,trial in enumerate(trial_indexes)
            ]
    test_dbs = [setup_index_performance_test(
                source_db=ocean_db_init,
                indexes=[*basic_indexes, *node.trial_indexes],
                test_database=node.database_name,
            ) for node in nodes]

    # =======================================
    # search
    # =======================================
    print("searching")

    for node, test_db in zip(nodes, test_dbs):
        try:
            performance = run_index_performance_test(test_db, scenarios)
            node.performance = performance
            node.error = sum(x.total_time for x in performance)
        except Exception:
            node.error = None
        nodes.append(node)

        # save output
        print("saving")
        print(json_output)
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
