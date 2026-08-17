from OceanDB.index_experiment import IndexSpec, IndexNode, Index, run_index_performance_test
from OceanDB.query_analysis import BaseQueryScenario
from OceanDB.OceanDB_Initializer import OceanDBInit
from OceanDB.data_access.along_track import AlongTrack, Mission
from OceanDB.query_analysis import BatchQueryScenario
from OceanDB.schemas.along_track_schema import along_track_schema

import pickle
import random
from datetime import datetime, timedelta
import itertools

from OceanDB.data_access.along_track import AlongTrack
from OceanDB.query_analysis import BatchQueryScenario
from OceanDB.schemas.along_track_schema import along_track_schema
import numpy as np


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


# =======================================
# setup output
# =======================================
n_trials = 50
seed = 1828
pickle_output = "no_mission_singleton_indexes.pickle"



# =======================================
# init oceandb
# =======================================
print("creating db")

ocean_db_init = OceanDBInit()
# try:
#     ocean_db_init.drop_database()
# except:
#     print("database already deleted")
#
# ocean_db_init.create_database()
# ocean_db_init.create_tables()
# ocean_db_init.create_partitions("2000-01-01", "2000-03-01")
#
# # ingest basin data
# oceandb_etl = BasinsETL()
# oceandb_etl.insert_basins_data()
# oceandb_etl.insert_basin_connections_data()
#
# # ingest along track data
# ingest_along_track(['all'], '2000-01-29', '2000-02-03', 4, False)


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
        time_window=timedelta(days=10),
        central_date=datetime(2022, 10, 15),
        resolution=2,
        # all missions
        ),
    batch_scenario_grid(
        method_name="geographic_nearest_neighbors_batch",
        radius=50_000,
        time_window=timedelta(days=10),
        central_date=datetime(2022, 10, 15),
        resolution=2,
        # all missions
        ),
    batch_scenario_grid(
        method_name="geographic_point_in_r_dt_batch",
        radius=50_000,
        time_window=timedelta(days=10),
        central_date=datetime(2022, 10, 15),
        resolution=2,
        missions=["s6a", "j3n"]
        ),
    batch_scenario_grid(
        method_name="geographic_nearest_neighbors_batch",
        radius=50_000,
        time_window=timedelta(days=10),
        central_date=datetime(2022, 10, 15),
        resolution=2,
        missions=["s6a", "j3n"]
        ),
]

# =======================================
# setup
# =======================================
index_spec = IndexSpec(
    """
    CREATE INDEX IF NOT EXISTS {name}
        ON along_track USING gist
        ({fields})
        WITH (buffering=auto)
    """
)

# =======================================
# build basic indexes
# =======================================
print("building basic fields")
for fields in [["mission", "basin_id"], ["along_track_point"], ["date_time"]]:
    Index(
        name=f"along_track_index_static_{'_'.join(fields)}",
        table="along_track",
        fields=fields,
        spec=index_spec,
    ).build(ocean_db_init)
print("done")

# =======================================
# search
# =======================================
print("searching")
index_fields = ["along_track_point", "date_time", "basin_id"]

nodes = []
for fields in itertools.permutations(index_fields):
    index = Index(
        name=f"along_track_index_experiment_{'_'.join(fields)}",
        table="along_track",
        fields=list(fields),
        spec=index_spec,
    )
    nodes.append(IndexNode([index]))

for node in nodes:
    try:
        performance = run_index_performance_test(node.indexes, ocean_db_init, scenarios)
        error = sum(x.total_time for x in performance)
        node.performance = performance
        node.error = error
    except Exception as ex:
        error = float("inf")
        node.error = error


    # save output
    print("saving")
    print(pickle_output)
    with open(pickle_output, "wb") as output_file:
        print("pickle.dumping")
        pickle.dump(nodes, output_file)
