from OceanDB.index_experiment import optuna_search, IndexSpec, IndexNode, Index
from OceanDB.query_analysis import BaseQueryScenario
from OceanDB.OceanDB_Initializer import OceanDBInit
from OceanDB.etl.basins_etl import BasinsETL
from OceanDB.commands.ingest import ingest_along_track

import pickle
import random
from datetime import datetime, timedelta
from pathlib import Path

from OceanDB.data_access.along_track import AlongTrack
from OceanDB.query_analysis import BatchQueryScenario
from OceanDB.schemas.along_track_schema import along_track_schema


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

def batch_scenario(
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


if __name__ == "__main__":
    # =======================================
    # setup output
    # =======================================
    n_trials = 50
    study_name = "geo_points_in_r_dt"
    seed = 1828
    pickle_output = "geo_points_in_r_dt.pickle"



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
    scenarios : list[BaseQueryScenario] = [
        batch_scenario(
            1,
            radius=50_000,
            time_window=timedelta(days=1),
            date_start=datetime(2000, 1, 28, 0, 0, 0),
            date_end=datetime(2000, 1, 30, 23, 59, 59),
            n_points=100
        ),
        # batch_scenario(
        #     2,
        #     radius=1_000_000,
        #     time_window=timedelta(days=1),
        #     date_start=datetime(2000, 1, 28, 0, 0, 0),
        #     date_end=datetime(2000, 1, 30, 23, 59, 59),
        # ),
        # batch_scenario(
        #     3,
        #     radius=500_000,
        #     time_window=timedelta(days=1),
        #     date_start=datetime(2000, 1, 30, 0, 0, 0),
        #     date_end=datetime(2000, 2, 2, 0, 0, 0),
        # ),
    ]


    # =======================================
    # create sample function
    # =======================================
    index_spec = IndexSpec(
        """
        CREATE INDEX IF NOT EXISTS {name}
            ON along_track USING gist
            ({fields})
            WITH (buffering=auto)
        """
    )
    index_fields = ["along_track_point", "date_time", "basin_id", "mission"]

    def sample_index_node(trial) -> IndexNode:
        ranked_fields = sorted(
            index_fields,
            key=lambda field: (
                trial.suggest_int(f"rank_{field}", 0, 100),
                field,
            ),
        )

        groups = [[ranked_fields[0]]]
        for position, field in enumerate(ranked_fields[1:], start=1):
            split_before = trial.suggest_categorical(
                f"split_before_{position}", [False, True]
            )
            if split_before:
                groups.append([field])
            else:
                groups[-1].append(field)

        indexes = [
            Index(
                name=f"optuna_along_track_idx_{trial.number}_{group_number}",
                table="along_track",
                fields=group,
                spec=index_spec,
            )
            for group_number, group in enumerate(groups)
        ]
        return IndexNode(indexes=indexes)



    # =======================================
    # search
    # =======================================
    print("searching")
    tried_nodes, study = optuna_search(
        sample_index_node,
        ocean_db_init,
        scenarios,
        n_trials=n_trials,
        study_name=study_name,
        random_seed=seed,
    )


    # =======================================
    # save output
    # =======================================
    output = {
        "tried_nodes": tried_nodes,
        "best_value": study.best_value,
        "best_params": study.best_params,
        "best_trial_number": study.best_trial.number,
        "trials_dataframe": study.trials_dataframe(),
    }
    output_path = Path(pickle_output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("wb") as output_file:
        pickle.dump(output, output_file)

