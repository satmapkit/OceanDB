import pytest
from typing import Generator
from pathlib import Path

from OceanDB.config import Config
from OceanDB.OceanDB_Initializer import OceanDBInit
from OceanDB.etl import BaseETL, EddyETL, AlongTrackETL


@pytest.fixture
def config():
    return Config(_env_file="tests/.env.test")


@pytest.fixture
def fresh_db(config: Config) -> Generator[OceanDBInit, None, None]:
    ocean_db_init = OceanDBInit(config=config)
    yield ocean_db_init
    ocean_db_init.drop_database()


@pytest.fixture
def db_with_db(fresh_db):
    db = fresh_db
    db.create_database()
    return db


@pytest.fixture
def db_with_tables(db_with_db):
    db = db_with_db
    db.create_tables()
    db.create_eddy_tables()
    db.create_partitions("1990-01-01", "2025-11-01")
    return db


@pytest.fixture
def db_with_indices(db_with_tables):
    db = db_with_tables
    db.create_indices()
    db.create_eddy_indices()
    db.create_partitions("1990-01-01", "2025-11-01")
    return db


@pytest.fixture
def db_with_basin_data(db_with_tables):
    db = db_with_tables
    oceandb_etl = BaseETL(config=db.config)
    oceandb_etl.insert_basins_data()
    oceandb_etl.insert_basin_connections_data()
    return oceandb_etl


@pytest.fixture
def db_with_cyclonic_eddy_data(db_with_basin_data: BaseETL) -> EddyETL:
    oceandb_etl = EddyETL(config=db_with_basin_data.config)
    eddy_directory = oceandb_etl.config.eddy_data_directory

    cyclonic_filepath = Path(f"{eddy_directory}/cyclonic.nc")
    oceandb_etl.ingest_eddy_data_file(cyclonic_filepath, cyclonic_type=-1)
    return oceandb_etl


@pytest.fixture
def db_with_alongtrack_data(db_with_basin_data: BaseETL) -> AlongTrackETL:
    oceandb_etl = AlongTrackETL(config=db_with_basin_data.config)
    alongtrack_directory = oceandb_etl.config.along_track_data_directory

    for i in range(1, 10):
        print(f"ingesting {alongtrack_directory}/2013010{i}.nc")
        oceandb_etl.process_along_track_file(
            Path(f"{alongtrack_directory}/required_underscores_j2_2013010{i}.nc")
        )
    return oceandb_etl


@pytest.fixture
def db_with_all_eddy_data(db_with_cyclonic_eddy_data: EddyETL) -> EddyETL:
    db = db_with_cyclonic_eddy_data
    # do anticyclonic
    return db


@pytest.fixture
def db_with_eddy_and_alongtrack_data(db_with_all_eddy_data: EddyETL) -> AlongTrackETL:
    db = db_with_all_eddy_data

    # add along track data
    db = AlongTrackETL(config=db.config)
    alongtrack_directory = db.config.along_track_data_directory

    for i in range(1, 10):
        print(f"ingesting {alongtrack_directory}/2013010{i}.nc")
        db.process_along_track_file(
            Path(f"{alongtrack_directory}/required_underscores_j2_2013010{i}.nc")
        )
    return db
