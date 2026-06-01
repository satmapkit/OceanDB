import os
from pathlib import Path
from typing import Generator

import pytest

from OceanDB.config import Config
from OceanDB.etl import AlongTrackETL, BasinsETL, EddyETL
from OceanDB.OceanDB_Initializer import OceanDBInit

TEST_PARTITION_START = "2012-12-01"
TEST_PARTITION_END = "2013-02-01"
TESTS_DIR = Path(__file__).resolve().parent.parent
REPO_ROOT = TESTS_DIR.parent


@pytest.fixture
def config():
    along_track_data_directory = os.environ.get(
        "OCEANDB_TEST_ALONG_TRACK_DATA_DIRECTORY",
        str(REPO_ROOT / "tests" / "data" / "along_track"),
    )
    eddy_data_directory = os.environ.get(
        "OCEANDB_TEST_EDDY_DATA_DIRECTORY",
        str(REPO_ROOT / "tests" / "data" / "eddy"),
    )

    return Config(
        _env_file=None,
        postgres_host=os.environ.get("OCEANDB_TEST_POSTGRES_HOST", "localhost"),
        postgres_port=int(os.environ.get("OCEANDB_TEST_POSTGRES_PORT", "5433")),
        postgres_username=os.environ.get("OCEANDB_TEST_POSTGRES_USERNAME", "postgres"),
        postgres_password=os.environ.get("OCEANDB_TEST_POSTGRES_PASSWORD", "postgres"),
        postgres_database=os.environ.get("OCEANDB_TEST_POSTGRES_DATABASE", "ocean"),
        along_track_data_directory=along_track_data_directory,
        eddy_data_directory=eddy_data_directory,
        copernicus_username=os.environ.get("OCEANDB_TEST_COPERNICUS_USERNAME", "none"),
        copernicus_password=os.environ.get("OCEANDB_TEST_COPERNICUS_PASSWORD", "none"),
    )


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
    db.create_partitions(TEST_PARTITION_START, TEST_PARTITION_END)
    return db


@pytest.fixture
def db_with_indices(db_with_tables):
    db = db_with_tables
    db.create_indices()
    db.create_eddy_indices()
    return db


@pytest.fixture
def db_with_basin_data(db_with_tables):
    db = db_with_tables
    oceandb_etl = BasinsETL(config=db.config)
    oceandb_etl.insert_basins_data()
    oceandb_etl.insert_basin_connections_data()
    return oceandb_etl


@pytest.fixture
def db_with_cyclonic_eddy_data(db_with_basin_data: BasinsETL) -> EddyETL:
    oceandb_etl = EddyETL(config=db_with_basin_data.config)
    eddy_directory = oceandb_etl.config.eddy_data_directory

    cyclonic_filepath = Path(f"{eddy_directory}/cyclonic.nc")
    oceandb_etl.ingest_eddy_data_file(
        cyclonic_filepath, cyclonic_type=-1, batch_size=10
    )
    return oceandb_etl


@pytest.fixture
def db_with_alongtrack_data(db_with_basin_data: BasinsETL) -> AlongTrackETL:
    oceandb_etl = AlongTrackETL(config=db_with_basin_data.config)
    alongtrack_directory = oceandb_etl.config.along_track_data_directory

    for i in range(1, 10):
        print(f"ingesting {alongtrack_directory}/2013010{i}.nc")
        oceandb_etl.process_along_track_file(
            Path(f"{alongtrack_directory}/required_underscores_j2_2013010{i}.nc"),
            batch_size=10,
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
            Path(f"{alongtrack_directory}/required_underscores_j2_2013010{i}.nc"),
            batch_size=10,
        )
    return db
