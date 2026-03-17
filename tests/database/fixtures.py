import pytest
from typing import Generator

from OceanDB.config import Config
from OceanDB.OceanDB_Initializer import OceanDBInit
from OceanDB.etl import BaseETL


@pytest.fixture
def config() -> Config:
    return Config(_env_file="tests/.env.test")


@pytest.fixture
def fresh_db(config: Config) -> Generator[OceanDBInit,None,None]:
    ocean_db_init = OceanDBInit(config=config)
    yield ocean_db_init
    ocean_db_init.drop_database()


@pytest.fixture
def db_with_db(fresh_db: OceanDBInit) -> OceanDBInit:
    db = fresh_db
    db.create_database()
    return db


@pytest.fixture
def db_with_tables(db_with_db: OceanDBInit) -> OceanDBInit:
    db = db_with_db
    db.create_tables()
    db.create_eddy_tables()
    return db


@pytest.fixture
def db_with_indices(db_with_tables: OceanDBInit) -> OceanDBInit:
    db = db_with_tables
    db.create_indices()
    db.create_eddy_indices()
    db.create_partitions("1990-01-01", "2025-11-01")
    return db


@pytest.fixture
def db_with_basin_data(db_with_tables: OceanDBInit) -> BaseETL:
    db = db_with_tables
    oceandb_etl = BaseETL(config=db.config)
    oceandb_etl.insert_basins_data()
    oceandb_etl.insert_basin_connections_data()
    return oceandb_etl
