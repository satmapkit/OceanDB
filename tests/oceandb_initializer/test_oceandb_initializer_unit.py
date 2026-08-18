import pytest

from OceanDB.etl import basins_etl as basins_etl_module
from OceanDB.OceanDB_Initializer import OceanDBInit
from tests.database.fixtures import *

pytestmark = pytest.mark.unit


def test_basic(config):
    ocean_db_init = OceanDBInit(config=config)
    assert ocean_db_init.username == "postgres"
    assert ocean_db_init.password == "postgres"
    assert ocean_db_init.port == 5433
    assert (
        ocean_db_init.connection_string
        == "host=localhost dbname=ocean port=5433 user=postgres password=postgres"
    )


def test_initialize_database_uses_initializer_config(monkeypatch, config):
    ocean_db_init = OceanDBInit(config=config)
    calls = []

    monkeypatch.setattr(
        ocean_db_init,
        "create_database",
        lambda: calls.append("create_database") or True,
    )
    monkeypatch.setattr(
        ocean_db_init,
        "create_tables",
        lambda: calls.append("create_tables"),
    )
    monkeypatch.setattr(
        ocean_db_init,
        "create_eddy_tables",
        lambda: calls.append("create_eddy_tables"),
    )
    monkeypatch.setattr(
        ocean_db_init,
        "create_partitions",
        lambda start, end: calls.append(("create_partitions", start, end)),
    )

    class FakeBasinsETL:
        def __init__(self, config):
            assert config is ocean_db_init.config
            calls.append("create_basins_etl")

        def insert_basins_data(self):
            calls.append("insert_basins_data")

        def insert_basin_connections_data(self):
            calls.append("insert_basin_connections_data")

    monkeypatch.setattr(basins_etl_module, "BasinsETL", FakeBasinsETL)

    result = ocean_db_init.initialize_database("2012-12-01", "2013-02-01")

    assert result == {"created_database": True, "initialized": True}
    assert calls == [
        "create_database",
        "create_tables",
        "create_eddy_tables",
        ("create_partitions", "2012-12-01", "2013-02-01"),
        "create_basins_etl",
        "insert_basins_data",
        "insert_basin_connections_data",
    ]
