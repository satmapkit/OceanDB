from OceanDB.etl.basins_etl import BasinsETL
from tests.database.fixtures import *


def test_read_basin_connections_txt_empty(config):
    etl = BasinsETL(config=config)
    connections = list(
        etl._read_basin_connections_txt(
            module="tests.basins_etl", filename="connection_table_empty.txt"
        )
    )

    assert connections == []


def test_read_basin_connections_txt_single(config):
    etl = BasinsETL(config=config)
    connections = list(
        etl._read_basin_connections_txt(
            module="tests.basins_etl", filename="connection_table_single.txt"
        )
    )

    assert connections == [(1, 1)]


def test_read_basin_connections_txt_no_key(config):
    etl = BasinsETL(config=config)

    with pytest.raises(ValueError):
        list(
            etl._read_basin_connections_txt(
                module="tests.basins_etl", filename="connection_table_no_key.txt"
            )
        )


def test_read_basin_connections_txt_bad_sep(config):
    etl = BasinsETL(config=config)
    with pytest.raises(ValueError):
        list(
            etl._read_basin_connections_txt(
                module="tests.basins_etl", filename="connection_table_bad_sep.txt"
            )
        )
