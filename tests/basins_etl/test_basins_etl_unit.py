from OceanDB.etl.basins_etl import BasinsETL
from tests.database.fixtures import *

pytestmark = pytest.mark.unit


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


def test_insert_basins_data_vacuums_after_inserting(monkeypatch):
    etl = BasinsETL()
    calls = []
    monkeypatch.setattr(etl, "_table_has_rows", lambda table: False)
    monkeypatch.setattr(
        etl, "_insert_csv", lambda **kwargs: calls.append("insert") or 1
    )
    monkeypatch.setattr(etl, "vacuum_analyze", lambda table: calls.append(table))

    etl.insert_basins_data()

    assert calls == ["insert", "basin"]
