import pytest

from OceanDB.etl.basins_etl import BasinsETL

pytestmark = pytest.mark.unit


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
