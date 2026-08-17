import pytest

from OceanDB import workflows

pytestmark = pytest.mark.unit


class FakeOceanDBInit:
    db_name = "oceandb_test"

    def __init__(self, database_exists: bool, table_exists: bool = False):
        self._database_exists = database_exists
        self._table_exists = table_exists
        self.checked_tables = []

    def database_exists(self):
        return self._database_exists

    def table_exists(self, table):
        self.checked_tables.append(table)
        return self._table_exists


def empty_discovery(missions, start_date=None, end_date=None):
    return {"missions": list(missions), "files": []}


def test_ingest_along_track_uses_initialized_database(monkeypatch):
    ocean_db_init = FakeOceanDBInit(database_exists=True, table_exists=True)
    initialize_calls = []
    monkeypatch.setattr(workflows, "OceanDBInit", lambda: ocean_db_init)
    monkeypatch.setattr(workflows, "discover_along_track_files", empty_discovery)
    monkeypatch.setattr(
        workflows,
        "initialize_database",
        lambda: initialize_calls.append(True),
    )

    result = workflows.ingest_along_track(["j3"])

    assert result["matched_count"] == 0
    assert ocean_db_init.checked_tables == ["along_track"]
    assert initialize_calls == []


def test_ingest_along_track_rejects_uninitialized_database(monkeypatch):
    ocean_db_init = FakeOceanDBInit(database_exists=False)
    monkeypatch.setattr(workflows, "OceanDBInit", lambda: ocean_db_init)

    with pytest.raises(RuntimeError, match="oceandb_test.*not initialized"):
        workflows.ingest_along_track(["j3"])

    assert ocean_db_init.checked_tables == []


def test_ingest_along_track_initializes_database_when_enabled(monkeypatch):
    ocean_db_init = FakeOceanDBInit(database_exists=True, table_exists=False)
    initialize_calls = []
    monkeypatch.setattr(workflows, "OceanDBInit", lambda: ocean_db_init)
    monkeypatch.setattr(workflows, "discover_along_track_files", empty_discovery)
    monkeypatch.setattr(
        workflows,
        "initialize_database",
        lambda: initialize_calls.append(True),
    )

    result = workflows.ingest_along_track(
        ["j3"],
        init_database_if_not_exists=True,
    )

    assert result["matched_count"] == 0
    assert ocean_db_init.checked_tables == ["along_track"]
    assert initialize_calls == [True]
