import pytest

from OceanDB import workflows

pytestmark = pytest.mark.unit


class FakeOceanDBInit:
    db_name = "oceandb_test"

    def __init__(self, database_exists: bool, table_exists: bool = False):
        self._database_exists = database_exists
        self._table_exists = table_exists
        self.checked_tables = []
        self.initialize_calls = 0

    def database_exists(self):
        return self._database_exists

    def table_exists(self, table):
        self.checked_tables.append(table)
        return self._table_exists

    def initialize_database(self):
        self.initialize_calls += 1


def empty_discovery(missions, start_date=None, end_date=None):
    return {"missions": list(missions), "files": []}


def test_ingest_along_track_uses_initialized_database(monkeypatch):
    ocean_db_init = FakeOceanDBInit(database_exists=True, table_exists=True)
    monkeypatch.setattr(workflows, "OceanDBInit", lambda: ocean_db_init)
    monkeypatch.setattr(workflows, "discover_along_track_files", empty_discovery)

    result = workflows.ingest_along_track(["j3"])

    assert result["matched_count"] == 0
    assert ocean_db_init.checked_tables == ["along_track"]
    assert ocean_db_init.initialize_calls == 0


def test_ingest_along_track_rejects_uninitialized_database(monkeypatch):
    ocean_db_init = FakeOceanDBInit(database_exists=False)
    monkeypatch.setattr(workflows, "OceanDBInit", lambda: ocean_db_init)

    with pytest.raises(RuntimeError, match="oceandb_test.*not initialized"):
        workflows.ingest_along_track(["j3"])

    assert ocean_db_init.checked_tables == []


def test_ingest_along_track_initializes_database_when_enabled(monkeypatch):
    ocean_db_init = FakeOceanDBInit(database_exists=True, table_exists=False)
    monkeypatch.setattr(workflows, "OceanDBInit", lambda: ocean_db_init)
    monkeypatch.setattr(workflows, "discover_along_track_files", empty_discovery)

    result = workflows.ingest_along_track(
        ["j3"],
        init_database_if_not_exists=True,
    )

    assert result["matched_count"] == 0
    assert ocean_db_init.checked_tables == ["along_track"]
    assert ocean_db_init.initialize_calls == 1
