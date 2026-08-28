from pathlib import Path

import pytest

import OceanDB.OceanDB_Initializer as initializer_module
from OceanDB.aviso import AVISO_EDDY_FILENAMES
from OceanDB.config import Config
from OceanDB.etl.eddy_etl import EddyETL

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


def create_etl(data_directory: Path) -> EddyETL:
    config = Config(_env_file=None, eddy_data_directory=str(data_directory))
    return EddyETL(config=config)


def configure_initializer(monkeypatch, etl, initializer):
    def create_initializer(config):
        assert config is etl.config
        return initializer

    monkeypatch.setattr(initializer_module, "OceanDBInit", create_initializer)


def test_ingest_rejects_uninitialized_database(monkeypatch, tmp_path):
    etl = create_etl(tmp_path)
    initializer = FakeOceanDBInit(database_exists=False)
    configure_initializer(monkeypatch, etl, initializer)

    with pytest.raises(RuntimeError, match="oceandb_test.*not initialized"):
        etl.ingest()

    assert initializer.checked_tables == []


def test_ingest_initializes_database_when_enabled(monkeypatch, tmp_path):
    etl = create_etl(tmp_path)
    initializer = FakeOceanDBInit(database_exists=True, table_exists=False)
    configure_initializer(monkeypatch, etl, initializer)
    calls = []
    monkeypatch.setattr(
        etl,
        "ingest_eddy_data_file",
        lambda file, cyclonic_type, offset: calls.append((file, cyclonic_type, offset)),
    )

    etl.ingest(only_ingest="cyclonic", init_database_if_not_exists=True)

    assert initializer.checked_tables == ["eddy"]
    assert initializer.initialize_calls == 1
    assert calls == [(tmp_path / AVISO_EDDY_FILENAMES[0], -1, 0)]


@pytest.mark.parametrize(
    ("only_ingest", "expected_indices"),
    [
        ("cyclonic", [0]),
        ("anticyclonic", [1]),
        ("both", [0, 1]),
    ],
)
def test_ingest_selects_files_and_emits_progress(
    monkeypatch,
    tmp_path,
    only_ingest,
    expected_indices,
):
    etl = create_etl(tmp_path)
    initializer = FakeOceanDBInit(database_exists=True, table_exists=True)
    configure_initializer(monkeypatch, etl, initializer)
    calls = []
    events = []
    monkeypatch.setattr(
        etl,
        "ingest_eddy_data_file",
        lambda file, cyclonic_type, offset: calls.append((file, cyclonic_type, offset)),
    )

    result = etl.ingest(
        only_ingest=only_ingest,
        offset_cyclonic=12,
        offset_anticyclonic=34,
        on_progress=events.append,
    )

    specs = [
        (tmp_path / AVISO_EDDY_FILENAMES[0], -1, 12),
        (tmp_path / AVISO_EDDY_FILENAMES[1], 1, 34),
    ]
    expected_specs = [specs[index] for index in expected_indices]
    assert initializer.checked_tables == ["eddy"]
    assert initializer.initialize_calls == 0
    assert calls == expected_specs
    assert [event["type"] for event in events] == [
        "eddy_file_start" for _ in expected_indices
    ]
    assert [event["filepath"] for event in events] == [
        spec[0] for spec in expected_specs
    ]
    assert result["processed_files"] == [
        {
            "kind": "cyclonic" if index == 0 else "anticyclonic",
            "filename": AVISO_EDDY_FILENAMES[index],
            "filepath": specs[index][0],
            "offset": specs[index][2],
        }
        for index in expected_indices
    ]
