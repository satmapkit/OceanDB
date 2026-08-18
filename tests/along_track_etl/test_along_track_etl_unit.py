from datetime import datetime
from pathlib import Path

import pytest

import OceanDB.OceanDB_Initializer as initializer_module
from OceanDB.config import Config
from OceanDB.etl import along_track_etl as along_track_etl_module
from OceanDB.etl.along_track_etl import AlongTrackETL

pytestmark = pytest.mark.unit

PRODUCT_DIRECTORY = "SEALEVEL_GLO_PHY_L3_MY_008_062"


def create_along_track_file(
    root: Path,
    mission: str,
    year: int,
    month: int,
    filename: str,
    *,
    low_rate: bool = False,
) -> Path:
    rate = "-lr" if low_rate else ""
    directory = (
        root
        / PRODUCT_DIRECTORY
        / f"cmems_obs-sl_glo_phy-ssh_my_{mission}{rate}-l3-duacs_PT1S_202411"
        / f"{year:04d}"
        / f"{month:02d}"
    )
    directory.mkdir(parents=True, exist_ok=True)
    file = directory / filename
    file.touch()
    return file


def create_etl(data_directory: Path) -> AlongTrackETL:
    config = Config(_env_file=None, along_track_data_directory=str(data_directory))
    return AlongTrackETL(config=config)


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


def configure_initializer(monkeypatch, etl, initializer):
    def create_initializer(config):
        assert config is etl.config
        return initializer

    monkeypatch.setattr(initializer_module, "OceanDBInit", create_initializer)


def test_discover_files_uses_configured_directory_and_date_range(tmp_path):
    december_file = create_along_track_file(tmp_path, "j3", 2012, 12, "december.nc")
    january_file = create_along_track_file(
        tmp_path, "j3", 2013, 1, "january.nc", low_rate=True
    )
    create_along_track_file(tmp_path, "j3", 2013, 2, "february.nc")
    create_along_track_file(tmp_path, "j2", 2013, 1, "other_mission.nc")

    result = create_etl(tmp_path).discover_files(
        ["j3"],
        start_date=datetime(2012, 12, 1),
        end_date=datetime(2013, 1, 31),
    )

    assert result["missions"] == ["j3"]
    assert set(result["files"]) == {december_file, january_file}


def test_discover_files_expands_all_missions(tmp_path):
    expected_file = create_along_track_file(tmp_path, "j3", 2013, 1, "j3.nc")
    etl = create_etl(tmp_path)

    result = etl.discover_files(["all"])

    assert result["missions"] == etl.missions
    assert result["files"] == [expected_file]


@pytest.mark.parametrize("missions", [["invalid"], ["j3", "invalid"]])
def test_discover_files_rejects_invalid_missions(tmp_path, missions):
    with pytest.raises(ValueError, match="invalid arguments"):
        create_etl(tmp_path).discover_files(missions)


def test_discover_files_rejects_reversed_date_range(tmp_path):
    with pytest.raises(ValueError, match="end_date must be >= start_date"):
        create_etl(tmp_path).discover_files(
            ["j3"],
            start_date=datetime(2013, 2, 1),
            end_date=datetime(2013, 1, 1),
        )


def test_ingest_uses_initialized_database(monkeypatch, tmp_path):
    etl = create_etl(tmp_path)
    initializer = FakeOceanDBInit(database_exists=True, table_exists=True)
    configure_initializer(monkeypatch, etl, initializer)
    monkeypatch.setattr(
        etl,
        "discover_files",
        lambda missions, start_date=None, end_date=None: {
            "missions": list(missions),
            "files": [],
        },
    )

    result = etl.ingest(["j3"])

    assert result["matched_count"] == 0
    assert initializer.checked_tables == ["along_track"]
    assert initializer.initialize_calls == 0


def test_ingest_rejects_uninitialized_database(monkeypatch, tmp_path):
    etl = create_etl(tmp_path)
    initializer = FakeOceanDBInit(database_exists=False)
    configure_initializer(monkeypatch, etl, initializer)

    with pytest.raises(RuntimeError, match="oceandb_test.*not initialized"):
        etl.ingest(["j3"])

    assert initializer.checked_tables == []


def test_ingest_initializes_database_when_enabled(monkeypatch, tmp_path):
    etl = create_etl(tmp_path)
    initializer = FakeOceanDBInit(database_exists=True, table_exists=False)
    configure_initializer(monkeypatch, etl, initializer)
    monkeypatch.setattr(
        etl,
        "discover_files",
        lambda missions, start_date=None, end_date=None: {
            "missions": list(missions),
            "files": [],
        },
    )

    result = etl.ingest(["j3"], init_database_if_not_exists=True)

    assert result["matched_count"] == 0
    assert initializer.checked_tables == ["along_track"]
    assert initializer.initialize_calls == 1


def test_ingest_skips_files_present_in_metadata(monkeypatch, tmp_path):
    etl = create_etl(tmp_path)
    initializer = FakeOceanDBInit(database_exists=True, table_exists=True)
    configure_initializer(monkeypatch, etl, initializer)
    files = [tmp_path / "first.nc", tmp_path / "second.nc"]
    monkeypatch.setattr(
        etl,
        "discover_files",
        lambda missions, start_date=None, end_date=None: {
            "missions": list(missions),
            "files": files,
        },
    )
    monkeypatch.setattr(etl, "query_metadata", lambda: {file.name for file in files})

    result = etl.ingest(["j3"])

    assert result == {
        "missions": ["j3"],
        "matched_count": 2,
        "skipped_count": 2,
        "ingested_count": 0,
        "results": [],
        "duration_seconds": 0.0,
    }


def test_ingest_processes_new_files_and_emits_progress(monkeypatch, tmp_path):
    etl = create_etl(tmp_path)
    initializer = FakeOceanDBInit(database_exists=True, table_exists=True)
    configure_initializer(monkeypatch, etl, initializer)
    skipped_file = tmp_path / "skipped.nc"
    ingest_file = tmp_path / "ingest.nc"
    processed_result = {"file_name": ingest_file.name}
    events = []

    monkeypatch.setattr(
        etl,
        "discover_files",
        lambda missions, start_date=None, end_date=None: {
            "missions": list(missions),
            "files": [skipped_file, ingest_file],
        },
    )
    monkeypatch.setattr(etl, "query_metadata", lambda: {skipped_file.name})
    monkeypatch.setattr(
        etl,
        "process_along_track_file",
        lambda file: processed_result,
    )

    class FakeResults:
        def __init__(self):
            self.calls = 0

        def next(self, timeout):
            assert timeout == 30
            self.calls += 1
            if self.calls == 1:
                raise along_track_etl_module.TimeoutError
            return processed_result

    class FakePool:
        def __init__(self, workers):
            assert workers == 2
            self.closed = False
            self.joined = False

        def imap_unordered(self, process, files):
            assert files == [ingest_file]
            assert process(ingest_file) == processed_result
            return FakeResults()

        def close(self):
            self.closed = True

        def join(self):
            self.joined = True

    pools = []

    def create_pool(workers):
        pool = FakePool(workers)
        pools.append(pool)
        return pool

    monkeypatch.setattr(along_track_etl_module, "Pool", create_pool)

    result = etl.ingest(["j3"], workers=2, on_progress=events.append)

    assert result["missions"] == ["j3"]
    assert result["matched_count"] == 2
    assert result["skipped_count"] == 1
    assert result["ingested_count"] == 1
    assert result["results"] == [processed_result]
    assert result["duration_seconds"] >= 0
    assert [event["type"] for event in events] == [
        "along_track_start",
        "along_track_wait",
        "along_track_file_complete",
    ]
    assert pools[0].closed is True
    assert pools[0].joined is True
