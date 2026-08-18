from datetime import datetime
from pathlib import Path

import pytest

from OceanDB.config import Config
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
