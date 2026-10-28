from datetime import datetime
from pathlib import Path
from shutil import copyfile

import netCDF4 as nc
import pytest

from OceanDB.config import Config
from OceanDB.etl.along_track_etl import AlongTrackETL

pytestmark = pytest.mark.unit

PRODUCT = "SEALEVEL_GLO_PHY_L3_MY_008_062"
NETCDF_FIXTURE = (
    Path(__file__).resolve().parents[1]
    / "data/along_track/required_underscores_j2_20130101.nc"
)
J3G_DIRECTORY = "cmems_obs-sl_glo_phy-ssh_my_j3g-l3-duacs_PT1S-i_202506"
J3G_FILENAME = "dt_global_j3g_phy_l3_1hz_20250618_20250701.nc"
# Literal upstream layouts keep this test independent of registry/ID construction.
PRODUCT_LAYOUTS: tuple[tuple[str, str, str], ...] = (
    (
        "h2ag",
        "cmems_obs-sl_glo_phy-ssh_my_h2ag-l3-duacs_PT1S_202411",
        "dt_global_h2ag_phy_l3_1hz_20130101_20241101.nc",
    ),
    (
        "swon",
        "cmems_obs-sl_glo_phy-ssh_my_swon-l3-duacs_PT1S_202411",
        "dt_global_swon_phy_l3_1hz_20130101_20241101.nc",
    ),
    (
        "swonc",
        "cmems_obs-sl_glo_phy-ssh_my_swonc-l3-duacs_PT1S_202411",
        "dt_global_swonc_phy_l3_1hz_20130101_20241101.nc",
    ),
    (
        "s6a",
        "cmems_obs-sl_glo_phy-ssh_my_s6a-lr-l3-duacs_PT1S_202411",
        "dt_global_s6a_lr_phy_l3_1hz_20130101_20241101.nc",
    ),
)


@pytest.fixture
def etl(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> AlongTrackETL:
    # Do not load a developer's .env file in these database-free tests.
    monkeypatch.chdir(tmp_path)
    return AlongTrackETL(config=Config(along_track_data_directory=str(tmp_path)))


@pytest.mark.parametrize(("mission", "directory", "filename"), PRODUCT_LAYOUTS)
def test_discovery_date_filter_and_real_extraction_preserve_mission(
    tmp_path: Path,
    etl: AlongTrackETL,
    mission: str,
    directory: str,
    filename: str,
) -> None:
    expected_files: set[Path] = set()
    for year, month in [(2012, 11), (2012, 12), (2013, 1), (2013, 2)]:
        file = tmp_path / PRODUCT / directory / str(year) / f"{month:02d}" / filename
        file.parent.mkdir(parents=True)
        copyfile(NETCDF_FIXTURE, file)
        if (year, month) in {(2012, 12), (2013, 1)}:
            expected_files.add(file)
    other_mission = (
        tmp_path
        / PRODUCT
        / "cmems_obs-sl_glo_phy-ssh_my_j2-l3-duacs_PT1S_202411"
        / "2013/01/dt_global_j2_phy_l3_1hz_20130101_20241101.nc"
    )
    other_mission.parent.mkdir(parents=True)
    other_mission.touch()

    discovery = etl.discover_files(
        [mission],
        start_date=datetime(2012, 12, 1),
        end_date=datetime(2013, 1, 31),
    )

    assert discovery["missions"] == [mission]
    assert set(discovery["files"]) == expected_files
    for file in discovery["files"]:
        with nc.Dataset(file) as dataset:
            metadata = etl.extract_dataset_metadata(dataset, file)
            rows = [
                row
                for batch in etl.extract_data_from_netcdf(dataset, 37, file)
                for row in batch
            ]
            assert len(rows) == len(dataset["latitude"]) == 100
        assert metadata.mission == mission
        assert metadata.file_name == filename
        assert metadata.observation_count == len(rows)
        assert {row["mission"] for row in rows} == {mission}
        assert {row["file_name"] for row in rows} == {filename}


def test_j3g_discovery_date_filter_and_real_extraction(
    tmp_path: Path, etl: AlongTrackETL
) -> None:
    # These are renamed legacy j2 observations, not a real j3g science fixture.
    expected_files: set[Path] = set()
    for month in [5, 6, 7, 8]:
        file = (
            tmp_path / PRODUCT / J3G_DIRECTORY / "2025" / f"{month:02d}" / J3G_FILENAME
        )
        file.parent.mkdir(parents=True)
        copyfile(NETCDF_FIXTURE, file)
        if month in {6, 7}:
            expected_files.add(file)

    discovery = etl.discover_files(
        ["j3g"],
        start_date=datetime(2025, 6, 18),
        end_date=datetime(2025, 7, 31),
    )

    assert discovery["missions"] == ["j3g"]
    assert set(discovery["files"]) == expected_files
    for file in discovery["files"]:
        with nc.Dataset(file) as dataset:
            metadata = etl.extract_dataset_metadata(dataset, file)
            rows = [
                row
                for batch in etl.extract_data_from_netcdf(dataset, 37, file)
                for row in batch
            ]
            assert len(rows) == len(dataset["latitude"]) == 100
        assert metadata.mission == "j3g"
        assert metadata.file_name == J3G_FILENAME
        assert metadata.observation_count == len(rows)
        assert {row["mission"] for row in rows} == {"j3g"}
        assert {row["file_name"] for row in rows} == {J3G_FILENAME}


@pytest.mark.parametrize(
    "directory",
    [
        "cmems_obs-sl_glo_phy-ssh_my_j3g-l3-duacs_PT1S_202411",
        "cmems_obs-sl_glo_phy-ssh_my_j3g-l3-duacs_PT1S_202506",
        "cmems_obs-sl_glo_phy-ssh_my_j3g-l3-duacs_PT1S-i_202411",
        "cmems_obs-sl_glo_phy-ssh_my_j3g-l3-duacs_PT1S-i_202611",
    ],
)
@pytest.mark.parametrize("missions", [["j3g"], ["all"]])
def test_j3g_discovery_ignores_incorrect_suffix_or_release(
    tmp_path: Path, etl: AlongTrackETL, directory: str, missions: list[str]
) -> None:
    file = tmp_path / PRODUCT / directory / "2025/06" / J3G_FILENAME
    file.parent.mkdir(parents=True)
    file.touch()

    assert etl.discover_files(missions)["files"] == []
    assert (
        etl.discover_files(
            missions,
            start_date=datetime(2025, 6, 1),
            end_date=datetime(2025, 6, 30),
        )["files"]
        == []
    )


@pytest.mark.parametrize("missions", [[], (), ["all"]])
def test_default_and_all_discover_new_missions(
    tmp_path: Path, etl: AlongTrackETL, missions: list[str] | tuple[str, ...]
) -> None:
    expected_files: set[Path] = set()
    for mission, directory, filename in PRODUCT_LAYOUTS:
        if mission == "s6a":
            continue
        file = tmp_path / PRODUCT / directory / "2013/01" / filename
        file.parent.mkdir(parents=True)
        file.touch()
        expected_files.add(file)
    j3g_file = tmp_path / PRODUCT / J3G_DIRECTORY / "2025/06" / J3G_FILENAME
    j3g_file.parent.mkdir(parents=True)
    j3g_file.touch()
    expected_files.add(j3g_file)

    discovery = etl.discover_files(missions)

    assert {"h2ag", "j3g", "swon", "swonc"} <= set(discovery["missions"])
    assert set(discovery["files"]) == expected_files


@pytest.mark.parametrize("missions", [["unknown"], ["h2ag", "unknown"]])
def test_discovery_rejects_unknown_missions(
    etl: AlongTrackETL, missions: list[str]
) -> None:
    with pytest.raises(ValueError, match="invalid arguments.*unknown"):
        etl.discover_files(missions)


def test_real_extraction_rejects_unknown_filename_mission(etl: AlongTrackETL) -> None:
    with nc.Dataset(NETCDF_FIXTURE) as dataset:
        with pytest.raises(ValueError, match="Unsupported mission 'unknown'"):
            next(
                etl.extract_data_from_netcdf(
                    dataset,
                    37,
                    Path("dt_global_unknown_phy_l3_1hz_20130101_20241101.nc"),
                )
            )
