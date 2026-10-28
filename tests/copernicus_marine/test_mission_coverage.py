from collections.abc import Iterable
from pathlib import Path

import pytest

from OceanDB.config import Config
from OceanDB.etl.copernicus_marine import OceanDBCopernicusMarine

pytestmark = pytest.mark.unit


class RecordingCopernicusClient:
    def __init__(self) -> None:
        self.login_calls: list[dict[str, object]] = []
        self.get_calls: list[dict[str, object]] = []

    def login(self, **kwargs: object) -> None:
        self.login_calls.append(kwargs)

    def get(self, **kwargs: object) -> dict[str, object]:
        self.get_calls.append(kwargs)
        return kwargs


@pytest.fixture
def downloader(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> tuple[OceanDBCopernicusMarine, RecordingCopernicusClient]:
    monkeypatch.chdir(tmp_path)
    client = RecordingCopernicusClient()
    config = Config(
        along_track_data_directory=str(tmp_path),
        copernicus_username="test-user",
        copernicus_password="test-password",
    )
    return OceanDBCopernicusMarine(config=config, client=client), client


@pytest.mark.parametrize(
    ("mission", "dataset_id", "name", "version"),
    [
        (
            "h2ag",
            "cmems_obs-sl_glo_phy-ssh_my_h2ag-l3-duacs_PT1S",
            "HaiYang-2A on its geodetic orbit (only for dt)",
            "202411",
        ),
        (
            "j3g",
            "cmems_obs-sl_glo_phy-ssh_my_j3g-l3-duacs_PT1S-i",
            "Jason-3 on its long-repeat orbit",
            "202506",
        ),
        (
            "swon",
            "cmems_obs-sl_glo_phy-ssh_my_swon-l3-duacs_PT1S",
            "SWOT-nadir",
            "202411",
        ),
        (
            "swonc",
            "cmems_obs-sl_glo_phy-ssh_my_swonc-l3-duacs_PT1S",
            "SWOT-nadir (CALVAL phase with 1 day cycle, only for dt)",
            "202411",
        ),
        (
            "s6a",
            "cmems_obs-sl_glo_phy-ssh_my_s6a-lr-l3-duacs_PT1S",
            "Sentinel-6A with LRM mode measurement",
            "202411",
        ),
    ],
)
def test_dataset_ids_and_names(
    mission: str, dataset_id: str, name: str, version: str
) -> None:
    datasets = OceanDBCopernicusMarine.build_copernicus_datasets([mission])

    assert len(datasets) == 1
    dataset = datasets[0]
    assert dataset.mission == mission
    assert dataset.dataset_mission == ("s6a-lr" if mission == "s6a" else mission)
    assert dataset.dataset_id == dataset_id
    assert dataset.name == name
    assert dataset.dataset_type == "my"
    assert dataset.version == version


@pytest.mark.parametrize("missions", [None, [], (), ["all"]])
def test_default_and_all_include_new_missions(
    missions: Iterable[str] | None,
) -> None:
    datasets = OceanDBCopernicusMarine.build_copernicus_datasets(missions)

    assert {"h2ag", "j3g", "swon", "swonc"} <= {dataset.mission for dataset in datasets}
    assert len(datasets) == 29
    assert sum(dataset.version == "202411" for dataset in datasets) == 28
    assert [dataset.mission for dataset in datasets if dataset.version == "202506"] == [
        "j3g"
    ]


def test_mixed_missions_keep_individual_default_versions() -> None:
    datasets = OceanDBCopernicusMarine.build_copernicus_datasets(
        ["j3", "j3g", "s6a"], version=None
    )

    assert [(dataset.dataset_id, dataset.version) for dataset in datasets] == [
        ("cmems_obs-sl_glo_phy-ssh_my_j3-l3-duacs_PT1S", "202411"),
        ("cmems_obs-sl_glo_phy-ssh_my_j3g-l3-duacs_PT1S-i", "202506"),
        ("cmems_obs-sl_glo_phy-ssh_my_s6a-lr-l3-duacs_PT1S", "202411"),
    ]


@pytest.mark.parametrize("missions", [["j3g"], ["j3", "j3g", "s6a"], ["all"]])
@pytest.mark.parametrize("version", ["202411", "202506", "custom-release", ""])
def test_explicit_version_override_applies_to_every_mission(
    missions: list[str], version: str
) -> None:
    datasets = OceanDBCopernicusMarine.build_copernicus_datasets(
        missions, version=version
    )

    assert {dataset.version for dataset in datasets} == {version}
    j3g = next(dataset for dataset in datasets if dataset.mission == "j3g")
    assert j3g.dataset_id == "cmems_obs-sl_glo_phy-ssh_my_j3g-l3-duacs_PT1S-i"


@pytest.mark.parametrize(
    ("missions", "existing_release_count"),
    [(["j3g"], 0), (["j3", "j3g", "s6a"], 2), (["all"], 28)],
)
def test_sync_omitted_version_uses_each_mission_default(
    downloader: tuple[OceanDBCopernicusMarine, RecordingCopernicusClient],
    missions: list[str],
    existing_release_count: int,
) -> None:
    marine, client = downloader

    results = marine.sync_copernicus_along_track_data(missions=missions, dry_run=True)

    assert len(client.login_calls) == 1
    assert len(results) == len(client.get_calls) == existing_release_count + 1
    assert (
        sum(call["dataset_version"] == "202411" for call in client.get_calls)
        == existing_release_count
    )
    assert [
        (call["dataset_id"], call["dataset_version"])
        for call in client.get_calls
        if call["dataset_version"] != "202411"
    ] == [("cmems_obs-sl_glo_phy-ssh_my_j3g-l3-duacs_PT1S-i", "202506")]


@pytest.mark.parametrize("version", ["202411", "202506", "custom-release", ""])
def test_sync_forwards_explicit_version_override_unchanged(
    downloader: tuple[OceanDBCopernicusMarine, RecordingCopernicusClient], version: str
) -> None:
    marine, client = downloader

    marine.sync_copernicus_along_track_data(
        missions=["all"], version=version, dry_run=True
    )

    assert len(client.get_calls) == 29
    assert {call["dataset_version"] for call in client.get_calls} == {version}
    assert (
        sum(
            call["dataset_id"] == "cmems_obs-sl_glo_phy-ssh_my_j3g-l3-duacs_PT1S-i"
            for call in client.get_calls
        )
        == 1
    )


@pytest.mark.parametrize("missions", [["unknown"], ["swon", "unknown"]])
def test_unknown_missions_are_rejected(missions: list[str]) -> None:
    with pytest.raises(ValueError, match="Invalid Copernicus mission.*unknown"):
        OceanDBCopernicusMarine.build_copernicus_datasets(missions)
