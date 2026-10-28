"""Exercise CLI release selection with the real downloader and a fake transport."""

from dataclasses import dataclass
from pathlib import Path

import pytest
from click.testing import CliRunner

from OceanDB.cli import cli
from OceanDB.commands import download as download_commands
from OceanDB.config import Config
from OceanDB.etl.copernicus_marine import OceanDBCopernicusMarine

pytestmark = pytest.mark.unit

J2_DATASET = "cmems_obs-sl_glo_phy-ssh_my_j2-l3-duacs_PT1S"
J3G_DATASET = "cmems_obs-sl_glo_phy-ssh_my_j3g-l3-duacs_PT1S-i"


@dataclass(frozen=True)
class EmptyDownload:
    number_of_files_to_download: int = 0
    total_size: float = 0.0


class RecordingTransport:
    def __init__(self) -> None:
        self.requests: list[dict[str, object]] = []
        self.logins = 0

    def login(self, **kwargs: object) -> None:
        self.logins += 1

    def get(self, **kwargs: object) -> EmptyDownload:
        self.requests.append(kwargs)
        return EmptyDownload()


@pytest.fixture
def transport(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> RecordingTransport:
    monkeypatch.chdir(tmp_path)
    config = Config(
        along_track_data_directory=str(tmp_path),
        copernicus_username="test",
        copernicus_password="test",
    )
    native = RecordingTransport()
    downloader = OceanDBCopernicusMarine(config=config, client=native)
    monkeypatch.setattr(download_commands, "Config", lambda: config)
    monkeypatch.setattr(
        download_commands, "create_copernicus_marine_client", lambda: downloader
    )
    return native


@pytest.mark.parametrize(("option", "repetitions"), [("--dry-run", 1), ("--yes", 2)])
def test_download_uses_each_missions_release(
    transport: RecordingTransport, option: str, repetitions: int
) -> None:
    result = CliRunner().invoke(cli, ["download", "j2", "j3g", option])

    assert result.exit_code == 0, result.output
    assert transport.logins == repetitions
    assert [
        (request["dataset_id"], request["dataset_version"])
        for request in transport.requests
    ] == [(J2_DATASET, "202411"), (J3G_DATASET, "202506")] * repetitions


def test_explicit_release_overrides_every_selected_mission(
    transport: RecordingTransport,
) -> None:
    result = CliRunner().invoke(
        cli,
        [
            "download",
            "j2",
            "j3g",
            "--dataset-version",
            "202701",
            "--dry-run",
        ],
    )

    assert result.exit_code == 0, result.output
    assert [
        (request["dataset_id"], request["dataset_version"])
        for request in transport.requests
    ] == [(J2_DATASET, "202701"), (J3G_DATASET, "202701")]


@pytest.mark.parametrize("missions", [[], ["all"]])
def test_all_missions_use_mixed_catalogue_releases(
    transport: RecordingTransport, missions: list[str]
) -> None:
    result = CliRunner().invoke(cli, ["download", *missions, "--dry-run"])

    assert result.exit_code == 0, result.output
    assert len(transport.requests) == 29
    assert len({request["dataset_id"] for request in transport.requests}) == 29
    assert (
        sum(request["dataset_version"] == "202411" for request in transport.requests)
        == 28
    )
    assert [
        request["dataset_version"]
        for request in transport.requests
        if request["dataset_id"] == J3G_DATASET
    ] == ["202506"]


def test_invalid_mission_does_not_reach_transport(
    transport: RecordingTransport,
) -> None:
    result = CliRunner().invoke(cli, ["download", "j3g", "unknown", "--dry-run"])

    assert result.exit_code != 0
    assert isinstance(result.exception, ValueError)
    assert "unknown" in str(result.exception)
    assert transport.logins == 0
    assert transport.requests == []
