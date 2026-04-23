from datetime import datetime
from types import SimpleNamespace
from typing import Any, cast

import pytest

from OceanDB.etl.copernicus_marine import OceanDBCopernicusMarine

pytestmark = pytest.mark.unit


class FakeCopernicusClient:
    def __init__(self):
        self.login_calls = []
        self.get_calls = []

    def login(self, **kwargs):
        self.login_calls.append(kwargs)

    def get(self, **kwargs):
        self.get_calls.append(kwargs)
        return kwargs


def test_build_copernicus_datasets_maps_s6a_to_lr_dataset_id():
    datasets = OceanDBCopernicusMarine.build_copernicus_datasets(["s6a"])

    assert len(datasets) == 1
    assert datasets[0].mission == "s6a"
    assert datasets[0].dataset_id == (
        "cmems_obs-sl_glo_phy-ssh_my_s6a-lr-l3-duacs_PT1S"
    )


def test_build_month_filters_expands_inclusive_month_range():
    filters = OceanDBCopernicusMarine.build_month_filters(
        datetime(2023, 12, 15),
        datetime(2024, 2, 1),
    )

    assert filters == [
        "*2023/12/*.nc",
        "*2024/01/*.nc",
        "*2024/02/*.nc",
    ]


def test_sync_uses_configured_output_and_month_filters(tmp_path):
    client = FakeCopernicusClient()
    config = SimpleNamespace(
        copernicus_username="user",
        copernicus_password="password",
        along_track_data_directory=str(tmp_path),
    )
    downloader = OceanDBCopernicusMarine(config=cast(Any, config), client=client)

    downloader.sync_copernicus_along_track_data(
        missions=["j3"],
        start_date=datetime(2024, 1, 1),
        end_date=datetime(2024, 2, 1),
        dry_run=True,
    )

    assert client.login_calls == [
        {
            "username": "user",
            "password": "password",
            "force_overwrite": True,
        }
    ]
    assert [call["filter"] for call in client.get_calls] == [
        "*2024/01/*.nc",
        "*2024/02/*.nc",
    ]
    assert {call["dataset_id"] for call in client.get_calls} == {
        "cmems_obs-sl_glo_phy-ssh_my_j3-l3-duacs_PT1S"
    }
    assert {call["output_directory"] for call in client.get_calls} == {tmp_path}
    assert all(call["dry_run"] for call in client.get_calls)


def test_summarize_get_results_counts_files_and_size():
    results = [
        SimpleNamespace(number_of_files_to_download=2, total_size=512),
        SimpleNamespace(number_of_files_to_download=3, total_size=1024),
    ]

    summary = OceanDBCopernicusMarine.summarize_get_results(results)

    assert summary.request_count == 2
    assert summary.file_count == 5
    assert summary.total_size_mb == 1536
    assert summary.total_size_gb == 1.5
