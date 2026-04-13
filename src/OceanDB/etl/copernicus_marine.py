from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable

from OceanDB.config import Config


@dataclass(frozen=True)
class CopernicusDataset:
    mission: str
    dataset_mission: str
    dataset_type: str
    version: str
    dataset_id: str
    name: str


@dataclass(frozen=True)
class CopernicusDownloadSummary:
    request_count: int
    file_count: int
    total_size_mb: float | None

    @property
    def total_size_gb(self) -> float | None:
        if self.total_size_mb is None:
            return None
        return self.total_size_mb / 1024


MISSION_DATASET_IDS = {
    "al": "al",
    "alg": "alg",
    "c2": "c2",
    "c2n": "c2n",
    "e1": "e1",
    "e1g": "e1g",
    "e2": "e2",
    "en": "en",
    "enn": "enn",
    "g2": "g2",
    "h2a": "h2a",
    "h2b": "h2b",
    "j1": "j1",
    "j1g": "j1g",
    "j1n": "j1n",
    "j2": "j2",
    "j2g": "j2g",
    "j2n": "j2n",
    "j3": "j3",
    "j3n": "j3n",
    "s3a": "s3a",
    "s3b": "s3b",
    "s6a": "s6a-lr",
    "tp": "tp",
    "tpn": "tpn",
}

MISSION_NAMES = {
    "e1": "ERS-1 (only for dt)",
    "e1g": "ERS-1 geodetic phase (only for dt)",
    "e2": "ERS-2 (only for dt)",
    "tp": "TOPEX/Poseidon (only for dt)",
    "tpn": "TOPEX/Poseidon on its new orbit (only for dt)",
    "g2": "GFO (only for dt)",
    "j1": "Jason-1 (only for dt)",
    "j1n": "Jason-1 on its new orbit (only for dt)",
    "j1g": "Jason-1 on its geodetic orbit (only for dt)",
    "j2": "OSTM/Jason-2 (only for dt)",
    "j2n": "OSTM/Jason-2 on its interleaved orbit",
    "j2g": "OSTM/Jason-2 on its long repeat orbit (LRO)",
    "j3": "Jason-3",
    "j3n": "Jason-3 on its new (interleaved) orbit",
    "en": "Envisat (only for dt)",
    "enn": "Envisat on its new orbit (only for dt)",
    "c2": "Cryosat-2",
    "c2n": "Cryosat-2 on its new orbit",
    "al": "Saral/AltiKa",
    "alg": "Saral/AltiKa on its geodetic orbit (only for dt)",
    "h2a": "HaiYang-2A (only for dt)",
    "h2b": "HaiYang-2B",
    "s3a": "Sentinel-3A",
    "s3b": "Sentinel-3B",
    "s6a": "Sentinel-6A with LRM mode measurement",
}


class OceanDBCopernicusMarine:
    def __init__(self, config: Config | None = None, client: Any = None):
        self.config = Config() if config is None else config
        if client is None:
            try:
                import copernicusmarine
            except ImportError as ex:
                raise ImportError(
                    "The copernicusmarine package is required for downloads. "
                    "Install OceanDB dependencies before running `oceandb download`."
                ) from ex
            client = copernicusmarine
        self.client = client

    def login(self) -> None:
        username = self.config.copernicus_username
        password = self.config.copernicus_password

        if not username or not password:
            raise ValueError(
                "Copernicus credentials are missing. Set COPERNICUS_USERNAME and "
                "COPERNICUS_PASSWORD in your .env file."
            )

        try:
            self.client.login(
                username=username,
                password=password,
                force_overwrite=True,
            )
        except Exception as ex:
            raise RuntimeError(
                "Copernicus Marine Service login did not succeed. "
                "Check COPERNICUS_USERNAME and COPERNICUS_PASSWORD in your .env file."
            ) from ex

    @staticmethod
    def summarize_get_results(results: Iterable[Any]) -> CopernicusDownloadSummary:
        request_count = 0
        file_count = 0
        total_size_mb = 0.0
        has_unknown_size = False

        for result in results:
            request_count += 1
            files = getattr(result, "files", None) or []
            result_file_count = getattr(result, "number_of_files_to_download", None)
            if result_file_count is None:
                result_file_count = len(files)
            file_count += result_file_count

            result_size = getattr(result, "total_size", None)
            if result_size is None:
                file_sizes = [getattr(file, "file_size", None) for file in files]
                known_sizes = [size for size in file_sizes if size is not None]
                if len(known_sizes) == len(file_sizes):
                    result_size = sum(known_sizes)

            if result_size is None:
                has_unknown_size = True
            else:
                total_size_mb += result_size

        return CopernicusDownloadSummary(
            request_count=request_count,
            file_count=file_count,
            total_size_mb=None if has_unknown_size else total_size_mb,
        )

    @staticmethod
    def format_size(size_mb: float | None) -> str:
        if size_mb is None:
            return "unknown size"
        if size_mb >= 1024:
            return f"{size_mb / 1024:.2f} GB"
        return f"{size_mb:.2f} MB"

    @staticmethod
    def build_copernicus_datasets(
        missions: Iterable[str] | None = None,
        *,
        dataset_type: str = "my",
        version: str = "202411",
    ) -> list[CopernicusDataset]:
        """Build a list of Copernicus Marine dataset configurations."""
        selected_missions = list(missions or MISSION_DATASET_IDS)
        if selected_missions == ["all"]:
            selected_missions = list(MISSION_DATASET_IDS)

        invalid_missions = [
            mission for mission in selected_missions if mission not in MISSION_DATASET_IDS
        ]
        if invalid_missions:
            raise ValueError(
                f"Invalid Copernicus mission(s): {invalid_missions}. "
                f"Expected one or more of: {sorted(MISSION_DATASET_IDS)}"
            )

        datasets: list[CopernicusDataset] = []
        for mission in selected_missions:
            dataset_mission = MISSION_DATASET_IDS[mission]
            datasets.append(
                CopernicusDataset(
                    mission=mission,
                    dataset_mission=dataset_mission,
                    dataset_type=dataset_type,
                    version=version,
                    dataset_id=(
                        f"cmems_obs-sl_glo_phy-ssh_{dataset_type}_"
                        f"{dataset_mission}-l3-duacs_PT1S"
                    ),
                    name=MISSION_NAMES.get(mission, mission),
                )
            )
        return datasets

    @staticmethod
    def build_month_filters(
        start_date: datetime | None = None,
        end_date: datetime | None = None,
    ) -> list[str] | None:
        if start_date is None and end_date is None:
            return None

        if start_date is not None and end_date is not None and end_date < start_date:
            raise ValueError("end_date must be greater than or equal to start_date")

        if start_date is None:
            start_date = end_date
        if end_date is None:
            end_date = start_date
        if start_date is None or end_date is None:
            return None

        filters = []
        year, month = start_date.year, start_date.month
        while (year < end_date.year) or (year == end_date.year and month <= end_date.month):
            filters.append(f"*{year:04d}/{month:02d}/*.nc")
            month += 1
            if month == 13:
                month = 1
                year += 1
        return filters

    def sync_copernicus_along_track_data(
        self,
        *,
        missions: Iterable[str] | None = None,
        output_directory: Path | str | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        dataset_type: str = "my",
        version: str = "202411",
        dry_run: bool = False,
        overwrite: bool = False,
    ) -> list[Any]:
        datasets = OceanDBCopernicusMarine.build_copernicus_datasets(
            missions=missions,
            dataset_type=dataset_type,
            version=version,
        )
        filters = OceanDBCopernicusMarine.build_month_filters(start_date, end_date)
        configured_output_directory = output_directory or self.config.along_track_data_directory
        if not configured_output_directory:
            raise ValueError(
                "No output directory configured. Set ALONG_TRACK_DATA_DIRECTORY "
                "or pass output_directory."
            )

        output_path = Path(configured_output_directory)
        output_path.mkdir(parents=True, exist_ok=True)
        self.login()

        results = []
        for dataset in datasets:
            active_filters = filters or [None]
            for file_filter in active_filters:
                filter_message = f" filter: {file_filter}" if file_filter else ""
                action = "Listing" if dry_run else "Downloading"
                print(
                    f"{action} dataset: {dataset.dataset_id} "
                    f"version: {dataset.version}{filter_message}"
                )
                get_kwargs = {
                    "dataset_id": dataset.dataset_id,
                    "output_directory": output_path,
                    "sync": not overwrite,
                    "dataset_version": dataset.version,
                    "dry_run": dry_run,
                    "overwrite": overwrite,
                }
                if file_filter:
                    get_kwargs["filter"] = file_filter

                get_result = self.client.get(**get_kwargs)
                results.append(get_result)

        return results
