from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable

from OceanDB.along_track_missions import MISSION_DEFINITIONS
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
        version: str | None = None,
    ) -> list[CopernicusDataset]:
        """Build Copernicus dataset configurations from the MY mission registry.

        :param missions: Mission identities, or None/empty/``all`` for all missions.
        :param dataset_type: Dataset type component of the dataset ID.
        :param version: Release override for every mission; None uses each mission's
            catalogue default (202506 for j3g, 202411 for the other missions).
        :returns: Configurations with resolved dataset IDs and release versions.
        :raises ValueError: If any requested mission is unknown.
        """
        selected_missions = list(missions or MISSION_DEFINITIONS)
        if selected_missions == ["all"]:
            selected_missions = list(MISSION_DEFINITIONS.keys())

        invalid_missions = [
            mission
            for mission in selected_missions
            if mission not in MISSION_DEFINITIONS
        ]
        if invalid_missions:
            raise ValueError(
                f"Invalid Copernicus mission(s): {invalid_missions}. "
                f"Expected one or more of: {sorted(MISSION_DEFINITIONS.keys())}"
            )

        datasets: list[CopernicusDataset] = []
        for mission in selected_missions:
            definition = MISSION_DEFINITIONS[mission]
            datasets.append(
                CopernicusDataset(
                    mission=mission,
                    dataset_mission=definition.shortname,
                    dataset_type=dataset_type,
                    version=definition.default_version if version is None else version,
                    dataset_id=(
                        f"cmems_obs-sl_glo_phy-ssh_{dataset_type}_"
                        f"{definition.shortname}-l3-duacs_PT1S{definition.dataset_suffix}"
                    ),
                    name=definition.longname,
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
        while (year < end_date.year) or (
            year == end_date.year and month <= end_date.month
        ):
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
        version: str | None = None,
        dry_run: bool = False,
        overwrite: bool = False,
    ) -> list[Any]:
        datasets = OceanDBCopernicusMarine.build_copernicus_datasets(
            missions=missions,
            dataset_type=dataset_type,
            version=version,
        )
        filters = OceanDBCopernicusMarine.build_month_filters(start_date, end_date)
        configured_output_directory = (
            output_directory or self.config.along_track_data_directory
        )
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
            active_filters: Iterable[str | None] = filters or [None]
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
