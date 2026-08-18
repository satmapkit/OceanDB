import os
import time
from collections.abc import Callable
from dataclasses import asdict, dataclass
from datetime import datetime
from functools import cached_property
from multiprocessing import Pool, TimeoutError
from pathlib import Path
from typing import Any, Iterator, Optional

import netCDF4 as nc
import numpy as np
from psycopg import sql
from psycopg.rows import dict_row

from OceanDB.etl.base_etl import OceanDBETL, batch
from OceanDB.ocean_data.basins import BasinMask
from OceanDB.ocean_data.ocean_data import ColumnField
from OceanDB.schemas.along_track_schema import (along_track_columns,
                                                along_track_columns_schema)
from OceanDB.utils.date_time_conversion import compute_date_time

EARLIEST_DATE = datetime(1990, 1, 1)
ProgressCallback = Callable[[dict[str, Any]], None]


def _emit(on_progress: ProgressCallback | None, event: dict[str, Any]) -> None:
    if on_progress is not None:
        on_progress(event)


@dataclass
class AlongTrackMetaData:
    """Structured representation of NetCDF global metadata."""

    file_name: str
    mission: Optional[str] = None
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None
    observation_count: Optional[int] = None
    conventions: Optional[str] = None
    metadata_conventions: Optional[str] = None
    cdm_data_type: Optional[str] = None
    comment: Optional[str] = None
    contact: Optional[str] = None
    creator_email: Optional[str] = None
    creator_name: Optional[str] = None
    creator_url: Optional[str] = None
    date_created: Optional[str] = None
    date_issued: Optional[str] = None
    date_modified: Optional[str] = None
    history: Optional[str] = None
    institution: Optional[str] = None
    keywords: Optional[str] = None
    license: Optional[str] = None
    platform: Optional[str] = None
    processing_level: Optional[str] = None
    product_version: Optional[str] = None
    project: Optional[str] = None
    references: Optional[str] = None
    software_version: Optional[str] = None
    source: Optional[str] = None
    ssalto_duacs_comment: Optional[str] = None
    summary: Optional[str] = None
    title: Optional[str] = None

    def to_dict(self):
        return asdict(self)

    @classmethod
    def from_netcdf(
        cls, ds: nc.Dataset, file_name: str, mission: str
    ) -> "AlongTrackMetaData":
        """Create AlongTrackMetaData from a NetCDF4 dataset."""

        if not isinstance(ds, nc.Dataset):
            raise TypeError("AlongTrackMetaData requires a NetCDF Dataset")

        def get(attr: str):
            return getattr(ds, attr, None)

        start_date = None
        end_date = None
        observation_count = None
        if "time" in ds.variables:
            time_var = ds.variables["time"]
            observation_count = len(time_var)
            if observation_count:
                date_times = compute_date_time(time_var, slice(0, observation_count))
                start_date = min(date_times)
                end_date = max(date_times)

        return cls(
            file_name=file_name,
            mission=mission,
            start_date=start_date,
            end_date=end_date,
            observation_count=observation_count,
            conventions=get("Conventions"),
            metadata_conventions=get("Metadata_Conventions"),
            cdm_data_type=get("cdm_data_type"),
            comment=get("comment"),
            contact=get("contact"),
            creator_email=get("creator_email"),
            creator_name=get("creator_name"),
            creator_url=get("creator_url"),
            date_created=get("date_created"),
            date_issued=get("date_issued"),
            date_modified=get("date_modified"),
            history=get("history"),
            institution=get("institution"),
            keywords=get("keywords"),
            license=get("license"),
            platform=get("platform"),
            processing_level=get("processing_level"),
            product_version=get("product_version"),
            project=get("project"),
            references=get("references"),
            software_version=get("software_version"),
            source=get("source"),
            ssalto_duacs_comment=get("ssalto_duacs_comment"),
            summary=get("summary"),
            title=get("title"),
        )


class AlongTrackETL(OceanDBETL):
    ocean_basin_table_name: str = "basin"
    ocean_basins_connections_table_name: str = "basin_connection"
    along_track_table_name: str = "along_track"
    along_track_metadata_table_name: str = "along_track_metadata"

    variable_add_offset: dict = dict()
    missions = [
        "al",
        "alg",
        "c2",
        "c2n",
        "e1g",
        "e1",
        "e2",
        "en",
        "enn",
        "g2",
        "h2a",
        "h2b",
        "j1g",
        "j1",
        "j1n",
        "j2g",
        "j2",
        "j2n",
        "j3",
        "j3n",
        "s3a",
        "s3b",
        "s6a",
        "tp",
        "tpn",
    ]

    @staticmethod
    def _iter_year_months(
        start: datetime | None, end: datetime | None
    ) -> Iterator[tuple[int, int]]:
        start = EARLIEST_DATE if start is None else start
        end = datetime.now() if end is None else end

        year, month = start.year, start.month
        while (year < end.year) or (year == end.year and month <= end.month):
            yield year, month
            month += 1
            if month == 13:
                month = 1
                year += 1

    def discover_files(
        self,
        missions: list[str] | tuple[str, ...],
        start_date: datetime | None = None,
        end_date: datetime | None = None,
    ) -> dict[str, Any]:
        start_date = start_date.replace(tzinfo=None) if start_date is not None else None
        end_date = end_date.replace(tzinfo=None) if end_date is not None else None
        selected_missions = list(missions)

        if not selected_missions or selected_missions == ["all"]:
            selected_missions = list(self.missions)

        invalid_missions = [
            mission for mission in selected_missions if mission not in self.missions
        ]
        if invalid_missions:
            raise ValueError(
                f"received invalid arguments {invalid_missions}. "
                f"Received missions must be from the following list {self.missions}"
            )

        if start_date and end_date and end_date < start_date:
            raise ValueError("end_date must be >= start_date")

        year_months = (
            None
            if start_date is None and end_date is None
            else list(self._iter_year_months(start_date, end_date))
        )
        prefix = "SEALEVEL_GLO_PHY_L3_MY_008_062"
        files: list[Path] = []

        for mission in selected_missions:
            file_structures = [
                f"cmems_obs-sl_glo_phy-ssh_my_{mission}-l3-duacs_PT1S_202411",
                f"cmems_obs-sl_glo_phy-ssh_my_{mission}-lr-l3-duacs_PT1S_202411",
            ]
            for structure in file_structures:
                ingest_directory = (
                    Path(self.config.along_track_data_directory) / prefix / structure
                )
                if not ingest_directory.exists():
                    continue
                if year_months is None:
                    files.extend(ingest_directory.rglob("*.nc"))
                    continue
                for year, month in year_months:
                    month_dir = ingest_directory / f"{year:04d}" / f"{month:02d}"
                    if month_dir.exists():
                        files.extend(month_dir.rglob("*.nc"))

        return {"missions": selected_missions, "files": files}

    def ingest(
        self,
        missions: list[str] | tuple[str, ...],
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        workers: int = 6,
        on_progress: ProgressCallback | None = None,
        init_database_if_not_exists: bool = False,
    ) -> dict[str, Any]:
        from OceanDB.OceanDB_Initializer import OceanDBInit

        ocean_db_init = OceanDBInit(config=self.config)
        database_initialized = (
            ocean_db_init.database_exists()
            and ocean_db_init.table_exists(self.along_track_table_name)
        )

        if not database_initialized:
            if not init_database_if_not_exists:
                raise RuntimeError(
                    f"Database '{ocean_db_init.db_name}' is not initialized. "
                    "Run database initialization first or set "
                    "init_database_if_not_exists=True."
                )
            ocean_db_init.initialize_database()

        discovery = self.discover_files(missions, start_date, end_date)
        nc_files: list[Path] = discovery["files"]
        if not nc_files:
            return {
                "missions": discovery["missions"],
                "matched_count": 0,
                "skipped_count": 0,
                "ingested_count": 0,
                "results": [],
                "duration_seconds": 0.0,
            }

        metadata_filenames = self.query_metadata()
        along_track_files = [
            file for file in nc_files if file.name not in metadata_filenames
        ]
        skipped_count = len(nc_files) - len(along_track_files)
        if not along_track_files:
            return {
                "missions": discovery["missions"],
                "matched_count": len(nc_files),
                "skipped_count": skipped_count,
                "ingested_count": 0,
                "results": [],
                "duration_seconds": 0.0,
            }

        _emit(
            on_progress,
            {
                "type": "along_track_start",
                "matched_count": len(nc_files),
                "skipped_count": skipped_count,
                "ingest_count": len(along_track_files),
                "ingest_mode": self.config.ingest_mode,
            },
        )

        start_ingest_time = time.perf_counter()
        completed = skipped_count
        total = len(nc_files)
        results_out: list[dict[str, Any]] = []
        multiprocessing_pool = Pool(workers)
        results = multiprocessing_pool.imap_unordered(
            self.process_along_track_file,
            along_track_files,
        )

        try:
            heartbeat_seconds = 30
            completed_new_files = 0
            total_new_files = len(along_track_files)

            while completed_new_files < total_new_files:
                try:
                    result = results.next(timeout=heartbeat_seconds)
                except TimeoutError:
                    _emit(
                        on_progress,
                        {
                            "type": "along_track_wait",
                            "completed": completed,
                            "total": total,
                            "completed_new_files": completed_new_files,
                            "total_new_files": total_new_files,
                            "active_workers": min(
                                workers, total_new_files - completed_new_files
                            ),
                        },
                    )
                    continue

                completed += 1
                completed_new_files += 1
                results_out.append(result)
                _emit(
                    on_progress,
                    {
                        "type": "along_track_file_complete",
                        "completed": completed,
                        "total": total,
                        "remaining": total - completed,
                        "result": result,
                    },
                )
        finally:
            multiprocessing_pool.close()
            multiprocessing_pool.join()

        duration_seconds = time.perf_counter() - start_ingest_time
        return {
            "missions": discovery["missions"],
            "matched_count": len(nc_files),
            "skipped_count": skipped_count,
            "ingested_count": len(along_track_files),
            "results": results_out,
            "duration_seconds": duration_seconds,
        }

    @cached_property
    def basin_mask_lookup(self) -> BasinMask:
        return BasinMask()

    @staticmethod
    def _extract_mission_from_filename(file: Path) -> str:
        file_parts = file.name.split("_")
        if len(file_parts) < 3:
            raise ValueError(f"Could not parse mission from file name: {file.name}")

        mission = file_parts[2]
        return mission

    def extract_dataset_metadata(
        self, ds: nc.Dataset, file: Path
    ) -> AlongTrackMetaData:
        mission = self._extract_mission_from_filename(file)
        return AlongTrackMetaData.from_netcdf(ds, file_name=file.name, mission=mission)

    @staticmethod
    def _coerce_smallint(value: Any) -> int | None:
        if np.ma.is_masked(value):
            return None

        if isinstance(value, np.generic):
            value = value.item()

        if value is None:
            return None

        if isinstance(value, float):
            if np.isnan(value):
                return None
            if not value.is_integer():
                raise ValueError(f"Expected integer-compatible value, received {value}")

        return int(value)

    @classmethod
    def _copy_value(cls, field: ColumnField, value: Any) -> Any:
        if field.postgres_type == "smallint":
            return cls._coerce_smallint(value)

        if np.ma.is_masked(value):
            return None

        if isinstance(value, np.generic):
            value = value.item()

        if value is None:
            return None

        if isinstance(value, float) and np.isnan(value):
            return None

        return value

    def extract_data_from_netcdf(
        self,
        ds: nc.Dataset,
        batch_size: int,
        file: Path,
    ) -> Iterator[batch[along_track_columns]]:
        """
        Parse & transform NetCDF file
        """

        # Mask, but don't scale
        ds.set_auto_mask(True)
        ds.set_auto_maskandscale(False)

        # pull out mission
        mission = self._extract_mission_from_filename(file)
        if mission not in self.missions:
            raise ValueError(f"Unsupported mission '{mission}' in file: {file.name}")

        # extract fields that are both expected and in the dataset
        derived_fields = {"file_name", "mission", "basin_id"}
        required_fields: list[tuple[along_track_columns, ColumnField]] = [
            (name, field)
            for name, field in along_track_columns_schema.items()
            if name not in derived_fields
        ]

        self.verify_netcdf_schema_scaling(
            ds=ds,
            schema={name: field for name, field in required_fields},
            context=f"[ALONG-TRACK] file={file.name}",
        )

        missing_variables = [
            field.netcdf_name
            for _, field in required_fields
            if field.netcdf_name not in ds.variables
        ]
        if missing_variables:
            missing = ", ".join(sorted(missing_variables))
            raise ValueError(f"Missing required variables in {file.name}: {missing}")

        fields_in_ds: list[tuple[along_track_columns, ColumnField]] = [
            (name, field)
            for name, field in required_fields
            if field.netcdf_name in ds.variables
        ]

        # certain fields have to be scaled
        # TODO:
        ds["latitude"].set_auto_scale(True)
        ds["longitude"].set_auto_scale(True)

        n_total = len(ds["latitude"])
        for start in range(0, n_total, batch_size):
            stop = min(start + batch_size, n_total)
            vars_slice: dict[along_track_columns, Any] = {
                name: field.from_netcdf(ds, slice(start, stop))
                for name, field in fields_in_ds
            }

            # file_name, mission, and basin_id are part of the DB schema but derived here
            # rather than read directly from the NetCDF dataset.
            vars_slice["file_name"] = [file.name] * len(vars_slice["latitude"])
            vars_slice["mission"] = [mission] * len(vars_slice["latitude"])
            latitude = vars_slice["latitude"]
            longitude = vars_slice["longitude"]
            vars_slice["basin_id"] = self.basin_mask_lookup.lookup(latitude, longitude)

            yield [
                {name: values[i] for name, values in vars_slice.items()}
                for i in range(stop - start)
            ]

    def import_along_track_data_to_postgresql(
        self, along_track_data: batch[along_track_columns]
    ):
        """
        Insert along-track rows into Postgres using the configured ingest mode.
        """
        self.import_schema_rows_to_postgresql(
            table_name=self.along_track_table_name,
            schema=along_track_columns_schema,
            data=along_track_data,
            value_adapter=self._copy_value,
            ignore_conflicts=False,
        )

    def import_metadata_to_psql(self, metadata: AlongTrackMetaData) -> None:
        """Insert metadata into along_track_metadata table, ignoring duplicates."""
        fields = [
            "file_name",
            "mission",
            "start_date",
            "end_date",
            "observation_count",
            "conventions",
            "metadata_conventions",
            "cdm_data_type",
            "comment",
            "contact",
            "creator_email",
            "creator_name",
            "creator_url",
            "date_created",
            "date_issued",
            "date_modified",
            "history",
            "institution",
            "keywords",
            "license",
            "platform",
            "processing_level",
            "product_version",
            "project",
            "references",  # reserved keyword — will be safely quoted
            "software_version",
            "source",
            "ssalto_duacs_comment",
            "summary",
            "title",
        ]

        query = sql.SQL("""
            INSERT INTO {table} ({fields})
            VALUES ({placeholders})
            ON CONFLICT (file_name) DO NOTHING;
        """).format(
            table=sql.Identifier(self.along_track_metadata_table_name),
            fields=sql.SQL(", ").join(sql.Identifier(f) for f in fields),
            placeholders=sql.SQL(", ").join(sql.Placeholder() * len(fields)),
        )

        with self.cursor(commit=True) as cur:
            cur.execute(query, tuple(metadata.__dict__.values()))

    def query_metadata(self):
        query = "SELECT file_name FROM along_track_metadata;"
        with self.cursor(row_factory=dict_row) as cur:
            cur.execute(query)
            rows = cur.fetchall()
        return set([metadata["file_name"] for metadata in rows])

    def summarize_ingested_missions(self) -> list[dict[str, Any]]:
        query = """
            SELECT
                mission,
                MIN(start_date) AS start_date,
                MAX(end_date) AS end_date,
                COALESCE(SUM(observation_count), 0) AS observation_count,
                COUNT(*) AS file_count
            FROM along_track_metadata
            WHERE mission IS NOT NULL
            GROUP BY mission
            ORDER BY mission
        """
        with self.cursor(row_factory=dict_row) as cur:
            cur.execute(query)
            return list(cur.fetchall())

    def process_along_track_file(self, file: Path, batch_size: int = 500000):
        """
        Processes an along track netcdf file & inserts into Postgres
        """
        start = time.perf_counter()
        worker_pid = os.getpid()
        self.debug_log(
            (
                f"[ALONG-TRACK] pid={worker_pid} starting file={file.name} "
                f"batch_size={batch_size} mode={self.config.ingest_mode}"
            )
        )
        dataset: nc.Dataset = self.load_netcdf(file)
        along_track_metadata: AlongTrackMetaData = self.extract_dataset_metadata(
            ds=dataset, file=file
        )
        self.debug_log(
            (
                f"[ALONG-TRACK] pid={worker_pid} parsed metadata file={file.name} "
                f"mission={along_track_metadata.mission} "
                f"observations={along_track_metadata.observation_count}"
            )
        )
        batch_count = 0
        row_count = 0
        for data_batch in self.extract_data_from_netcdf(
            ds=dataset, file=file, batch_size=batch_size
        ):
            batch_count += 1
            row_count += len(data_batch)
            self.debug_log(
                (
                    f"[ALONG-TRACK] pid={worker_pid} inserting file={file.name} "
                    f"batch={batch_count} rows={len(data_batch)} "
                    f"cumulative_rows={row_count}"
                )
            )
            self.import_along_track_data_to_postgresql(data_batch)
            self.debug_log(
                (
                    f"[ALONG-TRACK] pid={worker_pid} inserted file={file.name} "
                    f"batch={batch_count}"
                )
            )
        self.debug_log(
            f"[ALONG-TRACK] pid={worker_pid} writing metadata file={file.name}"
        )
        self.import_metadata_to_psql(metadata=along_track_metadata)
        duration = time.perf_counter() - start
        size_mb = file.stat().st_size / (1024 * 1024)
        self.debug_log(
            (
                f"[ALONG-TRACK] pid={worker_pid} finished file={file.name} "
                f"batches={batch_count} rows={row_count} "
                f"duration={duration:.2f}s"
            )
        )
        return {
            "file_name": file.name,
            "size_mb": size_mb,
            "duration_seconds": duration,
        }
