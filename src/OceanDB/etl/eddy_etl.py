from dataclasses import dataclass
import netCDF4 as nc
import psycopg as pg
from psycopg import sql
import time
import numpy as np
from typing import Iterator
from datetime import datetime, timedelta, timezone
from pathlib import Path

from OceanDB.etl import BaseETL

NDArray = np.ndarray


@dataclass
class EddyData:
    """Structured container for detected eddy observations."""

    amplitude: NDArray
    cost_association: NDArray
    effective_area: NDArray
    effective_contour_height: NDArray
    effective_contour_latitude: NDArray
    effective_contour_longitude: NDArray
    effective_contour_shape_error: NDArray
    effective_radius: NDArray
    inner_contour_height: NDArray
    latitude: NDArray
    latitude_max: NDArray
    longitude: NDArray
    longitude_max: NDArray
    num_contours: NDArray
    num_point_e: NDArray
    num_point_s: NDArray
    observation_flag: NDArray  # will normalize to bool
    observation_number: NDArray
    speed_area: NDArray
    speed_average: NDArray
    speed_contour_height: NDArray
    speed_contour_latitude: NDArray
    speed_contour_longitude: NDArray
    speed_contour_shape_error: NDArray
    speed_radius: NDArray
    date_time: NDArray
    track: NDArray

    def __post_init__(self) -> None:
        """Normalize and validate eddy data arrays."""
        if self.observation_flag.dtype != bool:
            self.observation_flag = self.observation_flag.astype(bool)

        # Basic shape validation (cheap, very effective)
        n = len(self.latitude)
        for name, value in vars(self).items():
            if len(value) != n:
                raise ValueError(
                    f"EddyData field '{name}' has length {len(value)} != {n}"
                )


class EddyETL(BaseETL):
    def __init__(self):
        super().__init__()

    def ingest_eddy_data_file(self, file: Path, cyclonic_type):
        """
        Processes & Ingests Eddy Data NetCDF file
        """
        dataset = self.load_netcdf(file)
        for eddy_data in self.extract_eddy_data_batches_from_netcdf(
            dataset, batch_size=500000
        ):
            start = time.perf_counter()
            self.import_eddy_data_to_postgresql(
                eddy_data=eddy_data, cyclonic_type=cyclonic_type
            )
            duration = time.perf_counter() - start
            print(f"✅ Ingested Eddy Data Points took {duration:.2f} seconds")

    def extract_eddy_data_batches_from_netcdf(
        self,
        ds: nc.Dataset,
        batch_size: int,
    ) -> Iterator[EddyData]:
        """
        Yield batches of eddy data from a NetCDF dataset.

        This function assumes the eddy time variable is stored as
        Unix seconds (uint32), despite metadata claiming
        'days since 1950-01-01'.

        Parameters
        ----------
        ds : netCDF4.Dataset
            Open NetCDF dataset
        batch_size : int
            Number of points per batch

        Yields
        ------
        dict
            Dictionary of arrays for one batch
        """
        ds.set_auto_mask(True)
        ds.set_auto_maskandscale(False)

        obs_var = ds.variables["observation_number"]
        n_total = obs_var.shape[0]

        time_var = ds.variables["time"]

        # Unix epoch (correct for this dataset)
        epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)

        for start in range(0, n_total, batch_size):
            stop = min(start + batch_size, n_total)
            sl = slice(start, stop)

            # ---- Time parsing (critical fix) ----
            raw_time = time_var[sl].astype("int64")
            date_time = [epoch + timedelta(seconds=int(t)) for t in raw_time]

            yield EddyData(
                amplitude=ds.variables["amplitude"][sl],
                cost_association=ds.variables["cost_association"][sl],
                effective_area=ds.variables["effective_area"][sl],
                effective_contour_height=ds.variables["effective_contour_height"][sl],
                effective_contour_latitude=ds.variables["effective_contour_latitude"][
                    sl
                ],
                effective_contour_longitude=ds.variables["effective_contour_longitude"][
                    sl
                ],
                effective_contour_shape_error=ds.variables[
                    "effective_contour_shape_error"
                ][sl],
                effective_radius=ds.variables["effective_radius"][sl],
                inner_contour_height=ds.variables["inner_contour_height"][sl],
                latitude=ds.variables["latitude"][sl],
                latitude_max=ds.variables["latitude_max"][sl],
                longitude=ds.variables["longitude"][sl],
                longitude_max=ds.variables["longitude_max"][sl],
                num_contours=ds.variables["num_contours"][sl],
                num_point_e=ds.variables["num_point_e"][sl],
                num_point_s=ds.variables["num_point_s"][sl],
                observation_flag=ds.variables["observation_flag"][sl],
                observation_number=ds.variables["observation_number"][sl],
                speed_area=ds.variables["speed_area"][sl],
                speed_average=ds.variables["speed_average"][sl],
                speed_contour_height=ds.variables["speed_contour_height"][sl],
                speed_contour_latitude=ds.variables["speed_contour_latitude"][sl],
                speed_contour_longitude=ds.variables["speed_contour_longitude"][sl],
                speed_contour_shape_error=ds.variables["speed_contour_shape_error"][sl],
                speed_radius=ds.variables["speed_radius"][sl],
                date_time=date_time,
                track=ds.variables["track"][sl],
            )

    def import_eddy_data_to_postgresql(self, eddy_data: EddyData, cyclonic_type: int):
        """
        Insert eddy records into PostgreSQL using INSERT statements.

        COPY implicitly casts 0/1 -> boolean
        INSERT Strict typing - smallint != boolean

        """

        COLUMNS = [
            "amplitude",
            "cost_association",
            "effective_area",
            "effective_contour_height",
            "effective_contour_shape_error",
            "effective_radius",
            "inner_contour_height",
            "latitude",
            "latitude_max",
            "longitude",
            "longitude_max",
            "num_contours",
            "num_point_e",
            "num_point_s",
            "observation_flag",
            "observation_number",
            "speed_area",
            "speed_average",
            "speed_contour_height",
            "speed_contour_shape_error",
            "speed_radius",
            "date_time",
            "track",
            "cyclonic_type",
        ]

        BOOLEAN_COLUMNS = {"observation_flag"}

        def normalize_value(val, column: str | None = None):
            """Convert numpy scalars to native, schema-correct Python types."""
            if val is None:
                return None

            # numpy → python
            if hasattr(val, "item"):
                val = val.item()

            # normalize booleans
            if column in BOOLEAN_COLUMNS:
                return bool(val)

            return val

        insert_query = sql.SQL(
            """
               INSERT INTO {} ({})
               VALUES ({})
           """
        ).format(
            sql.Identifier("public", "eddy"),
            sql.SQL(", ").join(map(sql.Identifier, COLUMNS)),
            sql.SQL(", ").join(sql.Placeholder() * len(COLUMNS)),
        )

        n_observations = len(eddy_data.observation_number)

        rows = []
        for i in range(n_observations):
            rows.append(
                [
                    normalize_value(eddy_data.amplitude[i]),
                    normalize_value(eddy_data.cost_association[i]),
                    normalize_value(eddy_data.effective_area[i]),
                    normalize_value(eddy_data.effective_contour_height[i]),
                    normalize_value(eddy_data.effective_contour_shape_error[i]),
                    normalize_value(eddy_data.effective_radius[i]),
                    normalize_value(eddy_data.inner_contour_height[i]),
                    normalize_value(eddy_data.latitude[i]),
                    normalize_value(eddy_data.latitude_max[i]),
                    normalize_value(eddy_data.longitude[i]),
                    normalize_value(eddy_data.longitude_max[i]),
                    normalize_value(eddy_data.num_contours[i]),
                    normalize_value(eddy_data.num_point_e[i]),
                    normalize_value(eddy_data.num_point_s[i]),
                    bool(
                        eddy_data.observation_flag[i]
                    ),  # already normalized, but explicit is OK
                    normalize_value(eddy_data.observation_number[i]),
                    normalize_value(eddy_data.speed_area[i]),
                    normalize_value(eddy_data.speed_average[i]),
                    normalize_value(eddy_data.speed_contour_height[i]),
                    normalize_value(eddy_data.speed_contour_shape_error[i]),
                    normalize_value(eddy_data.speed_radius[i]),
                    normalize_value(eddy_data.date_time[i]),
                    normalize_value(eddy_data.track[i]),
                    cyclonic_type,
                ]
            )

        try:
            with pg.connect(self.config.postgres_dsn) as conn:
                with conn.cursor() as cur:
                    cur.executemany(insert_query, rows)
        except Exception as e:
            print("INSERT FAILED:", e)
            raise
