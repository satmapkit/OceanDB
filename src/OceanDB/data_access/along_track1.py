"""
Projection-Aware Dataset Architecture

"""

from datetime import datetime, timedelta
from typing import Iterable, List, Literal, get_args
import numpy.typing as npt
import numpy as np

from OceanDB.data_access.base_query import BaseQuery
from OceanDB.data_access.projection_compiler import ProjectionCompiler
# from OceanDB.data_access.schema.along_track_schema import along_track_fields, along_track_schema
from OceanDB.ocean_data.dataset import Dataset


class AlongTrack(BaseQuery):
    """
    Query/Service object

    Runs queries, returns datasets and bundles them into OceanData

    Query service for along-track altimetry data.

    Executes parameterized geospatial and spatiotemporal SQL queries and
    returns domain-level AlongTrackDataset objects instead of raw rows.
    """

    Mission = Literal[
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
    all_missions = list(get_args(Mission))

    # Domain key used by BaseQuery metadata registry
    # ALONG_TRACK_DOMAIN = "along_track"

    along_track_nearest_neighbor_query = "queries/along_track/geographic_nearest_neighbor.sql"
    along_track_spatiotemporal_query = (
        "queries/along_track/geographic_points_in_spatialtemporal_window.sql"
    )

    projected_spatio_temporal_query_mask = "queries/along_track/geographic_points_in_spatialtemporal_projected_window_nomask.sql"
    projected_spatio_temporal_query_no_mask = (
        "queries/along_track/geographic_points_in_spatialtemporal_window.sql"
    )

    def __init__(self):
        super().__init__()

    def geographic_points_in_r_dt(
        self,
        fields: list[along_track_fields],
        latitudes: npt.NDArray,
        longitudes: npt.NDArray,
        dates: List[datetime],
        radii: List[float] | float = 500_000.0,
        time_window: timedelta = timedelta(days=10),
        missions: list[Mission] = all_missions,
    ) -> Iterable[Dataset[along_track_fields, npt.NDArray[np.floating]] | None]:
        """
        Query along-track points within spatial + temporal windows.

        Yields one AlongTrackDataset per query point, or None if empty.
        """

        # format what parameters we want out of the query
        query_string = self.load_sql_file(self.along_track_spatiotemporal_query)

        compiler = ProjectionCompiler(schema=along_track_schema)

        query_string = compiler.compile(
            sql_template=query_string,
            fields=fields,
        )

        if not isinstance(radii, list):
            radii = [float(radii)] * len(latitudes)

        # connected basins
        basin_ids = self.basin_mask(latitudes, longitudes)
        connected_basin_ids = list(map(self.basin_connection_map.get, basin_ids))

        # format params
        params = [
            {
                "longitude": lon,
                "latitude": lat,
                "distance": r,
                "central_date_time": dt,
                "time_delta": time_window,
                "connected_basin_ids": basins,
                "missions": missions,
            }
            for lat, lon, dt, basins, r in zip(
                latitudes, longitudes, dates, connected_basin_ids, radii
            )
        ]
        # execute the query
        return self.execute_batch_query(
            query=query_string,
            schema=along_track_schema,
            params=params,
            dataset_name="along_track"
        )

    def geographic_nearest_neighbors_dt(
        self,
        latitudes: npt.NDArray[np.floating],
        longitudes: npt.NDArray[np.floating],
        dates: List[datetime],
        fields: list[along_track_fields],
        time_window=timedelta(seconds=856710),
        missions: list[Mission] = all_missions,
    ) -> Iterable[Dataset[along_track_fields, npt.NDArray[np.floating]] | None]:
        """
        Given an array of spatiotemporal points, returns the THREE closest data points to each
        """
        query_string = self.load_sql_file(self.along_track_nearest_neighbor_query)
        compiler = ProjectionCompiler(schema=along_track_schema)

        query_string = compiler.compile(
            sql_template=query_string,
            fields=fields,
        )
        print(query_string)
        basin_ids = self.basin_mask(latitudes, longitudes)
        connected_basin_ids = list(map(self.basin_connection_map.get, basin_ids))
        params = [
            {
                "latitude": latitude,
                "longitude": longitude,
                "central_date_time": date,
                "connected_basin_ids": connected_basin_ids,
                "time_delta": str(time_window / 2),
                "missions": missions,
            }
            for latitude, longitude, date, connected_basin_ids in zip(
                latitudes, longitudes, dates, connected_basin_ids
            )
        ]

        return self.execute_batch_query(
            query=query_string,
            schema=along_track_schema,
            params=params,
            dataset_name="along_track"
        )
