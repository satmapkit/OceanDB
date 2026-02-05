from datetime import datetime, timedelta
from typing import Iterable, List, Literal, get_args, Any
import numpy.typing as npt
import numpy as np

from OceanDB.data_access.base_query import BaseQuery, QuerySpec
from OceanDB.ocean_data.dataset import Dataset
from OceanDB.schemas.along_track_schema import along_track_fields, along_track_schema


class AlongTrack(BaseQuery):
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

    def geographic_point_in_r_dt(
            self,
            fields: list,
            latitude: float,
            longitude: float,
            date: datetime,
            radius: float = 500_000.0,
            time_window: timedelta = timedelta(days=10),
            missions: list[Mission] = all_missions,
    ) -> Dataset[along_track_fields, Any] | None:
        """
        Query along-track points within spatial + temporal windows.

        Yields one Dataset per query point, or None if empty.
        """

        query_spec = QuerySpec(
            sql_template=self.load_sql_file(self.along_track_spatiotemporal_query),
            schema=along_track_schema
        )

        basin_ids = self.basin_mask(latitude, longitude)
        connected_basin_ids = self.basin_connection_map[basin_ids]

        params = {
                "longitude": longitude,
                "latitude": latitude,
                "distance": radius,
                "central_date_time": date,
                "time_delta": time_window,
                "connected_basin_ids": connected_basin_ids,
                "missions": missions,
            }


        return self.execute_query(
            query_spec=query_spec,
            fields=fields,
            params=params,
            dataset_name="along_track",
        )
