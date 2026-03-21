from datetime import datetime, timedelta
from typing import Literal, get_args, Any

from OceanDB.data_access.base_query import BaseReadQuery, QuerySpec
from OceanDB.ocean_data.dataset import Dataset
from OceanDB.schemas.along_track_schema import along_track_fields, along_track_schema


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


class AlongTrack(BaseReadQuery):
    all_missions = list(get_args(Mission))

    # Domain key used by BaseQuery metadata registry
    # ALONG_TRACK_DOMAIN = "along_track"

    _along_track_nearest_neighbor_query = (
        "queries/along_track/geographic_nearest_neighbor.sql"
    )
    _along_track_spatiotemporal_query = (
        "queries/along_track/geographic_points_in_spatialtemporal_window.sql"
    )

    _projected_spatio_temporal_query_mask = "queries/along_track/geographic_points_in_spatialtemporal_projected_window_nomask.sql"
    _projected_spatio_temporal_query_no_mask = (
        "queries/along_track/geographic_points_in_spatialtemporal_window.sql"
    )

    def geographic_point_in_r_dt(
        self,
        fields: list[along_track_fields],
        latitude: float,
        longitude: float,
        date: datetime,
        radius: float = 500_000.0,
        time_window: timedelta = timedelta(days=10),
        missions: list[Mission] = all_missions,
    ) -> Dataset[along_track_fields, Any] | None:
        """
        Query along-track points within spatial + temporal windows.

        Yields one Dataset, or None if empty.

        :param fields:
            List of requested along track fields to return

        :param latitude:
            Central latitude of the window

        :param longitude:
            Central longitude of the window

        :param date:
            Central date of the window

        :param radius:
            Radius of the spatial window in meters

        :param time_window:
            Radius of the temporal window
            (meaning any point from [date - time_window, date + time_window]
            could be include)

        :param missions:
            List of satellite missions to include in the query
            (defaults to all available, see :attr:`AlongTrack.all_missions`)

        :return:
            If no points found in the window, :code:`None` is returned.
            If points are found, then a :class:`Dataset <OceanDB.ocean_data.dataset.Dataset>`
            of requested fields is returned.
        """

        query_spec = QuerySpec(
            sql_template=self.load_sql_file(self._along_track_spatiotemporal_query),
            schema=along_track_schema,
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

        return self.execute_read_query(
            query_spec=query_spec,
            fields=fields,
            params=params,
            dataset_name="along_track",
        )

    def geographic_nearest_neighbors(
        self,
        fields: list,
        latitude: float,
        longitude: float,
        date: datetime,
        time_window: timedelta = timedelta(days=10),
        missions: list[Mission] = all_missions,
    ) -> Dataset[along_track_fields, Any] | None:
        """
        Query along-track points within spatial + temporal windows.

        Yields one Dataset per query point, or None if empty.
        """

        query_spec = QuerySpec(
            sql_template=self.load_sql_file(self._along_track_nearest_neighbor_query),
            schema=along_track_schema,
            mandatory_fields=["distance"],
        )

        basin_ids = self.basin_mask(latitude, longitude)
        connected_basin_ids = self.basin_connection_map[basin_ids]

        params = {
            "longitude": longitude,
            "latitude": latitude,
            "central_date_time": date,
            "time_delta": time_window,
            "connected_basin_ids": connected_basin_ids,
            "missions": missions,
        }

        return self.execute_read_query(
            query_spec=query_spec,
            fields=fields,
            params=params,
            dataset_name="along_track",
        )
