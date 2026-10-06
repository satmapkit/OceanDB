from datetime import datetime, timedelta
from functools import cached_property
from typing import Any, Generator, Literal

from OceanDB.data_access.base_query import BaseReadQuery, QuerySpec
from OceanDB.data_access.query_plan import BasinPlanner, QueryPlanner
from OceanDB.ocean_data.basins import BasinConnections, BasinMask
from OceanDB.ocean_data.dataset import Dataset
from OceanDB.schemas.along_track_schema import (along_track_fields,
                                                along_track_schema)

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
    # Domain key used by BaseQuery metadata registry
    # ALONG_TRACK_DOMAIN = "along_track"

    _along_track_nearest_neighbor_query = (
        "queries/along_track/geographic_nearest_neighbor.sql"
    )
    _along_track_nearest_neighbor_max_radius_query = (
        "queries/along_track/geographic_nearest_neighbor_max_radius.sql"
    )
    _along_track_spatiotemporal_query = (
        "queries/along_track/geographic_points_in_spatialtemporal_window.sql"
    )
    _along_track_nearest_neighbor_without_mission_query = (
        "queries/along_track/geographic_nearest_neighbor_without_mission.sql"
    )
    _along_track_nearest_neighbor_without_mission_max_radius_query = (
        "queries/along_track/"
        "geographic_nearest_neighbor_without_mission_max_radius.sql"
    )
    _along_track_spatiotemporal_without_mission_query = (
        "queries/along_track/"
        "geographic_points_in_spatialtemporal_window_without_mission.sql"
    )

    _projected_spatio_temporal_query_mask = "queries/along_track/geographic_points_in_spatialtemporal_projected_window_nomask.sql"
    _projected_spatio_temporal_query_no_mask = (
        "queries/along_track/geographic_points_in_spatialtemporal_window.sql"
    )

    @cached_property
    def basin_mask_lookup(self) -> BasinMask:
        return BasinMask()

    @cached_property
    def basin_connections(self) -> BasinConnections:
        return BasinConnections(config=self.config)

    def geographic_point_in_r_dt(
        self,
        fields: list[along_track_fields],
        latitude: float,
        longitude: float,
        date: datetime,
        radius: float = 500_000.0,
        time_window: timedelta = timedelta(days=10),
        missions: list[Mission] | None = None,
        planner: QueryPlanner = BasinPlanner(1, 16),
    ) -> Dataset[along_track_fields] | None:
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
            List of satellite missions to include in the query.
            If 'None' (default), uses all missions.

        :return:
            If no points found in the window, :code:`None` is returned.
            If points are found, then a :class:`Dataset <OceanDB.ocean_data.dataset.Dataset>`
            of requested fields is returned.
        """

        return next(
            self.geographic_point_in_r_dt_batch(
                fields=fields,
                latitudes=[latitude],
                longitudes=[longitude],
                dates=[date],
                radius=radius,
                time_window=time_window,
                missions=missions,
                planner=planner,
            )
        )

    def geographic_point_in_r_dt_batch(
        self,
        fields: list[along_track_fields],
        latitudes: list[float],
        longitudes: list[float],
        dates: list[datetime],
        radius: float = 500_000.0,
        time_window: timedelta = timedelta(days=10),
        missions: list[Mission] | None = None,
        planner: QueryPlanner = BasinPlanner(1, 16),
    ) -> Generator[Dataset[along_track_fields] | None, None, None]:
        """
        Query along-track points for multiple spatial + temporal windows.

        Yields one Dataset per query point, or None where no rows are returned.
        """

        results: list[Dataset[along_track_fields] | None] = [None] * len(latitudes)
        for index, result in self.geographic_point_in_r_dt_stream(
            fields=fields,
            latitudes=latitudes,
            longitudes=longitudes,
            dates=dates,
            radius=radius,
            time_window=time_window,
            missions=missions,
            planner=planner,
        ):
            results[index] = result
        yield from results

    def geographic_point_in_r_dt_stream(
        self,
        fields: list[along_track_fields],
        latitudes: list[float],
        longitudes: list[float],
        dates: list[datetime],
        radius: float = 500_000.0,
        time_window: timedelta = timedelta(days=10),
        missions: list[Mission] | None = None,
        planner: QueryPlanner = BasinPlanner(4, 16),
    ) -> Generator[tuple[int, Dataset[along_track_fields] | None], None, None]:
        """Stream indexed spatial-temporal query results as workers finish."""
        query_spec, params_batch, basin_ids = self._build_basin_query_batch(
            latitudes=latitudes,
            longitudes=longitudes,
            dates=dates,
            time_window=time_window,
            missions=missions,
            sql_with_missions=self._along_track_spatiotemporal_query,
            sql_without_missions=self._along_track_spatiotemporal_without_mission_query,
            extra_params={"distance": radius},
        )
        return self.execute_batch_read_query_stream(
            query_spec=query_spec,
            fields=fields,
            params_batch=params_batch,
            plan=planner.plan(params_batch=params_batch, basin_ids=basin_ids),
            dataset_name="along_track",
        )

    def geographic_nearest_neighbors(
        self,
        fields: list,
        latitude: float,
        longitude: float,
        date: datetime,
        time_window: timedelta = timedelta(days=10),
        missions: list[Mission] | None = None,
        max_radius: float | None = 500_000,
        planner: QueryPlanner = BasinPlanner(1, 16),
    ) -> Dataset[along_track_fields] | None:
        """
        Query along-track points within spatial + temporal windows.

        Yields one Dataset per query point, or None if empty.
        """

        return next(
            self.geographic_nearest_neighbors_batch(
                fields=fields,
                latitudes=[latitude],
                longitudes=[longitude],
                dates=[date],
                time_window=time_window,
                missions=missions,
                max_radius=max_radius,
                planner=planner,
            )
        )

    def geographic_nearest_neighbors_batch(
        self,
        fields: list[along_track_fields],
        latitudes: list[float],
        longitudes: list[float],
        dates: list[datetime],
        time_window: timedelta = timedelta(days=10),
        missions: list[Mission] | None = None,
        max_radius: float | None = 500_000,
        planner: QueryPlanner = BasinPlanner(1, 16),
    ) -> Generator[Dataset[along_track_fields] | None, None, None]:
        """
        Query nearest neighbors for multiple points using a prepared batch query.

        Yields one Dataset per query point, or None where no rows are returned.
        """

        results: list[Dataset[along_track_fields] | None] = [None] * len(latitudes)
        for index, result in self.geographic_nearest_neighbors_stream(
            fields=fields,
            latitudes=latitudes,
            longitudes=longitudes,
            dates=dates,
            time_window=time_window,
            missions=missions,
            max_radius=max_radius,
            planner=planner,
        ):
            results[index] = result
        yield from results

    def geographic_nearest_neighbors_stream(
        self,
        fields: list[along_track_fields],
        latitudes: list[float],
        longitudes: list[float],
        dates: list[datetime],
        time_window: timedelta = timedelta(days=10),
        missions: list[Mission] | None = None,
        max_radius: float | None = 500_000,
        planner: QueryPlanner = BasinPlanner(4, 16),
    ) -> Generator[tuple[int, Dataset[along_track_fields] | None], None, None]:
        if max_radius is None:
            sql_with_missions = self._along_track_nearest_neighbor_query
            sql_without_missions = (
                self._along_track_nearest_neighbor_without_mission_query
            )
        else:
            sql_with_missions = self._along_track_nearest_neighbor_max_radius_query
            sql_without_missions = (
                self._along_track_nearest_neighbor_without_mission_max_radius_query
            )
        """Stream indexed nearest-neighbor results as workers finish."""
        query_spec, params_batch, basin_ids = self._build_basin_query_batch(
            latitudes=latitudes,
            longitudes=longitudes,
            dates=dates,
            time_window=time_window,
            missions=missions,
            sql_with_missions=sql_with_missions,
            sql_without_missions=sql_without_missions,
            mandatory_fields=["distance"],
            extra_params={"max_radius": max_radius},
        )
        return self.execute_batch_read_query_stream(
            query_spec=query_spec,
            fields=fields,
            params_batch=params_batch,
            plan=planner.plan(params_batch=params_batch, basin_ids=basin_ids),
            dataset_name="along_track",
        )

    def _build_basin_query_batch(
        self,
        *,
        latitudes: list[float],
        longitudes: list[float],
        dates: list[datetime],
        time_window: timedelta,
        missions: list[Mission] | None,
        sql_with_missions: str,
        sql_without_missions: str,
        mandatory_fields: list[along_track_fields] | None = None,
        extra_params: dict[str, Any] | None = None,
    ) -> tuple[QuerySpec[along_track_fields], list[dict[str, Any]], list[int]]:
        sql_file = sql_without_missions if missions is None else sql_with_missions
        query_spec: QuerySpec[along_track_fields] = QuerySpec(
            sql_template=self.load_sql_file(sql_file),
            schema=along_track_schema,
            mandatory_fields=mandatory_fields or [],
        )
        basin_ids = [
            self.basin_mask_lookup.lookup(latitude, longitude)
            for latitude, longitude in zip(latitudes, longitudes, strict=True)
        ]
        connected_basin_ids = [
            self.basin_connections.connection_map[basin_id] for basin_id in basin_ids
        ]
        params_batch = []
        for latitude, longitude, date, connected_ids in zip(
            latitudes, longitudes, dates, connected_basin_ids, strict=True
        ):
            params = {
                "longitude": longitude,
                "latitude": latitude,
                "central_date_time": date,
                "time_delta": time_window,
                "connected_basin_ids": connected_ids,
            }
            if missions is not None:
                params["missions"] = missions
            if extra_params:
                params.update(extra_params)
            params_batch.append(params)
        return query_spec, params_batch, basin_ids
