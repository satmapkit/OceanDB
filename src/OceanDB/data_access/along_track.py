from datetime import datetime, timedelta
from functools import cached_property
from typing import Any, Generator, Literal

from OceanDB.data_access.base_query import BaseReadQuery, QuerySpec
from OceanDB.data_access.query_plan import plan_by_basin
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
    _along_track_spatiotemporal_query = (
        "queries/along_track/geographic_points_in_spatialtemporal_window.sql"
    )
    _along_track_nearest_neighbor_without_mission_query = (
        "queries/along_track/geographic_nearest_neighbor_without_mission.sql"
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
        *,
        n_jobs: int = 1,
        chunk_size: int = 16,
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
            n_jobs=n_jobs,
            chunk_size=chunk_size,
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
        *,
        n_jobs: int = 4,
        chunk_size: int = 16,
    ) -> Generator[tuple[int, Dataset[along_track_fields] | None], None, None]:
        """Stream indexed spatial-temporal query results as workers finish."""
        query_spec, params_batch, basin_ids = self._build_spatiotemporal_batch(
            fields,
            latitudes,
            longitudes,
            dates,
            radius,
            time_window,
            missions,
        )
        return self.execute_batch_read_query_stream(
            query_spec=query_spec,
            fields=fields,
            params_batch=params_batch,
            plan=plan_by_basin(basin_ids, n_jobs=n_jobs, chunk_size=chunk_size),
            dataset_name="along_track",
        )

    def _build_spatiotemporal_batch(
        self,
        fields: list[along_track_fields],
        latitudes: list[float],
        longitudes: list[float],
        dates: list[datetime],
        radius: float,
        time_window: timedelta,
        missions: list[Mission] | None,
    ) -> tuple[QuerySpec[along_track_fields], list[dict[str, Any]], list[int]]:
        sql_file = (
            self._along_track_spatiotemporal_without_mission_query
            if missions is None
            else self._along_track_spatiotemporal_query
        )
        query_spec: QuerySpec[along_track_fields] = QuerySpec(
            sql_template=self.load_sql_file(sql_file), schema=along_track_schema
        )
        basin_ids = [
            self.basin_mask_lookup.lookup(latitude, longitude)
            for latitude, longitude in zip(latitudes, longitudes, strict=True)
        ]
        connected_basin_ids = [
            self.basin_connections.connection_map[basin_id] for basin_id in basin_ids
        ]
        params_batch = [
            {
                "longitude": longitude,
                "latitude": latitude,
                "distance": radius,
                "central_date_time": date,
                "time_delta": time_window,
                "connected_basin_ids": connected_ids,
                **({"missions": missions} if missions is not None else {}),
            }
            for latitude, longitude, date, connected_ids in zip(
                latitudes, longitudes, dates, connected_basin_ids, strict=True
            )
        ]
        return query_spec, params_batch, basin_ids

    def geographic_nearest_neighbors(
        self,
        fields: list,
        latitude: float,
        longitude: float,
        date: datetime,
        time_window: timedelta = timedelta(days=10),
        missions: list[Mission] | None = None,
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
        *,
        n_jobs: int = 1,
        chunk_size: int = 16,
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
            n_jobs=n_jobs,
            chunk_size=chunk_size,
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
        *,
        n_jobs: int = 4,
        chunk_size: int = 16,
    ) -> Generator[tuple[int, Dataset[along_track_fields] | None], None, None]:
        """Stream indexed nearest-neighbor results as workers finish."""
        query_spec, params_batch, basin_ids = self._build_nearest_neighbor_batch(
            fields, latitudes, longitudes, dates, time_window, missions
        )
        return self.execute_batch_read_query_stream(
            query_spec=query_spec,
            fields=fields,
            params_batch=params_batch,
            plan=plan_by_basin(basin_ids, n_jobs=n_jobs, chunk_size=chunk_size),
            dataset_name="along_track",
        )

    def _build_nearest_neighbor_batch(
        self,
        fields: list[along_track_fields],
        latitudes: list[float],
        longitudes: list[float],
        dates: list[datetime],
        time_window: timedelta,
        missions: list[Mission] | None,
    ) -> tuple[QuerySpec[along_track_fields], list[dict[str, Any]], list[int]]:
        sql_file = (
            self._along_track_nearest_neighbor_without_mission_query
            if missions is None
            else self._along_track_nearest_neighbor_query
        )
        query_spec: QuerySpec[along_track_fields] = QuerySpec(
            sql_template=self.load_sql_file(sql_file),
            schema=along_track_schema,
            mandatory_fields=["distance"],
        )
        basin_ids = [
            self.basin_mask_lookup.lookup(latitude, longitude)
            for latitude, longitude in zip(latitudes, longitudes, strict=True)
        ]
        connected_basin_ids = [
            self.basin_connections.connection_map[basin_id] for basin_id in basin_ids
        ]
        params_batch = [
            {
                "longitude": longitude,
                "latitude": latitude,
                "central_date_time": date,
                "time_delta": time_window,
                "connected_basin_ids": connected_ids,
                **({"missions": missions} if missions is not None else {}),
            }
            for latitude, longitude, date, connected_ids in zip(
                latitudes, longitudes, dates, connected_basin_ids, strict=True
            )
        ]
        return query_spec, params_batch, basin_ids
