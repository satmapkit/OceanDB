from datetime import datetime, timedelta
from functools import cached_property
from math import isfinite
from typing import Any, Generator

from OceanDB.along_track_missions import Mission as Mission
from OceanDB.data_access.base_query import BaseReadQuery, QuerySpec
from OceanDB.data_access.query_plan import plan_by_basin
from OceanDB.ocean_data.basins import BasinConnections, BasinMask
from OceanDB.ocean_data.dataset import Dataset
from OceanDB.schemas.along_track_schema import (along_track_fields,
                                                along_track_schema)


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
        Return along-track observations within a radius and time window.

        Spatial membership uses WGS84 ellipsoidal distance in meters.
        Observations must also pass the mission and connected-basin filters.

        :param fields:
            Requested along-track fields. ``distance`` is included only when
            requested and uses the same WGS84 metric. Requested ``delta_t`` is
            signed seconds from ``date - date_time`` (center minus observation).
        :param latitude:
            Query-center latitude in degrees.
        :param longitude:
            Query-center longitude in degrees.
        :param date:
            Center of the temporal window.
        :param radius:
            Inclusive spatial radius in meters; defaults to ``500_000.0``.
        :param time_window:
            Temporal half-width; defaults to ten days. Both endpoints of
            ``[date - time_window, date + time_window]`` are included.
        :param missions:
            Canonical mission names from ``Mission``; ``None`` (default) selects
            all missions, while ``[]`` selects none.
        :returns:
            A :class:`Dataset <OceanDB.ocean_data.dataset.Dataset>` containing
            the requested fields, or ``None`` if no observations match.
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
        Query radius-and-time windows for equally sized lists of centers.

        Each center uses :meth:`geographic_point_in_r_dt` semantics, including
        WGS84 ellipsoidal distances, filtering, and projected-field meanings.

        :param fields:
            Requested along-track fields; ``distance`` remains optional.
        :param latitudes:
            Query-center latitudes in degrees.
        :param longitudes:
            Query-center longitudes in degrees, paired with latitudes by index.
        :param dates:
            Temporal centers, paired with coordinates by index.
        :param radius:
            Inclusive spatial radius in meters; defaults to ``500_000.0``.
        :param time_window:
            Inclusive temporal half-width around each date; defaults to ten days.
        :param missions:
            Canonical mission names from ``Mission``; ``None`` (default) selects
            all missions, while ``[]`` selects none.
        :param n_jobs:
            Number of worker jobs, at least one; defaults to ``1``.
        :param chunk_size:
            Maximum centers per basin-homogeneous chunk, at least one;
            defaults to ``16``.
        :returns:
            A generator yielding one Dataset or ``None`` per center in input
            order. ``None`` means no matching observations; empty input lists
            yield no results.
        :raises ValueError:
            If input list lengths differ, or ``n_jobs`` or ``chunk_size`` is
            less than one.
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
            plan=plan_by_basin(basin_ids, n_jobs=n_jobs, chunk_size=chunk_size),
            dataset_name="along_track",
        )

    def geographic_nearest_neighbors(
        self,
        fields: list[along_track_fields],
        latitude: float,
        longitude: float,
        date: datetime,
        time_window: timedelta = timedelta(days=10),
        missions: list[Mission] | None = None,
        *,
        max_radius: float | None = None,
    ) -> Dataset[along_track_fields] | None:
        """
        Return up to three nearest eligible along-track observations.

        Observations are ranked by ascending WGS84 ellipsoidal distance in meters
        after time, mission, connected-basin, and optional radius filtering.
        Exact distance ties are broken by ascending ``date_time``, then
        ``latitude``, then stored ``longitude``, including selection at the
        three-row cutoff.

        :param fields:
            Requested along-track fields. ``distance`` is always included,
            even if omitted here. Requested ``delta_t`` is signed seconds from
            ``date - date_time`` (center minus observation).
        :param latitude:
            Query-center latitude in degrees.
        :param longitude:
            Query-center longitude in degrees.
        :param date:
            Center of the temporal window.
        :param time_window:
            Temporal half-width; defaults to ten days. Both endpoints of
            ``[date - time_window, date + time_window]`` are included.
        :param missions:
            Canonical mission names from ``Mission``; ``None`` (default) selects
            all missions, while ``[]`` selects none.
        :param max_radius:
            Inclusive WGS84 ellipsoidal distance cap in meters; must be finite
            and non-negative. ``None`` (default) leaves distance unbounded.
        :returns:
            A :class:`Dataset <OceanDB.ocean_data.dataset.Dataset>` with up to
            three rows in the order above, including ``distance``, or ``None``
            if no observations match.
        :raises ValueError:
            If ``max_radius`` is negative, NaN, or infinite.
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
        max_radius: float | None = None,
        n_jobs: int = 1,
        chunk_size: int = 16,
    ) -> Generator[Dataset[along_track_fields] | None, None, None]:
        """
        Query up to three nearest observations for each input center.

        Each center uses :meth:`geographic_nearest_neighbors` semantics,
        including WGS84 ellipsoidal ranking, exact tie-breaking, filtering,
        and the signed meaning of requested ``delta_t``.

        :param fields:
            Requested along-track fields; ``distance`` is always included.
        :param latitudes:
            Query-center latitudes in degrees.
        :param longitudes:
            Query-center longitudes in degrees, paired with latitudes by index.
        :param dates:
            Temporal centers, paired with coordinates by index.
        :param time_window:
            Inclusive temporal half-width around each date; defaults to ten days.
        :param missions:
            Canonical mission names from ``Mission``; ``None`` (default) selects
            all missions, while ``[]`` selects none.
        :param max_radius:
            Inclusive WGS84 ellipsoidal distance cap in meters; must be finite
            and non-negative. ``None`` (default) leaves distance unbounded.
        :param n_jobs:
            Number of worker jobs, at least one; defaults to ``1``.
        :param chunk_size:
            Maximum centers per basin-homogeneous chunk, at least one;
            defaults to ``16``.
        :returns:
            A generator yielding one Dataset or ``None`` per center in input
            order. ``None`` means no matching observations; empty input lists
            yield no results.
        :raises ValueError:
            If input list lengths differ, ``n_jobs`` or ``chunk_size`` is less
            than one, or ``max_radius`` is negative, NaN, or infinite.
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
        max_radius: float | None = None,
        n_jobs: int = 4,
        chunk_size: int = 16,
    ) -> Generator[tuple[int, Dataset[along_track_fields] | None], None, None]:
        """Stream indexed nearest-neighbor results as workers finish.

        :param max_radius:
            Inclusive WGS84 distance cap in meters; must be finite and
            non-negative. ``None`` (default) leaves distance unbounded.
        """
        if max_radius is not None and (not isfinite(max_radius) or max_radius < 0):
            raise ValueError("max_radius must be finite and non-negative")

        query_spec, params_batch, basin_ids = self._build_basin_query_batch(
            latitudes=latitudes,
            longitudes=longitudes,
            dates=dates,
            time_window=time_window,
            missions=missions,
            sql_with_missions=self._along_track_nearest_neighbor_query,
            sql_without_missions=self._along_track_nearest_neighbor_without_mission_query,
            mandatory_fields=["distance"],
            extra_params={"max_radius": max_radius},
        )
        return self.execute_batch_read_query_stream(
            query_spec=query_spec,
            fields=fields,
            params_batch=params_batch,
            plan=plan_by_basin(basin_ids, n_jobs=n_jobs, chunk_size=chunk_size),
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
