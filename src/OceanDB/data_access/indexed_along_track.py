"""Scientific AlongTrack queries over index-owned, immutable windows."""

from __future__ import annotations

from collections.abc import Generator, Sequence
from concurrent.futures import Future, ThreadPoolExecutor
from datetime import datetime, timedelta
from functools import partial
from pathlib import Path
from queue import Queue
from threading import Event, Semaphore, Thread

import numpy as np
from numpy.typing import NDArray

from OceanDB.along_track_missions import Mission
from OceanDB.data_access.along_track_index import (
    _EPOCH,
    AlongTrackIndex,
)
from OceanDB.data_access.along_track_index import IndexedCacheInfo as IndexedCacheInfo
from OceanDB.data_access.along_track_index import (
    PreparedWindow,
    _microseconds,
)
from OceanDB.data_access.indexed_geometry import (
    MAX_WGS84_DISTANCE_M,
    exact_wgs84_distance_m,
    wgs84_distance_m,
)
from OceanDB.ocean_data.dataset import Dataset
from OceanDB.schemas.along_track_schema import along_track_fields, along_track_schema

_DECISION_MARGIN_M = 0.01


class IndexedAlongTrackBackend:
    """An OceanDB AlongTrack backend over locally downloaded NetCDF files.

    The instance is not generally thread-safe. Native streams run concurrent
    queries over immutable windows, with one owner of file/cache preparation.
    Other queries, preparation, and refresh are rejected during an active stream.
    Close partially consumed streams to release their workers and ownership.
    An explicit :meth:`refresh` discovers archive changes. Construction reuses a
    valid saved inventory without rescanning. New windows validate only selected
    sources; repeated queries reuse immutable prepared arrays without filesystem
    checks, even after source edits. Changed or missing selected files raise when
    a new window is prepared against an outdated inventory.

    :param data_root: ``ALONG_TRACK_DATA_DIRECTORY`` or its product directory.
    :param cache_dir: Directory for reusable, versioned local cache artifacts.
    :param cell_size_degrees: Spatial index cell width (must divide 180 and 360).
    :param max_cache_bytes: Approximate upper bound for retained per-file RAM,
        excluding active and in-flight windows.
    :param max_geometry_cache_bytes: Upper bound for retained query geometry.
    """

    def __init__(
        self,
        data_root: str | Path,
        cache_dir: str | Path,
        *,
        cell_size_degrees: float = 2.0,
        max_cache_bytes: int = 2_000_000_000,
        max_geometry_cache_bytes: int = 1_000_000_000,
    ) -> None:
        self._index = AlongTrackIndex(
            data_root,
            cache_dir,
            cell_size_degrees=cell_size_degrees,
            max_cache_bytes=max_cache_bytes,
            max_geometry_cache_bytes=max_geometry_cache_bytes,
        )
        self.data_root = self._index.data_root
        self.cache_dir = self._index.cache_dir
        self.stats = self._index.stats

    def refresh(self) -> None:
        """Discover archive changes and invalidate the current cached window.

        A failed refresh leaves the previous inventory and cached rows usable.

        :raises RuntimeError: If a stream is active on this instance.
        """
        self._index.refresh()

    def prepare_grid(
        self,
        latitudes: Sequence[float],
        longitudes: Sequence[float],
        *,
        radius: float = 500_000.0,
    ) -> None:
        """Warm index geometry for paired centers, not Cartesian grid axes.

        :param latitudes: Latitude of each center.
        :param longitudes: Longitude of each paired center.
        :param radius: Inclusive radius for reusable geometry, in meters.
        :raises ValueError: If the centers or radius are invalid.
        :raises RuntimeError: If a stream is active on this instance.
        """
        self._index.prepare_grid(latitudes, longitudes, radius=radius)

    def prepare_window(
        self,
        date: datetime,
        time_window: timedelta,
        missions: Sequence[Mission] | None = None,
    ) -> None:
        """Warm an immutable temporal window without querying it.

        Exact-key reuse performs no source checks; use :meth:`refresh` to
        incorporate archive edits.

        :param date: Center datetime.
        :param time_window: Nonnegative temporal half-width.
        :param missions: Literal mission selection, or ``None`` for all.
        :raises RuntimeError: If a source changed or a stream is active.
        """
        self._index.prepare_window(date, time_window, missions)

    def _select(
        self, window: PreparedWindow, latitude: float, longitude: float, radius: float
    ) -> tuple[NDArray[np.intp], NDArray[np.float64]]:
        """Apply the exact WGS84 predicate to conservative index candidates.

        :param window: Stable prepared observations for this query.
        :param latitude: Query-center latitude.
        :param longitude: Query-center longitude.
        :param radius: Inclusive WGS84 radius in meters.
        :returns: Row indices and their corresponding distances.
        """
        if not np.isfinite(radius) or radius < 0:
            raise ValueError("radius must be finite and nonnegative")
        active = window.rows
        candidates = window.candidates(latitude, longitude, radius)
        if not len(candidates):
            return candidates, np.empty(0, dtype=np.float64)
        distances = wgs84_distance_m(
            active["longitude"][candidates],
            active["latitude"][candidates],
            longitude,
            latitude,
        )
        decisive = np.abs(distances - radius) <= _DECISION_MARGIN_M
        if decisive.any():
            distances[decisive] = exact_wgs84_distance_m(
                active["longitude"][candidates[decisive]],
                active["latitude"][candidates[decisive]],
                longitude,
                latitude,
            )
        inside = distances <= radius
        return candidates[inside], distances[inside]

    def _nearest(
        self,
        window: PreparedWindow,
        latitude: float,
        longitude: float,
        max_radius: float | None,
    ) -> tuple[NDArray[np.intp], NDArray[np.float64]]:
        """Expand complete inner caps until three eligible rows are enclosed.

        :param window: Stable prepared observations for this query.
        :param latitude: Query-center latitude.
        :param longitude: Query-center longitude.
        :param max_radius: Inclusive cap, or ``None`` for global nearest.
        :returns: At most three rows in the contract's deterministic order.
        """
        limit = MAX_WGS84_DISTANCE_M if max_radius is None else max_radius
        search = min(limit, 150_000.0)
        while True:
            rows, distances = self._select(window, latitude, longitude, search)
            if search >= limit or (
                len(rows) >= 3 and np.count_nonzero(distances < search - 1e-5) >= 3
            ):
                break
            search = min(limit, search * 2)
        if not len(rows):
            return rows, distances
        active = window.rows
        top = np.argpartition(distances, min(3, len(rows)) - 1)[:3]
        decisive = np.any(
            np.abs(distances[:, None] - distances[top][None, :]) <= _DECISION_MARGIN_M,
            axis=1,
        )
        if decisive.any():
            distances[decisive] = exact_wgs84_distance_m(
                active["longitude"][rows[decisive]],
                active["latitude"][rows[decisive]],
                longitude,
                latitude,
            )
        order = np.lexsort(
            (
                active["longitude"][rows],
                active["latitude"][rows],
                active["time_us"][rows],
                distances,
            )
        )[:3]
        return rows[order], distances[order]

    def _materialize(
        self,
        window: PreparedWindow,
        fields: list[along_track_fields],
        rows: NDArray[np.intp],
        distances: NDArray[np.float64],
        date: datetime,
        *,
        nearest: bool,
    ) -> Dataset[along_track_fields] | None:
        """Project stored values into a schema-backed OceanDB dataset.

        :param window: Stable prepared observations and provenance.
        :param fields: Requested field names in result order.
        :param rows: Selected window row indices.
        :param distances: Exact WGS84 distances aligned with ``rows``.
        :param date: Query-center time for ``delta_t``.
        :param nearest: Include mandatory nearest-neighbor ``distance``.
        :returns: A normal OceanDB dataset or ``None`` for no matches.
        """
        if not len(rows):
            return None
        active = window.rows
        requested = list(dict.fromkeys(fields))
        if nearest and "distance" not in requested:
            requested.append("distance")
        data: dict[along_track_fields, np.ndarray] = {}
        for name in requested:
            if name not in along_track_schema:
                raise ValueError(f"Unknown AlongTrack field: {name}")
            values: NDArray[np.generic]
            if name == "distance":
                values = distances
            elif name == "delta_t":
                values = (_microseconds(date) - active["time_us"][rows]) / 1_000_000
            elif name == "date_time":
                values = np.asarray(
                    [
                        _EPOCH + timedelta(microseconds=int(us))
                        for us in active["time_us"][rows]
                    ],
                    dtype=object,
                )
            else:
                values = window.column(name, rows)
            data[name] = np.asarray(values)
        dtypes: dict[along_track_fields, type] = {}
        for name in requested:
            python_type = along_track_schema[name].python_type
            if python_type is not None:
                dtypes[name] = python_type
        return Dataset(
            name="along_track", data=data, dtypes=dtypes, schema=along_track_schema
        )

    def _query(
        self,
        window: PreparedWindow,
        fields: list[along_track_fields],
        latitude: float,
        longitude: float,
        date: datetime,
        radius: float | None,
        *,
        nearest: bool,
    ) -> Dataset[along_track_fields] | None:
        """Select and project one center against its captured window.

        :param window: Stable prepared observations and provenance.
        :param fields: Requested OceanDB columns.
        :param latitude: Query-center latitude.
        :param longitude: Query-center longitude.
        :param date: Center datetime for signed temporal offsets.
        :param radius: Inclusive radius or optional nearest cap.
        :param nearest: Select three nearest rows instead of radius membership.
        :returns: Projected observations, or ``None`` when no rows match.
        """
        rows, distances = (
            self._nearest(window, latitude, longitude, radius)
            if nearest
            else self._select(
                window, latitude, longitude, 500_000.0 if radius is None else radius
            )
        )
        return self._materialize(window, fields, rows, distances, date, nearest=nearest)

    def _batch(
        self,
        fields: list[along_track_fields],
        latitudes: list[float],
        longitudes: list[float],
        dates: list[datetime],
        radius: float | None,
        time_window: timedelta,
        missions: list[Mission] | None,
        *,
        nearest: bool,
    ) -> Generator[Dataset[along_track_fields] | None, None, None]:
        """Execute scalar centers over reusable prepared windows serially.

        :param fields: Requested OceanDB columns.
        :param latitudes: Query-center latitudes.
        :param longitudes: Query-center longitudes.
        :param dates: Query-center datetimes.
        :param radius: Inclusive radius or nearest cap.
        :param time_window: Temporal half-width.
        :param missions: Literal mission filter, or ``None`` for all.
        :param nearest: Select three nearest rows instead of all inside radius.
        :returns: One result per center in input order.
        """
        self._index.require_idle()
        if len(latitudes) != len(longitudes) or len(latitudes) != len(dates):
            raise ValueError("latitudes, longitudes, and dates must have equal lengths")
        unknown = set(fields).difference(along_track_schema)
        if unknown:
            raise ValueError(f"Unknown AlongTrack fields: {sorted(unknown)}")
        if radius is not None and (not np.isfinite(radius) or radius < 0):
            raise ValueError("radius must be finite and nonnegative")
        for latitude, longitude, date in zip(latitudes, longitudes, dates, strict=True):
            yield self._query(
                self._index.prepare_window(date, time_window, missions),
                fields,
                latitude,
                longitude,
                date,
                radius,
                nearest=nearest,
            )

    def _stream(
        self,
        fields: list[along_track_fields],
        latitudes: list[float],
        longitudes: list[float],
        dates: list[datetime],
        radius: float | None,
        time_window: timedelta,
        missions: list[Mission] | None,
        *,
        nearest: bool,
        n_jobs: int,
        chunk_size: int,
    ) -> Generator[tuple[int, Dataset[along_track_fields] | None], None, None]:
        """Prepare bounded work and publish native query-worker completions.

        :param fields: Requested OceanDB columns.
        :param latitudes: Paired center latitudes.
        :param longitudes: Paired center longitudes.
        :param dates: Paired UTC-naive center datetimes.
        :param radius: Radius or optional nearest cap.
        :param time_window: Inclusive temporal half-width.
        :param missions: Literal mission filter, or ``None`` for all.
        :param nearest: Select three nearest rows rather than radius membership.
        :param n_jobs: Maximum concurrent query workers.
        :param chunk_size: Maximum outstanding centers per worker.
        :returns: Indexed results in completion order.
        """
        if n_jobs < 1 or chunk_size < 1:
            raise ValueError("n_jobs and chunk_size must be at least 1")
        if len(latitudes) != len(longitudes) or len(latitudes) != len(dates):
            raise ValueError("latitudes, longitudes, and dates must have equal lengths")
        unknown = set(fields).difference(along_track_schema)
        if unknown:
            raise ValueError(f"Unknown AlongTrack fields: {sorted(unknown)}")
        if radius is not None and (not np.isfinite(radius) or radius < 0):
            raise ValueError("radius must be finite and nonnegative")
        if time_window < timedelta(0):
            raise ValueError("time_window must be nonnegative")
        if not latitudes:
            return
        with self._index.preparation() as prepare_window:
            results: Queue[
                tuple[int, Future[Dataset[along_track_fields] | None]]
                | BaseException
                | None
            ] = Queue()
            capacity = Semaphore(n_jobs * chunk_size)
            stopped = Event()
            fields = fields.copy()
            missions = None if missions is None else missions.copy()

            def publish(
                index: int, future: Future[Dataset[along_track_fields] | None]
            ) -> None:
                results.put((index, future))

            with ThreadPoolExecutor(
                max_workers=n_jobs, thread_name_prefix="oceandb-indexed-query"
            ) as executor:

                def prepare() -> None:
                    try:
                        for index, (latitude, longitude, date) in enumerate(
                            zip(latitudes, longitudes, dates, strict=True)
                        ):
                            capacity.acquire()
                            if stopped.is_set():
                                break
                            window = prepare_window(date, time_window, missions)
                            if stopped.is_set():
                                break
                            future = executor.submit(
                                self._query,
                                window,
                                fields,
                                latitude,
                                longitude,
                                date,
                                radius,
                                nearest=nearest,
                            )
                            del window
                            future.add_done_callback(partial(publish, index))
                    except BaseException as error:
                        results.put(error)
                    finally:
                        executor.shutdown(wait=True, cancel_futures=stopped.is_set())
                        results.put(None)

                producer = Thread(target=prepare, name="oceandb-indexed-prepare")
                producer.start()
                try:
                    while True:
                        completed = results.get()
                        if completed is None:
                            break
                        if isinstance(completed, BaseException):
                            raise completed
                        index, future = completed
                        yield index, future.result()
                        capacity.release()
                finally:
                    stopped.set()
                    capacity.release()
                    executor.shutdown(wait=True, cancel_futures=True)
                    producer.join()

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
        """Stream radius/time results from concurrent native query workers.

        :param fields: Requested fields; ``distance`` is optional, in meters.
        :param latitudes: Query-center latitudes in degrees.
        :param longitudes: Query-center longitudes, paired by input index.
        :param dates: Paired UTC-naive datetimes.
        :param radius: Inclusive WGS84 ellipsoidal radius in meters.
        :param time_window: Inclusive temporal half-width, default ten days.
        :param missions: ``None`` selects all missions; ``[]`` selects none.
        :param n_jobs: Maximum query threads, default four.
        :param chunk_size: Outstanding centers per worker, default sixteen.
        :returns: One ``(input_index, Dataset | None)`` per center, in completion
            order. ``None`` means no match, not failure. Empty input yields nothing.
        :raises ValueError: On iteration, for invalid inputs or nonpositive limits.
        :raises RuntimeError: If another stream is active or sources have changed.

        Preparation is serial; spatial queries run concurrently on read-only
        windows. At most ``n_jobs * chunk_size`` centers are outstanding,
        including buffered results. Different windows can coexist, so this is not
        a byte limit or part of ``max_cache_bytes``. Close a partially consumed
        generator: pending work is cancelled and running queries/preparation are
        awaited.
        Use this endpoint directly on the backend, not the PostgreSQL-only facade.
        """
        return self._stream(
            fields,
            latitudes,
            longitudes,
            dates,
            radius,
            time_window,
            missions,
            nearest=False,
            n_jobs=n_jobs,
            chunk_size=chunk_size,
        )

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
        """Stream up to three nearest observations for each center concurrently.

        Uses the buffering, ownership, and closure rules of
        :meth:`geographic_point_in_r_dt_stream`. Rows retain deterministic
        distance/date/latitude/stored-longitude ranking, independent of center
        completion order. ``distance`` is always included.

        :param fields: Requested OceanDB fields.
        :param latitudes: Query-center latitudes in degrees.
        :param longitudes: Query-center longitudes, paired by input index.
        :param dates: Paired UTC-naive datetimes.
        :param time_window: Inclusive temporal half-width, default ten days.
        :param missions: ``None`` selects all missions; ``[]`` selects none.
        :param max_radius: Inclusive finite, nonnegative WGS84 cap in meters,
            or ``None`` for unbounded nearest selection.
        :param n_jobs: Maximum query threads, default four.
        :param chunk_size: Outstanding centers per worker, default sixteen.
        :returns: One indexed Dataset or ``None`` per center, in completion order.
        :raises ValueError: On iteration, for invalid inputs or nonpositive limits.
        :raises RuntimeError: If another stream is active or sources have changed.
        """
        return self._stream(
            fields,
            latitudes,
            longitudes,
            dates,
            max_radius,
            time_window,
            missions,
            nearest=True,
            n_jobs=n_jobs,
            chunk_size=chunk_size,
        )

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
        """Return OceanDB observations in an inclusive spatial/time window."""
        return next(
            self.geographic_point_in_r_dt_batch(
                fields, [latitude], [longitude], [date], radius, time_window, missions
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
        """Yield radius-window results serially in input order."""
        if n_jobs != 1 or chunk_size < 1:
            raise ValueError("indexed batches require n_jobs=1 and positive chunk_size")
        return self._batch(
            fields,
            latitudes,
            longitudes,
            dates,
            radius,
            time_window,
            missions,
            nearest=False,
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
        """Return at most three nearest eligible observations."""
        return next(
            self.geographic_nearest_neighbors_batch(
                fields,
                [latitude],
                [longitude],
                [date],
                time_window,
                missions,
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
        """Yield three-nearest results serially in input order."""
        if n_jobs != 1 or chunk_size < 1:
            raise ValueError("indexed batches require n_jobs=1 and positive chunk_size")
        return self._batch(
            fields,
            latitudes,
            longitudes,
            dates,
            max_radius,
            time_window,
            missions,
            nearest=True,
        )
