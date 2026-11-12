"""Native indexed streams: science, concurrency, backpressure, and ownership."""

import os
from collections.abc import Generator, Sequence
from concurrent.futures import Future
from contextlib import closing
from datetime import datetime, timedelta, timezone
from pathlib import Path
from queue import Queue
from threading import Barrier, Event, Lock, Thread, current_thread
from threading import enumerate as enumerate_threads

import numpy as np
import pytest

from OceanDB.data_access.along_track import Mission
from OceanDB.data_access.along_track_index import PreparedWindow
from OceanDB.data_access.indexed_along_track import IndexedAlongTrackBackend
from OceanDB.ocean_data.dataset import Dataset
from OceanDB.schemas.along_track_schema import along_track_fields, along_track_schema
from tests.along_track.test_indexed_backend import (
    DATES,
    FULL_FIELDS,
    LATITUDE,
    LONGITUDE,
    NEAREST_FIELDS,
    _assert_source_row,
)
from tests.along_track.test_indexed_backend import (
    forbid_database_connections as forbid_database_connections,
)
from tests.along_track.test_indexed_backend import source_layout as source_layout

pytestmark = pytest.mark.unit


@pytest.fixture
def backend(source_layout: tuple[Path, Path, Path]) -> IndexedAlongTrackBackend:
    """Construct a direct backend over the synthetic archive.

    :param source_layout: Product root, cache, and literal source file.
    :returns: Backend without a PostgreSQL facade.
    """
    product, cache, _ = source_layout
    return IndexedAlongTrackBackend(product, cache)


def _stream(
    backend: IndexedAlongTrackBackend,
    nearest: bool,
    *,
    fields: list[along_track_fields] | None = None,
    latitudes: list[float] | None = None,
    longitudes: list[float] | None = None,
    dates: list[datetime] | None = None,
    radius: float | None = 0.0,
    time_window: timedelta = timedelta(0),
    missions: list[Mission] | None = None,
    n_jobs: int = 4,
    chunk_size: int = 16,
) -> Generator[tuple[int, Dataset[along_track_fields] | None], None, None]:
    """Apply lifecycle checks to both public streams.

    :param backend: Direct indexed backend.
    :param nearest: Use nearest ranking instead of radius membership.
    :param fields: Projection, defaulting to all fields.
    :param latitudes: Paired latitudes, defaulting to the source literals.
    :param longitudes: Paired longitudes, defaulting to the source literals.
    :param dates: Paired dates, defaulting to the source literals.
    :param radius: Spatial radius or optional nearest cap.
    :param time_window: Inclusive temporal half-width.
    :param missions: Literal mission selection.
    :param n_jobs: Query-worker count.
    :param chunk_size: Outstanding centers per worker.
    :returns: Unconsumed native stream.
    """
    fields = FULL_FIELDS if fields is None else fields
    latitudes = list(LATITUDE) if latitudes is None else latitudes
    longitudes = list(LONGITUDE) if longitudes is None else longitudes
    dates = list(DATES) if dates is None else dates
    if nearest:
        return backend.geographic_nearest_neighbors_stream(
            fields,
            latitudes,
            longitudes,
            dates,
            time_window,
            missions,
            max_radius=radius,
            n_jobs=n_jobs,
            chunk_size=chunk_size,
        )
    return backend.geographic_point_in_r_dt_stream(
        fields,
        latitudes,
        longitudes,
        dates,
        500_000.0 if radius is None else radius,
        time_window,
        missions,
        n_jobs=n_jobs,
        chunk_size=chunk_size,
    )


def _assert_workers_stopped() -> None:
    """Check that a finished or closed stream has joined all its workers."""
    assert not any(
        thread.name.startswith("oceandb-indexed-") for thread in enumerate_threads()
    )


@pytest.mark.parametrize("nearest", [False, True], ids=["radius", "nearest"])
@pytest.mark.parametrize("radius", [0.0, 111_000.0, None])
@pytest.mark.parametrize("missions", [None, [], ["j2"], ["j3"]])
def test_stream_matches_serial_science_and_preserves_missing_indices(
    backend: IndexedAlongTrackBackend,
    nearest: bool,
    radius: float | None,
    missions: list[Mission] | None,
) -> None:
    """Compare every public column, including NaNs and mandatory NN distance.

    :param backend: Synthetic direct backend.
    :param nearest: Query mode.
    :param radius: Radius or nearest cap.
    :param missions: Mission filter, including the distinct empty selection.
    """
    indices = [4, 1, 0, 2, 1]
    latitudes = [LATITUDE[index] for index in indices]
    longitudes = [LONGITUDE[index] for index in indices]
    dates = [DATES[index] for index in indices]
    dates[2] += timedelta(days=100)
    fields = NEAREST_FIELDS if nearest else FULL_FIELDS
    window = timedelta(microseconds=4)
    if nearest:
        expected = list(
            backend.geographic_nearest_neighbors_batch(
                fields,
                latitudes,
                longitudes,
                dates,
                window,
                missions,
                max_radius=radius,
            )
        )
    else:
        expected = list(
            backend.geographic_point_in_r_dt_batch(
                fields,
                latitudes,
                longitudes,
                dates,
                500_000.0 if radius is None else radius,
                window,
                missions,
            )
        )
    results = list(
        _stream(
            backend,
            nearest,
            fields=fields,
            latitudes=latitudes,
            longitudes=longitudes,
            dates=dates,
            radius=radius,
            time_window=window,
            missions=missions,
            chunk_size=1,
        )
    )
    assert sorted(index for index, _ in results) == list(range(len(indices)))
    assert dict(results)[2] is None
    for index, actual in results:
        reference = expected[index]
        if reference is None:
            assert actual is None
        else:
            assert isinstance(actual, Dataset)
            assert actual.name == reference.name
            assert actual.row_count == reference.row_count
            assert set(actual) == set(reference)
            for name in reference:
                np.testing.assert_equal(actual[name], reference[name])
                np.testing.assert_equal(
                    actual.get_unscaled(name), reference.get_unscaled(name)
                )
    _assert_workers_stopped()


@pytest.mark.parametrize("nearest", [False, True], ids=["radius", "nearest"])
def test_default_four_workers_overlap_and_publish_before_first_center_finishes(
    backend: IndexedAlongTrackBackend, monkeypatch: pytest.MonkeyPatch, nearest: bool
) -> None:
    """Force concurrent mixed windows without relying on sleeps or speed.

    :param backend: Synthetic direct backend.
    :param monkeypatch: Scoped method patcher.
    :param nearest: Query mode.
    """
    ready = Barrier(4, timeout=10)
    release_first = Event()
    threads: set[int | None] = set()
    thread_lock = Lock()
    query = backend._query

    def synchronized_query(
        window: PreparedWindow,
        fields: list[along_track_fields],
        latitude: float,
        longitude: float,
        date: datetime,
        radius: float | None,
        *,
        nearest: bool,
    ) -> Dataset[along_track_fields] | None:
        with thread_lock:
            threads.add(current_thread().ident)
        ready.wait()
        if date == DATES[0]:
            assert release_first.wait(10)
        return query(window, fields, latitude, longitude, date, radius, nearest=nearest)

    monkeypatch.setattr(backend, "_query", synchronized_query)
    if nearest:
        stream = backend.geographic_nearest_neighbors_stream(
            FULL_FIELDS,
            list(LATITUDE[:4]),
            list(LONGITUDE[:4]),
            list(DATES[:4]),
            timedelta(0),
            max_radius=0,
            chunk_size=1,
        )
    else:
        stream = backend.geographic_point_in_r_dt_stream(
            FULL_FIELDS,
            list(LATITUDE[:4]),
            list(LONGITUDE[:4]),
            list(DATES[:4]),
            radius=0,
            time_window=timedelta(0),
            chunk_size=1,
        )
    with closing(stream) as results:
        try:
            first = next(results)
            assert first[0] != 0
            assert len(threads) == 4
            release_first.set()
            completed = [first, *results]
        finally:
            release_first.set()
    assert sorted(index for index, _ in completed) == [0, 1, 2, 3]
    for index, result in completed:
        _assert_source_row(result, index)
    _assert_workers_stopped()


@pytest.mark.parametrize("nearest", [False, True], ids=["radius", "nearest"])
def test_stream_backpressure_and_early_close_bound_preparation(
    backend: IndexedAlongTrackBackend, monkeypatch: pytest.MonkeyPatch, nearest: bool
) -> None:
    """A paused consumer cannot cause unbounded preparation or result buffering.

    :param backend: Synthetic direct backend.
    :param monkeypatch: Scoped preparation patcher.
    :param nearest: Query mode.
    """
    prepared: Queue[datetime] = Queue()
    full = Event()
    prepare_window = backend._index._prepare_window

    def observe_preparation(
        date: datetime, time_window: timedelta, missions: Sequence[Mission] | None
    ) -> PreparedWindow:
        prepared.put(date)
        window = prepare_window(date, time_window, missions)
        if prepared.qsize() == 4:
            full.set()
        return window

    monkeypatch.setattr(backend._index, "_prepare_window", observe_preparation)
    with closing(
        _stream(
            backend,
            nearest,
            latitudes=[LATITUDE[1]] * 20,
            longitudes=[LONGITUDE[1]] * 20,
            dates=[DATES[1]] * 20,
            n_jobs=2,
            chunk_size=2,
        )
    ) as results:
        next(results)
        assert full.wait(10)
        assert prepared.qsize() == 4
        assert backend.stats.window_builds == 1
    assert prepared.qsize() == 4
    _assert_workers_stopped()
    backend.refresh()


@pytest.mark.parametrize("nearest", [False, True], ids=["radius", "nearest"])
def test_close_cancels_queued_queries_and_waits_for_running_query(
    backend: IndexedAlongTrackBackend, monkeypatch: pytest.MonkeyPatch, nearest: bool
) -> None:
    """Close must cancel queued futures rather than execute the remaining batch.

    :param backend: Synthetic direct backend.
    :param monkeypatch: Scoped worker and cancellation observers.
    :param nearest: Query mode.
    """
    running = Event()
    release = Event()
    queued = Event()
    cancelled = Event()
    executed: list[datetime] = []
    preparations = 0
    query = backend._query
    prepare_window = backend._index._prepare_window
    cancel = Future.cancel

    def observe_preparation(
        date: datetime, time_window: timedelta, missions: Sequence[Mission] | None
    ) -> PreparedWindow:
        nonlocal preparations
        window = prepare_window(date, time_window, missions)
        preparations += 1
        if preparations == 4:
            queued.set()
        return window

    def blocking_query(
        window: PreparedWindow,
        fields: list[along_track_fields],
        latitude: float,
        longitude: float,
        date: datetime,
        radius: float | None,
        *,
        nearest: bool,
    ) -> Dataset[along_track_fields] | None:
        executed.append(date)
        if date != DATES[0]:
            running.set()
            assert release.wait(10)
        return query(window, fields, latitude, longitude, date, radius, nearest=nearest)

    def observe_cancel(future: Future[object]) -> bool:
        result = cancel(future)
        if result:
            cancelled.set()
        return result

    monkeypatch.setattr(backend._index, "_prepare_window", observe_preparation)
    monkeypatch.setattr(backend, "_query", blocking_query)
    monkeypatch.setattr(Future, "cancel", observe_cancel)
    stream = _stream(
        backend,
        nearest,
        latitudes=list(LATITUDE[:4]),
        longitudes=list(LONGITUDE[:4]),
        dates=list(DATES[:4]),
        n_jobs=1,
        chunk_size=4,
    )
    closer = Thread(target=stream.close)
    try:
        assert next(stream)[0] == 0
        assert running.wait(10)
        assert queued.wait(10)
        closer.start()
        assert cancelled.wait(10)
        assert closer.is_alive()
    finally:
        release.set()
        if closer.ident is not None:
            closer.join(10)
        else:
            stream.close()
    assert not closer.is_alive()
    assert executed == [DATES[0], DATES[1]]
    _assert_workers_stopped()


@pytest.mark.parametrize("nearest", [False, True], ids=["radius", "nearest"])
def test_active_stream_rejects_other_operations_and_releases_ownership(
    backend: IndexedAlongTrackBackend, nearest: bool
) -> None:
    """Reject overlapping streams and external cache mutation, then allow reuse.

    :param backend: Synthetic direct backend.
    :param nearest: Query mode.
    """
    with closing(_stream(backend, nearest, chunk_size=1)) as results:
        next(results)
        with pytest.raises(RuntimeError, match="stream.*active"):
            next(_stream(backend, not nearest))
        with pytest.raises(RuntimeError, match="stream.*active"):
            backend.refresh()
        with pytest.raises(RuntimeError, match="stream.*active"):
            backend.prepare_grid([LATITUDE[0]], [LONGITUDE[0]])
        with pytest.raises(RuntimeError, match="stream.*active"):
            backend.prepare_window(DATES[0], timedelta(0))
        with pytest.raises(RuntimeError, match="stream.*active"):
            backend.geographic_nearest_neighbors(
                ["track"], LATITUDE[0], LONGITUDE[0], DATES[0]
            )
    assert len(list(_stream(backend, nearest))) == len(DATES)
    _assert_workers_stopped()


@pytest.mark.parametrize("nearest", [False, True], ids=["radius", "nearest"])
@pytest.mark.parametrize("preparation", [False, True], ids=["query", "preparation"])
def test_stream_propagates_failures_and_joins_workers(
    backend: IndexedAlongTrackBackend,
    monkeypatch: pytest.MonkeyPatch,
    nearest: bool,
    preparation: bool,
) -> None:
    """Errors never become missing-data results or strand preparation workers.

    :param backend: Synthetic direct backend.
    :param monkeypatch: Scoped failure injector.
    :param nearest: Query mode.
    :param preparation: Fail preparation rather than worker selection.
    """

    def fail(*args: object, **kwargs: object) -> None:
        raise ValueError("injected stream failure")

    with monkeypatch.context() as patch:
        if preparation:
            patch.setattr(backend._index, "_prepare_window", fail)
        else:
            patch.setattr(backend, "_query", fail)
        with pytest.raises(ValueError, match="injected stream failure"):
            list(_stream(backend, nearest, chunk_size=1))
    _assert_workers_stopped()
    backend.refresh()
    assert len(list(_stream(backend, nearest))) == len(DATES)


@pytest.mark.parametrize("nearest", [False, True], ids=["radius", "nearest"])
def test_empty_stream_validates_without_starting_work(
    backend: IndexedAlongTrackBackend, monkeypatch: pytest.MonkeyPatch, nearest: bool
) -> None:
    """Empty inputs must not create workers or prepare source windows.

    :param backend: Synthetic direct backend.
    :param monkeypatch: Scoped work prohibition.
    :param nearest: Query mode.
    """

    def fail(*args: object, **kwargs: object) -> None:
        pytest.fail("Empty streams must not start work")

    monkeypatch.setattr(
        "OceanDB.data_access.indexed_along_track.ThreadPoolExecutor", fail
    )
    monkeypatch.setattr(backend._index, "_prepare_window", fail)
    assert list(_stream(backend, nearest, latitudes=[], longitudes=[], dates=[])) == []
    with pytest.raises(ValueError, match="equal lengths"):
        next(_stream(backend, nearest, latitudes=[], longitudes=[], dates=[DATES[0]]))
    for n_jobs, chunk_size in [(0, 1), (-1, 1), (1, 0), (1, -1)]:
        with pytest.raises(ValueError, match="at least 1"):
            next(
                _stream(
                    backend,
                    nearest,
                    latitudes=[],
                    longitudes=[],
                    dates=[],
                    n_jobs=n_jobs,
                    chunk_size=chunk_size,
                )
            )
    for radius in [-1.0, float("nan"), float("inf")]:
        with pytest.raises(ValueError, match="radius must be finite"):
            next(
                _stream(
                    backend,
                    nearest,
                    latitudes=[],
                    longitudes=[],
                    dates=[],
                    radius=radius,
                )
            )
    with pytest.raises(ValueError, match="time_window must be nonnegative"):
        next(
            _stream(
                backend,
                nearest,
                latitudes=[],
                longitudes=[],
                dates=[],
                time_window=timedelta(days=-1),
            )
        )
    monkeypatch.delitem(along_track_schema, "track")
    with pytest.raises(ValueError, match="Unknown AlongTrack fields"):
        next(
            _stream(
                backend,
                nearest,
                fields=["track"],
                latitudes=[],
                longitudes=[],
                dates=[],
            )
        )


@pytest.mark.parametrize("nearest", [False, True], ids=["radius", "nearest"])
def test_stream_rejects_timezone_aware_centers(
    backend: IndexedAlongTrackBackend, nearest: bool
) -> None:
    """Preserve indexed UTC-naive time requirements across the thread boundary.

    :param backend: Synthetic direct backend.
    :param nearest: Query mode.
    """
    with pytest.raises(ValueError, match="UTC-naive"):
        list(
            _stream(
                backend,
                nearest,
                latitudes=[LATITUDE[0]],
                longitudes=[LONGITUDE[0]],
                dates=[DATES[0].replace(tzinfo=timezone.utc)],
            )
        )
    _assert_workers_stopped()


@pytest.mark.parametrize("nearest", [False, True], ids=["radius", "nearest"])
def test_stream_construction_and_unstarted_close_do_no_work(
    backend: IndexedAlongTrackBackend, monkeypatch: pytest.MonkeyPatch, nearest: bool
) -> None:
    """Constructing a stream must not claim ownership or start preparation.

    :param backend: Synthetic direct backend.
    :param monkeypatch: Scoped work prohibition.
    :param nearest: Query mode.
    """

    def fail(*args: object, **kwargs: object) -> None:
        pytest.fail("An unstarted stream must not create an executor")

    monkeypatch.setattr(
        "OceanDB.data_access.indexed_along_track.ThreadPoolExecutor", fail
    )
    stream = _stream(backend, nearest)
    assert not backend._index._stream_lock.locked()
    stream.close()
    assert backend.stats.window_builds == backend.stats.source_reads == 0
    _assert_workers_stopped()


def test_owner_preparation_releases_ownership_after_exception(
    backend: IndexedAlongTrackBackend,
) -> None:
    """Release exclusive preparation even when its caller raises.

    :param backend: Synthetic direct backend.
    """
    index = backend._index
    with pytest.raises(ValueError, match="injected owner failure"):
        with index.preparation() as prepare_window:
            assert index._stream_lock.locked()
            window = prepare_window(DATES[0], timedelta(0), None)
            assert index._window is window
            with pytest.raises(RuntimeError, match="stream.*active"):
                index.require_idle()
            with pytest.raises(RuntimeError, match="stream.*active"):
                index.prepare_window(DATES[0], timedelta(0))
            raise ValueError("injected owner failure")
    assert not index._stream_lock.locked()
    index.require_idle()
    assert index.prepare_window(DATES[0], timedelta(0)) is window


@pytest.mark.parametrize("nearest", [False, True], ids=["radius", "nearest"])
def test_stream_preserves_source_change_errors(
    backend: IndexedAlongTrackBackend,
    source_layout: tuple[Path, Path, Path],
    nearest: bool,
) -> None:
    """Changed sources must raise rather than appear as missing observations.

    :param backend: Backend with the original pinned inventory.
    :param source_layout: Product root, cache, and mutable source copy.
    :param nearest: Query mode.
    """
    _, _, source = source_layout
    stat = source.stat()
    os.utime(source, ns=(stat.st_atime_ns, stat.st_mtime_ns + 2_000_000_000))
    with pytest.raises(RuntimeError, match="source changed"):
        list(_stream(backend, nearest))
    _assert_workers_stopped()
    backend.refresh()
    assert len(list(_stream(backend, nearest))) == len(DATES)


@pytest.mark.parametrize("nearest", [False, True], ids=["radius", "nearest"])
def test_workers_share_prepared_arrays_and_preserve_geometry_statistics(
    backend: IndexedAlongTrackBackend, nearest: bool
) -> None:
    """Repeated centers reuse one window and account for every cached lookup.

    :param backend: Synthetic direct backend.
    :param nearest: Query mode.
    """
    backend.prepare_grid([LATITUDE[1]], [LONGITUDE[1]], radius=0)
    results = list(
        _stream(
            backend,
            nearest,
            latitudes=[LATITUDE[1]] * 60,
            longitudes=[LONGITUDE[1]] * 60,
            dates=[DATES[1]] * 60,
        )
    )
    assert len(results) == backend.stats.geometry_hits == 60
    assert backend.stats.source_reads == backend.stats.window_builds == 1
    for _, result in results:
        _assert_source_row(result, 1)
    _assert_workers_stopped()
