"""Prepared queries must retain their arrays and provenance across window changes."""

import gc
import shutil
from collections.abc import Sequence
from datetime import datetime, timedelta
from pathlib import Path
from weakref import ref

import netCDF4 as nc
import numpy as np
import pytest

from OceanDB.data_access.along_track import Mission
from OceanDB.data_access.indexed_along_track import IndexedAlongTrackBackend
from tests.along_track.test_indexed_backend import (
    DATES,
    FULL_FIELDS,
    LATITUDE,
    LONGITUDE,
    TIME_RAW,
    _assert_source_row,
    forbid_database_connections,
    source_layout,
)

pytestmark = pytest.mark.unit


@pytest.mark.parametrize("nearest", [False, True], ids=["radius", "nearest"])
def test_query_snapshot_survives_window_change_and_refresh(
    source_layout: tuple[Path, Path, Path], nearest: bool
) -> None:
    """Use the original rows and source metadata after another mission is active.

    :param source_layout: Synthetic product root, cache, and source file.
    :param nearest: Exercise nearest ranking rather than radius membership.
    """
    product, cache, source = source_layout
    other = source.with_name(source.name.replace("_j2_", "_j3_"))
    shutil.copyfile(source, other)
    with nc.Dataset(other, "r+") as dataset:
        variable = dataset["time"]
        variable.set_auto_maskandscale(False)
        variable[:] = np.asarray(TIME_RAW) + 100

    backend = IndexedAlongTrackBackend(product, cache)
    index = backend._index
    window = index.prepare_window(DATES[1], timedelta(0), ["j2"])
    assert index._window is window
    assert not any(
        array.flags.writeable
        for array in (window.rows, window._offsets, window._vectors)
    )

    replacement = index.prepare_window(
        DATES[1] + timedelta(days=100), timedelta(0), ["j3"]
    )
    assert index._window is replacement and replacement is not window
    assert replacement._sources[0].mission == "j3"
    for refresh in (False, True):
        if refresh:
            backend.refresh()
        _assert_source_row(
            backend._query(
                window,
                FULL_FIELDS,
                LATITUDE[1],
                LONGITUDE[1],
                DATES[1],
                0.0,
                nearest=nearest,
            ),
            1,
        )


def test_exact_window_returns_the_same_handle(
    source_layout: tuple[Path, Path, Path],
) -> None:
    """Publish one handle and reuse it for equivalent mission selections.

    :param source_layout: Synthetic product root, cache, and source file.
    """
    product, cache, _ = source_layout
    backend = IndexedAlongTrackBackend(product, cache)
    index = backend._index
    assert index._window is None
    window = index.prepare_window(DATES[1], timedelta(0), ["j2"])
    assert index.prepare_window(DATES[1], timedelta(0), ["j2", "j2"]) is window
    backend.prepare_window(DATES[1], timedelta(0))
    assert index._window is window
    assert backend.stats.window_builds == 1


def test_window_remains_usable_without_retaining_its_owner(
    source_layout: tuple[Path, Path, Path],
) -> None:
    """Keep observation dependencies alive without keeping the caches alive.

    :param source_layout: Synthetic product root, cache, and source file.
    """
    product, cache, source = source_layout
    backend = IndexedAlongTrackBackend(product, cache)
    window = backend._index.prepare_window(DATES[1], timedelta(0))
    owner = ref(backend._index)
    del backend
    gc.collect()
    assert owner() is None
    rows = window.candidates(LATITUDE[1], LONGITUDE[1], 0.0)
    np.testing.assert_array_equal(window.column("mission", rows), ["j2"])
    np.testing.assert_array_equal(window.column("file_name", rows), [source.name])


@pytest.mark.parametrize("nearest", [False, True], ids=["radius", "nearest"])
def test_query_uses_window_interface_without_private_layout_or_provenance(
    source_layout: tuple[Path, Path, Path],
    monkeypatch: pytest.MonkeyPatch,
    nearest: bool,
) -> None:
    """Query through candidates/column without knowing cell or source mappings.

    :param source_layout: Synthetic product root, cache, and source file.
    :param monkeypatch: Scoped owner preparation patcher.
    :param nearest: Exercise nearest ranking rather than radius membership.
    """
    product, cache, _ = source_layout
    backend = IndexedAlongTrackBackend(product, cache)
    window = backend._index.prepare_window(DATES[1], timedelta(0))

    class QueryWindow:
        rows = window.rows[
            [
                name
                for name in window.rows.dtype.names or ()
                if name not in ("file_id", "source_row")
            ]
        ]
        candidates = staticmethod(window.candidates)
        column = staticmethod(window.column)

    def prepare_window(
        date: datetime,
        time_window: timedelta,
        missions: Sequence[Mission] | None = None,
    ) -> QueryWindow:
        return QueryWindow()

    monkeypatch.setattr(backend._index, "prepare_window", prepare_window)
    if nearest:
        result = backend.geographic_nearest_neighbors(
            FULL_FIELDS,
            LATITUDE[1],
            LONGITUDE[1],
            DATES[1],
            time_window=timedelta(0),
            max_radius=0,
        )
    else:
        result = backend.geographic_point_in_r_dt(
            FULL_FIELDS,
            LATITUDE[1],
            LONGITUDE[1],
            DATES[1],
            radius=0,
            time_window=timedelta(0),
        )
    _assert_source_row(result, 1)
