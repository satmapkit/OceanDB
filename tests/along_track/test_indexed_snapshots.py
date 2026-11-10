"""Prepared queries must retain their arrays and provenance across window changes."""

import shutil
from datetime import timedelta
from pathlib import Path

import netCDF4 as nc
import numpy as np
import pytest

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
    backend.prepare_window(DATES[1], timedelta(0), ["j2"])
    window = backend._snapshot()
    assert window.rows is backend._active
    assert window.offsets is backend._offsets
    assert window.vectors is backend._vectors
    assert not any(
        array.flags.writeable for array in (window.rows, window.offsets, window.vectors)
    )

    backend.prepare_window(DATES[1] + timedelta(days=100), timedelta(0), ["j3"])
    assert backend._active_sources[0].mission == "j3"
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


def test_snapshot_requires_a_prepared_window(
    source_layout: tuple[Path, Path, Path],
) -> None:
    """Reject capture before preparation instead of constructing partial state.

    :param source_layout: Synthetic product root, cache, and source file.
    """
    product, cache, _ = source_layout
    with pytest.raises(RuntimeError, match="has not been prepared"):
        IndexedAlongTrackBackend(product, cache)._snapshot()
