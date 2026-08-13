import numpy as np
import pytest

from OceanDB.ocean_data.basins import BasinMask

pytestmark = pytest.mark.uses_database


def test_lookup_float():
    mask = BasinMask()
    out = mask.lookup(-32.32, -36.57)
    assert out == 7


def test_lookup_array():
    mask = BasinMask()
    lats = np.arange(-32.1, -32.0, 0.01)
    lons = np.arange(-36.1, -36.0, 0.01)
    out = mask.lookup(lats, lons)
    assert np.all(out == 7)


def test_basin_is_ocean_single_pos():
    mask = BasinMask()
    assert mask.basin_is_ocean(3)


def test_basin_is_ocean_single_neg():
    mask = BasinMask()
    assert not mask.basin_is_ocean(-1)
    assert not mask.basin_is_ocean(1000)


def test_basin_is_ocean_array():
    mask = BasinMask()
    out = mask.basin_is_ocean(np.array([3, 4, 5, -1, 1000]))
    assert np.all(
        np.equal(out, np.array([True, True, True, False, False], dtype=np.bool))
    )


def test_loc_is_ocean_int_pos():
    mask = BasinMask()
    assert mask.loc_is_ocean(-32.32, -36.57)
    assert mask.loc_is_ocean(32.52, -37.57)


def test_loc_is_ocean_int_neg():
    mask = BasinMask()
    assert not mask.loc_is_ocean(-9.27, -65.30)


def test_loc_is_ocean_int_array():
    mask = BasinMask()
    out = mask.loc_is_ocean(
        np.array([-32.32, 32.50, 73.46, 44.238]),
        np.array([-36.58, -37.3, -40.44, -86.575]),
    )
    assert np.all(np.equal(out, np.array([True, True, False, False], dtype=np.bool)))
