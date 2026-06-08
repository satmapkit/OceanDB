import numpy as np
import pytest

from OceanDB.utils.contour_conversion import to_polygon

pytestmark = pytest.mark.unit


def test_to_polygon_converts_each_selected_row_to_a_polygon():
    x = np.array(
        [
            [0.0, 1.0, 1.0, 0.0],
            [10.0, 11.0, 11.0, 10.0],
        ]
    )
    y = np.array(
        [
            [0.0, 0.0, 1.0, 1.0],
            [5.0, 5.0, 6.0, 6.0],
        ]
    )

    polygons = to_polygon(x, y, slice(None))

    assert len(polygons) == 2
    np.testing.assert_allclose(
        polygons[0].exterior.coords[:-1], np.column_stack([x[0], y[0]])
    )
    np.testing.assert_allclose(
        polygons[1].exterior.coords[:-1], np.column_stack([x[1], y[1]])
    )


def test_to_polygon_respects_row_slice():
    x = np.array(
        [
            [0.0, 1.0, 1.0, 0.0],
            [10.0, 11.0, 11.0, 10.0],
            [20.0, 21.0, 21.0, 20.0],
        ]
    )
    y = np.array(
        [
            [0.0, 0.0, 1.0, 1.0],
            [5.0, 5.0, 6.0, 6.0],
            [9.0, 9.0, 10.0, 10.0],
        ]
    )

    polygons = to_polygon(x, y, slice(1, 3))

    assert len(polygons) == 2
    assert polygons[0].bounds == (10.0, 5.0, 11.0, 6.0)
    assert polygons[1].bounds == (20.0, 9.0, 21.0, 10.0)


def test_to_polygon_returns_empty_list_for_empty_selection():
    x = np.array([[0.0, 1.0, 1.0, 0.0]])
    y = np.array([[0.0, 0.0, 1.0, 1.0]])

    polygons = to_polygon(x, y, slice(0, 0))

    assert polygons == []
