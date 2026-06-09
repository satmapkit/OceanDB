import netCDF4 as nc
import numpy as np
from shapely import Polygon


def to_polygon(x: nc.Variable, y: nc.Variable, rows: slice) -> list[Polygon]:
    """
    Given a variable in a netcdf dataset,
    convert the datetime to a format suitable for
    entry into a postgres database.

    :param var:
        The netcdf variable to convert

    :param rows:
        Which rows to convert

    :returns:
        list of polygons
    """
    return [
        Polygon(np.vstack([x_row, y_row]).transpose())
        for x_row, y_row in zip(x[rows], y[rows])
    ]
