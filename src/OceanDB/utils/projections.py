import numpy as np
import numpy.typing as npt


def latitude_longitude_bounds_for_transverse_mercator_box(
    lat0: float | npt.NDArray[np.floating],
    lon0: float | npt.NDArray[np.floating],
    Lx: float,
    Ly: float,
):
    """
    Given a bounding box in projected coordinates
    (in the transverse Mercator projection centered around lon0),
    compute a geographic bounding box that encloses the projected box.

    :param lat0:
        center latitude of the bounding box

    :param lon0:
        center longitude of the bounding box

    :param Lx:
        x-range of the bounding box in projected coordinates

    :param Ly:
        y-range of the bounding box in projected coordinates

    :return:
        #. center x-coordinate (projected)
        #. center y-coordinate (projected)
        #. minimum latitude of the bounding box
        #. minimum longitude of the bounding box
        #. maximum latitude of the bounding box
        #. maximum longitude of the bounding box

    """
    [x0, y0] = latitude_longitude_to_spherical_transverse_mercator(
        lat0, lon0, lon0=lon0
    )
    x = np.zeros(6)
    y = np.zeros(6)

    x[1] = x0 - Lx / 2
    y[1] = y0 - Ly / 2

    x[2] = x0 - Lx / 2
    y[2] = y0 + Ly / 2

    x[3] = x0
    y[3] = y0 + Ly / 2

    x[4] = x0
    y[4] = y0 - Ly / 2

    x[5] = x0 + Lx / 2
    y[5] = y0 - Ly / 2

    x[0] = x0 + Lx / 2
    y[0] = y0 + Ly / 2

    [lats, lons] = spherical_transverse_mercator_to_latitude_longitude(x, y, lon0)
    minLat = min(lats)
    maxLat = max(lats)
    minLon = min(lons)
    maxLon = max(lons)

    return x0, y0, minLat, minLon, maxLat, maxLon


def latitude_longitude_to_spherical_transverse_mercator(
    lat: float | npt.NDArray[np.floating],
    lon: float | npt.NDArray[np.floating],
    lon0: float | npt.NDArray[np.floating],
):
    """
    Project a lat/lon to x/y in the transverse Mercator projection centered around `lon0`.

    :param lat:
        single latitude or n-array of latitudes to project

    :param lon:
        single longitude or n-array longitudes to project

    :param lon0:
        reference longitude to center the projection around

    :returns:
        x, y as numpy arrays of same shape as lat and lon

    """
    k0 = 0.9996
    WGS84a = 6378137.0
    R = k0 * WGS84a
    phi = np.array(lat) * np.pi / 180
    deltaLambda = (np.array(lon) - np.array(lon0)) * np.pi / 180
    sinLambdaCosPhi = np.sin(deltaLambda) * np.cos(phi)
    x = (R / 2) * np.log((1 + sinLambdaCosPhi) / (1 - sinLambdaCosPhi))
    y = R * np.arctan(np.tan(phi) / np.cos(deltaLambda))
    return x, y


def spherical_transverse_mercator_to_latitude_longitude(
    x: float | npt.NDArray[np.floating],
    y: float | npt.NDArray[np.floating],
    lon0: float | npt.NDArray[np.floating],
):
    """ """
    k0 = 0.9996
    WGS84a = 6378137
    R = k0 * WGS84a
    lon = (180 / np.pi) * np.arctan(np.sinh(x / R) / np.cos(y / R)) + lon0
    lat = (180 / np.pi) * np.arcsin(np.sin(y / R) / np.cosh(x / R))
    return lat, lon
