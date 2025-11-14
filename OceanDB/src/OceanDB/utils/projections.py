import numpy as np
import numpy.typing as npt


def latitude_longitude_bounds_for_transverse_mercator_box(
        lat0: float|npt.NDArray[np.floating],
        lon0: float|npt.NDArray[np.floating],
        Lx: float,
        Ly: float
    ):
    """
    lat0:
    lon0:
    lat: latitude
    longitude: longitude
    """
    [x0, y0] = latitude_longitude_to_spherical_transverse_mercator(lat0, lon0, lon0=lon0)
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
        lat: float|npt.NDArray[np.floating],
        lon: float|npt.NDArray[np.floating],
        lon0: float|npt.NDArray[np.floating]
    ):
    """
    Accepts a latitude & longitude and returns a projected coordinate according to transverse mercator
    lat: latitude
    longitude: longitude
    lon0:
    """
    k0 = 0.9996
    WGS84a = 6378137.
    R = k0 * WGS84a
    phi = np.array(lat) * np.pi / 180
    deltaLambda = (np.array(lon) - np.array(lon0)) * np.pi / 180
    sinLambdaCosPhi = np.sin(deltaLambda) * np.cos(phi)
    x = (R / 2) * np.log((1 + sinLambdaCosPhi) / (1 - sinLambdaCosPhi))
    y = R * np.arctan(np.tan(phi) / np.cos(deltaLambda))
    return x, y


def spherical_transverse_mercator_to_latitude_longitude(
        x: float|npt.NDArray[np.floating],
        y: float|npt.NDArray[np.floating],
        lon0: float|npt.NDArray[np.floating]):
    """

    """
    k0 = 0.9996
    WGS84a = 6378137
    R = k0 * WGS84a
    lon = (180 / np.pi) * np.arctan(np.sinh(x / R) / np.cos(y / R)) + lon0
    lat = (180 / np.pi) * np.arcsin(np.sin(y / R) / np.cosh(x / R))
    return lat, lon
