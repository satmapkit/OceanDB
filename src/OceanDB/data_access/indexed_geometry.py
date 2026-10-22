"""Conservative spatial indexing and WGS84 distances for serial AlongTrack queries.

The distance and grid algorithms are adapted from QuickMap's uncommitted
working-tree ``src/quickmap/geometry.py`` (base revision
``1e48eed889a097882515380e2764cfad650831ef``, source SHA256
``fc2ad7fee6dc6ec2e500920471271c2d3d8195bf6c3fb85d9d5fb7cdee13c720``).
Copyright (c) 2026 Cimarron Wortham, MIT; the complete notice is distributed
as ``QUICKMAP_LICENSE.txt`` alongside this module.
Cell covers are only candidate filters; inclusive membership and nearest-neighbor
ranking use WGS84 distances, with :func:`exact_wgs84_distance_m` resolving
decisive boundaries and ties without a spatial selection tolerance.
"""

import math
from importlib import import_module
from typing import Protocol, runtime_checkable

import numpy as np

# Smaller than the minimum WGS84 meridional radius of curvature, so a
# WGS84 geodesic of length r cannot have a spherical angular separation
# exceeding r / CAP_EARTH_RADIUS_M in geodetic latitude/longitude coordinates.
CAP_EARTH_RADIUS_M = 6_335_000.0
WGS84_A_M = 6_378_137.0
WGS84_F = 1 / 298.257223563
WGS84_B_M = WGS84_A_M * (1 - WGS84_F)
MAX_WGS84_DISTANCE_M = 20_003_932.0


@runtime_checkable
class _InverseGeodesic(Protocol):
    """The small GeographicLib interface needed for Vincenty's fallback."""

    def Inverse(
        self, lat1: float, lon1: float, lat2: float, lon2: float
    ) -> dict[str, float]:
        """Return an inverse geodesic record including ``s12``.

        :param lat1: Starting latitude in degrees.
        :param lon1: Starting longitude in degrees.
        :param lat2: Ending latitude in degrees.
        :param lon2: Ending longitude in degrees.
        :returns: GeographicLib's inverse record with distance in meters.
        """
        ...


def exact_wgs84_distance_m(
    longitudes: np.ndarray, latitudes: np.ndarray, longitude: float, latitude: float
) -> np.ndarray:
    """Resolve decisive boundary/tie distances with GeographicLib's inverse.

    The vectorized Vincenty path is used for the broad candidate set; this
    scalar path is reserved for candidates within 1 cm of a radius or a
    nearest-neighbor cutoff, where sub-millimeter rounding could alter which
    observations are selected or their deterministic order.

    :param longitudes: Candidate longitudes in degrees.
    :param latitudes: Candidate latitudes in degrees.
    :param longitude: Query-center longitude in degrees.
    :param latitude: Query-center latitude in degrees.
    :returns: Exact inverse-geodesic distances in meters.
    :raises TypeError: If GeographicLib's required interface is unavailable.
    """
    geodesic_type = getattr(import_module("geographiclib.geodesic"), "Geodesic", None)
    geodesic = getattr(geodesic_type, "WGS84", None)
    if not isinstance(geodesic, _InverseGeodesic):
        raise TypeError("GeographicLib does not provide a WGS84 inverse geodesic")
    return np.asarray(
        [
            geodesic.Inverse(float(latitude), float(longitude), float(lat), float(lon))[
                "s12"
            ]
            for lon, lat in zip(longitudes, latitudes, strict=True)
        ],
        dtype=np.float64,
    )


def unit_vectors(
    longitude: np.ndarray | float, latitude: np.ndarray | float
) -> np.ndarray:
    """Return unit-sphere vectors for broadcastable geodetic coordinates.

    :param longitude: Longitudes in degrees, scalar or array.
    :param latitude: Latitudes in degrees, scalar or array.
    :returns: Vectors with a final dimension of length three. At the poles,
        longitude has no effect.
    """
    lon = np.deg2rad(longitude)
    lat = np.deg2rad(latitude)
    coslat = np.where(np.abs(latitude) == 90, 0.0, np.cos(lat))
    return np.stack((coslat * np.cos(lon), coslat * np.sin(lon), np.sin(lat)), axis=-1)


def wgs84_distance_m(
    longitudes: np.ndarray, latitudes: np.ndarray, longitude: float, latitude: float
) -> np.ndarray:
    """Compute inverse WGS84 ellipsoidal distances from coordinates to a center.

    Vincenty's inverse formula is vectorized for ordinary points; the rare
    nonconvergent, near-antipodal points use GeographicLib instead. Callers
    should refine near-boundary and near-tie distances using
    :func:`exact_wgs84_distance_m` before final selection and ranking.

    :param longitudes: Observation longitudes in degrees, broadcastable with
        ``latitudes``.
    :param latitudes: Observation latitudes in degrees.
    :param longitude: Query-center longitude in degrees.
    :param latitude: Query-center latitude in degrees.
    :returns: WGS84 geodesic distances in meters, with broadcast input shape.
    """
    lon, lat, target_lon, target_lat = np.broadcast_arrays(
        np.asarray(longitudes, dtype=float),
        np.asarray(latitudes, dtype=float),
        np.asarray(longitude, dtype=float),
        np.asarray(latitude, dtype=float),
    )
    shape = lon.shape
    lon1 = np.deg2rad(lon.ravel())
    lat1 = np.deg2rad(lat.ravel())
    lon2 = np.deg2rad(target_lon.ravel())
    lat2 = np.deg2rad(target_lat.ravel())
    reduced1 = np.arctan((1 - WGS84_F) * np.tan(lat1))
    reduced2 = np.arctan((1 - WGS84_F) * np.tan(lat2))
    sin_u1, cos_u1 = np.sin(reduced1), np.cos(reduced1)
    sin_u2, cos_u2 = np.sin(reduced2), np.cos(reduced2)
    delta_lon = (lon1 - lon2 + np.pi) % (2 * np.pi) - np.pi
    lam = delta_lon.copy()
    converged = np.zeros(lam.shape, dtype=bool)

    for _ in range(100):
        sin_lam, cos_lam = np.sin(lam), np.cos(lam)
        sin_sigma = np.sqrt(
            (cos_u2 * sin_lam) ** 2 + (cos_u1 * sin_u2 - sin_u1 * cos_u2 * cos_lam) ** 2
        )
        cos_sigma = sin_u1 * sin_u2 + cos_u1 * cos_u2 * cos_lam
        sigma = np.arctan2(sin_sigma, cos_sigma)
        sin_alpha = np.divide(
            cos_u1 * cos_u2 * sin_lam,
            sin_sigma,
            out=np.zeros_like(sin_sigma),
            where=sin_sigma != 0,
        )
        cos2_alpha = 1 - sin_alpha**2
        cos2_sigma_m = cos_sigma - np.divide(
            2 * sin_u1 * sin_u2,
            cos2_alpha,
            out=np.zeros_like(cos_sigma),
            where=cos2_alpha > 1e-15,
        )
        coefficient = WGS84_F / 16 * cos2_alpha * (4 + WGS84_F * (4 - 3 * cos2_alpha))
        updated = delta_lon + (1 - coefficient) * WGS84_F * sin_alpha * (
            sigma
            + coefficient
            * sin_sigma
            * (cos2_sigma_m + coefficient * cos_sigma * (-1 + 2 * cos2_sigma_m**2))
        )
        newly_converged = (~converged) & (np.abs(updated - lam) < 1e-12)
        lam = np.where(converged, lam, updated)
        converged |= newly_converged
        if np.all(converged):
            break

    sin_lam, cos_lam = np.sin(lam), np.cos(lam)
    sin_sigma = np.sqrt(
        (cos_u2 * sin_lam) ** 2 + (cos_u1 * sin_u2 - sin_u1 * cos_u2 * cos_lam) ** 2
    )
    cos_sigma = sin_u1 * sin_u2 + cos_u1 * cos_u2 * cos_lam
    sigma = np.arctan2(sin_sigma, cos_sigma)
    sin_alpha = np.divide(
        cos_u1 * cos_u2 * sin_lam,
        sin_sigma,
        out=np.zeros_like(sin_sigma),
        where=sin_sigma != 0,
    )
    cos2_alpha = 1 - sin_alpha**2
    cos2_sigma_m = cos_sigma - np.divide(
        2 * sin_u1 * sin_u2,
        cos2_alpha,
        out=np.zeros_like(cos_sigma),
        where=cos2_alpha > 1e-15,
    )
    u2 = cos2_alpha * (WGS84_A_M**2 - WGS84_B_M**2) / WGS84_B_M**2
    a_coeff = 1 + u2 / 16384 * (4096 + u2 * (-768 + u2 * (320 - 175 * u2)))
    b_coeff = u2 / 1024 * (256 + u2 * (-128 + u2 * (74 - 47 * u2)))
    delta_sigma = (
        b_coeff
        * sin_sigma
        * (
            cos2_sigma_m
            + b_coeff
            / 4
            * (
                cos_sigma * (-1 + 2 * cos2_sigma_m**2)
                - b_coeff
                / 6
                * cos2_sigma_m
                * (-3 + 4 * sin_sigma**2)
                * (-3 + 4 * cos2_sigma_m**2)
            )
        )
    )
    distances = WGS84_B_M * a_coeff * (sigma - delta_sigma)
    distances[sin_sigma == 0] = 0
    same_pole = (np.abs(lat1) == np.pi / 2) & (lat1 == lat2)
    distances[same_pole] = 0

    if np.any(~converged):
        missing = np.flatnonzero(~converged)
        distances[missing] = exact_wgs84_distance_m(
            np.rad2deg(lon1[missing]),
            np.rad2deg(lat1[missing]),
            longitude,
            latitude,
        )
    return distances.reshape(shape)


def cell_dimensions(cell_size: float) -> tuple[int, int]:
    """Return longitude and latitude cell counts for a global regular grid.

    :param cell_size: Cell width in degrees; must evenly divide 180 and 360.
    :returns: ``(longitude_count, latitude_count)``.
    :raises ValueError: If the cell width is invalid.
    """
    size = float(cell_size)
    if not math.isfinite(size) or size <= 0 or size > 180:
        raise ValueError("cell_size_degrees must be in (0, 180]")
    nx, ny = round(360 / size), round(180 / size)
    if not math.isclose(nx * size, 360) or not math.isclose(ny * size, 180):
        raise ValueError("cell_size_degrees must divide both 180 and 360")
    if nx * ny > 10_000_000:
        raise ValueError("Spatial cell index is too large; increase cell_size_degrees")
    return nx, ny


def cell_ids(
    longitude: np.ndarray | float, latitude: np.ndarray | float, cell_size: float
) -> np.ndarray:
    """Assign broadcastable points to row-major global grid cells.

    :param longitude: Longitude in degrees (wrapped to ``[0, 360)``).
    :param latitude: Latitude in degrees (the north pole uses the top row).
    :param cell_size: Cell width in degrees.
    :returns: Row-major integer IDs, with broadcast input shape.
    :raises ValueError: If the cell width is invalid.
    """
    nx, ny = cell_dimensions(cell_size)
    x = np.floor((np.asarray(longitude) % 360) / cell_size).astype(np.int32)
    y = np.clip(np.floor((np.asarray(latitude) + 90) / cell_size), 0, ny - 1).astype(
        np.int32
    )
    return y * nx + np.minimum(x, nx - 1)


def cap_cells(x0: float, y0: float, r: float, cell_size: float) -> np.ndarray:
    """Return a conservative cell cover for a WGS84 radius query.

    The cover and any chord-distance screen are only candidate pruning steps.
    A caller must use :func:`wgs84_distance_m` for final inclusive membership;
    this cover may include observations outside the requested radius.

    :param x0: Center longitude in degrees.
    :param y0: Center latitude in degrees.
    :param r: Nonnegative radius in meters.
    :param cell_size: Cell width in degrees.
    :returns: Unique row-major cell IDs covering all eligible coordinates.
    :raises ValueError: If coordinates, radius, or cell width are invalid.
    """
    nx, ny = cell_dimensions(cell_size)
    if not all(math.isfinite(value) for value in (x0, y0, r)):
        raise ValueError("Longitude, latitude, and radius must be finite")
    if not -90 <= y0 <= 90:
        raise ValueError("Latitude must be between -90 and 90 degrees")
    if r < 0:
        raise ValueError("r must be a nonnegative radius in meters")
    if r >= MAX_WGS84_DISTANCE_M:
        return np.arange(nx * ny, dtype=np.int32)

    x0 = x0 % 360
    angular = min(r / CAP_EARTH_RADIUS_M, math.pi)
    delta = math.degrees(angular)
    # Degree padding protects exact cell-edge and pole tangencies from roundoff.
    padding = 1e-10
    lower = max(-90, y0 - delta - padding)
    upper = min(90, y0 + delta + padding)
    first = max(0, int(math.floor((lower + 90) / cell_size)))
    last = min(ny - 1, int(math.floor((upper + 90) / cell_size)))
    if abs(y0) + delta + padding >= 90:
        xs = np.arange(nx, dtype=np.int32)
    else:
        width = math.degrees(
            math.asin(min(1.0, math.sin(angular) / math.cos(math.radians(y0))))
        )
        a = math.floor((x0 - width - padding) / cell_size)
        b = math.floor((x0 + width + padding) / cell_size)
        xs = np.unique(np.arange(a, b + 1, dtype=np.int32) % nx)
    ys = np.arange(first, last + 1, dtype=np.int32)
    return (ys[:, None] * nx + xs).ravel()
