"""Deterministic geometry, native batches and canonical all-record rows."""

import hashlib
import json
import math
from collections import Counter
from datetime import datetime, timedelta

import numpy as np

from OceanDB.ocean_data.basins import BasinMask

CONFIGURATIONS = ("ad_a", "ad_b", "amd", "bad", "abd", "bma", "bmad", "mba")
WINDOW_FIELDS = ("longitude", "latitude", "sla_filtered", "delta_t")
RECORD_FIELDS = ("longitude", "latitude", "sla_filtered", "date_time")
DATES = tuple(
    (datetime(2022, 10, 26) + timedelta(days=i)).date().isoformat() for i in range(12)
)
SCENARIOS = ("radius_all", "nn_all", "radius_filtered", "nn_filtered")
RADIUS_M = 50_000.0
SPACING_M = 25_000.0
RECORD_CHUNK_SIZE = 32
EARTH_RADIUS_M = 6_371_008.8
RECORD_DTYPE = np.dtype(
    [
        ("longitude", "<f8"),
        ("latitude", "<f8"),
        ("sla_filtered", "<f8"),
        ("timestamp_us", "<i8"),
    ]
)


def destination(latitude, longitude, bearing, distance):
    """Spherical great-circle projection for reproducible nominal grid spacing."""
    lat, lon, azimuth = map(math.radians, (latitude, longitude, bearing))
    arc = distance / EARTH_RADIUS_M
    end_lat = math.asin(
        math.sin(lat) * math.cos(arc)
        + math.cos(lat) * math.sin(arc) * math.cos(azimuth)
    )
    end_lon = lon + math.atan2(
        math.sin(azimuth) * math.sin(arc) * math.cos(lat),
        math.cos(arc) - math.sin(lat) * math.sin(end_lat),
    )
    return [math.degrees(end_lat), (math.degrees(end_lon) + 180) % 360 - 180]


def local_grid(center=(30.0, -40.0), rows=16, columns=32, spacing_m=SPACING_M):
    """A 512-center North Atlantic patch by default, with adjacent row turns.

    Spacing is nominal spherical distance, not exact WGS84 spheroid distance.
    PostGIS retains its existing geography radius semantics. No point is silently
    dropped or reordered based on a query result or a land mask.
    """
    if rows < 1 or columns < 1 or not math.isfinite(spacing_m) or spacing_m <= 0:
        raise ValueError("Grid dimensions and finite spacing must be positive")
    lat, lon = center
    if not all(map(math.isfinite, center)) or not -80 <= lat <= 80:
        raise ValueError("Use a finite patch center between 80 S and 80 N")
    points = []
    for row in range(rows):
        northing = (row - (rows - 1) / 2) * spacing_m
        row_center = destination(lat, lon, 0 if northing >= 0 else 180, abs(northing))
        order = range(columns) if row % 2 == 0 else reversed(range(columns))
        for column in order:
            easting = (column - (columns - 1) / 2) * spacing_m
            points.append(
                destination(*row_center, 90 if easting >= 0 else 270, abs(easting))
            )
    return points


def execute_batch(along_track, job, samples):
    """Materialize one planned batch; the owning runner supplies timeout/reset.

    Initialize basin metadata before timing/reset. Fingerprints and mean-field
    statistics run after this function's native timing, not inside it.
    """
    points = [samples[job["sample"]][index] for index in job["point_indices"]]
    kwargs = dict(
        fields=job["fields"],
        latitudes=[p[0] for p in points],
        longitudes=[p[1] for p in points],
        missions=job["missions"],
    )
    if job["temporal_scope"] == "entire_stored_record":
        iterator = along_track.geographic_point_in_r_batch(
            **kwargs, radius=job["radius_m"]
        )
    else:
        kwargs.update(
            dates=[datetime.fromisoformat(job["date"])] * len(points),
            time_window=timedelta(days=job.get("half_window_days", 5)),
        )
        if job["method"] == "geographic_point_in_r_dt_batch":
            iterator = along_track.geographic_point_in_r_dt_batch(
                **kwargs, radius=job["radius_m"]
            )
        else:
            iterator = along_track.geographic_nearest_neighbors_batch(
                **kwargs, max_radius=None
            )
    values = [
        None if result is None else tuple(result[field] for field in job["fields"])
        for result in iterator
    ]
    if len(values) != len(points):
        raise ValueError("Native batch did not return one result per requested center")
    return values


def canonical_record(values):
    """Exact row fingerprint layout; timestamps stay integer microseconds."""
    if values is None:
        return np.empty(0, dtype=RECORD_DTYPE)
    longitude, latitude, sla, timestamps = values
    if len({len(v) for v in values}) != 1:
        raise ValueError("Materialized fields must have aligned rows")
    dates = np.asarray(timestamps, dtype="datetime64[us]")
    if np.any(np.isnat(dates)):
        raise ValueError("All-record observations require valid date_time values")
    rows = np.empty(len(longitude), dtype=RECORD_DTYPE)
    for field, values in zip(
        ("longitude", "latitude", "sla_filtered"),
        (longitude, latitude, sla),
        strict=True,
    ):
        rows[field] = values
        rows[field][rows[field] == 0] = 0
        rows[field][np.isnan(rows[field])] = np.nan
    rows["timestamp_us"] = dates.astype("int64")
    return np.sort(rows, order=RECORD_DTYPE.names)


def summarize_record(values):
    """Outside timing: exact canonical hashes and per-center row volume."""
    arrays = [canonical_record(v) for v in values]
    counts = [len(a) for a in arrays]
    hashes = [hashlib.sha256(a.tobytes()).hexdigest() for a in arrays]
    months = Counter()
    for a in arrays:
        keys, sizes = np.unique(
            a["timestamp_us"]
            .astype("datetime64[us]")
            .astype("datetime64[M]")
            .astype(str),
            return_counts=True,
        )
        months.update({key: int(size) for key, size in zip(keys, sizes, strict=True)})
    return dict(
        points=len(values),
        rows=sum(counts),
        empty_locations=counts.count(0),
        counts=counts,
        row_hashes=hashes,
        sha256=hashlib.sha256(
            json.dumps(dict(counts=counts, hashes=hashes), sort_keys=True).encode()
        ).hexdigest(),
        returned_months=dict(months),
        canonical_bytes=sum(a.nbytes for a in arrays),
        byte_qualification="Canonical numeric payload only; excludes Python/libpq overhead and is not measured peak memory",
    )
