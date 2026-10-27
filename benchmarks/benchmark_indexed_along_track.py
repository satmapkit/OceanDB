"""Synthetic-only, database-free indexed along-track baseline.

No actual user workload was supplied. All generated sources, prepared artifacts,
and JSON evidence stay in a unique directory under ``/tmp/opencode``. Timings do
not include equivalence checks or lazy access to scaled result columns.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import platform
import statistics
import subprocess
import sys
import tempfile
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from importlib import import_module, metadata, resources
from pathlib import Path
from time import get_clock_info, perf_counter
from typing import Literal, Protocol, TypeAlias, TypeVar, runtime_checkable

import netCDF4 as nc
import numpy as np
import numpy.typing as npt

from OceanDB.data_access import indexed_along_track as indexed
from OceanDB.data_access.along_track import Mission
from OceanDB.ocean_data import ocean_data
from OceanDB.ocean_data.dataset import Dataset
from OceanDB.schemas import along_track_schema as schema
from OceanDB.schemas.along_track_schema import along_track_fields

BASE_REVISION = "766db427da69f0f682604bd2dfa7e33ae6a9fb95"
REPO = Path(__file__).resolve().parents[1]
EPOCH = datetime(2024, 1, 1)
DATE = EPOCH + timedelta(days=4)
HALF_WINDOW = timedelta(days=1)
RADIUS_M = 300_000.0
CENTERS = ((0.0, 180.0), (0.75, 180.75), (-1.0, 179.5), (45.0, 20.0))
ADJACENT_DATES = tuple(DATE + timedelta(hours=hours) for hours in (6, 12, 24))
MISSIONS: tuple[Mission, ...] = ("j2", "j3", "s3a")
SCIENCE: tuple[along_track_fields, ...] = (
    "sla_unfiltered",
    "sla_filtered",
    "dac",
    "ocean_tide",
    "internal_tide",
    "lwe",
    "mdt",
    "tpa_correction",
)
FIELDS: tuple[along_track_fields, ...] = (
    "latitude",
    "longitude",
    "date_time",
    "file_name",
    "mission",
    "track",
    "cycle",
    "basin_id",
    *SCIENCE,
    "delta_t",
    "distance",
)
CODE_PATHS = (
    "benchmarks/benchmark_indexed_along_track.py",
    "src/OceanDB/data_access/indexed_along_track.py",
    "src/OceanDB/data_access/indexed_geometry.py",
    "src/OceanDB/ocean_data/basins.py",
    "src/OceanDB/ocean_data/dataset.py",
    "src/OceanDB/ocean_data/ocean_data.py",
    "src/OceanDB/schemas/along_track_schema.py",
)
Kind = Literal["radius", "nearest"]
Scalar: TypeAlias = str | int | float | datetime
Result: TypeAlias = Dataset[along_track_fields] | None
Key: TypeAlias = tuple[datetime, float, float]
T = TypeVar("T")
PROBE_TIMING_METRICS = (
    "decode_seconds",
    "file_load_inclusive_seconds",
    "prepare_window_inclusive_seconds",
    "file_load_excluding_decode_seconds",
    "window_excluding_file_load_seconds",
    "query_wall_excluding_window_seconds",
)


@dataclass
class ProbeTimings:
    """Serial nested boundary timings, separate from public backend statistics."""

    decode_seconds: float = 0.0
    file_load_inclusive_seconds: float = 0.0
    prepare_window_inclusive_seconds: float = 0.0
    decode_calls: int = 0
    file_load_calls: int = 0
    prepare_window_calls: int = 0

    def snapshot(self) -> dict[str, int | float]:
        """Copy cumulative timing totals and instrumentation work counts.

        :returns: Inclusive seconds, call counts, and exact wrapper clock-read count.
        """
        return {
            "decode_seconds": self.decode_seconds,
            "file_load_inclusive_seconds": self.file_load_inclusive_seconds,
            "prepare_window_inclusive_seconds": self.prepare_window_inclusive_seconds,
            "decode_calls": self.decode_calls,
            "file_load_calls": self.file_load_calls,
            "prepare_window_calls": self.prepare_window_calls,
            "perf_counter_reads": 2
            * (self.decode_calls + self.file_load_calls + self.prepare_window_calls),
        }


class TimedIndexedBackend(indexed.IndexedAlongTrackBackend):
    """Benchmark-only serial probes around the existing preparation boundaries.

    Each override delegates exactly once, returns the original object unchanged,
    and retains exception behavior. No public statistics or cache policy changes.
    Future extraction must move these probes to equivalent ownership boundaries.
    """

    def __init__(
        self,
        data_root: str | Path,
        cache_dir: str | Path,
        *,
        cell_size_degrees: float = 2.0,
        max_cache_bytes: int = 2_000_000_000,
        max_geometry_cache_bytes: int = 1_000_000_000,
    ) -> None:
        """Initialize timers before constructing the otherwise unchanged backend.

        :param data_root: Generated source root, as for the original constructor.
        :param cache_dir: Run-owned cache directory.
        :param cell_size_degrees: Original spatial cell width.
        :param max_cache_bytes: Original file-array retention budget.
        :param max_geometry_cache_bytes: Original geometry-array retention budget.
        """
        self.probes = ProbeTimings()
        super().__init__(
            data_root,
            cache_dir,
            cell_size_degrees=cell_size_degrees,
            max_cache_bytes=max_cache_bytes,
            max_geometry_cache_bytes=max_geometry_cache_bytes,
        )

    def _prepare_file(
        self, source: indexed._Source
    ) -> np.ndarray[tuple[int], np.dtype[np.void]]:
        """Time complete source decoding/validation, excluding artifact publication.

        :param source: Original pinned source record at this private boundary.
        :returns: The exact structured row array returned by the original method.
        """
        started = perf_counter()
        try:
            return super()._prepare_file(source)
        finally:
            self.probes.decode_seconds += perf_counter() - started
            self.probes.decode_calls += 1

    def _load_file(
        self, source: indexed._Source
    ) -> np.ndarray[tuple[int], np.dtype[np.void]]:
        """Time complete file loading, including nested decoding when needed.

        :param source: Original pinned source record at this private boundary.
        :returns: The original retained, mapped, or freshly prepared row array.
        """
        started = perf_counter()
        try:
            return super()._load_file(source)
        finally:
            self.probes.file_load_inclusive_seconds += perf_counter() - started
            self.probes.file_load_calls += 1

    def prepare_window(
        self,
        date: datetime,
        time_window: timedelta,
        missions: Sequence[Mission] | None = None,
    ) -> None:
        """Time the whole window call, including exact-key hits and source checks.

        :param date: Original center datetime.
        :param time_window: Original temporal half-width.
        :param missions: Original literal mission selection, unchanged.
        """
        started = perf_counter()
        try:
            super().prepare_window(date, time_window, missions)
        finally:
            self.probes.prepare_window_inclusive_seconds += perf_counter() - started
            self.probes.prepare_window_calls += 1


@runtime_checkable
class InverseGeodesic(Protocol):
    """Narrow runtime-checked boundary for GeographicLib's untyped package."""

    def Inverse(self, lat1: float, lon1: float, lat2: float, lon2: float) -> object:
        """Compute an inverse record whose distance is validated by the oracle.

        :param lat1: Starting latitude in degrees.
        :param lon1: Starting longitude in degrees.
        :param lat2: Ending latitude in degrees.
        :param lon2: Ending longitude in degrees.
        :returns: GeographicLib record, validated before use.
        """
        ...


def oracle_distance(
    geodesic: InverseGeodesic, query: tuple[float, float], row: Row
) -> float:
    """Validate GeographicLib's untyped return value before using its distance.

    :param geodesic: Runtime-checked WGS84 inverse interface.
    :param query: Paired query latitude and longitude.
    :param row: Generator-owned observation.
    :returns: Finite, nonnegative WGS84 distance in meters.
    :raises TypeError: If the installed package returns an unsupported record.
    """
    record = geodesic.Inverse(*query, row.latitude, row.longitude)
    if not isinstance(record, dict):
        raise TypeError("GeographicLib inverse did not return a record")
    distance: object = record.get("s12")
    if (
        not isinstance(distance, (int, float))
        or not math.isfinite(distance)
        or distance < 0
    ):
        raise TypeError("GeographicLib inverse did not return a valid distance")
    return float(distance)


@dataclass(frozen=True)
class Row:
    """Generator-owned scientific values, independent of the production reader."""

    date: datetime
    latitude: float
    longitude: float
    file_name: str
    mission: Mission
    track: int
    cycle: int
    basin: int
    science: tuple[int | None, ...]

    @property
    def key(self) -> Key:
        """Return the scientific membership key.

        :returns: Exact datetime, decoded latitude, and stored longitude.
        """
        return self.date, self.latitude, self.longitude


@dataclass(frozen=True)
class Query:
    """One fixed, ordered center in a measured query batch."""

    latitude: float
    longitude: float
    date: datetime


@dataclass(frozen=True)
class Options:
    """Validated CLI fixture scale and independent retention budgets."""

    rows_per_file: int
    files: int
    repetitions: int
    seed: int
    max_cache_bytes: int
    max_geometry_cache_bytes: int


def digest(value: object) -> str:
    """Hash a canonical JSON description.

    :param value: JSON-compatible value.
    :returns: SHA256 of sorted, compact UTF-8 JSON.
    """
    payload = json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)
    return hashlib.sha256(payload.encode()).hexdigest()


def basin_assets() -> tuple[npt.NDArray[np.int16], dict[int, set[int]], dict[str, str]]:
    """Read basin assets directly, without production lookup/connection helpers.

    :returns: Raster, direct-plus-self adjacency, and asset hashes.
    """
    root = resources.files("OceanDB.data")
    names = (
        "basin_masks/new_basin_mask.nc",
        "basins/basin_connection_table.txt",
    )
    content = {name: root.joinpath(name).read_bytes() for name in names}
    with nc.Dataset("benchmark-basin.nc", memory=content[names[0]]) as dataset:
        dataset.set_auto_mask(False)
        raster = np.asarray(dataset.variables["basinmask"][:], dtype=np.int16)
    connections: dict[int, set[int]] = {}
    for line in content[names[1]].decode().splitlines():
        if line.startswith("HDR"):
            continue
        basin, adjacent = line.split(":", maxsplit=1)
        identifier = int(basin)
        connections[identifier] = {
            identifier,
            *(int(item) for item in adjacent.split(",")),
        }
    return (
        raster,
        connections,
        {name: hashlib.sha256(data).hexdigest() for name, data in content.items()},
    )


def basin_at(raster: npt.NDArray[np.int16], latitude: float, longitude: float) -> int:
    """Look up an independent oracle label in the packaged sixth-degree raster.

    :param raster: Directly read packaged basin raster.
    :param latitude: Valid latitude in ``[-90, 90)``.
    :param longitude: Stored longitude, wrapped only for this lookup.
    :returns: Integer basin label, including land.
    """
    return int(
        raster[math.floor((latitude + 90) * 6), math.floor((longitude % 360) * 6)]
    )


def generate_sources(
    root: Path, options: Options, raster: npt.NDArray[np.int16]
) -> tuple[list[Row], list[dict[str, object]]]:
    """Generate deterministic NetCDF3 sources and independent expected rows.

    :param root: New run-owned source directory.
    :param options: Validated scale and seed.
    :param raster: Independently loaded basin labels.
    :returns: Oracle rows and relative-path source provenance.
    """
    rng = np.random.default_rng(options.seed)
    rows: list[Row] = []
    inventory: list[dict[str, object]] = []
    for file_index in range(options.files):
        mission = MISSIONS[file_index % len(MISSIONS)]
        path = (
            root
            / mission
            / "2024"
            / "01"
            / f"dt_global_{mission}_synthetic_{file_index:04d}.nc"
        )
        path.parent.mkdir(parents=True, exist_ok=True)
        lat_raw = rng.integers(
            -2_000_000, 2_000_001, options.rows_per_file, dtype=np.int32
        )
        lon_raw = rng.integers(
            178_000_000, 182_000_001, options.rows_per_file, dtype=np.int32
        )
        # Integer microseconds are exactly representable as f8 at this scale.
        times = np.asarray(
            [
                index * 8 * 86_400_000_000 // (options.rows_per_file - 1) + file_index
                for index in range(options.rows_per_file)
            ],
            dtype=np.float64,
        )
        science_raw = rng.integers(
            -2_000, 2_001, (len(SCIENCE), options.rows_per_file), dtype=np.int16
        )
        science_raw[:, ::11] = 32767
        science_raw[:, 1::17] = -32766
        correction = 32767 if file_index % 2 else 31 + file_index % 100
        science_raw[-1, :] = correction
        track = 100 + file_index % 30_000
        cycle = 10 + file_index % 30_000
        with nc.Dataset(path, "w", format="NETCDF3_CLASSIC") as dataset:
            dataset.setncattr("title", "SYNTHETIC ONLY; no user workload supplied")
            dataset.createDimension("time", options.rows_per_file)
            time = dataset.createVariable("time", "f8", ("time",))
            time.units = "microseconds since 2024-01-01 00:00:00"
            time.calendar = "proleptic_gregorian"
            time[:] = times
            for name, values in (("latitude", lat_raw), ("longitude", lon_raw)):
                variable = dataset.createVariable(name, "i4", ("time",))
                variable.scale_factor = 0.000001
                variable.add_offset = 0.0
                variable.set_auto_maskandscale(False)
                variable[:] = values
            for name, value in (("track", track), ("cycle", cycle)):
                variable = dataset.createVariable(name, "i2", ())
                variable[...] = value
            for index, name in enumerate(SCIENCE):
                variable = dataset.createVariable(
                    name,
                    "i2",
                    () if name == "tpa_correction" else ("time",),
                    fill_value=32767,
                )
                variable.missing_value = np.int16(-32766)
                variable.scale_factor = 0.001
                variable.add_offset = 0.0
                variable.set_auto_maskandscale(False)
                variable[...] = (
                    correction if name == "tpa_correction" else science_raw[index]
                )
        for index in range(options.rows_per_file):
            latitude = float(lat_raw[index]) * 0.000001
            longitude = float(lon_raw[index]) * 0.000001
            rows.append(
                Row(
                    EPOCH + timedelta(microseconds=int(times[index])),
                    latitude,
                    longitude,
                    path.name,
                    mission,
                    track,
                    cycle,
                    basin_at(raster, latitude, longitude),
                    tuple(
                        None if int(value) in (32767, -32766) else int(value)
                        for value in science_raw[:, index]
                    ),
                )
            )
        inventory.append(
            {
                "path": path.relative_to(root).as_posix(),
                "mission": mission,
                "rows": options.rows_per_file,
                "start": rows[-options.rows_per_file].date.isoformat(),
                "end": rows[-1].date.isoformat(),
                "bytes": path.stat().st_size,
                "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
            }
        )
    if len({row.key for row in rows}) != len(rows):
        raise ValueError("Synthetic generator produced duplicate scientific keys")
    return rows, inventory


def schema_metadata(
    schema: dict[along_track_fields, ocean_data.OceanDataField],
) -> dict[str, object]:
    """Snapshot all result-schema metadata, including SQL/source definitions.

    :param schema: Schema exposed by a result or the reference schema module.
    :returns: JSON-compatible metadata, with stable names for types/callables.
    """
    result: dict[str, object] = {}
    for name, field in schema.items():
        python_type = field.python_type
        item: dict[str, object] = {
            "kind": type(field).__name__,
            "export_name": field.export_name,
            "python_type": (
                None
                if python_type is None
                else f"{python_type.__module__}.{python_type.__qualname__}"
            ),
            "scaling": field.scaling,
            "offset": field.offset,
        }
        if isinstance(field, ocean_data.ColumnField):
            process = field.process_from_netcdf
            item.update(
                {
                    "postgres_table_name": field.postgres_table_name,
                    "postgres_column_name": field.postgres_column_name,
                    "postgres_type": field.postgres_type,
                    "netcdf_unique_name": field.netcdf_unique_name,
                    "process_from_netcdf": (
                        None
                        if process is None
                        else f"{process.__module__}.{process.__qualname__}"
                    ),
                }
            )
        elif isinstance(field, ocean_data.DerivedField):
            item.update(
                {"expression": field.expression, "postgres_type": field.postgres_type}
            )
        else:
            raise TypeError(f"Unrecognized result field type: {type(field)}")
        result[name] = item
    return result


class Oracle:
    """Independent fixture values and GeographicLib exhaustive selections."""

    def __init__(
        self,
        rows: list[Row],
        raster: npt.NDArray[np.int16],
        connections: dict[int, set[int]],
    ) -> None:
        """Precompute independent distances outside every measured phase.

        :param rows: Generator-owned rows, not decoded production output.
        :param raster: Directly read basin raster.
        :param connections: Parsed direct-plus-self connection sets.
        """
        self.rows = rows
        self.distances: dict[tuple[float, float], dict[Key, float]] = {}
        geodesic_type: object = getattr(
            import_module("geographiclib.geodesic"), "Geodesic", None
        )
        geodesic: object = getattr(geodesic_type, "WGS84", None)
        if not isinstance(geodesic, InverseGeodesic):
            raise TypeError("GeographicLib does not expose a WGS84 inverse interface")
        self.schema = schema_metadata(schema.along_track_schema)
        assert set(schema.along_track_schema) == set(FIELDS)
        for name, field in schema.along_track_schema.items():
            assert field.export_name == name
            assert field.scaling == (0.001 if name in SCIENCE else 1)
            assert field.offset == 0
            expected_type: type = (
                datetime
                if name == "date_time"
                else (
                    str
                    if name in ("file_name", "mission")
                    else int if name in ("track", "cycle", "basin_id") else np.float64
                )
            )
            assert field.python_type is expected_type
        for latitude, longitude in CENTERS:
            basin = basin_at(raster, latitude, longitude)
            allowed = connections.get(basin, {basin})
            self.distances[(latitude, longitude)] = {
                row.key: oracle_distance(geodesic, (latitude, longitude), row)
                for row in rows
                if row.basin in allowed
            }

    def expected(self, query: Query, kind: Kind) -> list[tuple[Row, float]]:
        """Exhaustively select rows using oracle time, basin, and WGS84 distance.

        :param query: Fixed center and date.
        :param kind: Radius membership or deterministically ranked nearest three.
        :returns: Expected rows with independent distances; radius has no order contract.
        """
        distances = self.distances[(query.latitude, query.longitude)]
        selected = [
            (row, distances[row.key])
            for row in self.rows
            if query.date - HALF_WINDOW <= row.date <= query.date + HALF_WINDOW
            and row.key in distances
            and distances[row.key] <= RADIUS_M
        ]
        if kind == "nearest":
            selected.sort(key=lambda pair: (pair[1], *pair[0].key))
            return selected[:3]
        return selected

    def check(self, result: Result, query: Query, kind: Kind) -> dict[str, object]:
        """Check complete values, schema, masks, metadata, membership, and ordering.

        :param result: Materialized result, accessed only outside query timing.
        :param query: Corresponding fixed query center.
        :param kind: Query semantics, including mandatory nearest ordering/distance.
        :returns: Row count, dtype metadata, and separate membership/sequence hashes.
        """
        expected = self.expected(query, kind)
        if not expected:
            assert result is None
            return {
                "rows": 0,
                "membership_sha256": digest([]),
                "sequence_sha256": digest([]),
            }
        assert isinstance(result, Dataset)
        assert result.name == "along_track" and result.row_count == len(expected)
        assert list(result) == list(FIELDS)
        assert schema_metadata(dict(result.schema)) == self.schema
        dates = np.asarray(result["date_time"], dtype=object)
        latitudes = np.asarray(result["latitude"], dtype=np.float64)
        longitudes = np.asarray(result["longitude"], dtype=np.float64)
        keys: list[Key] = []
        for date, latitude, longitude in zip(dates, latitudes, longitudes, strict=True):
            if not isinstance(date, datetime):
                raise TypeError("Result date_time is not a datetime")
            keys.append((date, float(latitude), float(longitude)))
        expected_by_key = {row.key: (row, distance) for row, distance in expected}
        assert len(set(keys)) == len(keys)
        assert set(keys) == set(expected_by_key)
        if kind == "nearest":
            assert keys == [row.key for row, _ in expected]
            assert "distance" in result
        dtypes: dict[str, str] = {}
        for name in FIELDS:
            values: list[Scalar] = []
            for key in keys:
                row, distance = expected_by_key[key]
                if name in SCIENCE:
                    packed = row.science[SCIENCE.index(name)]
                    value: Scalar = float("nan") if packed is None else packed
                else:
                    ordinary: dict[along_track_fields, Scalar] = {
                        "date_time": row.date,
                        "latitude": row.latitude,
                        "longitude": row.longitude,
                        "file_name": row.file_name,
                        "mission": row.mission,
                        "track": row.track,
                        "cycle": row.cycle,
                        "basin_id": row.basin,
                        "distance": distance,
                        "delta_t": (query.date - row.date).total_seconds(),
                    }
                    value = ordinary[name]
                values.append(value)
            dtype = (
                "O"
                if name == "date_time"
                else (
                    "str"
                    if name in ("file_name", "mission")
                    else (
                        "i2"
                        if name == "basin_id"
                        else "i8" if name in ("track", "cycle") else "f8"
                    )
                )
            )
            wanted = np.asarray(values, dtype=dtype)
            unscaled = np.asarray(result.get_unscaled(name))
            scaled = np.asarray(result[name])
            assert unscaled.dtype == wanted.dtype and scaled.dtype == wanted.dtype
            dtypes[name] = unscaled.dtype.str
            if np.issubdtype(wanted.dtype, np.number):
                np.testing.assert_array_equal(np.isnan(unscaled), np.isnan(wanted))
                np.testing.assert_array_equal(np.isnan(scaled), np.isnan(wanted))
                tolerance = 0.0001 if name == "distance" else 0
                np.testing.assert_allclose(
                    unscaled, wanted, rtol=0, atol=tolerance, equal_nan=True
                )
                physical = wanted * 0.001 if name in SCIENCE else wanted
                np.testing.assert_allclose(
                    scaled, physical, rtol=0, atol=max(tolerance, 1e-12), equal_nan=True
                )
            else:
                np.testing.assert_array_equal(unscaled, wanted)
                np.testing.assert_array_equal(scaled, wanted)
        serialized = [(date.isoformat(), lat, lon) for date, lat, lon in keys]
        return {
            "rows": result.row_count,
            "dtypes": dtypes,
            "membership_sha256": digest(sorted(serialized)),
            "sequence_sha256": digest(serialized),
        }


def stats_snapshot(stats: indexed.IndexedCacheInfo) -> dict[str, int | float]:
    """Copy counters and retained-array gauges before/after a phase.

    :param stats: Public cumulative backend statistics.
    :returns: Numeric statistics with no live reference to mutable counters.
    """
    return {
        "source_reads": stats.source_reads,
        "catalog_reads": stats.catalog_reads,
        "disk_hits": stats.disk_hits,
        "memory_hits": stats.memory_hits,
        "window_builds": stats.window_builds,
        "preparation_seconds": stats.preparation_seconds,
        "cached_bytes": stats.cached_bytes,
        "active_bytes": stats.active_bytes,
        "peak_retained_bytes": stats.peak_retained_bytes,
        "geometry_bytes": stats.geometry_bytes,
        "geometry_builds": stats.geometry_builds,
        "geometry_hits": stats.geometry_hits,
    }


def disk_bytes(path: Path) -> int:
    """Count logical artifact file bytes, not allocated blocks or RSS.

    :param path: Run-owned cache directory.
    :returns: Sum of file sizes, measured outside phase timers.
    """
    return sum(item.stat().st_size for item in path.rglob("*") if item.is_file())


def measure(
    phase: str,
    operation: Callable[[], T],
    cache: Path,
    backend: TimedIndexedBackend | None,
    *,
    query: bool = False,
) -> tuple[T, dict[str, object]]:
    """Time one operation, collecting statistics and disk footprint outside timing.

    :param phase: Explicit cache/process-state phase name.
    :param operation: Constructor, fully consumed query batch, or grid preparation.
    :param cache: Unique run-owned artifact directory.
    :param backend: Existing backend, or ``None`` when constructing a new instance.
    :param query: Whether to derive query wall time outside window calls.
    :returns: Operation result and raw timing/counter evidence.
    """
    before = stats_snapshot(
        backend.stats if backend is not None else indexed.IndexedCacheInfo()
    )
    probes_before = (
        backend.probes.snapshot() if backend is not None else ProbeTimings().snapshot()
    )
    before_disk = disk_bytes(cache)
    started = perf_counter()
    value = operation()
    elapsed = perf_counter() - started
    owner = value if isinstance(value, TimedIndexedBackend) else backend
    if owner is None:
        raise TypeError("Measured operation has no backend statistics")
    after = stats_snapshot(owner.stats)
    probes_after = owner.probes.snapshot()
    probes_delta = {
        name: probes_after[name] - probes_before[name] for name in probes_before
    }
    decode = float(probes_delta["decode_seconds"])
    file_load = float(probes_delta["file_load_inclusive_seconds"])
    window = float(probes_delta["prepare_window_inclusive_seconds"])
    probe_timing_seconds = {
        "decode_seconds": decode,
        "file_load_inclusive_seconds": file_load,
        "prepare_window_inclusive_seconds": window,
        "file_load_excluding_decode_seconds": file_load - decode,
        "window_excluding_file_load_seconds": window - file_load,
    }
    if query:
        probe_timing_seconds["query_wall_excluding_window_seconds"] = elapsed - window
    return value, {
        "phase": phase,
        "wall_seconds": elapsed,
        "stats_before": before,
        "stats_after": after,
        "stats_delta": {name: after[name] - before[name] for name in before},
        "probes_before": probes_before,
        "probes_after": probes_after,
        "probes_delta": probes_delta,
        "probe_timing_seconds": probe_timing_seconds,
        "artifact_file_bytes_before": before_disk,
        "artifact_file_bytes_after": disk_bytes(cache),
    }


def execute_queries(
    backend: indexed.IndexedAlongTrackBackend, queries: Sequence[Query], kind: Kind
) -> list[Result]:
    """Fully consume an ordered batch using fixed fields, budgets, and missions.

    :param backend: Backend under measurement.
    :param queries: Ordered paired centers/dates; never reordered for cache locality.
    :param kind: Radius or capped three-nearest query.
    :returns: Materialized datasets; scaled columns are not yet accessed.
    """
    fields = [name for name in FIELDS if kind == "radius" or name != "distance"]
    latitudes = [query.latitude for query in queries]
    longitudes = [query.longitude for query in queries]
    dates = [query.date for query in queries]
    if kind == "radius":
        return list(
            backend.geographic_point_in_r_dt_batch(
                fields,
                latitudes,
                longitudes,
                dates,
                radius=RADIUS_M,
                time_window=HALF_WINDOW,
                missions=list(MISSIONS),
                n_jobs=1,
                chunk_size=16,
            )
        )
    return list(
        backend.geographic_nearest_neighbors_batch(
            fields,
            latitudes,
            longitudes,
            dates,
            time_window=HALF_WINDOW,
            missions=list(MISSIONS),
            max_radius=RADIUS_M,
            n_jobs=1,
            chunk_size=16,
        )
    )


def run_sample(
    source: Path,
    cache: Path,
    options: Options,
    oracle: Oracle,
    kind: Kind,
    repetition: int,
) -> list[dict[str, object]]:
    """Measure separate cold, warm, adjacent, restart, and geometry phases.

    :param source: Generated multi-source fixture root.
    :param cache: New empty directory unique to this repetition and query kind.
    :param options: Validated workload scale and retention budgets.
    :param oracle: Independent scientific oracle.
    :param kind: Radius or nearest workload.
    :param repetition: Zero-based repeated-sample index.
    :returns: Raw per-phase measurements, including equivalence fingerprints.
    """
    records: list[dict[str, object]] = []
    baseline = [Query(latitude, longitude, DATE) for latitude, longitude in CENTERS]
    adjacent = [Query(CENTERS[0][0], CENTERS[0][1], date) for date in ADJACENT_DATES]

    def construct() -> TimedIndexedBackend:
        """Construct a backend using only this run's generated sources/cache.

        :returns: A new backend in this same Python process.
        """
        return TimedIndexedBackend(
            source,
            cache,
            cell_size_degrees=2.0,
            max_cache_bytes=options.max_cache_bytes,
            max_geometry_cache_bytes=options.max_geometry_cache_bytes,
        )

    def query_phase(name: str, queries: list[Query]) -> None:
        """Time consumed queries and check their complete outputs afterward.

        :param name: Phase/cache-state label.
        :param queries: Exact ordered centers for this phase.
        """
        window_calls = backend.probes.prepare_window_calls
        results, record = measure(
            name,
            lambda: execute_queries(backend, queries, kind),
            cache,
            backend,
            query=True,
        )
        assert backend.probes.prepare_window_calls - window_calls == len(queries)
        record["results"] = [
            oracle.check(result, query, kind)
            for result, query in zip(results, queries, strict=True)
        ]
        record["queries"] = [
            {
                "latitude": query.latitude,
                "longitude": query.longitude,
                "date": query.date.isoformat(),
            }
            for query in queries
        ]
        records.append(record)

    backend, construction = measure(
        "artifact_cold_constructor_catalog", construct, cache, None
    )
    records.append(construction)
    assert backend.stats.catalog_reads == options.files
    query_phase("artifact_cold_first_decode_window_query", baseline)
    assert (
        backend.stats.source_reads == options.files and backend.stats.window_builds == 1
    )
    assert (
        backend.probes.decode_calls == backend.probes.file_load_calls == options.files
    )
    builds = backend.stats.window_builds
    reads = backend.stats.source_reads
    query_phase("warm_identical_window", baseline)
    assert backend.stats.window_builds == builds and backend.stats.source_reads == reads
    query_phase("ordered_adjacent_windows", adjacent)
    assert backend.stats.window_builds == builds + len(adjacent)
    # Drop the original instance before measuring a new one. This is deliberately
    # NOT a subprocess/interpreter restart, and the OS page cache is untouched.
    del backend
    backend, construction = measure(
        "restarted_backend_same_process_constructor", construct, cache, None
    )
    records.append(construction)
    assert backend.stats.catalog_reads == 0
    query_phase("restarted_backend_same_process_prepared_file_query", baseline)
    assert backend.stats.source_reads == 0 and backend.stats.disk_hits == options.files
    assert (
        backend.probes.decode_calls == 0
        and backend.probes.file_load_calls == options.files
    )
    query_phase("direct_geometry_same_window", baseline)
    builds = backend.stats.window_builds
    radii = (RADIUS_M,) if kind == "radius" else (150_000.0, RADIUS_M)

    def prepare_geometry() -> None:
        """Prepare all radius covers used by this fixed workload separately."""
        for radius in radii:
            backend.prepare_grid(
                [latitude for latitude, _ in CENTERS],
                [longitude for _, longitude in CENTERS],
                radius=radius,
            )

    _, grid = measure(
        "grid_geometry_preparation_only", prepare_geometry, cache, backend
    )
    grid["radii_m"] = radii
    records.append(grid)
    query_phase("prepared_geometry_same_window", baseline)
    assert backend.stats.window_builds == builds
    for record in records:
        record.update({"kind": kind, "repetition": repetition, "cache": cache.name})
    return records


def summarize(
    records: list[dict[str, object]], *, metric: str = "wall_seconds"
) -> list[dict[str, object]]:
    """Summarize repeated wall/probe times without a regression threshold.

    :param records: Raw phase records for both query kinds.
    :param metric: Phase wall time or one named boundary/derived probe metric.
    :returns: Median, MAD, range, and relative noise for each phase/kind.
    """
    groups: dict[tuple[str, str], list[float]] = {}
    for record in records:
        kind, phase = record["kind"], record["phase"]
        if metric == "wall_seconds":
            wall = record["wall_seconds"]
        else:
            timings = record["probe_timing_seconds"]
            if not isinstance(timings, dict):
                raise TypeError("Invalid probe timing record")
            if metric not in timings:
                continue
            wall = timings[metric]
        if (
            not isinstance(kind, str)
            or not isinstance(phase, str)
            or not isinstance(wall, float)
        ):
            raise TypeError("Invalid raw timing record")
        groups.setdefault((kind, phase), []).append(wall)
    result: list[dict[str, object]] = []
    for (kind, phase), values in sorted(groups.items()):
        median = statistics.median(values)
        mad = statistics.median(abs(value - median) for value in values)
        result.append(
            {
                "kind": kind,
                "phase": phase,
                "metric": metric,
                "samples": len(values),
                "median_seconds": median,
                "mad_seconds": mad,
                "min_seconds": min(values),
                "max_seconds": max(values),
                "relative_mad": mad / median if median else None,
                "relative_range": (
                    (max(values) - min(values)) / median if median else None
                ),
            }
        )
    return result


def git_output(*arguments: str) -> str:
    """Run a read-only provenance command without inspecting environment secrets.

    :param arguments: Git arguments run from the repository root.
    :returns: Command output stripped of trailing whitespace.
    """
    return subprocess.check_output(["git", *arguments], cwd=REPO, text=True).rstrip()


def main() -> None:
    """Parse the CLI and write run-owned synthetic evidence outside the repository.

    :raises AssertionError: If any independent scientific or phase-state check fails.
    """
    parser = argparse.ArgumentParser(description=__doc__)
    if not __debug__:
        parser.error(
            "Run without -O: independent equivalence checks require assertions"
        )
    parser.add_argument(
        "--rows-per-file",
        type=int,
        default=5_000,
        help="Synthetic rows per file (minimum 6; default: 5000)",
    )
    parser.add_argument(
        "--files",
        type=int,
        default=6,
        help="Multi-source fixture file count (minimum 2; default: 6)",
    )
    parser.add_argument(
        "--repetitions",
        type=int,
        default=5,
        help="Repeated samples with independent artifact-cold caches (minimum 2; default: 5)",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=20240101,
        help="NumPy PCG64 generator seed (default: 20240101)",
    )
    parser.add_argument(
        "--max-cache-bytes",
        type=int,
        default=64_000_000,
        help="Retained file-array budget, not RSS/disk quota (default: 64000000)",
    )
    parser.add_argument(
        "--max-geometry-cache-bytes",
        type=int,
        default=8_000_000,
        help="Independent retained geometry-array budget (default: 8000000)",
    )
    arguments = parser.parse_args()
    options = Options(
        arguments.rows_per_file,
        arguments.files,
        arguments.repetitions,
        arguments.seed,
        arguments.max_cache_bytes,
        arguments.max_geometry_cache_bytes,
    )
    if options.rows_per_file < 6 or options.files < 2 or options.repetitions < 2:
        parser.error("Require rows-per-file >= 6, files >= 2, and repetitions >= 2")
    if min(options.seed, options.max_cache_bytes, options.max_geometry_cache_bytes) < 0:
        parser.error("Seed and byte budgets must be nonnegative")
    started_at = datetime.now(timezone.utc).isoformat()
    run = Path(tempfile.mkdtemp(prefix="indexed-along-track-", dir="/tmp/opencode"))
    print(f"SYNTHETIC ONLY; generated sources/cache/evidence: {run}", flush=True)
    raster, connections, assets = basin_assets()
    rows, inventory = generate_sources(run / "sources", options, raster)
    oracle = Oracle(rows, raster, connections)
    workload: dict[str, object] = {
        "fixture_version": 1,
        "seed": options.seed,
        "rng": "NumPy default_rng/PCG64",
        "rows_per_file": options.rows_per_file,
        "files": options.files,
        "missions": MISSIONS,
        "fields_radius": FIELDS,
        "fields_nearest_requested": [name for name in FIELDS if name != "distance"],
        "fields_nearest_returned": FIELDS,
        "centers_lat_lon": CENTERS,
        "date": DATE.isoformat(),
        "adjacent_dates_ordered": [date.isoformat() for date in ADJACENT_DATES],
        "half_window_microseconds": 86_400_000_000,
        "radius_m": RADIUS_M,
        "nearest_max_radius_m": RADIUS_M,
        "cell_size_degrees": 2.0,
        "n_jobs": 1,
        "chunk_size": 16,
        "max_cache_bytes": options.max_cache_bytes,
        "max_geometry_cache_bytes": options.max_geometry_cache_bytes,
        "repetitions": options.repetitions,
        "fixture_description": "Eight-day coverage per file; microsecond timestamps; packed microdegree coordinates near (0,180); scalar track/cycle/correction; int16 science with fill and missing sentinels. Fourth center has no matches.",
    }
    records: list[dict[str, object]] = []
    for repetition in range(options.repetitions):
        for kind in ("radius", "nearest"):
            records.extend(
                run_sample(
                    run / "sources",
                    run / f"cache-{repetition}-{kind}",
                    options,
                    oracle,
                    kind,
                    repetition,
                )
            )
    versions = {
        name: metadata.version(name)
        for name in ("OceanDB", "numpy", "netCDF4", "geographiclib")
    }
    report: dict[str, object] = {
        "scope": "SYNTHETIC ONLY: no actual user workload supplied; not real-data, PostgreSQL-parity, or throughput evidence.",
        "started_at_utc": started_at,
        "finished_at_utc": datetime.now(timezone.utc).isoformat(),
        "invocation": [sys.executable, *sys.argv],
        "cwd": str(REPO),
        "run_directory": str(run),
        "starting_revision_requested": BASE_REVISION,
        "revision": git_output("rev-parse", "HEAD"),
        "working_tree_status_paths": git_output("status", "--short"),
        "code_sha256": {
            name: hashlib.sha256((REPO / name).read_bytes()).hexdigest()
            for name in CODE_PATHS
        },
        "versions": {
            "python": sys.version,
            "packages": versions,
            "netcdf_c": nc.__netcdf4libversion__,
            "hdf5": nc.__hdf5libversion__,
        },
        "machine": {
            "platform": platform.platform(),
            "machine": platform.machine(),
            "processor": platform.processor(),
        },
        "workload": workload,
        "workload_sha256": digest(workload),
        "source_inventory": inventory,
        "source_inventory_sha256": digest(inventory),
        "basin_asset_sha256": assets,
        "schema_metadata": oracle.schema,
        "schema_sha256": digest(oracle.schema),
        "equivalence": {
            "status": "passed for every measured result",
            "oracle": "Generator-owned rows; direct basin asset parsing; exhaustive GeographicLib WGS84 inverse; inclusive time/radius and exact nearest rank.",
            "checks": "All fields, result name/count/schema metadata, column order/dtypes, provenance, exact stored coordinates/times, packed/scaled values and NaN masks, signed delta_t; nearest mandatory distance and ordered keys.",
            "distance_absolute_tolerance_m": 0.0001,
            "physical_absolute_tolerance": 1e-12,
            "radius_sequence_policy": "Membership asserted independently by scientific key. Sequence hashes are recorded separately, not asserted as a new radius ordering contract.",
        },
        "measurement_notes": {
            "query_wall_seconds": "Fully consumed serial batch, including validation/materialization; excludes oracle checks and lazy scaled-column access. Constructors and prepare_grid have separately named timers.",
            "preparation_seconds": "Original public cumulative statistic, recorded separately and not replaced: SUM of decode/cache I/O + filtering/copying + spatial window assembly on builds; excludes exact-key hit calls. Nested probe bookkeeping can perturb it.",
            "probe_boundaries": "Benchmark-local serial subclass delegates once at _prepare_file (decode/read/validation), _load_file (all loading including decode), and prepare_window (complete call including key normalization and exact-key-hit source-stat safety checks). No algorithms, results, or public counters changed. After extraction, relocate probes to equivalent boundaries, including the private _Source annotation.",
            "probe_nesting": "Inclusive timers nest: decode inside file load inside prepare_window inside query wall. Do not add inclusive totals. Per-phase probes_before/after/delta are cumulative measurements; probe_timing_seconds also records file_load - decode, window - file_load, and, for query phases only, wall - window.",
            "probe_residuals": "file_load_excluding_decode_seconds includes cache/source checks, artifact publication/mapping and LRU handling; window_excluding_file_load_seconds includes orchestration, temporal filtering/copying, duplicate checks/index assembly and exact-hit checks, NOT isolated pure spatial assembly. query_wall_excluding_window_seconds is overhead including list/batch handling, inventory validation, geometry/selection/geodesics/projection and outer probe bookkeeping, NOT pure selection.",
            "probe_overhead": "Every wrapped call adds two perf_counter reads, delegation/finally overhead and timing/call-count updates; probes_delta records exact wrapped call and clock-read counts. Inner probe bookkeeping remains in enclosing timers/residuals. Cost is NOT calibrated or subtracted; compare runs using the same probes, not the old uninstrumented baseline as a speed regression.",
            "cache_state": "Artifact-cold caches only. Fixtures were just written. All samples and backend restarts run in the SAME Python process. OS page cache is neither flushed nor claimed cold; no user's cache is used.",
            "memory": "Raw *_bytes statistics describe retained arrays/high-water gauges, NOT RSS, temporary allocations, or memory-mapped resident pages. RSS is not measured. Gauge deltas can be negative.",
            "disk": "Logical artifact file bytes, excluding generated source files; no disk quota or allocated-block claim.",
            "noise": "Fixed phase order, radius before nearest, no warmup discard; machine scheduling, interpreter and OS cache state uncontrolled. Median, MAD and range summarize these samples, not confidence intervals.",
            "regression_threshold": "PENDING USER AGREEMENT after baseline noise review; no pass/fail speed threshold selected.",
        },
        "raw_phases": records,
        "timing_summary": summarize(records),
        "probe_timing_summary": [
            item
            for metric in PROBE_TIMING_METRICS
            for item in summarize(records, metric=metric)
        ],
        "probe_clock": {
            "name": "perf_counter",
            "resolution_seconds": get_clock_info("perf_counter").resolution,
            "implementation": get_clock_info("perf_counter").implementation,
            "overhead_calibrated_or_subtracted": False,
        },
        "generated_source_bytes": sum(
            path.stat().st_size for path in (run / "sources").rglob("*.nc")
        ),
    }
    output = run / "results.json"
    output.write_text(
        json.dumps(report, indent=2, sort_keys=True, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    print(f"Equivalence passed; {len(records)} raw phase samples. Results: {output}")
    print(
        "Regression threshold: PENDING USER AGREEMENT; retained-array bytes are not RSS."
    )


if __name__ == "__main__":
    main()
