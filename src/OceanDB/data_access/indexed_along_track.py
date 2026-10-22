from __future__ import annotations

import json
import os
import tempfile
import warnings
from collections import OrderedDict
from collections.abc import Generator, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from hashlib import sha256
from importlib import resources
from pathlib import Path
from time import perf_counter
from typing import get_args

import netCDF4 as nc
import numpy as np

from OceanDB.data_access.along_track import Mission
from OceanDB.data_access.indexed_geometry import (MAX_WGS84_DISTANCE_M,
                                                  cap_cells, cell_dimensions,
                                                  cell_ids,
                                                  exact_wgs84_distance_m,
                                                  unit_vectors,
                                                  wgs84_distance_m)
from OceanDB.ocean_data.basins import BasinConnections, BasinMask
from OceanDB.ocean_data.dataset import Dataset
from OceanDB.schemas.along_track_schema import (along_track_columns_schema,
                                                along_track_fields,
                                                along_track_schema)

_PRODUCT = "SEALEVEL_GLO_PHY_L3_MY_008_062"
_EPOCH = datetime(1950, 1, 1)
_CACHE_VERSION = 2
_DECISION_MARGIN_M = 0.01
_SUPPORTED_MISSIONS = frozenset(get_args(Mission))
_SCIENCE = tuple(
    name for name, field in along_track_columns_schema.items() if field.scaling != 1
)
_SOURCE = ("track", "cycle", *_SCIENCE)
_FILE_DTYPE = np.dtype(
    [
        ("time_us", "<i8"),
        ("longitude", "<f8"),
        ("latitude", "<f8"),
        ("basin_id", "<i2"),
        ("source_row", "<i8"),
        ("track", "<i8"),
        ("cycle", "<i8"),
        *((name, "<f8") for name in _SCIENCE),
    ]
)
_WINDOW_DTYPE = np.dtype([*_FILE_DTYPE.descr, ("file_id", "<i4")])


def _microseconds(value: datetime) -> int:
    """Convert a supported UTC-naive datetime without floating-day rounding.

    :param value: Source or query datetime.
    :returns: Microseconds relative to the source epoch.
    :raises ValueError: If a timezone-aware datetime is supplied.
    """
    if value.tzinfo is not None:
        raise ValueError("AlongTrack datetimes must be UTC-naive")
    delta = value - _EPOCH
    return (delta.days * 86_400 + delta.seconds) * 1_000_000 + delta.microseconds


def _atomic_json(path: Path, payload: object) -> None:
    """Replace a cache manifest only after writing a complete temporary file.

    :param path: Destination manifest.
    :param payload: JSON-serializable data.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(dir=path.parent, suffix=".tmp")
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            json.dump(payload, stream, separators=(",", ":"), allow_nan=False)
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


@dataclass(frozen=True)
class _Source:
    path: Path
    size: int
    mtime_ns: int
    start_us: int
    end_us: int
    mission: str


@dataclass
class IndexedCacheInfo:
    """Observable source, preparation, and retained-memory costs."""

    source_reads: int = 0
    catalog_reads: int = 0
    disk_hits: int = 0
    memory_hits: int = 0
    window_builds: int = 0
    preparation_seconds: float = 0.0
    cached_bytes: int = 0
    active_bytes: int = 0
    peak_retained_bytes: int = 0
    geometry_bytes: int = 0
    geometry_builds: int = 0
    geometry_hits: int = 0


class IndexedAlongTrackBackend:
    """An OceanDB AlongTrack backend over locally downloaded NetCDF files.

    The instance owns a mutable active window and is **not thread-safe**. Only
    the four serial windowed radius/nearest methods are supported. An explicit
    :meth:`refresh` is needed to include newly added source files; changed or
    missing files from the existing snapshot instead raise before reuse.

    :param data_root: ``ALONG_TRACK_DATA_DIRECTORY`` or its product directory.
    :param cache_dir: Directory for reusable, versioned local cache artifacts.
    :param cell_size_degrees: Spatial index cell width (must divide 180 and 360).
    :param max_cache_bytes: Approximate upper bound for retained per-file RAM.
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
        root = Path(data_root).expanduser().resolve()
        self.data_root = root / _PRODUCT if (root / _PRODUCT).is_dir() else root
        if not self.data_root.is_dir():
            raise FileNotFoundError(
                f"AlongTrack source directory does not exist: {root}"
            )
        self.cache_dir = Path(cache_dir).expanduser().resolve()
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self._nx, self._ny = cell_dimensions(cell_size_degrees)
        self._cell_size = cell_size_degrees
        if max_cache_bytes < 0:
            raise ValueError("max_cache_bytes must be nonnegative")
        if max_geometry_cache_bytes < 0:
            raise ValueError("max_geometry_cache_bytes must be nonnegative")
        self._max_cache_bytes = max_cache_bytes
        self._max_geometry_cache_bytes = max_geometry_cache_bytes
        self._basins = BasinMask()
        self._connections = BasinConnections().connection_map
        self._basin_signature = self._asset_signature()
        self.stats = IndexedCacheInfo()
        self._files: OrderedDict[Path, np.ndarray] = OrderedDict()
        self._grid_geometry: dict[
            tuple[float, float, float], tuple[int, np.ndarray, np.ndarray]
        ] = {}
        self._active: np.ndarray | None = None
        self._vectors: np.ndarray | None = None
        self._offsets: np.ndarray | None = None
        self._active_key: tuple[int, int, tuple[str, ...]] | None = None
        self._active_sources: tuple[_Source, ...] = ()
        self._sources: list[_Source] = []
        self.refresh()

    @staticmethod
    def _asset_signature() -> str:
        """Fingerprint the packaged basin assets for cache invalidation.

        :returns: Hash of raster and direct-connection data.
        """
        assets = resources.files("OceanDB.data")
        digest = sha256()
        for path in (
            "basin_masks/new_basin_mask.nc",
            "basins/basin_connection_table.txt",
        ):
            digest.update(assets.joinpath(path).read_bytes())
        return digest.hexdigest()

    @staticmethod
    def _mission(path: Path) -> str:
        """Use the mission parsed by existing PostgreSQL ingestion.

        :param path: Source file path.
        :returns: Literal mission string embedded in its basename.
        :raises ValueError: If the filename cannot supply a mission.
        """
        parts = path.name.split("_")
        if len(parts) < 3 or not parts[2]:
            raise ValueError(f"Could not parse mission from file name: {path.name}")
        if parts[2] not in _SUPPORTED_MISSIONS:
            raise ValueError(f"Unsupported mission {parts[2]!r} in {path.name}")
        return parts[2]

    @staticmethod
    def _times(variable: nc.Variable) -> np.ndarray:
        """Decode CF times to exact Python microseconds as in PostgreSQL ETL.

        :param variable: Source time variable.
        :returns: One integer microsecond count per source observation.
        :raises ValueError: If the calendar or times cannot be represented.
        """
        raw = np.ma.asarray(variable[:])
        if raw.ndim != 1 or np.ma.getmaskarray(raw).any():
            raise ValueError("AlongTrack time must be a complete one-dimensional array")
        calendar = getattr(variable, "calendar", "standard")
        if calendar not in ("standard", "gregorian", "proleptic_gregorian"):
            raise ValueError(f"Unsupported AlongTrack calendar: {calendar}")
        decoded = nc.num2date(
            raw,
            variable.units,
            calendar=calendar,
            only_use_cftime_datetimes=False,
            only_use_python_datetimes=True,
        )
        values = [decoded] if isinstance(decoded, datetime) else decoded
        timestamps: list[int] = []
        for value in values:
            if not isinstance(value, datetime):
                raise ValueError(f"Unsupported AlongTrack datetime: {value!r}")
            timestamps.append(_microseconds(value))
        return np.asarray(timestamps, dtype=np.int64)

    def _coverage(self, path: Path) -> tuple[int, int]:
        """Read actual observation bounds when catalog metadata cannot be reused.

        :param path: NetCDF source file.
        :returns: Inclusive first and last observation times in microseconds.
        """
        with nc.Dataset(path) as dataset:
            if "time" not in dataset.variables:
                raise ValueError(f"Missing required time variable in {path}")
            times = self._times(dataset.variables["time"])
        self.stats.catalog_reads += 1
        if not len(times):
            raise ValueError(f"No observations in AlongTrack source: {path}")
        return int(times.min()), int(times.max())

    def refresh(self) -> None:
        """Reinventory local sources and invalidate the in-memory snapshot.

        New files become visible only after this explicit operation. Prepared
        files for unchanged sources remain available on disk.
        """
        self._files.clear()
        self._active = self._vectors = self._offsets = None
        self._active_key = None
        self._active_sources = ()
        self.stats.cached_bytes = self.stats.active_bytes = 0
        catalog_path = (
            self.cache_dir
            / f"catalog-{sha256(str(self.data_root).encode()).hexdigest()[:20]}.json"
        )
        previous: dict[str, dict[str, object]] = {}
        if catalog_path.exists():
            try:
                manifest = json.loads(catalog_path.read_text(encoding="utf-8"))
                if manifest["version"] == _CACHE_VERSION and manifest["root"] == str(
                    self.data_root
                ):
                    previous = {entry["path"]: entry for entry in manifest["sources"]}
            except (OSError, ValueError, KeyError, TypeError):
                previous = {}
        paths = sorted(self.data_root.glob("*/*/*/*.nc"))
        if not paths:
            raise FileNotFoundError(
                f"No mission/year/month/*.nc files under {self.data_root}"
            )
        self._sources = []
        serializable: list[dict[str, object]] = []
        for path in paths:
            relative = str(path.relative_to(self.data_root))
            stat = path.stat()
            cached = previous.get(relative)
            if (
                cached is not None
                and cached.get("size") == stat.st_size
                and cached.get("mtime_ns") == stat.st_mtime_ns
                and isinstance(cached.get("start_us"), int)
                and isinstance(cached.get("end_us"), int)
            ):
                start, end = cached["start_us"], cached["end_us"]
                assert isinstance(start, int) and isinstance(end, int)
            else:
                start, end = self._coverage(path)
            source = _Source(
                path, stat.st_size, stat.st_mtime_ns, start, end, self._mission(path)
            )
            self._sources.append(source)
            serializable.append(
                {
                    "path": relative,
                    "size": source.size,
                    "mtime_ns": source.mtime_ns,
                    "start_us": start,
                    "end_us": end,
                }
            )
        _atomic_json(
            catalog_path,
            {
                "version": _CACHE_VERSION,
                "root": str(self.data_root),
                "sources": serializable,
            },
        )

    def prepare_grid(
        self,
        latitudes: Sequence[float],
        longitudes: Sequence[float],
        *,
        radius: float = 500_000.0,
    ) -> None:
        """Reuse basin labels, vectors, and cell covers across moving windows.

        This optional operation takes paired centers, not Cartesian grid axes.
        Queries outside the cache or at a different radius use the direct path.

        :param latitudes: Latitude of each center.
        :param longitudes: Longitude of each paired center.
        :param radius: Inclusive radius for the reusable cell cover, in meters.
        :raises ValueError: If the centers or radius are invalid.
        """
        if len(latitudes) != len(longitudes):
            raise ValueError("latitudes and longitudes must have equal lengths")
        if not np.isfinite(radius) or radius < 0:
            raise ValueError("radius must be finite and nonnegative")
        for latitude, longitude in zip(latitudes, longitudes, strict=True):
            key = (longitude, latitude, radius)
            if key in self._grid_geometry:
                continue
            cells = cap_cells(longitude, latitude, radius, self._cell_size)
            vector = unit_vectors(longitude, latitude)
            cost = cells.nbytes + vector.nbytes
            if cost + self.stats.geometry_bytes > self._max_geometry_cache_bytes:
                continue
            basin = int(self._basins.lookup(latitude, longitude))
            cells.setflags(write=False)
            vector.setflags(write=False)
            self._grid_geometry[key] = (basin, cells, vector)
            self.stats.geometry_bytes += cost
            self.stats.geometry_builds += 1
            self._record_retained()

    def _record_retained(self) -> None:
        """Track the high-water size of retained arrays, excluding temporaries."""
        retained = (
            self.stats.cached_bytes
            + self.stats.active_bytes
            + self.stats.geometry_bytes
        )
        self.stats.peak_retained_bytes = max(self.stats.peak_retained_bytes, retained)

    @staticmethod
    def _check_source(source: _Source) -> None:
        """Reject an invalidated pinned source rather than return stale rows.

        :param source: Catalogued file and its source metadata.
        :raises RuntimeError: If a source has gone missing or changed.
        """
        try:
            stat = source.path.stat()
        except FileNotFoundError as error:
            raise RuntimeError(
                f"AlongTrack source disappeared: {source.path}; call refresh()"
            ) from error
        if (stat.st_size, stat.st_mtime_ns) != (source.size, source.mtime_ns):
            raise RuntimeError(
                f"AlongTrack source changed: {source.path}; call refresh()"
            )

    def _validate_inventory(self) -> None:
        """Validate all pinned sources before relying on cached time coverage.

        A previously unselected file could have moved into the current window.
        This check runs once per public query/batch, rather than per batch center.
        New file discovery remains an explicit :meth:`refresh`.
        """
        for source in self._sources:
            self._check_source(source)

    def _prepare_file(self, source: _Source) -> np.ndarray:
        """Decode one source into aligned packed columns and row provenance.

        :param source: Pinned source record.
        :returns: Prepared columns for all observations in the file.
        :raises ValueError: If the source format cannot match OceanDB ingestion.
        """
        with nc.Dataset(source.path) as dataset:
            missing = set(("time", "latitude", "longitude", *_SOURCE)).difference(
                dataset.variables
            )
            if missing:
                raise ValueError(
                    f"Missing required variables in {source.path}: {sorted(missing)}"
                )
            time_us = self._times(dataset.variables["time"])
            count = len(time_us)
            dataset.variables["latitude"].set_auto_scale(True)
            dataset.variables["longitude"].set_auto_scale(True)
            latitude = np.ma.asarray(dataset.variables["latitude"][:], dtype=np.float64)
            longitude = np.ma.asarray(
                dataset.variables["longitude"][:], dtype=np.float64
            )
            if (
                latitude.shape != (count,)
                or longitude.shape != (count,)
                or np.ma.getmaskarray(latitude).any()
                or np.ma.getmaskarray(longitude).any()
            ):
                raise ValueError(f"Invalid AlongTrack coordinates in {source.path}")
            lat = np.asarray(latitude, dtype=np.float64)
            lon = np.asarray(longitude, dtype=np.float64)
            if (
                not np.isfinite(lat).all()
                or not np.isfinite(lon).all()
                or (lat < -90).any()
                or (lat >= 90).any()
            ):
                raise ValueError(f"Invalid AlongTrack coordinates in {source.path}")
            rows = np.empty(count, dtype=_FILE_DTYPE)
            rows["time_us"] = time_us
            rows["latitude"] = lat
            rows["longitude"] = lon
            rows["basin_id"] = self._basins.lookup(lat, lon)
            rows["source_row"] = np.arange(count)
            for name in _SOURCE:
                variable = dataset.variables[name]
                if variable.ndim != 0 and variable.shape != (count,):
                    raise ValueError(f"Unsupported {name} shape in {source.path}")
                variable.set_auto_maskandscale(False)
                raw = np.asarray(variable[:])
                values = (
                    np.full(count, raw, dtype=np.float64)
                    if variable.ndim == 0
                    else np.asarray(raw, dtype=np.float64)
                )
                invalid = ~np.isfinite(values)
                if "_FillValue" not in variable.ncattrs():
                    default_fill = nc.default_fillvals.get(variable.dtype.str[1:])
                    if default_fill is not None:
                        invalid |= values == default_fill
                for attribute in ("_FillValue", "missing_value"):
                    if attribute in variable.ncattrs():
                        sentinel = variable.getncattr(attribute)
                        invalid |= values == sentinel
                if name in _SCIENCE:
                    expected_scale = along_track_columns_schema[name].scaling
                    if not np.isclose(
                        float(getattr(variable, "scale_factor", 1)),
                        expected_scale,
                        rtol=1e-6,
                        atol=0,
                    ) or not np.isclose(
                        float(getattr(variable, "add_offset", 0)),
                        0,
                        rtol=0,
                        atol=1e-12,
                    ):
                        warnings.warn(
                            f"Source scaling for {name} differs from OceanDB schema in {source.path}; "
                            "using OceanDB's packed-value scaling",
                            RuntimeWarning,
                            stacklevel=2,
                        )
                    values[invalid] = np.nan
                elif invalid.any() or (values != np.floor(values)).any():
                    raise ValueError(
                        f"Unsupported missing or nonintegral {name} in {source.path}"
                    )
                rows[name] = values
            keys = np.rec.fromarrays(
                (rows["time_us"], rows["latitude"], rows["longitude"])
            )
            if len(np.unique(keys)) != count:
                raise ValueError(
                    f"Duplicate AlongTrack (date_time, latitude, longitude) source key in {source.path}"
                )
        self._check_source(source)
        self.stats.source_reads += 1
        return rows

    def _load_file(self, source: _Source) -> np.ndarray:
        """Use a valid memory/disk cache or prepare an unchanged source.

        :param source: Pinned source record.
        :returns: Source rows with the indexed cache dtype.
        :raises RuntimeError: If the source changed after cataloguing.
        """
        self._check_source(source)
        if source.path in self._files:
            self.stats.memory_hits += 1
            self._files.move_to_end(source.path)
            return self._files[source.path]
        identity = json.dumps(
            [
                _CACHE_VERSION,
                str(source.path),
                source.size,
                source.mtime_ns,
                self._basin_signature,
                _FILE_DTYPE.descr,
            ],
            separators=(",", ":"),
        )
        key = sha256(identity.encode()).hexdigest()
        path = self.cache_dir / "files" / f"{key}.npy"
        rows: np.ndarray | None = None
        if path.exists():
            try:
                candidate = np.load(path, mmap_mode="r", allow_pickle=False)
                if candidate.dtype != _FILE_DTYPE or candidate.ndim != 1:
                    raise ValueError("Prepared file schema differs from decoder")
                rows = candidate
                self.stats.disk_hits += 1
            except (OSError, ValueError, KeyError):
                rows = None
        if rows is None:
            prepared = self._prepare_file(source)
            path.parent.mkdir(parents=True, exist_ok=True)
            descriptor, temporary = tempfile.mkstemp(dir=path.parent, suffix=".tmp")
            try:
                with os.fdopen(descriptor, "wb") as stream:
                    np.save(stream, prepared, allow_pickle=False)
                os.replace(temporary, path)
            finally:
                if os.path.exists(temporary):
                    os.unlink(temporary)
            rows = np.load(path, mmap_mode="r", allow_pickle=False)
        self._files[source.path] = rows
        self.stats.cached_bytes += rows.nbytes
        self._record_retained()
        while self.stats.cached_bytes > self._max_cache_bytes and self._files:
            _, evicted = self._files.popitem(last=False)
            self.stats.cached_bytes -= evicted.nbytes
        return rows

    def prepare_window(
        self,
        date: datetime,
        time_window: timedelta,
        missions: Sequence[Mission] | None = None,
    ) -> None:
        """Prepare the window once for repeated spatial queries.

        :param date: Center datetime.
        :param time_window: Nonnegative temporal half-width.
        :param missions: Literal mission selection, or ``None`` for all.
        """
        center = _microseconds(date)
        if time_window < timedelta(0):
            raise ValueError("time_window must be nonnegative")
        half = (
            time_window.days * 86_400 + time_window.seconds
        ) * 1_000_000 + time_window.microseconds
        selection = (
            tuple(sorted(set(missions)))
            if missions is not None
            else tuple(sorted({s.mission for s in self._sources}))
        )
        key = (center, half, selection)
        if self._active_key == key:
            for source in self._active_sources:
                self._check_source(source)
            return
        started = perf_counter()
        lower, upper = center - half, center + half
        selected = tuple(
            source
            for source in self._sources
            if source.mission in selection
            and source.start_us <= upper
            and source.end_us >= lower
        )
        self._active = self._vectors = self._offsets = None
        self._active_key = None
        self.stats.active_bytes = 0
        chunks: list[np.ndarray] = []
        for file_id, source in enumerate(selected):
            data = self._load_file(source)
            mask = (data["time_us"] >= lower) & (data["time_us"] <= upper)
            window = np.empty(int(mask.sum()), dtype=_WINDOW_DTYPE)
            for name in _FILE_DTYPE.names or ():
                window[name] = data[name][mask]
            window["file_id"] = file_id
            chunks.append(window)
        active = np.concatenate(chunks) if chunks else np.empty(0, dtype=_WINDOW_DTYPE)
        # PostgreSQL's scientific unique key must not be silently deduplicated.
        if len(active):
            keys = np.rec.fromarrays(
                (active["time_us"], active["latitude"], active["longitude"])
            )
            if len(np.unique(keys)) != len(active):
                raise ValueError(
                    "Duplicate AlongTrack (date_time, latitude, longitude) source key"
                )
        cells = cell_ids(active["longitude"], active["latitude"], self._cell_size)
        order = np.argsort(cells, kind="stable")
        self._active = active[order]
        counts = np.bincount(cells, minlength=self._nx * self._ny)
        self._offsets = np.r_[0, np.cumsum(counts)]
        self._vectors = unit_vectors(
            self._active["longitude"], self._active["latitude"]
        )
        self._active.setflags(write=False)
        self._offsets.setflags(write=False)
        self._vectors.setflags(write=False)
        self._active_sources = selected
        self._active_key = key
        self.stats.window_builds += 1
        self.stats.active_bytes = (
            self._active.nbytes + self._vectors.nbytes + self._offsets.nbytes
        )
        self._record_retained()
        self.stats.preparation_seconds += perf_counter() - started

    def _select(
        self, latitude: float, longitude: float, radius: float
    ) -> tuple[np.ndarray, np.ndarray]:
        """Prune by cell and basin, then apply exact WGS84 radius selection.

        :param latitude: Query-center latitude.
        :param longitude: Query-center longitude.
        :param radius: Inclusive WGS84 radius in meters.
        :returns: Active row indices and their corresponding distances.
        """
        active, offsets, vectors = self._active, self._offsets, self._vectors
        if active is None or offsets is None or vectors is None:
            raise RuntimeError("AlongTrack window has not been prepared")
        if not np.isfinite(radius) or radius < 0:
            raise ValueError("radius must be finite and nonnegative")
        geometry = self._grid_geometry.get((longitude, latitude, radius))
        if geometry is None:
            basin = int(self._basins.lookup(latitude, longitude))
            cells = cap_cells(longitude, latitude, radius, self._cell_size)
            center = None
        else:
            self.stats.geometry_hits += 1
            basin, cells, center = geometry
        connected = self._connections[basin]
        segments = [
            np.arange(offsets[cell], offsets[cell + 1])
            for cell in cells
            if offsets[cell] < offsets[cell + 1]
        ]
        if not segments:
            return np.empty(0, dtype=np.intp), np.empty(0, dtype=np.float64)
        candidates = np.concatenate(segments)
        candidates = candidates[np.isin(active["basin_id"][candidates], connected)]
        if not len(candidates):
            return candidates, np.empty(0, dtype=np.float64)
        # The spherical filter only over-selects; the final WGS84 predicate below
        # does not use a radius epsilon. The cell cover includes roundoff padding.
        if radius < MAX_WGS84_DISTANCE_M:
            if center is None:
                center = unit_vectors(longitude, latitude)
            difference = vectors[candidates] - center
            chord_squared = np.einsum("ij,ij->i", difference, difference)
            angle = min((radius + 1e-5) / 6_335_000.0, np.pi)
            threshold = 2 * np.sin(angle / 2)
            candidates = candidates[chord_squared <= threshold * threshold + 1e-14]
        distances = wgs84_distance_m(
            active["longitude"][candidates],
            active["latitude"][candidates],
            longitude,
            latitude,
        )
        decisive = np.abs(distances - radius) <= _DECISION_MARGIN_M
        if decisive.any():
            distances[decisive] = exact_wgs84_distance_m(
                active["longitude"][candidates[decisive]],
                active["latitude"][candidates[decisive]],
                longitude,
                latitude,
            )
        inside = distances <= radius
        return candidates[inside], distances[inside]

    def _nearest(
        self, latitude: float, longitude: float, max_radius: float | None
    ) -> tuple[np.ndarray, np.ndarray]:
        """Expand complete inner caps until three eligible rows are enclosed.

        :param latitude: Query-center latitude.
        :param longitude: Query-center longitude.
        :param max_radius: Inclusive cap, or ``None`` for global nearest.
        :returns: At most three rows in the contract's deterministic order.
        """
        limit = MAX_WGS84_DISTANCE_M if max_radius is None else max_radius
        search = min(limit, 150_000.0)
        while True:
            rows, distances = self._select(latitude, longitude, search)
            if search >= limit or (
                len(rows) >= 3 and np.count_nonzero(distances < search - 1e-5) >= 3
            ):
                break
            search = min(limit, search * 2)
        if not len(rows):
            return rows, distances
        active = self._active
        if active is None:
            raise RuntimeError("AlongTrack window has not been prepared")
        top = np.argpartition(distances, min(3, len(rows)) - 1)[:3]
        decisive = np.any(
            np.abs(distances[:, None] - distances[top][None, :]) <= _DECISION_MARGIN_M,
            axis=1,
        )
        if decisive.any():
            distances[decisive] = exact_wgs84_distance_m(
                active["longitude"][rows[decisive]],
                active["latitude"][rows[decisive]],
                longitude,
                latitude,
            )
        order = np.lexsort(
            (
                active["longitude"][rows],
                active["latitude"][rows],
                active["time_us"][rows],
                distances,
            )
        )[:3]
        return rows[order], distances[order]

    def _materialize(
        self,
        fields: list[along_track_fields],
        rows: np.ndarray,
        distances: np.ndarray,
        date: datetime,
        *,
        nearest: bool,
    ) -> Dataset[along_track_fields] | None:
        """Project stored values into a schema-backed OceanDB dataset.

        :param fields: Requested field names in result order.
        :param rows: Selected active-window row indices.
        :param distances: Exact WGS84 distances aligned with ``rows``.
        :param date: Query-center time for ``delta_t``.
        :param nearest: Include mandatory nearest-neighbor ``distance``.
        :returns: A normal OceanDB dataset or ``None`` for no matches.
        """
        if not len(rows):
            return None
        active = self._active
        if active is None:
            raise RuntimeError("AlongTrack window has not been prepared")
        requested = list(dict.fromkeys(fields))
        if nearest and "distance" not in requested:
            requested.append("distance")
        data: dict[along_track_fields, np.ndarray] = {}
        for name in requested:
            if name not in along_track_schema:
                raise ValueError(f"Unknown AlongTrack field: {name}")
            if name == "distance":
                values = distances
            elif name == "delta_t":
                values = (_microseconds(date) - active["time_us"][rows]) / 1_000_000
            elif name == "date_time":
                values = np.asarray(
                    [
                        _EPOCH + timedelta(microseconds=int(us))
                        for us in active["time_us"][rows]
                    ],
                    dtype=object,
                )
            elif name == "file_name":
                values = np.asarray(
                    [
                        self._active_sources[int(index)].path.name
                        for index in active["file_id"][rows]
                    ],
                    dtype=str,
                )
            elif name == "mission":
                values = np.asarray(
                    [
                        self._active_sources[int(index)].mission
                        for index in active["file_id"][rows]
                    ],
                    dtype=str,
                )
            else:
                values = active[name][rows]
            data[name] = np.asarray(values)
        dtypes: dict[along_track_fields, type] = {}
        for name in requested:
            python_type = along_track_schema[name].python_type
            if python_type is not None:
                dtypes[name] = python_type
        return Dataset(
            name="along_track", data=data, dtypes=dtypes, schema=along_track_schema
        )

    def _batch(
        self,
        fields: list[along_track_fields],
        latitudes: list[float],
        longitudes: list[float],
        dates: list[datetime],
        radius: float | None,
        time_window: timedelta,
        missions: list[Mission] | None,
        *,
        nearest: bool,
    ) -> Generator[Dataset[along_track_fields] | None, None, None]:
        """Execute scalar centers over reusable prepared windows serially.

        :param fields: Requested OceanDB columns.
        :param latitudes: Query-center latitudes.
        :param longitudes: Query-center longitudes.
        :param dates: Query-center datetimes.
        :param radius: Inclusive radius or nearest cap.
        :param time_window: Temporal half-width.
        :param missions: Literal mission filter, or ``None`` for all.
        :param nearest: Select three nearest rows instead of all inside radius.
        :returns: One result per center in input order.
        """
        if len(latitudes) != len(longitudes) or len(latitudes) != len(dates):
            raise ValueError("latitudes, longitudes, and dates must have equal lengths")
        unknown = set(fields).difference(along_track_schema)
        if unknown:
            raise ValueError(f"Unknown AlongTrack fields: {sorted(unknown)}")
        if radius is not None and (not np.isfinite(radius) or radius < 0):
            raise ValueError("radius must be finite and nonnegative")
        if latitudes:
            self._validate_inventory()
        for latitude, longitude, date in zip(latitudes, longitudes, dates, strict=True):
            self.prepare_window(date, time_window, missions)
            rows, distances = (
                self._nearest(latitude, longitude, radius)
                if nearest
                else self._select(
                    latitude, longitude, 500_000.0 if radius is None else radius
                )
            )
            yield self._materialize(fields, rows, distances, date, nearest=nearest)

    def geographic_point_in_r_dt(
        self,
        fields: list[along_track_fields],
        latitude: float,
        longitude: float,
        date: datetime,
        radius: float = 500_000.0,
        time_window: timedelta = timedelta(days=10),
        missions: list[Mission] | None = None,
    ) -> Dataset[along_track_fields] | None:
        """Return OceanDB observations in an inclusive spatial/time window."""
        return next(
            self.geographic_point_in_r_dt_batch(
                fields, [latitude], [longitude], [date], radius, time_window, missions
            )
        )

    def geographic_point_in_r_dt_batch(
        self,
        fields: list[along_track_fields],
        latitudes: list[float],
        longitudes: list[float],
        dates: list[datetime],
        radius: float = 500_000.0,
        time_window: timedelta = timedelta(days=10),
        missions: list[Mission] | None = None,
        *,
        n_jobs: int = 1,
        chunk_size: int = 16,
    ) -> Generator[Dataset[along_track_fields] | None, None, None]:
        """Yield radius-window results serially in input order."""
        if n_jobs != 1 or chunk_size < 1:
            raise ValueError("indexed batches require n_jobs=1 and positive chunk_size")
        return self._batch(
            fields,
            latitudes,
            longitudes,
            dates,
            radius,
            time_window,
            missions,
            nearest=False,
        )

    def geographic_nearest_neighbors(
        self,
        fields: list[along_track_fields],
        latitude: float,
        longitude: float,
        date: datetime,
        time_window: timedelta = timedelta(days=10),
        missions: list[Mission] | None = None,
        *,
        max_radius: float | None = None,
    ) -> Dataset[along_track_fields] | None:
        """Return at most three nearest eligible observations."""
        return next(
            self.geographic_nearest_neighbors_batch(
                fields,
                [latitude],
                [longitude],
                [date],
                time_window,
                missions,
                max_radius=max_radius,
            )
        )

    def geographic_nearest_neighbors_batch(
        self,
        fields: list[along_track_fields],
        latitudes: list[float],
        longitudes: list[float],
        dates: list[datetime],
        time_window: timedelta = timedelta(days=10),
        missions: list[Mission] | None = None,
        *,
        max_radius: float | None = None,
        n_jobs: int = 1,
        chunk_size: int = 16,
    ) -> Generator[Dataset[along_track_fields] | None, None, None]:
        """Yield three-nearest results serially in input order."""
        if n_jobs != 1 or chunk_size < 1:
            raise ValueError("indexed batches require n_jobs=1 and positive chunk_size")
        return self._batch(
            fields,
            latitudes,
            longitudes,
            dates,
            max_radius,
            time_window,
            missions,
            nearest=True,
        )
