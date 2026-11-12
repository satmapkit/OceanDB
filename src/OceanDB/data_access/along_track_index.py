"""Source inventory, reusable caches, and immutable AlongTrack query windows."""

from __future__ import annotations

import json
import os
import tempfile
import warnings
from _thread import LockType
from collections import OrderedDict
from collections.abc import Callable, Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta
from hashlib import sha256
from importlib import resources
from pathlib import Path
from threading import Lock
from time import perf_counter
from typing import get_args

import netCDF4 as nc
import numpy as np
from numpy.typing import NDArray

from OceanDB.along_track_missions import Mission
from OceanDB.data_access.indexed_geometry import (
    MAX_WGS84_DISTANCE_M,
    cap_cells,
    cell_dimensions,
    cell_ids,
    unit_vectors,
)
from OceanDB.ocean_data.basins import BasinConnections, BasinMask
from OceanDB.schemas.along_track_schema import along_track_columns_schema

_PRODUCT = "SEALEVEL_GLO_PHY_L3_MY_008_062"
_EPOCH = datetime(1950, 1, 1)
_CACHE_VERSION = 2
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
_GridGeometry = tuple[int, NDArray[np.int32], NDArray[np.float64]]


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
    """A source's pinned fingerprint, decoded coverage, and mission identity."""

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


@dataclass(frozen=True)
class PreparedWindow:
    """Read-only rows with their provenance and fully loaded pruning geometry.

    Windows retain their dependencies, not their index owner, and remain usable
    after another window is prepared or the source inventory is refreshed.
    Exact WGS84 membership and ranking belong to the query backend.
    """

    rows: NDArray[np.void]
    _offsets: NDArray[np.int64]
    _vectors: NDArray[np.float64]
    _sources: tuple[_Source, ...]
    _basins: BasinMask
    _connections: dict[int, list[int]]
    _grid_geometry: dict[tuple[float, float, float], _GridGeometry]
    _cell_size: float
    _stats: IndexedCacheInfo
    _stats_lock: LockType

    def candidates(
        self, latitude: float, longitude: float, radius: float
    ) -> NDArray[np.intp]:
        """Conservatively prune by cell, direct basin connection, and chord.

        :param latitude: Query-center latitude in degrees.
        :param longitude: Query-center longitude in degrees.
        :param radius: Inclusive WGS84 radius in meters.
        :returns: Candidate row indices, possibly outside the exact radius.
        :raises ValueError: If the radius or center is invalid.
        """
        active, offsets, vectors = self.rows, self._offsets, self._vectors
        if not np.isfinite(radius) or radius < 0:
            raise ValueError("radius must be finite and nonnegative")
        geometry = self._grid_geometry.get((longitude, latitude, radius))
        if geometry is None:
            basin = int(self._basins.lookup(latitude, longitude))
            cells = cap_cells(longitude, latitude, radius, self._cell_size)
            center = None
        else:
            with self._stats_lock:
                self._stats.geometry_hits += 1
            basin, cells, center = geometry
        connected = self._connections[basin]
        segments = [
            np.arange(offsets[cell], offsets[cell + 1], dtype=np.intp)
            for cell in cells
            if offsets[cell] < offsets[cell + 1]
        ]
        if not segments:
            return np.empty(0, dtype=np.intp)
        candidates = np.concatenate(segments)
        candidates = candidates[np.isin(active["basin_id"][candidates], connected)]
        if not len(candidates):
            return candidates
        # This screen only over-selects; exact WGS84 membership uses no epsilon.
        if radius < MAX_WGS84_DISTANCE_M:
            if center is None:
                center = unit_vectors(longitude, latitude)
            difference = vectors[candidates] - center
            chord_squared = np.einsum("ij,ij->i", difference, difference)
            angle = min((radius + 1e-5) / 6_335_000.0, np.pi)
            threshold = 2 * np.sin(angle / 2)
            candidates = candidates[chord_squared <= threshold * threshold + 1e-14]
        return candidates

    def column(self, name: str, rows: NDArray[np.intp]) -> NDArray[np.generic]:
        """Project a stored column or resolve its file provenance.

        :param name: Stored column name, ``file_name``, or ``mission``.
        :param rows: Selected indices into this window's rows.
        :returns: Values aligned with the selected rows.
        """
        if name == "file_name":
            return np.asarray(
                [
                    self._sources[int(index)].path.name
                    for index in self.rows["file_id"][rows]
                ],
                dtype=str,
            )
        if name == "mission":
            return np.asarray(
                [
                    self._sources[int(index)].mission
                    for index in self.rows["file_id"][rows]
                ],
                dtype=str,
            )
        return self.rows[name][rows]


class AlongTrackIndex:
    """Own source discovery, decoding, caches, and serial window preparation.

    Construction reuses a valid saved inventory without rescanning. New windows
    validate only selected sources; exact-key hits reuse the same immutable
    window without filesystem checks. Call :meth:`refresh` to discover changes.
    The instance is not generally thread-safe. :meth:`preparation` gives a stream
    exclusive preparation ownership while query workers retain immutable windows.

    :param data_root: ``ALONG_TRACK_DATA_DIRECTORY`` or its product directory.
    :param cache_dir: Directory for reusable, versioned local cache artifacts.
    :param cell_size_degrees: Cell width, dividing both 180 and 360 degrees.
    :param max_cache_bytes: Approximate retained per-file RAM limit, not windows.
    :param max_geometry_cache_bytes: Upper bound for retained grid geometry.
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
        self._stream_lock = Lock()
        self._stats_lock = Lock()
        self._files: OrderedDict[Path, NDArray[np.void]] = OrderedDict()
        self._grid_geometry: dict[tuple[float, float, float], _GridGeometry] = {}
        self._window: PreparedWindow | None = None
        self._active_key: tuple[int, int, tuple[str, ...]] | None = None
        self._catalog_path = (
            self.cache_dir
            / f"catalog-{sha256(str(self.data_root).encode()).hexdigest()[:20]}.json"
        )
        sources = self._read_catalog()
        self._sources: list[_Source] = sources if sources is not None else []
        self._missions = tuple(sorted({source.mission for source in self._sources}))
        if sources is None:
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
    def _times(
        variable: nc.Variable, *, bounds_only: bool = False
    ) -> NDArray[np.int64]:
        """Decode CF times to exact Python microseconds as in PostgreSQL ETL.

        :param variable: Source time variable.
        :param bounds_only: Decode only extrema after validating every raw time.
        :returns: Integer microseconds for each observation, or just its bounds.
        :raises ValueError: If the calendar or times cannot be represented.
        """
        raw = np.ma.asarray(variable[:])
        if (
            raw.ndim != 1
            or np.ma.getmaskarray(raw).any()
            or raw.dtype.kind not in "iuf"
            or not np.isfinite(raw.data).all()
        ):
            raise ValueError(
                "AlongTrack time must be a complete finite one-dimensional numeric array"
            )
        calendar = getattr(variable, "calendar", "standard")
        if calendar not in ("standard", "gregorian", "proleptic_gregorian"):
            raise ValueError(f"Unsupported AlongTrack calendar: {calendar}")
        if bounds_only and raw.size:
            # Supported CF units/calendars are monotonic in the numeric time.
            raw = np.ma.asarray((raw.min(), raw.max()))
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
            times = self._times(dataset.variables["time"], bounds_only=True)
        self.stats.catalog_reads += 1
        if not len(times):
            raise ValueError(f"No observations in AlongTrack source: {path}")
        return int(times.min()), int(times.max())

    def _catalog_identity(self, value: object) -> bool:
        """Check the root and version shared by complete and progress manifests.

        :param value: Decoded JSON header or manifest.
        :returns: Whether the manifest belongs to this source root and decoder.
        """
        return (
            isinstance(value, dict)
            and type(value.get("version")) is int
            and value.get("version") == _CACHE_VERSION
            and value.get("root") == str(self.data_root)
        )

    def _catalog_source(self, value: object) -> _Source:
        """Validate a source record without accessing its filesystem metadata.

        :param value: Decoded JSON source record.
        :returns: A source pinned to this root with supported coverage and mission.
        :raises ValueError: If a field, path, or coverage bound is invalid.
        """
        if not isinstance(value, dict):
            raise ValueError("Invalid AlongTrack catalog source")
        relative: object = value.get("path")
        size: object = value.get("size")
        mtime_ns: object = value.get("mtime_ns")
        start: object = value.get("start_us")
        end: object = value.get("end_us")
        if not isinstance(relative, str) or "\\" in relative or "\x00" in relative:
            raise ValueError("Invalid AlongTrack catalog path")
        path = Path(relative)
        if (
            path.is_absolute()
            or len(path.parts) != 4
            or ".." in path.parts
            or path.as_posix() != relative
            or path.suffix != ".nc"
        ):
            raise ValueError("Unsafe AlongTrack catalog path")
        if (
            type(size) is not int
            or not 0 <= size <= 2**63 - 1
            or type(mtime_ns) is not int
            or not 0 <= mtime_ns <= 2**63 - 1
            or type(start) is not int
            or type(end) is not int
            or not _microseconds(datetime.min)
            <= start
            <= end
            <= _microseconds(datetime.max)
        ):
            raise ValueError("Invalid AlongTrack catalog fingerprint or coverage")
        return _Source(
            self.data_root / path, size, mtime_ns, start, end, self._mission(path)
        )

    def _read_catalog(self) -> list[_Source] | None:
        """Load only a complete validated inventory, never discover source files.

        :returns: The saved snapshot, or ``None`` when it must be rebuilt.
        """
        try:
            manifest: object = json.loads(self._catalog_path.read_bytes())
            if not isinstance(manifest, dict) or not self._catalog_identity(manifest):
                return None
            entries: object = manifest.get("sources")
            if not isinstance(entries, list) or not entries:
                return None
            sources = [self._catalog_source(entry) for entry in entries]
            if len({source.path for source in sources}) != len(sources):
                return None
            return sources
        except (OSError, ValueError, TypeError):
            return None

    def _catalog_entry(self, source: _Source) -> dict[str, str | int]:
        """Serialize the shared complete-catalog and journal source record.

        :param source: Completed source coverage and fingerprint.
        :returns: Root-relative JSON fields.
        """
        return {
            "path": source.path.relative_to(self.data_root).as_posix(),
            "size": source.size,
            "mtime_ns": source.mtime_ns,
            "start_us": source.start_us,
            "end_us": source.end_us,
        }

    def _resume_catalog(self, path: Path) -> dict[Path, _Source]:
        """Recover validated completed entries and discard an unfinished tail.

        :param path: Separate append-only coverage progress journal.
        :returns: Reusable candidates, still requiring current source stat checks.
        """
        sources: dict[Path, _Source] = {}
        # shortcut: one catalog writer per cache; add a process lock for concurrent refreshes.
        with path.open("a+b") as stream:
            stream.seek(0)
            header = stream.readline()
            try:
                valid = header.endswith(b"\n") and self._catalog_identity(
                    json.loads(header)
                )
            except (ValueError, TypeError):
                valid = False
            if not valid:
                stream.seek(0)
                stream.truncate()
                identity = {"version": _CACHE_VERSION, "root": str(self.data_root)}
                stream.write(json.dumps(identity).encode() + b"\n")
                stream.flush()
                return sources
            while True:
                offset = stream.tell()
                line = stream.readline()
                if not line:
                    break
                if not line.endswith(b"\n"):
                    stream.truncate(offset)
                    break
                try:
                    source = self._catalog_source(json.loads(line))
                except (ValueError, TypeError):
                    continue
                sources[source.path] = source
        return sources

    def refresh(self) -> None:
        """Publish a complete new inventory before invalidating the RAM snapshot.

        New files become visible only after this explicit operation. A failed
        refresh leaves the previous inventory and prepared RAM/disk rows usable.
        Completed coverage work is journaled for recovery on the next attempt.

        :raises RuntimeError: If a stream is active on this instance.
        """
        self.require_idle()
        previous = {source.path: source for source in self._read_catalog() or ()}
        journal_path = self._catalog_path.with_suffix(".progress.jsonl")
        previous.update(self._resume_catalog(journal_path))
        paths = sorted(self.data_root.glob("*/*/*/*.nc"))
        if not paths:
            raise FileNotFoundError(
                f"No mission/year/month/*.nc files under {self.data_root}"
            )
        sources: list[_Source] = []
        with journal_path.open("ab") as journal:
            for path in paths:
                mission = self._mission(path)
                stat = path.stat()
                cached = previous.get(path)
                if cached is not None and (cached.size, cached.mtime_ns) == (
                    stat.st_size,
                    stat.st_mtime_ns,
                ):
                    source = cached
                else:
                    start, end = self._coverage(path)
                    source = _Source(
                        path, stat.st_size, stat.st_mtime_ns, start, end, mission
                    )
                    self._check_source(source)
                    journal.write(
                        json.dumps(self._catalog_entry(source)).encode() + b"\n"
                    )
                    journal.flush()
                sources.append(source)
        _atomic_json(
            self._catalog_path,
            {
                "version": _CACHE_VERSION,
                "root": str(self.data_root),
                "sources": [self._catalog_entry(source) for source in sources],
            },
        )
        self._sources = sources
        self._missions = tuple(sorted({source.mission for source in sources}))
        self._files.clear()
        self._window = None
        self._active_key = None
        self.stats.cached_bytes = self.stats.active_bytes = 0
        try:
            journal_path.unlink()
        except OSError:
            pass

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
        :raises RuntimeError: If a stream is active on this instance.
        """
        self.require_idle()
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

    def _prepare_file(self, source: _Source) -> NDArray[np.void]:
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

    def _load_file(self, source: _Source) -> NDArray[np.void]:
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
        rows: NDArray[np.void] | None = None
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
    ) -> PreparedWindow:
        """Validate selected sources and return immutable rows for spatial queries.

        An already prepared key returns the same object without source checks.
        Call :meth:`refresh` to incorporate archive edits, including coverage
        changes in previously unselected files.

        :param date: Center datetime.
        :param time_window: Nonnegative temporal half-width.
        :param missions: Literal mission selection, or ``None`` for all.
        :returns: A stable window with its own pruning and provenance references.
        :raises RuntimeError: If a source changed or a stream is active.
        """
        self.require_idle()
        return self._prepare_window(date, time_window, missions)

    def require_idle(self) -> None:
        """Reject external operations while a stream owns preparation.

        :raises RuntimeError: If a stream is active on this instance.
        """
        if self._stream_lock.locked():
            raise RuntimeError(
                "An indexed stream is active; close it before other operations"
            )

    @contextmanager
    def preparation(
        self,
    ) -> Iterator[
        Callable[[datetime, timedelta, Sequence[Mission] | None], PreparedWindow]
    ]:
        """Reserve serial preparation for a stream's exclusive producer.

        Query workers use returned windows without acquiring this lock.

        :returns: A context yielding the producer's window-preparation callable.
        :raises RuntimeError: If another stream owns preparation.
        """
        # shortcut: one stream per instance; coordinate cache mutation before allowing overlap.
        if not self._stream_lock.acquire(blocking=False):
            raise RuntimeError("An indexed stream is already active on this instance")
        try:
            yield self._prepare_window
        finally:
            self._stream_lock.release()

    def _prepare_window(
        self,
        date: datetime,
        time_window: timedelta,
        missions: Sequence[Mission] | None = None,
    ) -> PreparedWindow:
        """Prepare a window under the caller's exclusive ownership.

        :param date: Center datetime.
        :param time_window: Nonnegative temporal half-width.
        :param missions: Literal mission selection, or ``None`` for all.
        :returns: The reused or newly prepared immutable window.
        """
        center = _microseconds(date)
        if time_window < timedelta(0):
            raise ValueError("time_window must be nonnegative")
        half = (
            time_window.days * 86_400 + time_window.seconds
        ) * 1_000_000 + time_window.microseconds
        selection = (
            tuple(sorted(set(missions))) if missions is not None else self._missions
        )
        key = (center, half, selection)
        if self._active_key == key and self._window is not None:
            return self._window
        started = perf_counter()
        # Empty and disk-cached windows must also load lazy basin data before queries.
        _ = self._basins.data
        lower, upper = center - half, center + half
        selected = tuple(
            source
            for source in self._sources
            if source.mission in selection
            and source.start_us <= upper
            and source.end_us >= lower
        )
        self._window = None
        self._active_key = None
        self.stats.active_bytes = 0
        chunks: list[NDArray[np.void]] = []
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
        active = active[order]
        counts = np.bincount(cells, minlength=self._nx * self._ny)
        offsets = np.concatenate((np.zeros(1, dtype=np.int64), np.cumsum(counts)))
        vectors = unit_vectors(active["longitude"], active["latitude"])
        active.setflags(write=False)
        offsets.setflags(write=False)
        vectors.setflags(write=False)
        prepared = PreparedWindow(
            active,
            offsets,
            vectors,
            selected,
            self._basins,
            self._connections,
            self._grid_geometry,
            self._cell_size,
            self.stats,
            self._stats_lock,
        )
        self._window = prepared
        self._active_key = key
        self.stats.window_builds += 1
        self.stats.active_bytes = active.nbytes + vectors.nbytes + offsets.nbytes
        self._record_retained()
        self.stats.preparation_seconds += perf_counter() - started
        return prepared
