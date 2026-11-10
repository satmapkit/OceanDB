"""Database-free complete-catalog and interrupted-discovery regressions."""

import json
import os
import shutil
from dataclasses import replace
from datetime import datetime, timedelta
from pathlib import Path

import netCDF4 as nc
import numpy as np
import pytest

from OceanDB.data_access import indexed_along_track as indexed
from tests.data.along_track_reference.generate import SOURCE_NAME, TIME_RAW, TIME_UTC

pytestmark = pytest.mark.unit

_ASSET = Path(__file__).parents[1] / "data" / "along_track_reference" / SOURCE_NAME
_DATE = datetime.fromisoformat(TIME_UTC[0].removesuffix("Z"))


@pytest.fixture
def source_layout(tmp_path: Path) -> tuple[Path, Path, Path]:
    """Copy the literal NetCDF fixture into the discovery layout.

    :param tmp_path: Isolated pytest temporary directory.
    :returns: Product root, cache directory, and source path.
    """
    product = tmp_path / "product"
    month = product / "mission" / "2024" / "01"
    month.mkdir(parents=True)
    source = month / SOURCE_NAME
    shutil.copyfile(_ASSET, source)
    return product, tmp_path / "cache", source


def _append_source(source: Path, day: int = 3) -> Path:
    """Add a second source without changing the original source fingerprint.

    :param source: Original literal source.
    :param day: Distinct basename day, not an assumption about decoded times.
    :returns: Newly copied source path.
    """
    added = source.with_name(source.name.replace("20240102.nc", f"202401{day:02d}.nc"))
    shutil.copyfile(source, added)
    return added


def test_warm_constructor_is_scan_free_and_keeps_saved_visibility(
    source_layout: tuple[Path, Path, Path], monkeypatch: pytest.MonkeyPatch
) -> None:
    """Appended sources remain invisible across a scan-free restart.

    :param source_layout: Product root, cache directory, and source.
    :param monkeypatch: Scoped method patcher.
    """
    product, cache, source = source_layout
    first = indexed.IndexedAlongTrackBackend(product, cache)
    added = _append_source(source)
    stat = Path.stat
    dataset = nc.Dataset

    def forbid_discovery(*args: object, **kwargs: object) -> None:
        pytest.fail("Warm construction must not discover sources")

    def guarded_stat(path: Path, *, follow_symlinks: bool = True) -> os.stat_result:
        assert path not in (source, added), "Warm construction must not stat sources"
        return stat(path, follow_symlinks=follow_symlinks)

    def guarded_dataset(
        filename: str | Path, *, memory: bytes | None = None
    ) -> nc.Dataset:
        assert Path(filename) not in (
            source,
            added,
        ), "Warm construction must not read sources"
        return dataset(filename, memory=memory)

    monkeypatch.setattr(Path, "glob", forbid_discovery)
    monkeypatch.setattr(Path, "stat", guarded_stat)
    monkeypatch.setattr(nc, "Dataset", guarded_dataset)
    restarted = indexed.IndexedAlongTrackBackend(product, cache)
    assert restarted._sources == first._sources
    assert restarted.stats.catalog_reads == restarted.stats.source_reads == 0
    assert [entry.path for entry in restarted._sources] == [source]


def test_explicit_refresh_discovers_and_publishes_actual_coverage(
    source_layout: tuple[Path, Path, Path],
) -> None:
    """Refresh reuses unchanged coverage and publishes decoded rather than filename times.

    :param source_layout: Product root, cache directory, and source.
    """
    product, cache, source = source_layout
    backend = indexed.IndexedAlongTrackBackend(product, cache)
    backend.prepare_window(_DATE, timedelta(0))
    added = _append_source(source)
    with nc.Dataset(added, "r+") as dataset:
        dataset["time"][:] = dataset["time"][:] + 100
    assert backend.stats.cached_bytes > 0 and backend.stats.active_bytes > 0
    backend.refresh()
    assert [entry.path for entry in backend._sources] == [source, added]
    assert backend.stats.catalog_reads == 2
    assert backend.stats.cached_bytes == backend.stats.active_bytes == 0
    first, second = backend._sources
    assert first.start_us == indexed._microseconds(_DATE)
    assert first.end_us == indexed._microseconds(
        datetime.fromisoformat(TIME_UTC[-1].removesuffix("Z"))
    )
    assert second.start_us == first.start_us + 100 * 86_400_000_000
    assert second.end_us == first.end_us + 100 * 86_400_000_000
    restarted = indexed.IndexedAlongTrackBackend(product, cache)
    assert restarted._sources == backend._sources
    assert restarted.stats.catalog_reads == 0


@pytest.mark.parametrize(
    ("field", "invalid"),
    [
        ("manifest", []),
        ("version", True),
        ("version", "2"),
        ("version", -1),
        ("root", "/another/root"),
        ("sources", {}),
        ("sources", []),
        ("entry", None),
        ("duplicate", None),
        ("path", None),
        ("path", f"/mission/2024/01/{SOURCE_NAME}"),
        ("path", f"../2024/01/{SOURCE_NAME}"),
        ("path", f"mission/../01/{SOURCE_NAME}"),
        ("path", f"mission/2024/./01/{SOURCE_NAME}"),
        ("path", f"mission//2024/01/{SOURCE_NAME}"),
        ("path", f"mission\\2024\\01\\{SOURCE_NAME}"),
        ("path", f"mission/2024/01/{SOURCE_NAME}\x00"),
        ("path", f"mission/2024/{SOURCE_NAME}"),
        ("path", f"mission/2024/01/{SOURCE_NAME.removesuffix('.nc')}.txt"),
        ("path", f"mission/2024/01/{SOURCE_NAME.replace('_j2_', '_unknown_')}"),
        ("size", True),
        ("size", "1"),
        ("size", -1),
        ("size", 2**63),
        ("mtime_ns", True),
        ("mtime_ns", "1"),
        ("mtime_ns", -1),
        ("mtime_ns", 2**63),
        ("start_us", True),
        ("start_us", 1.0),
        ("start_us", -(2**63)),
        ("start_us", 2**63),
        ("end_us", None),
        ("end_us", -1),
        ("end_us", 2**63),
    ],
)
def test_invalid_catalog_is_rebuilt_without_trusting_records(
    source_layout: tuple[Path, Path, Path], field: str, invalid: object
) -> None:
    """Reject malformed schemas, unsafe paths, missions, and metadata ranges.

    :param source_layout: Product root, cache directory, and source.
    :param field: Manifest or record component to corrupt.
    :param invalid: Invalid JSON-compatible replacement.
    """
    product, cache, _ = source_layout
    first = indexed.IndexedAlongTrackBackend(product, cache)
    catalog = first._catalog_path
    original = catalog.read_bytes()
    manifest: object = json.loads(original)
    assert isinstance(manifest, dict)
    entries = manifest["sources"]
    assert isinstance(entries, list)
    if field == "manifest":
        manifest = invalid
    elif field in ("version", "root", "sources"):
        manifest[field] = invalid
    elif field == "entry":
        entries[0] = invalid
    elif field == "duplicate":
        entries.append(entries[0])
    else:
        entries[0][field] = invalid
    catalog.write_text(json.dumps(manifest), encoding="utf-8")
    rebuilt = indexed.IndexedAlongTrackBackend(product, cache)
    assert rebuilt._sources == first._sources
    assert rebuilt.stats.catalog_reads == 1
    assert catalog.read_bytes() == original


@pytest.mark.parametrize("damaged", [b"{", b"\xff", b"null"])
def test_unreadable_catalog_is_rebuilt(
    source_layout: tuple[Path, Path, Path], damaged: bytes
) -> None:
    """Treat invalid JSON and invalid encoding as cache misses.

    :param source_layout: Product root, cache directory, and source.
    :param damaged: Incomplete or non-manifest file contents.
    """
    product, cache, _ = source_layout
    first = indexed.IndexedAlongTrackBackend(product, cache)
    first._catalog_path.write_bytes(damaged)
    rebuilt = indexed.IndexedAlongTrackBackend(product, cache)
    assert rebuilt._sources == first._sources
    assert rebuilt.stats.catalog_reads == 1


@pytest.mark.parametrize("interruption", [RuntimeError, KeyboardInterrupt])
@pytest.mark.parametrize("tail", ["empty", "truncated", "invalid", "unterminated"])
def test_cold_discovery_resumes_completed_work_but_not_partial_inventory(
    source_layout: tuple[Path, Path, Path],
    monkeypatch: pytest.MonkeyPatch,
    interruption: type[BaseException],
    tail: str,
) -> None:
    """Retain flushed entries across failures while rediscovering the full source list.

    :param source_layout: Product root, cache directory, and source.
    :param monkeypatch: Scoped method patcher.
    :param interruption: Exception simulating failure or process interruption.
    :param tail: Optional invalid or unterminated source records.
    """
    product, cache, source = source_layout
    second = _append_source(source)
    coverage = indexed.IndexedAlongTrackBackend._coverage
    reads: list[Path] = []
    publishes: list[Path] = []
    atomic_json = indexed._atomic_json

    def interrupted(
        backend: indexed.IndexedAlongTrackBackend, path: Path
    ) -> tuple[int, int]:
        reads.append(path)
        if path == second:
            raise interruption("interrupted coverage")
        return coverage(backend, path)

    def publish(path: Path, payload: object) -> None:
        publishes.append(path)
        atomic_json(path, payload)

    monkeypatch.setattr(indexed.IndexedAlongTrackBackend, "_coverage", interrupted)
    monkeypatch.setattr(indexed, "_atomic_json", publish)
    with pytest.raises(interruption, match="interrupted coverage"):
        indexed.IndexedAlongTrackBackend(product, cache)
    assert reads == [source, second]
    assert publishes == []
    assert list(cache.glob("catalog-*.json")) == []
    journal = next(cache.glob("*.progress.jsonl"))
    lines = journal.read_bytes().splitlines()
    assert len(lines) == 2
    second_entry = json.loads(lines[1])
    second_entry["path"] = second.relative_to(product).as_posix()
    second_entry["size"] = second.stat().st_size
    second_entry["mtime_ns"] = second.stat().st_mtime_ns
    with journal.open("ab") as stream:
        if tail == "truncated":
            stream.write(b'{"path":')
        elif tail == "invalid":
            second_entry["end_us"] = -1
            stream.write(json.dumps(second_entry).encode() + b'\n{"path":')
        elif tail == "unterminated":
            stream.write(json.dumps(second_entry).encode())
    third = _append_source(source, 4)
    reads.clear()

    def resumed(
        backend: indexed.IndexedAlongTrackBackend, path: Path
    ) -> tuple[int, int]:
        reads.append(path)
        return coverage(backend, path)

    monkeypatch.setattr(indexed.IndexedAlongTrackBackend, "_coverage", resumed)
    rebuilt = indexed.IndexedAlongTrackBackend(product, cache)
    assert reads == [second, third]
    assert [entry.path for entry in rebuilt._sources] == [source, second, third]
    assert rebuilt.stats.catalog_reads == 2
    assert publishes == [rebuilt._catalog_path]
    assert not journal.exists()


@pytest.mark.parametrize(
    "corruption", ["truncated", "schema", "version", "root", "boolean"]
)
def test_invalid_journal_identity_does_not_reuse_completed_entries(
    source_layout: tuple[Path, Path, Path], corruption: str
) -> None:
    """Require a complete root/version header before reusing journal records.

    :param source_layout: Product root, cache directory, and source.
    :param corruption: Invalid or incomplete journal header component.
    """
    product, cache, _ = source_layout
    first = indexed.IndexedAlongTrackBackend(product, cache)
    record = json.dumps(first._catalog_entry(first._sources[0])).encode() + b"\n"
    journal = first._catalog_path.with_suffix(".progress.jsonl")
    header: dict[str, str | int] = {
        "version": indexed._CACHE_VERSION,
        "root": str(product),
    }
    if corruption == "version":
        header["version"] = -1
    elif corruption == "root":
        header["root"] = "/another/root"
    elif corruption == "boolean":
        header["version"] = True
    elif corruption == "schema":
        header = {}
    invalid_header = json.dumps(header).encode() + b"\n"
    if corruption == "truncated":
        invalid_header = b'{"version":'
    journal.write_bytes(invalid_header + record)
    first._catalog_path.unlink()
    rebuilt = indexed.IndexedAlongTrackBackend(product, cache)
    assert rebuilt._sources == first._sources
    assert rebuilt.stats.catalog_reads == 1


def test_journal_entry_requires_current_source_fingerprint(
    source_layout: tuple[Path, Path, Path],
) -> None:
    """Never reuse journal coverage after the source metadata changes.

    :param source_layout: Product root, cache directory, and source.
    """
    product, cache, source = source_layout
    first = indexed.IndexedAlongTrackBackend(product, cache)
    manifest = json.loads(first._catalog_path.read_bytes())
    journal = first._catalog_path.with_suffix(".progress.jsonl")
    journal.write_text(
        json.dumps({"version": manifest["version"], "root": manifest["root"]})
        + "\n"
        + json.dumps(manifest["sources"][0])
        + "\n",
        encoding="utf-8",
    )
    first._catalog_path.unlink()
    stat = source.stat()
    os.utime(source, ns=(stat.st_atime_ns, stat.st_mtime_ns + 1_000_000_000))
    rebuilt = indexed.IndexedAlongTrackBackend(product, cache)
    assert rebuilt.stats.catalog_reads == 1
    assert rebuilt._sources[0].mtime_ns != first._sources[0].mtime_ns


@pytest.mark.parametrize("failure", ["coverage", "publish"])
def test_failed_refresh_preserves_complete_inventory_and_prepared_cache(
    source_layout: tuple[Path, Path, Path],
    monkeypatch: pytest.MonkeyPatch,
    failure: str,
) -> None:
    """Keep the old on-disk and RAM snapshots until publication succeeds.

    :param source_layout: Product root, cache directory, and source.
    :param monkeypatch: Scoped method patcher.
    :param failure: Fail during coverage or atomic catalog publication.
    """
    product, cache, source = source_layout
    backend = indexed.IndexedAlongTrackBackend(product, cache)
    backend.prepare_window(_DATE, timedelta(0))
    inventory = backend._sources
    active, vectors, offsets = backend._active, backend._vectors, backend._offsets
    active_key, active_sources = backend._active_key, backend._active_sources
    files = dict(backend._files)
    before = replace(backend.stats)
    original = backend._catalog_path.read_bytes()
    added = _append_source(source)

    def fail(*args: object, **kwargs: object) -> None:
        raise OSError("injected refresh failure")

    with monkeypatch.context() as scoped:
        if failure == "coverage":
            scoped.setattr(backend, "_coverage", fail)
        else:
            scoped.setattr(indexed, "_atomic_json", fail)
        with pytest.raises(OSError, match="injected refresh failure"):
            backend.refresh()
    assert backend._catalog_path.read_bytes() == original
    assert backend._sources is inventory
    assert (
        backend._active is active
        and backend._vectors is vectors
        and backend._offsets is offsets
    )
    assert (
        backend._active_key == active_key and backend._active_sources is active_sources
    )
    assert backend._files.keys() == files.keys()
    assert all(backend._files[path] is rows for path, rows in files.items())
    assert backend.stats == replace(
        before, catalog_reads=before.catalog_reads + (failure == "publish")
    )
    backend.prepare_window(_DATE, timedelta(0))
    assert backend._active is active
    restarted = indexed.IndexedAlongTrackBackend(product, cache)
    assert restarted._sources == inventory
    assert restarted.stats.catalog_reads == 0
    backend.refresh()
    assert [entry.path for entry in backend._sources] == [source, added]
    assert backend.stats.catalog_reads == before.catalog_reads + 1
    assert backend.stats.cached_bytes == backend.stats.active_bytes == 0


def test_coverage_read_rejects_source_changed_during_decoding(
    source_layout: tuple[Path, Path, Path],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Check source fingerprints both before and after a coverage read.

    :param source_layout: Product root, cache directory, and source.
    :param monkeypatch: Scoped method patcher.
    """
    product, cache, source = source_layout
    coverage = indexed.IndexedAlongTrackBackend._coverage

    def changing(
        backend: indexed.IndexedAlongTrackBackend, path: Path
    ) -> tuple[int, int]:
        bounds = coverage(backend, path)
        stat = path.stat()
        os.utime(path, ns=(stat.st_atime_ns, stat.st_mtime_ns + 1_000_000_000))
        return bounds

    monkeypatch.setattr(indexed.IndexedAlongTrackBackend, "_coverage", changing)
    with pytest.raises(RuntimeError, match="source changed"):
        indexed.IndexedAlongTrackBackend(product, cache)
    assert list(cache.glob("catalog-*.json")) == []
    assert len(next(cache.glob("*.progress.jsonl")).read_bytes().splitlines()) == 1
    monkeypatch.setattr(indexed.IndexedAlongTrackBackend, "_coverage", coverage)
    assert indexed.IndexedAlongTrackBackend(product, cache)._sources[0].path == source


@pytest.mark.parametrize("calendar", ["standard", "gregorian", "proleptic_gregorian"])
@pytest.mark.parametrize(
    ("dtype", "units", "raw", "expected"),
    [
        (
            "f8",
            "days since 1950-01-01",
            (1.5, -2, 0, -0.125),
            (129_600_000_000, -172_800_000_000, 0, -10_800_000_000),
        ),
        (
            "i8",
            "microseconds since 1950-01-01",
            (1, -1, 0, -86_400_000_001, 86_400_000_001),
            (1, -1, 0, -86_400_000_001, 86_400_000_001),
        ),
        (
            "f8",
            "days since 1950-01-01",
            (TIME_RAW[2], TIME_RAW[1], TIME_RAW[0], TIME_RAW[-1]),
            tuple(
                indexed._microseconds(
                    datetime.fromisoformat(TIME_UTC[i].removesuffix("Z"))
                )
                for i in (2, 1, 0, -1)
            ),
        ),
    ],
)
def test_coverage_decodes_only_extrema_with_exact_full_decoder_bounds(
    source_layout: tuple[Path, Path, Path],
    monkeypatch: pytest.MonkeyPatch,
    calendar: str,
    dtype: str,
    units: str,
    raw: tuple[float, ...],
    expected: tuple[int, ...],
) -> None:
    """Keep full row decoding exact while reducing unsorted source coverage to two values.

    :param source_layout: Product root, cache directory, and source.
    :param monkeypatch: Scoped function patcher.
    :param calendar: Supported CF calendar.
    :param dtype: Source numeric storage type.
    :param units: CF time units.
    :param raw: Unsorted times, including negative values or microsecond edges.
    :param expected: Independent decoded integer microseconds for all rows.
    """
    product, cache, _ = source_layout
    backend = indexed.IndexedAlongTrackBackend(product, cache)
    path = product.parent / "times.nc"
    with nc.Dataset(path, "w") as dataset:
        dataset.createDimension("obs", len(raw))
        variable = dataset.createVariable("time", dtype, ("obs",))
        variable.units, variable.calendar = units, calendar
        variable[:] = np.asarray(raw, dtype=dtype)
    num2date = nc.num2date
    decoded_counts: list[int] = []

    def decode(
        values: np.ndarray,
        units: str,
        *,
        calendar: str,
        only_use_cftime_datetimes: bool,
        only_use_python_datetimes: bool,
    ) -> object:
        decoded_counts.append(len(values))
        return num2date(
            values,
            units,
            calendar=calendar,
            only_use_cftime_datetimes=only_use_cftime_datetimes,
            only_use_python_datetimes=only_use_python_datetimes,
        )

    monkeypatch.setattr(nc, "num2date", decode)
    with nc.Dataset(path) as dataset:
        full = backend._times(dataset["time"])
        bounds = backend._times(dataset["time"], bounds_only=True)
    np.testing.assert_array_equal(full, expected)
    np.testing.assert_array_equal(bounds, [min(expected), max(expected)])
    assert backend._coverage(path) == (min(expected), max(expected))
    assert decoded_counts == [len(raw), 2, 2]


@pytest.mark.parametrize("bounds_only", [False, True])
@pytest.mark.parametrize(
    "raw",
    [
        np.asarray(0.0),
        np.asarray([[0.0, 1.0], [2.0, 3.0]]),
        np.asarray([0.0, nc.default_fillvals["f8"], 1.0]),
        np.asarray([0.0, np.nan, 1.0]),
        np.asarray([0.0, np.inf, 1.0]),
        np.asarray([0.0, -np.inf, 1.0]),
        np.asarray(["0", "x", "1"], dtype="S1"),
    ],
)
def test_time_bounds_validate_the_entire_raw_array(
    tmp_path: Path,
    raw: np.ndarray,
    bounds_only: bool,
) -> None:
    """Reject malformed shape, masked or nonfinite interior values before bounds extraction.

    :param tmp_path: Isolated pytest temporary directory.
    :param raw: Malformed raw time variable, including invalid interior values.
    :param bounds_only: Exercise full row decoding or coverage-only decoding.
    """
    with nc.Dataset(tmp_path / "invalid-times.nc", "w") as dataset:
        dimensions = tuple(f"axis_{axis}" for axis in range(raw.ndim))
        for name, length in zip(dimensions, raw.shape, strict=True):
            dataset.createDimension(name, length)
        variable = dataset.createVariable("time", raw.dtype, dimensions)
        variable.units = "days since 1950-01-01"
        variable[...] = raw
        with pytest.raises(
            ValueError, match="complete finite one-dimensional numeric array"
        ):
            indexed.IndexedAlongTrackBackend._times(variable, bounds_only=bounds_only)


@pytest.mark.parametrize("bounds_only", [False, True])
@pytest.mark.parametrize("invalid", ["calendar", "units", "overflow"])
def test_time_bounds_preserve_cf_decoder_validation(
    tmp_path: Path,
    bounds_only: bool,
    invalid: str,
) -> None:
    """Reject unsupported calendars, units, and out-of-range finite timestamps in either mode.

    :param tmp_path: Isolated pytest temporary directory.
    :param bounds_only: Exercise full row decoding or coverage-only decoding.
    :param invalid: CF metadata or timestamp to invalidate.
    """
    with nc.Dataset(tmp_path / "invalid-cf.nc", "w") as dataset:
        dataset.createDimension("obs", 3)
        variable = dataset.createVariable("time", "f8", ("obs",))
        variable.units = "bananas" if invalid == "units" else "days since 1950-01-01"
        variable.calendar = "360_day" if invalid == "calendar" else "standard"
        variable[:] = [0, 1e30 if invalid == "overflow" else 0.5, 1]
        with pytest.raises((ValueError, OverflowError)):
            indexed.IndexedAlongTrackBackend._times(variable, bounds_only=bounds_only)


def test_empty_time_bounds_still_reject_empty_source_coverage(
    source_layout: tuple[Path, Path, Path],
) -> None:
    """Avoid reducing empty arrays while preserving the empty-source coverage error.

    :param source_layout: Product root, cache directory, and source.
    """
    product, cache, _ = source_layout
    backend = indexed.IndexedAlongTrackBackend(product, cache)
    path = product.parent / "empty.nc"
    with nc.Dataset(path, "w") as dataset:
        dataset.createDimension("obs", 0)
        variable = dataset.createVariable("time", "f8", ("obs",))
        variable.units = "days since 1950-01-01"
        for bounds_only in (False, True):
            decoded = backend._times(variable, bounds_only=bounds_only)
            assert decoded.shape == (0,) and decoded.dtype == np.dtype("int64")
    with pytest.raises(ValueError, match="No observations"):
        backend._coverage(path)
