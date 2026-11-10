"""Database-free complete-catalog and interrupted-discovery regressions."""

import shutil
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
