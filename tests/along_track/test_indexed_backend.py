"""Database-free scientific and cache regressions for the indexed backend.

The source asset is synthetic. Expected values come from the independent literals
in ``tests/data/along_track_reference/generate.py``, not the indexed reader.
"""

import hashlib
import os
import re
import shutil
from collections.abc import Generator, Sequence
from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta
from pathlib import Path
from typing import NoReturn

import netCDF4 as nc
import numpy as np
import psycopg
import pytest

from OceanDB.data_access.along_track import AlongTrack, AlongTrackBackend, Mission
from OceanDB.data_access.indexed_along_track import IndexedAlongTrackBackend, _Source
from OceanDB.ocean_data.dataset import Dataset
from OceanDB.schemas import along_track_schema as schema
from tests.data.along_track_reference.generate import (BASIN_ID, CYCLE,
                                                       LATITUDE, LONGITUDE,
                                                       ROW_COUNT, SCIENCE,
                                                       SOURCE_NAME, TIME_RAW,
                                                       TIME_UTC, TRACK)

pytestmark = pytest.mark.unit

ASSET = Path(__file__).parents[1] / "data" / "along_track_reference" / SOURCE_NAME
SOURCE_HASH = "29fbd4c7f1731a21f4167033325af279a17fd50a25759b6a09cb94e7457c28c4"
DATES = tuple(datetime.fromisoformat(value.removesuffix("Z")) for value in TIME_UTC)
SOURCE_KEYS = tuple(
    (SOURCE_NAME, date, latitude, longitude)
    for date, latitude, longitude in zip(DATES, LATITUDE, LONGITUDE, strict=True)
)
FULL_FIELDS: list[schema.along_track_fields] = [
    *schema.along_track_columns_schema,
    "distance",
    "delta_t",
]
NEAREST_FIELDS: list[schema.along_track_fields] = [
    *schema.along_track_columns_schema,
    "delta_t",
]


@pytest.fixture(autouse=True)
def forbid_database_connections(monkeypatch: pytest.MonkeyPatch) -> None:
    """Fail if any indexed query attempts to connect to PostgreSQL.

    :param monkeypatch: Pytest's scoped attribute patcher.
    """

    def fail_connect(*args: object, **kwargs: object) -> NoReturn:
        pytest.fail("The indexed backend must not connect to PostgreSQL")

    monkeypatch.setattr(psycopg, "connect", fail_connect)


@pytest.fixture
def source_layout(tmp_path: Path) -> tuple[Path, Path, Path]:
    """Copy the literal asset into a temporary OceanDB product root.

    :param tmp_path: Isolated pytest temporary directory.
    :returns: Product root (mission/year/month parent), cache directory, source.
    """
    product = tmp_path / "SEALEVEL_GLO_PHY_L3_MY_008_062"
    month = (
        product / "cmems_obs-sl_glo_phy-ssh_my_j2-l3-duacs_PT1S_202411" / "2024" / "01"
    )
    month.mkdir(parents=True)
    source = month / SOURCE_NAME
    shutil.copyfile(ASSET, source)
    assert hashlib.sha256(source.read_bytes()).hexdigest() == SOURCE_HASH
    return product, tmp_path / "cache", source


@pytest.fixture
def indexed_client(source_layout: tuple[Path, Path, Path]) -> AlongTrack:
    """Inject a source-backed reader; never instantiate the default SQL reader.

    :param source_layout: Isolated source product root and disk cache.
    :returns: Public AlongTrack facade using the indexed backend.
    """
    product, cache, _ = source_layout
    return AlongTrack(
        backend=IndexedAlongTrackBackend(data_root=product, cache_dir=cache)
    )


def _assert_source_row(
    result: Dataset[schema.along_track_fields] | None, index: int
) -> None:
    """Compare a complete singleton result with its original source row.

    :param result: Result for an exact coincident spatial/temporal center.
    :param index: Original zero-based NetCDF observation index.
    """
    assert isinstance(result, Dataset)
    assert result.name == "along_track"
    assert result.row_count == 1
    assert set(result) == set(schema.along_track_schema)
    key = tuple(
        result[name][0] for name in ("file_name", "date_time", "latitude", "longitude")
    )
    # The file hash and row offset are test-only provenance, not public columns.
    assert (SOURCE_HASH, SOURCE_KEYS.index(key)) == (SOURCE_HASH, index)
    expected: dict[schema.along_track_fields, str | int | float | datetime] = {
        "latitude": LATITUDE[index],
        "longitude": LONGITUDE[index],
        "date_time": DATES[index],
        "file_name": SOURCE_NAME,
        "mission": "j2",
        "track": TRACK[index],
        "cycle": CYCLE[index],
        "basin_id": BASIN_ID[index],
    }
    for field, value in expected.items():
        np.testing.assert_array_equal(result[field], np.asarray([value], dtype=object))
    assert result["date_time"][0].microsecond == DATES[index].microsecond
    for field, oracle in SCIENCE.items():
        missing = oracle.packed[index] is None
        np.testing.assert_array_equal(np.isnan(result.get_unscaled(field)), [missing])
        np.testing.assert_array_equal(np.isnan(result[field]), [missing])
        np.testing.assert_allclose(
            result.get_unscaled(field),
            np.asarray([oracle.packed[index]], dtype=float),
            rtol=0,
            atol=0,
            equal_nan=True,
        )
        np.testing.assert_allclose(
            result[field],
            np.asarray([oracle.physical[index]], dtype=float),
            rtol=0,
            atol=1e-12,
            equal_nan=True,
        )
    np.testing.assert_array_equal(result["distance"], [0.0])
    np.testing.assert_array_equal(result["delta_t"], [0.0])


@dataclass
class _WorkEvents:
    """Separate complete inventory passes from additional source safety checks."""

    inventory_passes: int = 0
    inventory_checks: list[Path] = field(default_factory=list)
    selected_checks: list[Path] = field(default_factory=list)
    windows: list[datetime] = field(default_factory=list)


def _observe_work(
    backend: IndexedAlongTrackBackend, monkeypatch: pytest.MonkeyPatch
) -> _WorkEvents:
    """Observe narrow preparation boundaries without inspecting window arrays.

    :param backend: Constructed backend whose subsequent work is observed.
    :param monkeypatch: Scoped method patcher.
    :returns: Mutable event log; all wrapped methods retain their behavior.
    """
    events = _WorkEvents()
    in_inventory = False
    validate_inventory = backend._validate_inventory
    check_source = backend._check_source
    prepare_window = backend.prepare_window

    def observe_inventory() -> None:
        nonlocal in_inventory
        events.inventory_passes += 1
        in_inventory = True
        try:
            validate_inventory()
        finally:
            in_inventory = False

    def observe_source(source: _Source) -> None:
        checks = events.inventory_checks if in_inventory else events.selected_checks
        checks.append(source.path)
        check_source(source)

    def observe_window(
        date: datetime,
        time_window: timedelta,
        missions: Sequence[Mission] | None = None,
    ) -> None:
        events.windows.append(date)
        prepare_window(date, time_window, missions)

    monkeypatch.setattr(backend, "_validate_inventory", observe_inventory)
    monkeypatch.setattr(backend, "_check_source", observe_source)
    monkeypatch.setattr(backend, "prepare_window", observe_window)
    return events


def _query_batch(
    client: AlongTrackBackend,
    nearest: bool,
    fields: list[schema.along_track_fields],
    latitudes: list[float],
    longitudes: list[float],
    dates: list[datetime],
    *,
    radius: float = 0.0,
    time_window: timedelta = timedelta(0),
    missions: list[Mission] | None = None,
    n_jobs: int = 1,
    chunk_size: int = 16,
) -> Generator[Dataset[schema.along_track_fields] | None, None, None]:
    """Apply the same lifecycle case to either public batch operation.

    :param client: Direct backend or facade with the four-method contract.
    :param nearest: Use nearest rather than radius selection.
    :param fields: Requested projection.
    :param latitudes: Paired center latitudes.
    :param longitudes: Paired center longitudes.
    :param dates: Paired center dates.
    :param radius: Radius or nearest cap, in meters.
    :param time_window: Temporal half-window.
    :param missions: Mission selection.
    :param n_jobs: Worker limit forwarded unchanged.
    :param chunk_size: Chunk limit forwarded unchanged.
    :returns: Unconsumed public batch generator.
    """
    if nearest:
        return client.geographic_nearest_neighbors_batch(
            fields,
            latitudes,
            longitudes,
            dates,
            time_window,
            missions,
            max_radius=radius,
            n_jobs=n_jobs,
            chunk_size=chunk_size,
        )
    return client.geographic_point_in_r_dt_batch(
        fields,
        latitudes,
        longitudes,
        dates,
        radius,
        time_window,
        missions,
        n_jobs=n_jobs,
        chunk_size=chunk_size,
    )


def _copy_shifted_mission_source(
    product: Path, source: Path, mission: Mission, days: int
) -> Path:
    """Create a second valid mission with nonduplicate literal observation times.

    :param product: Temporary product root.
    :param source: Original synthetic fixture copy.
    :param mission: Mission embedded in the new source name and directory.
    :param days: Integral day offset applied to the independent raw time literals.
    :returns: New source path, created before backend construction.
    """
    month = (
        product
        / f"cmems_obs-sl_glo_phy-ssh_my_{mission}-l3-duacs_PT1S_202411"
        / "2024"
        / "01"
    )
    month.mkdir(parents=True)
    other = month / SOURCE_NAME.replace("_j2_", f"_{mission}_")
    shutil.copyfile(source, other)
    with nc.Dataset(other, "r+") as dataset:
        variable = dataset["time"]
        variable.set_auto_maskandscale(False)
        variable[:] = np.asarray(TIME_RAW) + days
    return other


def _assert_projected_source_rows(
    result: Dataset[schema.along_track_fields] | None,
    fields: list[schema.along_track_fields],
    indices: Sequence[int],
    date: datetime,
    distances: Sequence[float],
    *,
    nearest: bool = False,
) -> None:
    """Check every projected column against source literals, not the reader.

    :param result: Query result using the original single-mission source.
    :param fields: Requested fields, excluding implicit nearest distance.
    :param indices: Expected source rows, in date order or required nearest order.
    :param date: Temporal center for signed offsets.
    :param distances: Independent WGS84 distances aligned with expected rows.
    :param nearest: Preserve nearest ordering and require mandatory distance.
    """
    assert isinstance(result, Dataset)
    assert result.name == "along_track"
    assert result.row_count == len(indices)
    assert set(result) == set(fields) | ({"distance"} if nearest else set())
    order = np.arange(len(indices))
    if not nearest and len(indices) > 1:
        order = np.argsort(result["date_time"])
    expected: dict[
        schema.along_track_fields, Sequence[str | int | float | datetime]
    ] = {
        "date_time": [DATES[index] for index in indices],
        "latitude": [LATITUDE[index] for index in indices],
        "longitude": [LONGITUDE[index] for index in indices],
        "track": [TRACK[index] for index in indices],
        "cycle": [CYCLE[index] for index in indices],
        "basin_id": [BASIN_ID[index] for index in indices],
        "file_name": [SOURCE_NAME] * len(indices),
        "mission": ["j2"] * len(indices),
    }
    for name in result:
        if name in SCIENCE:
            oracle = SCIENCE[name]
            for actual, values in (
                (result.get_unscaled(name)[order], oracle.packed),
                (result[name][order], oracle.physical),
            ):
                literal = np.asarray([values[index] for index in indices], dtype=float)
                np.testing.assert_array_equal(np.isnan(actual), np.isnan(literal))
                np.testing.assert_allclose(
                    actual, literal, rtol=0, atol=1e-12, equal_nan=True
                )
        elif name == "distance":
            np.testing.assert_allclose(
                result[name][order], distances, rtol=0, atol=1e-4
            )
        elif name == "delta_t":
            np.testing.assert_array_equal(
                result[name][order],
                [(date - DATES[index]).total_seconds() for index in indices],
            )
        else:
            np.testing.assert_array_equal(
                result[name][order], np.asarray(expected[name], dtype=object)
            )


def test_all_four_queries_preserve_source_rows_and_scalar_correction_mask(
    indexed_client: AlongTrack,
) -> None:
    """Project every field and map each returned key to its literal source row."""
    for index, (latitude, longitude, date) in enumerate(
        zip(LATITUDE, LONGITUDE, DATES, strict=True)
    ):
        _assert_source_row(
            indexed_client.geographic_point_in_r_dt(
                FULL_FIELDS,
                latitude,
                longitude,
                date,
                radius=0,
                time_window=timedelta(0),
                missions=["j2"],
            ),
            index,
        )
        # NN must add distance even when the caller does not request it.
        _assert_source_row(
            indexed_client.geographic_nearest_neighbors(
                NEAREST_FIELDS,
                latitude,
                longitude,
                date,
                time_window=timedelta(0),
                missions=["j2"],
                max_radius=0,
            ),
            index,
        )
    radius_results = list(
        indexed_client.geographic_point_in_r_dt_batch(
            FULL_FIELDS,
            list(LATITUDE),
            list(LONGITUDE),
            list(DATES),
            radius=0,
            time_window=timedelta(0),
            missions=["j2"],
            chunk_size=2,
        )
    )
    nearest_results = list(
        indexed_client.geographic_nearest_neighbors_batch(
            NEAREST_FIELDS,
            list(LATITUDE),
            list(LONGITUDE),
            list(DATES),
            time_window=timedelta(0),
            missions=["j2"],
            max_radius=0,
            chunk_size=2,
        )
    )
    assert len(radius_results) == len(nearest_results) == ROW_COUNT
    for index, (radius, nearest) in enumerate(
        zip(radius_results, nearest_results, strict=True)
    ):
        _assert_source_row(radius, index)
        _assert_source_row(nearest, index)
    # Row 4 lies on land: basin 0 must not be dropped as an ocean-only filter.
    assert BASIN_ID[4] == 0


def test_narrow_projection_keeps_science_alignment_and_signed_delta_t(
    indexed_client: AlongTrack,
) -> None:
    """Missing projected values must not drop a row or corrupt its neighbors."""
    fields: list[schema.along_track_fields] = [
        "date_time",
        "track",
        "sla_filtered",
        "tpa_correction",
        "delta_t",
    ]
    result = indexed_client.geographic_point_in_r_dt(
        fields,
        LATITUDE[1],
        LONGITUDE[1],
        DATES[1],
        radius=111_000,
        time_window=timedelta(microseconds=4),
    )
    assert isinstance(result, Dataset)
    assert set(result) == set(fields)  # Radius distance is optional.
    assert result.row_count == 2
    order = np.argsort(result["date_time"])
    np.testing.assert_array_equal(
        result["date_time"][order], np.asarray(DATES[1:3], dtype=object)
    )
    np.testing.assert_array_equal(result["track"][order], TRACK[1:3])
    np.testing.assert_allclose(
        result.get_unscaled("sla_filtered")[order],
        [199, np.nan],
        rtol=0,
        atol=0,
        equal_nan=True,
    )
    np.testing.assert_allclose(
        result["sla_filtered"][order],
        [0.199, np.nan],
        rtol=0,
        atol=1e-12,
        equal_nan=True,
    )
    assert np.isnan(result["tpa_correction"]).all()
    assert np.isnan(result.get_unscaled("tpa_correction")).all()
    np.testing.assert_array_equal(result["delta_t"][order], [0.0, -0.000004])

    nearest = indexed_client.geographic_nearest_neighbors(
        fields,
        LATITUDE[1],
        LONGITUDE[1],
        DATES[1],
        time_window=timedelta(microseconds=4),
        max_radius=111_000,
    )
    assert isinstance(nearest, Dataset)
    assert set(nearest) == {*fields, "distance"}
    assert nearest.row_count == 2
    np.testing.assert_array_equal(
        nearest["date_time"], np.asarray(DATES[1:3], dtype=object)
    )
    np.testing.assert_allclose(
        nearest["distance"],
        [0.0, 110574.38855779878],
        rtol=0,
        atol=1e-4,
    )


def test_time_edges_radius_edges_and_nearest_cap_are_inclusive(
    indexed_client: AlongTrack,
) -> None:
    """Check microseconds across midnight and millimeters around WGS84 radius."""
    center = datetime(2024, 1, 2)
    fields: list[schema.along_track_fields] = ["date_time", "delta_t", "distance"]
    on_edges = indexed_client.geographic_point_in_r_dt(
        fields,
        LATITUDE[1],
        LONGITUDE[1],
        center,
        radius=111_000,
        time_window=timedelta(microseconds=2),
    )
    assert isinstance(on_edges, Dataset)
    order = np.argsort(on_edges["date_time"])
    np.testing.assert_array_equal(
        on_edges["date_time"][order], np.asarray(DATES[1:3], dtype=object)
    )
    np.testing.assert_array_equal(on_edges["delta_t"][order], [0.000002, -0.000002])
    assert (
        indexed_client.geographic_point_in_r_dt(
            fields,
            LATITUDE[1],
            LONGITUDE[1],
            center,
            radius=111_000,
            time_window=timedelta(microseconds=1),
        )
        is None
    )

    # The same position is admitted by exactly zero radius and zero half-window.
    zero = indexed_client.geographic_point_in_r_dt(
        fields,
        LATITUDE[1],
        LONGITUDE[1],
        DATES[1],
        radius=0,
        time_window=timedelta(0),
    )
    assert isinstance(zero, Dataset) and zero.row_count == 1
    np.testing.assert_array_equal(zero["distance"], [0.0])
    zero_nn = indexed_client.geographic_nearest_neighbors(
        ["date_time"],
        LATITUDE[1],
        LONGITUDE[1],
        DATES[1],
        time_window=timedelta(0),
        max_radius=0,
    )
    assert isinstance(zero_nn, Dataset) and zero_nn.row_count == 1
    assert zero_nn["date_time"][0] == DATES[1]

    # GeographicLib WGS84 inverse for (0, 180) -> (1, 180), in meters.
    boundary = 110574.38855779878
    for cap, expected in (
        (boundary - 0.001, [DATES[1]]),
        (boundary + 0.001, [DATES[1], DATES[2]]),
    ):
        radius = indexed_client.geographic_point_in_r_dt(
            fields,
            LATITUDE[1],
            LONGITUDE[1],
            center,
            radius=cap,
            time_window=timedelta(microseconds=2),
        )
        nearest = indexed_client.geographic_nearest_neighbors(
            ["date_time"],
            LATITUDE[1],
            LONGITUDE[1],
            center,
            time_window=timedelta(microseconds=2),
            max_radius=cap,
        )
        assert isinstance(radius, Dataset) and isinstance(nearest, Dataset)
        np.testing.assert_array_equal(
            np.sort(radius["date_time"]), np.asarray(expected, dtype=object)
        )
        np.testing.assert_array_equal(
            nearest["date_time"], np.asarray(expected, dtype=object)
        )


def test_unbounded_nearest_does_not_inherit_radius_default(
    indexed_client: AlongTrack,
) -> None:
    """A same-basin point over 500 km away is still a nearest candidate."""
    # The packaged raster labels (0, 190) and source row 1 as basin 5.
    distant = indexed_client.geographic_nearest_neighbors(
        ["date_time"],
        0.0,
        190.0,
        DATES[1],
        time_window=timedelta(0),
    )
    assert isinstance(distant, Dataset)
    assert distant.row_count == 1
    assert distant["date_time"][0] == DATES[1]
    assert distant["distance"][0] > 500_000
    assert (
        indexed_client.geographic_nearest_neighbors(
            ["date_time"],
            0.0,
            190.0,
            DATES[1],
            time_window=timedelta(0),
            max_radius=500_000,
        )
        is None
    )


def test_default_ten_day_half_window_includes_its_endpoint(
    indexed_client: AlongTrack,
) -> None:
    """Omitting the half-window still selects the exact ten-day boundary."""
    on_edge = DATES[1] + timedelta(days=10)
    outside = on_edge + timedelta(microseconds=1)
    radius = indexed_client.geographic_point_in_r_dt(
        ["date_time"],
        LATITUDE[1],
        LONGITUDE[1],
        on_edge,
        radius=0,
    )
    nearest = indexed_client.geographic_nearest_neighbors(
        ["date_time"],
        LATITUDE[1],
        LONGITUDE[1],
        on_edge,
        max_radius=0,
    )
    for result in (radius, nearest):
        assert isinstance(result, Dataset)
        np.testing.assert_array_equal(
            result["date_time"], np.asarray([DATES[1]], dtype=object)
        )
    assert (
        indexed_client.geographic_point_in_r_dt(
            ["date_time"],
            LATITUDE[1],
            LONGITUDE[1],
            outside,
            radius=0,
        )
        is None
    )
    assert (
        indexed_client.geographic_nearest_neighbors(
            ["date_time"],
            LATITUDE[1],
            LONGITUDE[1],
            outside,
            max_radius=0,
        )
        is None
    )


@pytest.mark.parametrize(
    "missions, expected", [(None, True), (["j2"], True), ([], False), (["j3"], False)]
)
def test_mission_filters_and_no_matches(
    indexed_client: AlongTrack, missions: list[Mission] | None, expected: bool
) -> None:
    """Literal mission filters apply to both radius and nearest queries."""
    radius = indexed_client.geographic_point_in_r_dt(
        ["mission"],
        LATITUDE[0],
        LONGITUDE[0],
        DATES[0],
        radius=0,
        time_window=timedelta(0),
        missions=missions,
    )
    nearest = indexed_client.geographic_nearest_neighbors(
        ["mission"],
        LATITUDE[0],
        LONGITUDE[0],
        DATES[0],
        time_window=timedelta(0),
        missions=missions,
    )
    for result in (radius, nearest):
        if expected:
            assert isinstance(result, Dataset)
            np.testing.assert_array_equal(result["mission"], ["j2"])
        else:
            assert result is None


def test_batches_return_none_for_missing_centers_in_input_order(
    indexed_client: AlongTrack,
) -> None:
    """Empty batches and unmatched centers must not shift later results."""
    fields: list[schema.along_track_fields] = ["date_time", "track"]
    assert list(indexed_client.geographic_point_in_r_dt_batch(fields, [], [], [])) == []
    assert (
        list(indexed_client.geographic_nearest_neighbors_batch(fields, [], [], []))
        == []
    )
    latitudes = [LATITUDE[0], LATITUDE[0], LATITUDE[4]]
    longitudes = [LONGITUDE[0], LONGITUDE[0], LONGITUDE[4]]
    dates = [DATES[0], datetime(2030, 1, 1), DATES[4]]
    radius = list(
        indexed_client.geographic_point_in_r_dt_batch(
            fields,
            latitudes,
            longitudes,
            dates,
            radius=0,
            time_window=timedelta(0),
            chunk_size=1,
        )
    )
    nearest = list(
        indexed_client.geographic_nearest_neighbors_batch(
            fields,
            latitudes,
            longitudes,
            dates,
            time_window=timedelta(0),
            max_radius=0,
            chunk_size=1,
        )
    )
    for results in (radius, nearest):
        assert len(results) == 3
        assert results[1] is None
        for position, source_index in ((0, 0), (2, 4)):
            result = results[position]
            assert isinstance(result, Dataset)
            np.testing.assert_array_equal(
                result["date_time"], np.asarray([DATES[source_index]], dtype=object)
            )
            np.testing.assert_array_equal(result["track"], [TRACK[source_index]])


def test_zero_column_radius_projection_preserves_legacy_dataset_shape(
    indexed_client: AlongTrack,
) -> None:
    """An eligible row with ``fields=[]`` is not an empty-match ``None``."""
    result = indexed_client.geographic_point_in_r_dt(
        [],
        LATITUDE[0],
        LONGITUDE[0],
        DATES[0],
        radius=0,
        time_window=timedelta(0),
    )
    assert isinstance(result, Dataset)
    assert len(result) == 0 and result.row_count == 0


@pytest.mark.parametrize("nearest", [False, True], ids=["radius", "nearest"])
def test_indexed_batches_defer_work_until_first_next(
    source_layout: tuple[Path, Path, Path],
    monkeypatch: pytest.MonkeyPatch,
    nearest: bool,
) -> None:
    """Constructing either direct batch must not validate or prepare sources.

    :param source_layout: Temporary product root, cache, and literal source.
    :param monkeypatch: Scoped work-boundary patcher.
    :param nearest: Exercise the nearest rather than radius batch.
    """
    product, cache, source = source_layout
    backend = IndexedAlongTrackBackend(product, cache)
    catalog_reads = backend.stats.catalog_reads
    assert catalog_reads == 1
    events = _observe_work(backend, monkeypatch)
    results = _query_batch(
        backend, nearest, FULL_FIELDS, [LATITUDE[0]], [LONGITUDE[0]], [DATES[0]]
    )
    assert events == _WorkEvents()
    assert backend.stats.catalog_reads == catalog_reads
    assert (
        backend.stats.source_reads
        == backend.stats.disk_hits
        == backend.stats.memory_hits
        == 0
    )
    assert backend.stats.window_builds == backend.stats.active_bytes == 0

    _assert_source_row(next(results), 0)
    assert events.inventory_passes == 1
    assert events.inventory_checks == [source]
    assert events.windows == [DATES[0]]
    assert backend.stats.catalog_reads == catalog_reads
    assert backend.stats.source_reads == backend.stats.window_builds == 1
    results.close()


@pytest.mark.parametrize("nearest", [False, True], ids=["radius", "nearest"])
@pytest.mark.parametrize("n_jobs, chunk_size", [(0, 16), (2, 16), (1, 0), (1, -1)])
def test_direct_batch_worker_and_chunk_errors_are_eager(
    source_layout: tuple[Path, Path, Path],
    monkeypatch: pytest.MonkeyPatch,
    nearest: bool,
    n_jobs: int,
    chunk_size: int,
) -> None:
    """Direct wrappers validate their limits without starting the generator.

    :param source_layout: Temporary product root, cache, and source.
    :param monkeypatch: Scoped work-boundary patcher.
    :param nearest: Exercise the nearest rather than radius batch.
    :param n_jobs: Invalid worker count, or one for chunk cases.
    :param chunk_size: Invalid chunk size, or the default for worker cases.
    """
    product, cache, _ = source_layout
    backend = IndexedAlongTrackBackend(product, cache)
    events = _observe_work(backend, monkeypatch)
    with pytest.raises(ValueError, match="n_jobs=1 and positive chunk_size"):
        _query_batch(
            backend,
            nearest,
            ["track"],
            [],
            [],
            [],
            n_jobs=n_jobs,
            chunk_size=chunk_size,
        )
    assert events == _WorkEvents()
    assert backend.stats.source_reads == backend.stats.window_builds == 0


@pytest.mark.parametrize("nearest", [False, True], ids=["radius", "nearest"])
@pytest.mark.parametrize("problem", ["shape", "fields", "negative", "nan", "infinite"])
def test_direct_batch_shape_fields_and_radius_errors_are_deferred(
    source_layout: tuple[Path, Path, Path],
    monkeypatch: pytest.MonkeyPatch,
    nearest: bool,
    problem: str,
) -> None:
    """Keep direct shape, projection, and radius validation at first iteration.

    :param source_layout: Temporary product root, cache, and source.
    :param monkeypatch: Scoped schema and work-boundary patcher.
    :param nearest: Exercise the nearest rather than radius batch.
    :param problem: Runtime validation failure to introduce.
    """
    product, cache, _ = source_layout
    backend = IndexedAlongTrackBackend(product, cache)
    events = _observe_work(backend, monkeypatch)
    longitudes = [] if problem == "shape" else [LONGITUDE[0]]
    radius = {"negative": -1.0, "nan": float("nan"), "infinite": float("inf")}.get(
        problem, 0.0
    )
    message = "equal lengths" if problem == "shape" else "radius must be finite"
    if problem == "fields":
        monkeypatch.delitem(schema.along_track_schema, "track")
        message = "Unknown AlongTrack fields"
    results = _query_batch(
        backend,
        nearest,
        ["track"],
        [LATITUDE[0]],
        longitudes,
        [DATES[0]],
        radius=radius,
    )
    assert events == _WorkEvents()
    with pytest.raises(ValueError, match=message):
        next(results)
    assert events == _WorkEvents()
    assert backend.stats.source_reads == backend.stats.window_builds == 0


@pytest.mark.parametrize("nearest", [False, True], ids=["radius", "nearest"])
@pytest.mark.parametrize("empty", [False, True], ids=["nonempty", "empty"])
def test_facade_batches_defer_field_checks_until_first_next(
    source_layout: tuple[Path, Path, Path],
    monkeypatch: pytest.MonkeyPatch,
    nearest: bool,
    empty: bool,
) -> None:
    """Reject unknown fields on iteration, including through the facade.

    :param source_layout: Temporary product root, cache, and source.
    :param monkeypatch: Scoped shared-schema and work-boundary patcher.
    :param nearest: Exercise the nearest rather than radius batch.
    :param empty: Supply no centers while retaining projection validation.
    """
    product, cache, _ = source_layout
    backend = IndexedAlongTrackBackend(product, cache)
    client = AlongTrack(backend=backend)
    events = _observe_work(backend, monkeypatch)
    # Mutate the shared mapping rather than rebinding only the backend's alias:
    # an incorrectly eager facade validator must see the same unknown field.
    monkeypatch.delitem(schema.along_track_schema, "track")
    results = _query_batch(
        client,
        nearest,
        ["track"],
        [] if empty else [LATITUDE[0]],
        [] if empty else [LONGITUDE[0]],
        [] if empty else [DATES[0]],
    )
    assert events == _WorkEvents()
    with pytest.raises(ValueError, match="Unknown AlongTrack fields"):
        next(results)
    assert events == _WorkEvents()
    assert backend.stats.source_reads == backend.stats.window_builds == 0


@pytest.mark.parametrize("empty", [False, True], ids=["nonempty", "empty"])
@pytest.mark.parametrize("radius", [-1.0, float("nan"), float("inf")])
def test_facade_radius_batches_defer_radius_checks_until_first_next(
    source_layout: tuple[Path, Path, Path],
    monkeypatch: pytest.MonkeyPatch,
    empty: bool,
    radius: float,
) -> None:
    """Keep radius validation deferred, unlike the facade's nearest-cap check.

    :param source_layout: Temporary product root, cache, and source.
    :param monkeypatch: Scoped work-boundary patcher.
    :param empty: Supply no centers while retaining radius validation.
    :param radius: Negative or nonfinite radius.
    """
    product, cache, _ = source_layout
    backend = IndexedAlongTrackBackend(product, cache)
    client = AlongTrack(backend=backend)
    events = _observe_work(backend, monkeypatch)
    results = client.geographic_point_in_r_dt_batch(
        ["track"],
        [] if empty else [LATITUDE[0]],
        [] if empty else [LONGITUDE[0]],
        [] if empty else [DATES[0]],
        radius=radius,
    )
    assert events == _WorkEvents()
    with pytest.raises(ValueError, match="radius must be finite"):
        next(results)
    assert events == _WorkEvents()
    assert backend.stats.source_reads == backend.stats.window_builds == 0


@pytest.mark.parametrize("nearest", [False, True], ids=["radius", "nearest"])
@pytest.mark.parametrize("facade", [False, True], ids=["direct", "facade"])
def test_partial_batch_consumption_prepares_only_consumed_centers(
    source_layout: tuple[Path, Path, Path],
    monkeypatch: pytest.MonkeyPatch,
    nearest: bool,
    facade: bool,
) -> None:
    """Closing a nonmonotonic batch after one result must not prepare its future.

    :param source_layout: Temporary product root, cache, and source.
    :param monkeypatch: Scoped work-boundary patcher.
    :param nearest: Exercise the nearest rather than radius batch.
    :param facade: Enter through AlongTrack instead of the direct backend.
    """
    product, cache, _ = source_layout
    backend = IndexedAlongTrackBackend(product, cache)
    client: AlongTrackBackend = AlongTrack(backend=backend) if facade else backend
    events = _observe_work(backend, monkeypatch)
    indices = [4, 0, 5]
    results = _query_batch(
        client,
        nearest,
        FULL_FIELDS,
        [LATITUDE[index] for index in indices],
        [LONGITUDE[index] for index in indices],
        [DATES[index] for index in indices],
        chunk_size=3,
    )
    assert events == _WorkEvents()
    _assert_source_row(next(results), 4)
    assert events.windows == [DATES[4]]
    assert backend.stats.window_builds == backend.stats.source_reads == 1
    results.close()
    with pytest.raises(StopIteration):
        next(results)
    assert events.windows == [DATES[4]]
    assert events.inventory_passes == 1
    assert backend.stats.window_builds == backend.stats.source_reads == 1


@pytest.mark.parametrize("nearest", [False, True], ids=["radius", "nearest"])
@pytest.mark.parametrize("facade", [False, True], ids=["direct", "facade"])
@pytest.mark.parametrize("problem", [None, "fields", "radius"])
def test_empty_batch_skips_inventory_and_preparation(
    source_layout: tuple[Path, Path, Path],
    monkeypatch: pytest.MonkeyPatch,
    nearest: bool,
    facade: bool,
    problem: str | None,
) -> None:
    """Empty batches skip sources but still validate fields and spatial limits.

    :param source_layout: Temporary product root, cache, and source.
    :param monkeypatch: Scoped schema and work-boundary patcher.
    :param nearest: Exercise the nearest rather than radius batch.
    :param facade: Enter through AlongTrack instead of the direct backend.
    :param problem: Optional invalid projection or negative spatial limit.
    """
    product, cache, _ = source_layout
    backend = IndexedAlongTrackBackend(product, cache)
    client: AlongTrackBackend = AlongTrack(backend=backend) if facade else backend
    events = _observe_work(backend, monkeypatch)
    catalog_reads = backend.stats.catalog_reads
    radius = -1.0 if problem == "radius" else 0.0
    if problem == "fields":
        monkeypatch.delitem(schema.along_track_schema, "track")
    if problem == "radius" and nearest and facade:
        with pytest.raises(ValueError, match="max_radius must be finite"):
            _query_batch(client, nearest, ["track"], [], [], [], radius=radius)
    else:
        results = _query_batch(client, nearest, ["track"], [], [], [], radius=radius)
        assert events == _WorkEvents()
        if problem is None:
            assert list(results) == []
        else:
            message = (
                "Unknown AlongTrack fields"
                if problem == "fields"
                else "radius must be finite"
            )
            with pytest.raises(ValueError, match=message):
                next(results)
    assert events == _WorkEvents()
    assert backend.stats.catalog_reads == catalog_reads
    assert (
        backend.stats.source_reads
        == backend.stats.disk_hits
        == backend.stats.memory_hits
        == 0
    )
    assert backend.stats.window_builds == backend.stats.active_bytes == 0


@pytest.mark.parametrize("nearest", [False, True], ids=["radius", "nearest"])
def test_nonempty_batch_validates_all_pinned_sources_once(
    source_layout: tuple[Path, Path, Path],
    monkeypatch: pytest.MonkeyPatch,
    nearest: bool,
) -> None:
    """Separate inventory-wide checks from decode, load, and active-key checks.

    :param source_layout: Temporary product root, cache, and selected source.
    :param monkeypatch: Scoped work-boundary patcher.
    :param nearest: Exercise the nearest rather than radius batch.
    """
    product, cache, source = source_layout
    other = _copy_shifted_mission_source(product, source, "j3", 100)
    backend = IndexedAlongTrackBackend(product, cache)
    events = _observe_work(backend, monkeypatch)
    indices = [0, 0, 1]
    results = _query_batch(
        backend,
        nearest,
        FULL_FIELDS,
        [LATITUDE[index] for index in indices],
        [LONGITUDE[index] for index in indices],
        [DATES[index] for index in indices],
    )
    assert events == _WorkEvents()
    for position, index in enumerate(indices):
        _assert_source_row(next(results), index)
        assert events.inventory_passes == 1
        assert events.inventory_checks == [source, other]
        # First load checks before and after decode; the repeated window checks
        # its active source; the third center checks the retained file on load.
        assert events.selected_checks == [source] * (position + 2)
        assert events.windows == [DATES[value] for value in indices[: position + 1]]
    with pytest.raises(StopIteration):
        next(results)
    assert events.inventory_passes == 1
    assert backend.stats.source_reads == backend.stats.memory_hits == 1
    assert backend.stats.window_builds == 2


@pytest.mark.parametrize("nearest", [False, True], ids=["radius", "nearest"])
@pytest.mark.parametrize("facade", [False, True], ids=["direct", "facade"])
@pytest.mark.parametrize("mutation", ["changed", "deleted"])
def test_unselected_pinned_source_mutation_before_iteration_fails_closed(
    source_layout: tuple[Path, Path, Path],
    monkeypatch: pytest.MonkeyPatch,
    nearest: bool,
    facade: bool,
    mutation: str,
) -> None:
    """Check unselected inventory before reusing an already warm exact window.

    :param source_layout: Temporary product root, cache, and selected source.
    :param monkeypatch: Scoped work-boundary patcher.
    :param nearest: Exercise the nearest rather than radius batch.
    :param facade: Enter through AlongTrack instead of the direct backend.
    :param mutation: Change time coverage or delete the previously unselected file.
    """
    product, cache, source = source_layout
    other = _copy_shifted_mission_source(product, source, "j3", 100)
    backend = IndexedAlongTrackBackend(product, cache)
    client: AlongTrackBackend = AlongTrack(backend=backend) if facade else backend
    warm = _query_batch(
        client, nearest, FULL_FIELDS, [LATITUDE[0]], [LONGITUDE[0]], [DATES[0]]
    )
    _assert_source_row(next(warm), 0)
    warm.close()
    assert backend.stats.source_reads == backend.stats.window_builds == 1
    events = _observe_work(backend, monkeypatch)
    results = _query_batch(
        client, nearest, FULL_FIELDS, [LATITUDE[0]], [LONGITUDE[0]], [DATES[0]]
    )
    assert events == _WorkEvents()
    if mutation == "changed":
        old_stat = other.stat()
        with nc.Dataset(other, "r+") as dataset:
            variable = dataset["time"]
            variable.set_auto_maskandscale(False)
            variable[:] = TIME_RAW
        os.utime(other, ns=(old_stat.st_atime_ns, old_stat.st_mtime_ns + 2_000_000_000))
        message = "source changed"
    else:
        other.unlink()
        message = "source disappeared"
    with pytest.raises(RuntimeError, match=message):
        next(results)
    assert events.inventory_passes == 1
    assert events.inventory_checks == [source, other]
    assert events.selected_checks == events.windows == []
    assert backend.stats.source_reads == backend.stats.window_builds == 1


@pytest.mark.parametrize("nearest", [False, True], ids=["radius", "nearest"])
def test_exact_window_reuse_canonicalizes_missions(
    source_layout: tuple[Path, Path, Path], nearest: bool
) -> None:
    """Canonical mission lists hit only the current exact temporal window.

    :param source_layout: Temporary product root, cache, and original source.
    :param nearest: Exercise the nearest rather than radius batch.
    """
    product, cache, source = source_layout
    _copy_shifted_mission_source(product, source, "j3", 1)
    backend = IndexedAlongTrackBackend(product, cache)
    fields: list[schema.along_track_fields] = ["date_time", "mission"]
    selections: list[list[Mission] | None] = [
        None,
        ["j3", "j2"],
        ["j2", "j3", "j2"],
        ["j3", "j2", "j3"],
        None,
    ]
    cases: list[
        tuple[list[Mission] | None, datetime, timedelta, list[Mission], int]
    ] = [
        (missions, DATES[0], timedelta(days=1), ["j2", "j3"], 1)
        for missions in selections
    ]
    cases.extend(
        [
            ([], DATES[0], timedelta(days=1), [], 2),
            ([], DATES[0], timedelta(days=1), [], 2),
            (None, DATES[0], timedelta(days=1), ["j2", "j3"], 3),
            # Widening and overlapping keys rebuild, as does returning to an old key.
            (None, DATES[0], timedelta(days=1, microseconds=1), ["j2", "j3"], 4),
            (None, DATES[0] + timedelta(hours=12), timedelta(days=1), ["j2", "j3"], 5),
            (None, DATES[0], timedelta(days=1), ["j2", "j3"], 6),
            # Adjacent inclusive intervals share only their endpoint, not a key.
            (None, DATES[0] + timedelta(days=2), timedelta(days=1), ["j3"], 7),
        ]
    )
    for missions, date, half_window, expected_missions, builds in cases:
        (result,) = _query_batch(
            backend,
            nearest,
            fields,
            [LATITUDE[0]],
            [LONGITUDE[0]],
            [date],
            time_window=half_window,
            missions=missions,
        )
        assert backend.stats.window_builds == builds
        if not expected_missions:
            assert result is None
            continue
        assert isinstance(result, Dataset)
        assert result.name == "along_track"
        assert set(result) == set(fields) | ({"distance"} if nearest else set())
        assert result.row_count == len(expected_missions)
        order = (
            np.arange(result.row_count) if nearest else np.argsort(result["date_time"])
        )
        np.testing.assert_array_equal(result["mission"][order], expected_missions)
        np.testing.assert_array_equal(
            result["date_time"][order],
            np.asarray(
                [
                    DATES[0] + timedelta(days=mission == "j3")
                    for mission in expected_missions
                ],
                dtype=object,
            ),
        )
        if nearest:
            np.testing.assert_array_equal(result["distance"], [0.0] * result.row_count)
    assert backend.stats.source_reads == 2
    assert backend.stats.disk_hits == 0


def test_exact_window_reuse_ignores_projection_radius_and_query_kind(
    source_layout: tuple[Path, Path, Path],
) -> None:
    """Vary all non-temporal inputs while checking complete independent results.

    :param source_layout: Temporary product root, cache, and literal source.
    """
    product, cache, _ = source_layout
    backend = IndexedAlongTrackBackend(product, cache)
    client = AlongTrack(backend=backend)
    date = datetime(2024, 1, 2)
    half_window = timedelta(microseconds=2)
    boundary = 110574.38855779878  # Independent WGS84 inverse: (0, 180) -> (1, 180).
    fields: list[schema.along_track_fields] = ["date_time", "sla_filtered", "delta_t"]
    result = client.geographic_point_in_r_dt(
        fields, 0.0, 180.0, date, radius=0, time_window=half_window
    )
    _assert_projected_source_rows(result, fields, [1], date, [0.0])
    assert backend.stats.window_builds == 1

    fields = ["date_time", "track", "longitude", "distance"]
    result = client.geographic_point_in_r_dt(
        fields, 0.0, 180.0, date, radius=111_000, time_window=half_window
    )
    _assert_projected_source_rows(result, fields, [1, 2], date, [0.0, boundary])
    assert backend.stats.window_builds == 1

    fields = ["date_time", "latitude", "sla_filtered", "delta_t"]
    result = client.geographic_point_in_r_dt(
        fields, 1.0, 180.0, date, radius=0, time_window=half_window
    )
    _assert_projected_source_rows(result, fields, [2], date, [0.0])
    assert backend.stats.window_builds == 1

    result = client.geographic_nearest_neighbors(
        fields, 1.0, 180.0, date, time_window=half_window, max_radius=111_000
    )
    _assert_projected_source_rows(
        result, fields, [2, 1], date, [0.0, boundary], nearest=True
    )
    assert backend.stats.window_builds == 1

    result = client.geographic_nearest_neighbors(
        [], 0.0, 180.0, date, time_window=half_window, max_radius=0
    )
    _assert_projected_source_rows(result, [], [1], date, [0.0], nearest=True)
    assert backend.stats.window_builds == 1

    fields = ["date_time", "track", "delta_t"]
    result = client.geographic_nearest_neighbors(
        fields, 0.0, 180.0, date, time_window=half_window
    )
    _assert_projected_source_rows(
        result, fields, [1, 2], date, [0.0, boundary], nearest=True
    )
    assert backend.stats.window_builds == 1
    assert (
        client.geographic_point_in_r_dt(
            fields, 0.0, 190.0, date, radius=0, time_window=half_window
        )
        is None
    )
    assert backend.stats.window_builds == backend.stats.source_reads == 1
    assert backend.stats.memory_hits == backend.stats.disk_hits == 0


def test_disk_cache_survives_restart_and_adjacent_time_windows(
    source_layout: tuple[Path, Path, Path],
) -> None:
    """Persist an index and return the same rows across nearby queries/restart."""
    product, cache, _ = source_layout
    first_backend = IndexedAlongTrackBackend(product, cache)
    first = AlongTrack(backend=first_backend)
    fields: list[schema.along_track_fields] = ["date_time", "sla_filtered"]
    for date, half_window, expected in (
        (DATES[1], timedelta(0), [DATES[1]]),
        (DATES[2], timedelta(0), [DATES[2]]),
        (datetime(2024, 1, 2), timedelta(microseconds=2), list(DATES[1:3])),
    ):
        result = first.geographic_point_in_r_dt(
            fields,
            LATITUDE[1],
            LONGITUDE[1],
            date,
            radius=111_000,
            time_window=half_window,
        )
        assert isinstance(result, Dataset)
        np.testing.assert_array_equal(
            np.sort(result["date_time"]), np.asarray(expected, dtype=object)
        )
    assert first_backend.stats.source_reads == 1
    assert first_backend.stats.memory_hits >= 2
    assert first_backend.stats.window_builds == 3
    assert first_backend.stats.active_bytes > 0
    assert any(
        path.is_file() for path in cache.rglob("*")
    ), "expected a persistent disk cache"

    restarted_backend = IndexedAlongTrackBackend(product, cache)
    restarted = AlongTrack(backend=restarted_backend)
    again = restarted.geographic_point_in_r_dt(
        fields,
        LATITUDE[1],
        LONGITUDE[1],
        datetime(2024, 1, 2),
        radius=111_000,
        time_window=timedelta(microseconds=2),
    )
    assert isinstance(again, Dataset)
    assert restarted_backend.stats.disk_hits >= 1
    assert restarted_backend.stats.source_reads == 0
    order = np.argsort(again["date_time"])
    np.testing.assert_array_equal(
        again["date_time"][order], np.asarray(DATES[1:3], dtype=object)
    )
    np.testing.assert_allclose(
        again.get_unscaled("sla_filtered")[order],
        [199, np.nan],
        rtol=0,
        atol=0,
        equal_nan=True,
    )


def test_changed_source_invalidates_persisted_rows(
    source_layout: tuple[Path, Path, Path],
) -> None:
    """Never serve old scientific values when source bytes change on disk."""
    product, cache, source = source_layout
    first = AlongTrack(backend=IndexedAlongTrackBackend(product, cache))
    before = first.geographic_point_in_r_dt(
        ["sla_filtered"],
        LATITUDE[4],
        LONGITUDE[4],
        DATES[4],
        radius=0,
        time_window=timedelta(0),
    )
    assert isinstance(before, Dataset)
    np.testing.assert_array_equal(before.get_unscaled("sla_filtered"), [-1])

    old_stat = source.stat()
    with nc.Dataset(source, "r+") as ds:
        ds["sla_filtered"].set_auto_maskandscale(False)
        ds["sla_filtered"][4] = -1234
    # Guarantee a distinct metadata fingerprint on coarse-resolution filesystems.
    os.utime(source, ns=(old_stat.st_atime_ns, old_stat.st_mtime_ns + 2_000_000_000))

    with pytest.raises(RuntimeError, match="source changed"):
        first.geographic_point_in_r_dt(
            ["sla_filtered"],
            LATITUDE[4],
            LONGITUDE[4],
            DATES[4],
            radius=0,
            time_window=timedelta(0),
        )

    restarted = AlongTrack(backend=IndexedAlongTrackBackend(product, cache))
    after = restarted.geographic_point_in_r_dt(
        ["sla_filtered"],
        LATITUDE[4],
        LONGITUDE[4],
        DATES[4],
        radius=0,
        time_window=timedelta(0),
    )
    assert isinstance(after, Dataset)
    np.testing.assert_array_equal(after.get_unscaled("sla_filtered"), [-1234])
    np.testing.assert_allclose(after["sla_filtered"], [-1.234], rtol=0, atol=1e-12)


def test_deleted_source_is_not_served_from_disk_cache(
    source_layout: tuple[Path, Path, Path],
) -> None:
    """A cached observation disappears when its only backing file is removed."""
    product, cache, source = source_layout
    first = AlongTrack(backend=IndexedAlongTrackBackend(product, cache))
    fields: list[schema.along_track_fields] = ["date_time"]
    assert (
        first.geographic_nearest_neighbors(
            fields,
            LATITUDE[0],
            LONGITUDE[0],
            DATES[0],
            time_window=timedelta(0),
            max_radius=0,
        )
        is not None
    )
    source.unlink()
    with pytest.raises(RuntimeError, match="source disappeared"):
        first.geographic_nearest_neighbors(
            fields,
            LATITUDE[0],
            LONGITUDE[0],
            DATES[0],
            time_window=timedelta(0),
            max_radius=0,
        )
    # With no sources left the backend fails closed instead of replaying disk rows.
    with pytest.raises(FileNotFoundError, match="No mission/year/month"):
        IndexedAlongTrackBackend(product, cache)


def test_corrupt_prepared_file_is_rebuilt_from_source(
    source_layout: tuple[Path, Path, Path],
) -> None:
    """A truncated disk cache must not masquerade as a valid indexed source.

    :param source_layout: Temporary product root, cache directory, and source.
    """
    product, cache, _ = source_layout
    first = AlongTrack(backend=IndexedAlongTrackBackend(product, cache))
    result = first.geographic_point_in_r_dt(
        ["date_time"],
        LATITUDE[0],
        LONGITUDE[0],
        DATES[0],
        radius=0,
        time_window=timedelta(0),
    )
    assert isinstance(result, Dataset)
    prepared_files = list((cache / "files").glob("*.npy"))
    assert len(prepared_files) == 1
    prepared_files[0].write_bytes(b"truncated-cache")

    backend = IndexedAlongTrackBackend(product, cache)
    rebuilt = AlongTrack(backend=backend).geographic_point_in_r_dt(
        ["date_time"],
        LATITUDE[0],
        LONGITUDE[0],
        DATES[0],
        radius=0,
        time_window=timedelta(0),
    )
    assert isinstance(rebuilt, Dataset)
    np.testing.assert_array_equal(
        rebuilt["date_time"], np.asarray([DATES[0]], dtype=object)
    )
    assert backend.stats.source_reads == 1
    assert backend.stats.disk_hits == 0


def test_default_netcdf_fill_without_attributes_stays_missing(
    source_layout: tuple[Path, Path, Path],
) -> None:
    """Implicit NetCDF int16 fill masks match PostgreSQL ingestion.

    :param source_layout: Temporary product root, cache directory, and source.
    """
    product, cache, source = source_layout
    with nc.Dataset(source, "r+") as dataset:
        variable = dataset["sla_filtered"]
        variable.delncattr("_FillValue")
        variable.delncattr("missing_value")
        variable.set_auto_maskandscale(False)
        variable[0] = np.int16(nc.default_fillvals["i2"])

    with nc.Dataset(source) as dataset:
        assert np.ma.is_masked(dataset["sla_filtered"][0])
    client = AlongTrack(backend=IndexedAlongTrackBackend(product, cache))
    result = client.geographic_point_in_r_dt(
        ["sla_filtered", "track"],
        LATITUDE[0],
        LONGITUDE[0],
        DATES[0],
        radius=0,
        time_window=timedelta(0),
    )
    assert isinstance(result, Dataset) and result.row_count == 1
    assert np.isnan(result.get_unscaled("sla_filtered")[0])
    assert np.isnan(result["sla_filtered"][0])
    assert result["track"][0] == TRACK[0]


def test_changed_file_outside_window_invalidates_cached_coverage(
    source_layout: tuple[Path, Path, Path],
) -> None:
    """A previously unselected source cannot silently move into a window.

    :param source_layout: Temporary product root, cache directory, and source.
    """
    product, cache, source = source_layout
    other = source.with_name(SOURCE_NAME.replace("20240102.nc", "20240412.nc"))
    shutil.copyfile(source, other)
    with nc.Dataset(other, "r+") as dataset:
        variable = dataset["time"]
        variable.set_auto_maskandscale(False)
        variable[:] = np.asarray(TIME_RAW) + 100

    client = AlongTrack(backend=IndexedAlongTrackBackend(product, cache))
    fields: list[schema.along_track_fields] = ["date_time"]
    initial = client.geographic_point_in_r_dt(
        fields,
        LATITUDE[0],
        LONGITUDE[0],
        DATES[0],
        radius=0,
        time_window=timedelta(0),
    )
    assert isinstance(initial, Dataset) and initial.row_count == 1
    old_stat = other.stat()
    with nc.Dataset(other, "r+") as dataset:
        variable = dataset["time"]
        variable.set_auto_maskandscale(False)
        variable[:] = TIME_RAW
    os.utime(other, ns=(old_stat.st_atime_ns, old_stat.st_mtime_ns + 2_000_000_000))
    with pytest.raises(RuntimeError, match="source changed"):
        client.geographic_point_in_r_dt(
            fields,
            LATITUDE[0],
            LONGITUDE[0],
            DATES[0],
            radius=0,
            time_window=timedelta(0),
        )


def test_unrecognized_source_mission_is_not_silently_included(
    source_layout: tuple[Path, Path, Path],
) -> None:
    """Indexed inventory rejects files PostgreSQL ETL would reject.

    :param source_layout: Temporary product root, cache directory, and source.
    """
    product, cache, source = source_layout
    shutil.copyfile(source, source.with_name(SOURCE_NAME.replace("_j2_", "_unknown_")))
    with pytest.raises(ValueError, match="Unsupported mission"):
        IndexedAlongTrackBackend(product, cache)


def test_duplicate_within_file_outside_query_window_fails_preparation(
    source_layout: tuple[Path, Path, Path],
) -> None:
    """A lazy file preparation still validates all rows in that file.

    :param source_layout: Temporary product root, cache directory, and source.
    """
    product, cache, source = source_layout
    with nc.Dataset(source, "r+") as dataset:
        for name in ("time", "latitude", "longitude"):
            variable = dataset[name]
            variable.set_auto_maskandscale(False)
            variable[5] = variable[4]
    client = AlongTrack(backend=IndexedAlongTrackBackend(product, cache))
    with pytest.raises(ValueError, match="Duplicate AlongTrack"):
        client.geographic_point_in_r_dt(
            ["date_time"],
            LATITUDE[0],
            LONGITUDE[0],
            DATES[0],
            radius=0,
            time_window=timedelta(0),
        )


@pytest.mark.parametrize("conflicting", [False, True], ids=["identical", "conflicting"])
def test_duplicate_scientific_keys_in_two_sources_fail(
    source_layout: tuple[Path, Path, Path],
    conflicting: bool,
) -> None:
    """Neither identical nor conflicting rows may win by source-file order."""
    product, cache, source = source_layout
    duplicate = source.with_name(SOURCE_NAME.replace("20240102.nc", "20240103.nc"))
    shutil.copyfile(source, duplicate)
    if conflicting:
        with nc.Dataset(duplicate, "r+") as ds:
            ds["sla_filtered"].set_auto_maskandscale(False)
            ds["sla_filtered"][0] = 999
    with pytest.raises(ValueError, match=re.compile("duplicate|unique", re.IGNORECASE)):
        query = AlongTrack(backend=IndexedAlongTrackBackend(product, cache))
        query.geographic_point_in_r_dt(
            ["date_time"],
            LATITUDE[0],
            LONGITUDE[0],
            DATES[0],
            radius=0,
            time_window=timedelta(0),
        )


def test_nearest_exact_ties_use_date_not_source_order(
    source_layout: tuple[Path, Path, Path],
) -> None:
    """The three-row cutoff uses chronological ties after spatial reordering.

    :param source_layout: Temporary product root, cache directory, and source.
    """
    product, cache, source = source_layout
    with nc.Dataset(source, "r+") as dataset:
        for name, raw in (("latitude", 0), ("longitude", 180_000_000)):
            variable = dataset[name]
            variable.set_auto_maskandscale(False)
            variable[:4] = raw
        times = dataset["time"]
        times.set_auto_maskandscale(False)
        times[:4] = np.asarray(TIME_RAW[:4])[::-1]

    client = AlongTrack(backend=IndexedAlongTrackBackend(product, cache))
    result = client.geographic_nearest_neighbors(
        ["date_time", "sla_filtered"],
        0.0,
        180.0,
        DATES[2],
        time_window=timedelta(seconds=10),
        max_radius=0,
    )
    assert isinstance(result, Dataset) and result.row_count == 3
    np.testing.assert_array_equal(
        result["date_time"], np.asarray(DATES[:3], dtype=object)
    )
    np.testing.assert_array_equal(result["distance"], [0.0, 0.0, 0.0])
    np.testing.assert_allclose(
        result.get_unscaled("sla_filtered"),
        [np.nan, np.nan, 199],
        rtol=0,
        atol=0,
        equal_nan=True,
    )


def test_nearest_coordinate_ties_use_latitude_then_stored_longitude(
    source_layout: tuple[Path, Path, Path],
) -> None:
    """Four equal-distance points compete for three deterministic slots.

    :param source_layout: Temporary product root, cache directory, and source.
    """
    product, cache, source = source_layout
    with nc.Dataset(source, "r+") as dataset:
        for name, values in (
            ("latitude", [500_000, 500_000, -500_000, -500_000]),
            ("longitude", [180_500_000, 179_500_000, 180_500_000, 179_500_000]),
        ):
            variable = dataset[name]
            variable.set_auto_maskandscale(False)
            variable[:4] = values
        variable = dataset["time"]
        variable.set_auto_maskandscale(False)
        variable[:4] = TIME_RAW[0]

    client = AlongTrack(backend=IndexedAlongTrackBackend(product, cache))
    result = client.geographic_nearest_neighbors(
        ["latitude", "longitude", "date_time"],
        0.0,
        180.0,
        DATES[0],
        time_window=timedelta(0),
    )
    assert isinstance(result, Dataset) and result.row_count == 3
    np.testing.assert_array_equal(result["latitude"], [-0.5, -0.5, 0.5])
    np.testing.assert_array_equal(result["longitude"], [179.5, 180.5, 179.5])
    np.testing.assert_array_equal(
        result["date_time"], np.asarray([DATES[0]] * 3, dtype=object)
    )
    np.testing.assert_array_equal(result["distance"], [result["distance"][0]] * 3)


def test_dateline_high_latitude_and_grid_geometry_reuse(
    source_layout: tuple[Path, Path, Path],
) -> None:
    """Conservative cells retain tiny dateline-crossing WGS84 matches.

    :param source_layout: Temporary product root, cache directory, and source.
    """
    product, cache, source = source_layout
    with nc.Dataset(source, "r+") as dataset:
        for name, values in (
            ("latitude", [75_000_000, -1_000_000]),
            ("longitude", [359_999_999, 359_999_999]),
        ):
            variable = dataset[name]
            variable.set_auto_maskandscale(False)
            variable[:2] = values

    backend = IndexedAlongTrackBackend(product, cache)
    backend.prepare_grid([75.0, -1.0], [0.0, 0.0], radius=1)
    client = AlongTrack(backend=backend)
    for index, latitude in ((0, 75.0), (1, -1.0)):
        result = client.geographic_point_in_r_dt(
            ["date_time", "distance", "latitude", "longitude"],
            latitude,
            0.0,
            DATES[index],
            radius=1,
            time_window=timedelta(0),
        )
        assert isinstance(result, Dataset) and result.row_count == 1
        assert result["date_time"][0] == DATES[index]
        assert result["distance"][0] < 1
        assert result["longitude"][0] == 359.999999
    assert backend.stats.geometry_builds == 2
    assert backend.stats.geometry_hits >= 2
    assert backend.stats.peak_retained_bytes >= backend.stats.active_bytes


def test_float32_scale_metadata_preserves_packed_oceandb_values(
    source_layout: tuple[Path, Path, Path],
) -> None:
    """A float32-encoded 0.001 scale is the same physical source contract.

    :param source_layout: Temporary product root, cache directory, and source.
    """
    product, cache, source = source_layout
    with nc.Dataset(source, "r+") as dataset:
        variable = dataset["sla_filtered"]
        variable.delncattr("scale_factor")
        variable.setncattr("scale_factor", np.float32(0.001))

    client = AlongTrack(backend=IndexedAlongTrackBackend(product, cache))
    result = client.geographic_point_in_r_dt(
        ["sla_filtered"],
        LATITUDE[0],
        LONGITUDE[0],
        DATES[0],
        radius=0,
        time_window=timedelta(0),
    )
    assert isinstance(result, Dataset)
    np.testing.assert_array_equal(result.get_unscaled("sla_filtered"), [148])
    np.testing.assert_allclose(result["sla_filtered"], [0.148], rtol=0, atol=1e-12)


def test_mismatched_source_scaling_warns_but_uses_oceandb_schema(
    source_layout: tuple[Path, Path, Path],
) -> None:
    """Match ETL's warning and schema scaling rather than source metadata.

    :param source_layout: Temporary product root, cache directory, and source.
    """
    product, cache, source = source_layout
    with nc.Dataset(source, "r+") as dataset:
        dataset["sla_filtered"].setncattr("scale_factor", 0.002)

    client = AlongTrack(backend=IndexedAlongTrackBackend(product, cache))
    with pytest.warns(RuntimeWarning, match="differs from OceanDB schema"):
        result = client.geographic_point_in_r_dt(
            ["sla_filtered"],
            LATITUDE[0],
            LONGITUDE[0],
            DATES[0],
            radius=0,
            time_window=timedelta(0),
        )
    assert isinstance(result, Dataset)
    np.testing.assert_array_equal(result.get_unscaled("sla_filtered"), [148])
    np.testing.assert_allclose(result["sla_filtered"], [0.148], rtol=0, atol=1e-12)


def test_geographiclib_boundary_is_inclusive_not_vincenty_rounded(
    source_layout: tuple[Path, Path, Path],
) -> None:
    """At the exact WGS84 boundary, both radius and nearest admit the row.

    :param source_layout: Temporary product root, cache directory, and source.
    """
    product, cache, source = source_layout
    with nc.Dataset(source, "r+") as dataset:
        for name, value in (("latitude", 1_000_000), ("longitude", 0)):
            variable = dataset[name]
            variable.set_auto_maskandscale(False)
            variable[0] = value

    client = AlongTrack(backend=IndexedAlongTrackBackend(product, cache))
    boundary = 110574.38855779878  # GeographicLib WGS84: (0, 0) to (1, 0).
    for result in (
        client.geographic_point_in_r_dt(
            ["distance"],
            0.0,
            0.0,
            DATES[0],
            radius=boundary,
            time_window=timedelta(0),
        ),
        client.geographic_nearest_neighbors(
            ["distance"],
            0.0,
            0.0,
            DATES[0],
            max_radius=boundary,
            time_window=timedelta(0),
        ),
    ):
        assert isinstance(result, Dataset) and result.row_count == 1
        np.testing.assert_array_equal(result["distance"], [boundary])


def test_nearest_geodesic_tie_not_broken_by_vincenty_rounding(
    source_layout: tuple[Path, Path, Path],
) -> None:
    """Equal WGS84 distances use date order even if Vincenty differs by 2 nm.

    :param source_layout: Temporary product root, cache directory, and source.
    """
    product, cache, source = source_layout
    with nc.Dataset(source, "r+") as dataset:
        for name, values in (
            ("latitude", [45_000_000, 45_000_000]),
            ("longitude", [179_000_000, 181_000_000]),
            ("time", np.asarray(TIME_RAW[:2])[::-1]),
        ):
            variable = dataset[name]
            variable.set_auto_maskandscale(False)
            variable[:2] = values

    client = AlongTrack(backend=IndexedAlongTrackBackend(product, cache))
    result = client.geographic_nearest_neighbors(
        ["date_time", "longitude"],
        45.0,
        180.0,
        DATES[0],
        max_radius=80_000,
        time_window=timedelta(seconds=2),
    )
    assert isinstance(result, Dataset) and result.row_count == 2
    np.testing.assert_array_equal(
        result["date_time"], np.asarray(DATES[:2], dtype=object)
    )
    np.testing.assert_array_equal(result["longitude"], [181.0, 179.0])
    np.testing.assert_array_equal(result["distance"], [result["distance"][0]] * 2)


def test_cell_count_has_a_safe_bound(source_layout: tuple[Path, Path, Path]) -> None:
    """Finer-than-supported grids fail before allocating dense offsets.

    :param source_layout: Temporary product root, cache directory, and source.
    """
    product, cache, _ = source_layout
    with pytest.raises(ValueError, match="index is too large"):
        IndexedAlongTrackBackend(product, cache, cell_size_degrees=0.001)


# These exact integer offsets encode the independent TIME_UTC literals, without
# asking the production decoder (or floating-day arithmetic) to make an oracle.
_REFERENCE_TIME_US = (-876_544, -2, 2, 1_654_321, 2_000_000, 3_333_333)
_SECOND_SOURCE_SHIFT = timedelta(seconds=7)


@dataclass(frozen=True)
class _ProjectionOracleRow:
    """Identify a literal source row independently of window/file indices."""

    file_name: str
    mission: Mission
    source_index: int
    date_time: datetime
    latitude: float
    longitude: float
    basin_id: int
    distance: float = 0.0


def _reference_projection_oracle(
    source: Path, *, second_mission: bool = False
) -> tuple[_ProjectionOracleRow, ...]:
    """Describe unchanged coordinates and explicitly shifted reference times.

    :param source: Filename to expect in projected provenance.
    :param second_mission: Select the seven-second-shifted j3 clone.
    :returns: One independent identity/coordinate oracle per source row.
    """
    return tuple(
        _ProjectionOracleRow(
            file_name=source.name,
            mission="j3" if second_mission else "j2",
            source_index=index,
            date_time=date + (_SECOND_SOURCE_SHIFT if second_mission else timedelta(0)),
            latitude=latitude,
            longitude=longitude,
            basin_id=basin_id,
        )
        for index, (date, latitude, longitude, basin_id) in enumerate(
            zip(DATES, LATITUDE, LONGITUDE, BASIN_ID, strict=True)
        )
    )


def _clone_shifted_reference_source(source: Path) -> Path:
    """Make a nonduplicate j3 source with explicitly transformed raw literals.

    Valid row-science integers increase by 100, while both missing sentinels
    remain unchanged. The missing scalar correction becomes the valid -123.
    Track/cycle increase by 1000/10. Physical expectations independently add
    0.1 to the existing physical literals; they never use a production decoder.

    :param source: Original temporary reference file to clone.
    :returns: Newly written j3 source alongside the original j2 file.
    """
    destination = source.with_name(source.name.replace("_j2_", "_j3_"))
    shutil.copyfile(source, destination)
    with nc.Dataset(destination, "r+") as dataset:
        variable = dataset["time"]
        variable.set_auto_maskandscale(False)
        variable.units = "microseconds since 2024-01-02 00:00:00"
        variable[:] = np.asarray(_REFERENCE_TIME_US, dtype=np.int64) + 7_000_000
        for name, values in (("track", TRACK), ("cycle", CYCLE)):
            variable = dataset[name]
            variable.set_auto_maskandscale(False)
            variable[:] = np.asarray(values) + (1000 if name == "track" else 10)
        for name, oracle in SCIENCE.items():
            variable = dataset[name]
            variable.set_auto_maskandscale(False)
            if isinstance(oracle.raw, int):
                variable[...] = -123
            else:
                variable[:] = [
                    value if value in (32767, -32766) else value + 100
                    for value in oracle.raw
                ]
    return destination


def _assert_complete_projection(
    result: Dataset[schema.along_track_fields] | None,
    expected_rows: Sequence[_ProjectionOracleRow],
    center: datetime,
    *,
    ordered: bool = False,
) -> None:
    """Compare all metadata, packed/scaled science, masks, and derived columns.

    Radius membership is compared by scientific/provenance key, without adding
    a radius-order contract. Nearest callers supply the required ranked order.

    :param result: Complete radius or nearest result.
    :param expected_rows: Independent source-row oracles, ranked for nearest.
    :param center: Query time used for signed center-minus-observation seconds.
    :param ordered: Preserve the supplied nearest sequence rather than sort keys.
    """
    assert isinstance(result, Dataset)
    assert result.name == "along_track"
    assert result.schema == schema.along_track_schema
    assert result.row_count == len(expected_rows)
    assert set(result) == set(schema.along_track_schema)
    assert len(
        {(row.date_time, row.latitude, row.longitude) for row in expected_rows}
    ) == len(expected_rows)
    if ordered:
        rows = tuple(expected_rows)
        order = np.arange(result.row_count)
    else:
        rows = tuple(
            sorted(
                expected_rows,
                key=lambda row: (
                    row.file_name,
                    row.date_time,
                    row.latitude,
                    row.longitude,
                ),
            )
        )
        order = np.lexsort(
            (
                result["longitude"],
                result["latitude"],
                result["date_time"],
                result["file_name"],
            )
        )
    metadata: dict[
        schema.along_track_fields, Sequence[str | int | float | datetime]
    ] = {
        "file_name": [row.file_name for row in rows],
        "mission": [row.mission for row in rows],
        "date_time": [row.date_time for row in rows],
        "latitude": [row.latitude for row in rows],
        "longitude": [row.longitude for row in rows],
        "basin_id": [row.basin_id for row in rows],
        "track": [
            TRACK[row.source_index] + (1000 if row.mission == "j3" else 0)
            for row in rows
        ],
        "cycle": [
            CYCLE[row.source_index] + (10 if row.mission == "j3" else 0) for row in rows
        ],
    }
    for field, values in metadata.items():
        np.testing.assert_array_equal(
            result[field][order], np.asarray(values, dtype=object)
        )
        np.testing.assert_array_equal(
            result.get_unscaled(field)[order], np.asarray(values, dtype=object)
        )
    for field, oracle in SCIENCE.items():
        packed: list[float] = []
        physical: list[float] = []
        for row in rows:
            if row.mission == "j3" and field == "tpa_correction":
                packed.append(-123.0)
                physical.append(-0.123)
                continue
            raw_value = oracle.packed[row.source_index]
            physical_value = oracle.physical[row.source_index]
            packed.append(
                np.nan
                if raw_value is None
                else float(raw_value + (100 if row.mission == "j3" else 0))
            )
            physical.append(
                np.nan
                if physical_value is None
                else physical_value + (0.1 if row.mission == "j3" else 0.0)
            )
        actual_packed = result.get_unscaled(field)[order]
        actual_physical = result[field][order]
        np.testing.assert_array_equal(np.isnan(actual_packed), np.isnan(packed))
        np.testing.assert_array_equal(np.isnan(actual_physical), np.isnan(physical))
        np.testing.assert_allclose(
            actual_packed, packed, rtol=0, atol=0, equal_nan=True
        )
        np.testing.assert_allclose(
            actual_physical, physical, rtol=0, atol=1e-12, equal_nan=True
        )
    np.testing.assert_allclose(
        result["distance"][order],
        [row.distance for row in rows],
        rtol=0,
        atol=1e-4,
    )
    np.testing.assert_array_equal(
        result["delta_t"][order],
        [(center - row.date_time).total_seconds() for row in rows],
    )


def _prepared_artifact_fingerprints(cache: Path) -> dict[str, tuple[int, int, str]]:
    """Record prepared-file bytes and metadata without changing their contents.

    :param cache: Temporary indexed-backend disk-cache directory.
    :returns: Prepared basenames mapped to size, mtime, and content SHA256.
    """
    return {
        path.name: (
            path.stat().st_size,
            path.stat().st_mtime_ns,
            hashlib.sha256(path.read_bytes()).hexdigest(),
        )
        for path in (cache / "files").glob("*.npy")
    }


def test_multifile_projection_preserves_provenance_after_cell_sort(
    source_layout: tuple[Path, Path, Path],
) -> None:
    """Keep two missions aligned through cell sorting and the nearest cutoff.

    :param source_layout: Isolated product root, disk cache, and literal asset.
    """
    product, cache, source = source_layout
    second = _clone_shifted_reference_source(source)
    # Each source straddles the wrapped 180-degree cell boundary; the two files
    # interleave in cell order, unlike j2-then-j3 inventory order or source rows.
    longitudes = (185, 181, 183, 179, 177, 175)
    second_longitudes = (184, 180, 182, 178, 176, 174)
    for path, values in ((source, longitudes), (second, second_longitudes)):
        with nc.Dataset(path, "r+") as dataset:
            variable = dataset["latitude"]
            variable.set_auto_maskandscale(False)
            variable[:] = 0
            variable = dataset["longitude"]
            variable.set_auto_maskandscale(False)
            variable[:] = np.asarray(values, dtype=np.int64) * 1_000_000
    # Equatorial WGS84 distances are independent GeographicLib inverse literals;
    # basin labels were pinned once from the packaged raster, not query results.
    j2_distances = (
        556597.4539663679,
        111319.49079327357,
        333958.4723798207,
        111319.49079327357,
        333958.4723798207,
        556597.4539663679,
    )
    j3_distances = (
        445277.96317309426,
        0.0,
        222638.98158654713,
        222638.98158654713,
        445277.96317309426,
        667916.9447596414,
    )
    first_rows = tuple(
        replace(
            row, latitude=0.0, longitude=float(lon), basin_id=basin, distance=distance
        )
        for row, lon, basin, distance in zip(
            _reference_projection_oracle(source),
            longitudes,
            (5, 5, 5, 4, 4, 4),
            j2_distances,
            strict=True,
        )
    )
    second_rows = tuple(
        replace(
            row, latitude=0.0, longitude=float(lon), basin_id=basin, distance=distance
        )
        for row, lon, basin, distance in zip(
            _reference_projection_oracle(second, second_mission=True),
            second_longitudes,
            (5, 5, 5, 4, 4, 5),
            j3_distances,
            strict=True,
        )
    )
    client = AlongTrack(backend=IndexedAlongTrackBackend(product, cache))
    center = datetime(2024, 1, 2, 0, 0, 5)
    radius = client.geographic_point_in_r_dt(
        FULL_FIELDS,
        0.0,
        180.0,
        center,
        radius=700_000,
        time_window=timedelta(seconds=20),
        missions=["j3", "j2"],
    )
    _assert_complete_projection(radius, (*first_rows, *second_rows), center)
    nearest = client.geographic_nearest_neighbors(
        NEAREST_FIELDS,
        0.0,
        180.0,
        center,
        time_window=timedelta(seconds=20),
        missions=["j2", "j3"],
        max_radius=700_000,
    )
    # Zero distance wins first; the two one-degree ties then use observation
    # time. The nine remaining radius members must not leak past the cutoff.
    _assert_complete_projection(
        nearest, (second_rows[1], first_rows[1], first_rows[3]), center, ordered=True
    )


def test_refresh_visibility_geometry_and_cumulative_counters(
    source_layout: tuple[Path, Path, Path],
) -> None:
    """Discover new files explicitly while keeping geometry and disk artifacts.

    :param source_layout: Isolated product root, disk cache, and literal asset.
    """
    product, cache, source = source_layout
    backend = IndexedAlongTrackBackend(product, cache)
    backend.prepare_grid([LATITUDE[0]], [LONGITUDE[0]], radius=0)
    client = AlongTrack(backend=backend)
    original = client.geographic_point_in_r_dt(
        FULL_FIELDS,
        LATITUDE[0],
        LONGITUDE[0],
        DATES[0],
        radius=0,
        time_window=timedelta(0),
    )
    _assert_source_row(original, 0)
    artifacts = _prepared_artifact_fingerprints(cache)
    assert len(artifacts) == 1
    second = _clone_shifted_reference_source(source)
    new_date = DATES[0] + _SECOND_SOURCE_SHIFT
    assert (
        client.geographic_point_in_r_dt(
            FULL_FIELDS,
            LATITUDE[0],
            LONGITUDE[0],
            new_date,
            radius=0,
            time_window=timedelta(0),
        )
        is None
    )
    # Restore a nonempty active window and exercise a decoded-file memory hit
    # before testing which current-byte fields and cumulative counters refresh.
    _assert_source_row(
        client.geographic_point_in_r_dt(
            FULL_FIELDS,
            LATITUDE[0],
            LONGITUDE[0],
            DATES[0],
            radius=0,
            time_window=timedelta(0),
        ),
        0,
    )
    before = replace(backend.stats)
    assert before.cached_bytes > 0 and before.active_bytes > 0
    assert before.geometry_bytes > 0 and before.geometry_hits > 0
    assert before.memory_hits > 0 and before.window_builds > 0
    backend.refresh()
    # The only cumulative cost of this refresh is cataloging the added file.
    assert backend.stats == replace(
        before,
        catalog_reads=before.catalog_reads + 1,
        cached_bytes=0,
        active_bytes=0,
    )
    assert _prepared_artifact_fingerprints(cache) == artifacts
    _assert_source_row(
        client.geographic_point_in_r_dt(
            FULL_FIELDS,
            LATITUDE[0],
            LONGITUDE[0],
            DATES[0],
            radius=0,
            time_window=timedelta(0),
        ),
        0,
    )
    assert backend.stats.disk_hits == before.disk_hits + 1
    assert backend.stats.source_reads == before.source_reads
    assert backend.stats.memory_hits == before.memory_hits
    assert backend.stats.window_builds == before.window_builds + 1
    assert backend.stats.geometry_builds == before.geometry_builds
    assert backend.stats.geometry_hits == before.geometry_hits + 1
    assert backend.stats.geometry_bytes == before.geometry_bytes
    assert _prepared_artifact_fingerprints(cache) == artifacts
    new_result = client.geographic_point_in_r_dt(
        FULL_FIELDS,
        LATITUDE[0],
        LONGITUDE[0],
        new_date,
        radius=0,
        time_window=timedelta(0),
    )
    _assert_complete_projection(
        new_result,
        [_reference_projection_oracle(second, second_mission=True)[0]],
        new_date,
    )
    assert backend.stats.source_reads == before.source_reads + 1
    assert backend.stats.disk_hits == before.disk_hits + 1
    assert backend.stats.geometry_builds == before.geometry_builds
    assert backend.stats.geometry_hits == before.geometry_hits + 2
    after_artifacts = _prepared_artifact_fingerprints(cache)
    assert len(after_artifacts) == 2
    assert {name: after_artifacts[name] for name in artifacts} == artifacts


@pytest.mark.parametrize("nearest", [False, True], ids=["radius", "nearest"])
def test_returned_dataset_survives_eviction_window_change_and_refresh(
    source_layout: tuple[Path, Path, Path], nearest: bool
) -> None:
    """Retain read and unread scaled columns across eviction and refresh.

    :param source_layout: Isolated product root, disk cache, and literal asset.
    :param nearest: Exercise a ranked nearest result instead of radius membership.
    """
    product, cache, source = source_layout
    second = _clone_shifted_reference_source(source)
    # One six-row prepared file fits, but the two files cannot both be retained.
    backend = IndexedAlongTrackBackend(product, cache, max_cache_bytes=1024)
    client = AlongTrack(backend=backend)
    center = DATES[1]
    if nearest:
        retained = client.geographic_nearest_neighbors(
            NEAREST_FIELDS,
            LATITUDE[1],
            LONGITUDE[1],
            center,
            time_window=timedelta(microseconds=4),
            max_radius=111_000,
        )
    else:
        retained = client.geographic_point_in_r_dt(
            FULL_FIELDS,
            LATITUDE[1],
            LONGITUDE[1],
            center,
            radius=111_000,
            time_window=timedelta(microseconds=4),
        )
    assert isinstance(retained, Dataset)
    assert retained.row_count == 2
    # Only this science column is accessed before eviction. In particular, the
    # valid/NaN values in other scaled columns remain genuinely lazy until last.
    np.testing.assert_allclose(
        retained["sla_filtered"][np.argsort(retained["date_time"])],
        [199e-3, np.nan],
        rtol=0,
        atol=1e-12,
        equal_nan=True,
    )
    first_state = replace(backend.stats)
    assert 0 < first_state.cached_bytes <= 1024
    assert first_state.cached_bytes * 2 > 1024
    second_center = center + _SECOND_SOURCE_SHIFT
    also_retained = client.geographic_point_in_r_dt(
        FULL_FIELDS,
        LATITUDE[1],
        LONGITUDE[1],
        second_center,
        radius=111_000,
        time_window=timedelta(microseconds=4),
    )
    assert isinstance(also_retained, Dataset) and also_retained.row_count == 2
    second_state = replace(backend.stats)
    assert second_state.source_reads == first_state.source_reads + 1
    assert second_state.window_builds == first_state.window_builds + 1
    assert second_state.cached_bytes == first_state.cached_bytes
    assert second_state.memory_hits == first_state.memory_hits
    # Request a third window in the first source. A disk hit, not a memory hit,
    # proves the first file was evicted rather than merely switching windows.
    _assert_source_row(
        client.geographic_point_in_r_dt(
            FULL_FIELDS,
            LATITUDE[4],
            LONGITUDE[4],
            DATES[4],
            radius=0,
            time_window=timedelta(0),
        ),
        4,
    )
    assert backend.stats.disk_hits == second_state.disk_hits + 1
    assert backend.stats.memory_hits == second_state.memory_hits
    assert backend.stats.source_reads == second_state.source_reads
    assert backend.stats.window_builds == second_state.window_builds + 1
    backend.refresh()
    assert backend.stats.cached_bytes == backend.stats.active_bytes == 0
    first_rows = _reference_projection_oracle(source)
    second_rows = _reference_projection_oracle(second, second_mission=True)
    boundary = 110574.38855779878  # Independent WGS84 (0, 180) -> (1, 180).
    _assert_complete_projection(
        retained,
        [first_rows[1], replace(first_rows[2], distance=boundary)],
        center,
        ordered=nearest,
    )
    _assert_complete_projection(
        also_retained,
        [second_rows[1], replace(second_rows[2], distance=boundary)],
        second_center,
    )
