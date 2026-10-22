"""Database-free scientific and cache regressions for the indexed backend.

The source asset is synthetic. Expected values come from the independent literals
in ``tests/data/along_track_reference/generate.py``, not the indexed reader.
"""

import hashlib
import os
import re
import shutil
from datetime import datetime, timedelta
from pathlib import Path
from typing import NoReturn

import netCDF4 as nc
import numpy as np
import psycopg
import pytest

from OceanDB.data_access.along_track import AlongTrack, Mission
from OceanDB.data_access.indexed_along_track import IndexedAlongTrackBackend
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
