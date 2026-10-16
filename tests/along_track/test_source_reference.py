"""Regress a provisional literal source through real ETL and query surfaces."""

import hashlib
import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Literal

import netCDF4 as nc
import numpy as np
import pytest
from psycopg import sql
from psycopg.errors import UniqueViolation

from OceanDB.data_access.along_track import AlongTrack
from OceanDB.etl import AlongTrackETL, BasinsETL
from OceanDB.ocean_data.dataset import Dataset
from OceanDB.OceanDB_Initializer import OceanDBInit
from OceanDB.schemas import along_track_schema as schema
from tests.data.along_track_reference.generate import (ASSET_NAME,
                                                       BASE_CODE_REVISION,
                                                       BASIN_ID, CYCLE,
                                                       DEPENDENCY_PATHS,
                                                       GLOBAL_ATTRIBUTES,
                                                       LATITUDE, LONGITUDE,
                                                       REPO_ROOT, ROW_COUNT,
                                                       SCIENCE, SOURCE_NAME,
                                                       TIME_UTC, TRACK,
                                                       generate,
                                                       source_variables)
from tests.database.fixtures import config as config
from tests.database.fixtures import db_with_basin_data as db_with_basin_data
from tests.database.fixtures import db_with_db as db_with_db
from tests.database.fixtures import db_with_tables as db_with_tables
from tests.database.fixtures import fresh_db as fresh_db

REFERENCE_DIRECTORY = Path(__file__).parents[1] / "data" / "along_track_reference"
ASSET_PATH = REFERENCE_DIRECTORY / ASSET_NAME
SOURCE_SHA256 = hashlib.sha256(ASSET_PATH.read_bytes()).hexdigest()
DATES = tuple(datetime.fromisoformat(value.removesuffix("Z")) for value in TIME_UTC)
SOURCE_KEYS = tuple(
    (SOURCE_NAME, date, latitude, longitude)
    for date, latitude, longitude in zip(DATES, LATITUDE, LONGITUDE, strict=True)
)
StoredValue = str | int | float | datetime | None
FIELDS: list[schema.along_track_fields] = [
    *schema.along_track_columns_schema,
    "delta_t",
]


@pytest.mark.unit
def test_reference_asset_and_manifest_regenerate_identically(tmp_path: Path) -> None:
    generate(tmp_path)
    for name in (ASSET_NAME, "manifest.json"):
        assert (tmp_path / name).read_bytes() == (
            REFERENCE_DIRECTORY / name
        ).read_bytes()
    manifest: object = json.loads((REFERENCE_DIRECTORY / "manifest.json").read_bytes())
    assert isinstance(manifest, dict)
    assert manifest["status"] == "PROVISIONAL"
    assert "#197" in manifest["pending"]
    assert "NOT PROVIDED" in manifest["recent_daily_benchmark_identity"]
    assert manifest["origin_base_code_revision"] == BASE_CODE_REVISION
    source = manifest["source"]
    assert isinstance(source, dict)
    assert source["sha256"] == SOURCE_SHA256
    assert source["size_bytes"] == ASSET_PATH.stat().st_size
    assert source["row_count"] == ROW_COUNT
    assert source["path"] == ASSET_NAME and source["ingest_filename"] == SOURCE_NAME
    assert source["original_row_indices_0based"] == list(range(ROW_COUNT))
    assert source["actual_time_coverage_utc"] == {
        "start": TIME_UTC[0],
        "end": TIME_UTC[-1],
    }
    pins = manifest["dependency_sha256"]
    assert isinstance(pins, dict) and set(pins) == set(DEPENDENCY_PATHS)
    for path, expected_hash in pins.items():
        assert isinstance(path, str) and isinstance(expected_hash, str)
        assert (
            hashlib.sha256((REPO_ROOT / path).read_bytes()).hexdigest() == expected_hash
        )


@pytest.mark.unit
def test_reference_raw_variables_metadata_and_decoded_coordinates() -> None:
    with nc.Dataset(ASSET_PATH) as ds:
        assert ds.data_model == "NETCDF3_CLASSIC"
        assert len(ds.dimensions["time"]) == ROW_COUNT
        assert set(ds.ncattrs()) == set(GLOBAL_ATTRIBUTES)
        for name, value in GLOBAL_ATTRIBUTES.items():
            assert ds.getncattr(name) == value
        assert set(ds.variables) == set(source_variables())
        for name, spec in source_variables().items():
            var = ds[name]
            var.set_auto_maskandscale(False)
            assert var.dtype == np.dtype(spec.dtype)
            assert var.dimensions == spec.dimensions
            assert set(var.ncattrs()) == set(spec.attributes)
            for attribute, attribute_value in spec.attributes.items():
                assert var.getncattr(attribute) == attribute_value
            np.testing.assert_array_equal(
                var[:], np.asarray(spec.raw, dtype=spec.dtype)
            )
        for name, expected in (("latitude", LATITUDE), ("longitude", LONGITUDE)):
            ds[name].set_auto_maskandscale(True)
            np.testing.assert_array_equal(ds[name][:], expected)
        assert all(0 <= value < 360 for value in LONGITUDE)


def _expected_row(index: int) -> dict[schema.along_track_columns, StoredValue]:
    """Associate every source field with one original, zero-based row.

    :param index: Original source row index.
    :returns: Literal database values, including scientific SQL NULLs.
    """
    row: dict[schema.along_track_columns, StoredValue] = {
        "latitude": LATITUDE[index],
        "longitude": LONGITUDE[index],
        "date_time": DATES[index],
        "file_name": SOURCE_NAME,
        "mission": "j2",
        "track": TRACK[index],
        "cycle": CYCLE[index],
        "basin_id": BASIN_ID[index],
    }
    row.update({name: oracle.packed[index] for name, oracle in SCIENCE.items()})
    return row


def _stored_rows(etl: AlongTrackETL) -> list[tuple[object, ...]]:
    """Snapshot complete aligned stored rows, including their generated IDs.

    :param etl: Client connected to the shared disposable database.
    :returns: Rows in source-time order, with no independent column sorting.
    """
    with etl.cursor() as cursor:
        cursor.execute(
            sql.SQL(
                "SELECT id, {fields} FROM along_track ORDER BY date_time, latitude, longitude"
            ).format(
                fields=sql.SQL(", ").join(
                    sql.Identifier(name) for name in schema.along_track_columns_schema
                )
            )
        )
        rows: list[tuple[object, ...]] = cursor.fetchall()
    return rows


def _assert_returned_row(
    result: Dataset[schema.along_track_fields] | None, index: int
) -> None:
    """Compare a complete returned row against independent source literals.

    :param result: One source-centered query result.
    :param index: Original zero-based source row expected at this center.
    """
    assert result is not None and result.row_count == 1
    assert set(result) == set(schema.along_track_schema)
    # Reconcile the returned key to the test-only (source hash, row index).
    key = tuple(
        result[name][0] for name in ("file_name", "date_time", "latitude", "longitude")
    )
    identity = (SOURCE_SHA256, SOURCE_KEYS.index(key))
    assert identity == (SOURCE_SHA256, index)
    for name, expected in _expected_row(index).items():
        if name not in SCIENCE:
            np.testing.assert_array_equal(
                result[name], np.asarray([expected], dtype=object)
            )
    assert result["date_time"][0].microsecond == DATES[index].microsecond
    for name, oracle in SCIENCE.items():
        expected_mask = [oracle.physical[index] is None]
        np.testing.assert_array_equal(np.isnan(result[name]), expected_mask)
        np.testing.assert_array_equal(
            np.isnan(result.get_unscaled(name)), expected_mask
        )
        np.testing.assert_allclose(
            result.get_unscaled(name),
            np.asarray([oracle.packed[index]], dtype=float),
            rtol=0,
            atol=0,
            equal_nan=True,
        )
        np.testing.assert_allclose(
            result[name],
            np.asarray([oracle.physical[index]], dtype=float),
            rtol=0,
            atol=1e-12,
            equal_nan=True,
        )
    np.testing.assert_array_equal(result["distance"], [0.0])
    np.testing.assert_array_equal(result["delta_t"], [0.0])


@pytest.mark.uses_database
@pytest.mark.parametrize("mode", ["insert", "copy"])
def test_source_survives_real_etl_queries_and_duplicate_rejection(
    db_with_basin_data: BasinsETL, tmp_path: Path, mode: Literal["insert", "copy"]
) -> None:
    # Copy the config, not its database name or the shared fixture's settings.
    config_copy = db_with_basin_data.config.model_copy(
        update={
            "ingest_mode": mode,
            "along_track_data_directory": str(tmp_path),
        }
    )
    OceanDBInit(config=config_copy).create_partitions("2024-01-01", "2024-02-01")
    source = tmp_path / SOURCE_NAME
    source.write_bytes(ASSET_PATH.read_bytes())
    assert hashlib.sha256(source.read_bytes()).hexdigest() == SOURCE_SHA256
    etl = AlongTrackETL(config=config_copy)
    etl.process_along_track_file(source, batch_size=2)
    before = _stored_rows(etl)
    assert len(before) == ROW_COUNT
    for index, stored in enumerate(before):
        expected = _expected_row(index)
        assert stored[1:] == tuple(
            expected[name] for name in schema.along_track_columns_schema
        )
    with etl.cursor() as cursor:
        cursor.execute(
            "SELECT file_name, mission, start_date, end_date, observation_count "
            "FROM along_track_metadata"
        )
        assert cursor.fetchall() == [
            (SOURCE_NAME, "j2", DATES[0], DATES[-1], ROW_COUNT)
        ]

    client = AlongTrack(
        config=config_copy
    )  # Real packaged raster and database connectivity.
    for index, (date, latitude, longitude) in enumerate(
        zip(DATES, LATITUDE, LONGITUDE, strict=True)
    ):
        _assert_returned_row(
            client.geographic_point_in_r_dt(
                fields=[*FIELDS, "distance"],
                latitude=latitude,
                longitude=longitude,
                date=date,
                radius=0,
                time_window=timedelta(0),
                missions=["j2"],
            ),
            index,
        )
        # Omit distance: the nearest API must add its mandatory distance field.
        _assert_returned_row(
            client.geographic_nearest_neighbors(
                fields=FIELDS,
                latitude=latitude,
                longitude=longitude,
                date=date,
                max_radius=0,
                time_window=timedelta(0),
                missions=["j2"],
            ),
            index,
        )
    radius_results = list(
        client.geographic_point_in_r_dt_batch(
            fields=[*FIELDS, "distance"],
            latitudes=list(LATITUDE),
            longitudes=list(LONGITUDE),
            dates=list(DATES),
            radius=0,
            time_window=timedelta(0),
            missions=["j2"],
            chunk_size=2,
        )
    )
    nearest_results = list(
        client.geographic_nearest_neighbors_batch(
            fields=FIELDS,
            latitudes=list(LATITUDE),
            longitudes=list(LONGITUDE),
            dates=list(DATES),
            max_radius=0,
            time_window=timedelta(0),
            missions=["j2"],
            chunk_size=2,
        )
    )
    assert len(radius_results) == len(nearest_results) == ROW_COUNT
    for index, (radius, nearest) in enumerate(
        zip(radius_results, nearest_results, strict=True)
    ):
        _assert_returned_row(radius, index)
        _assert_returned_row(nearest, index)

    with pytest.raises(UniqueViolation):
        etl.process_along_track_file(source, batch_size=ROW_COUNT)
    assert _stored_rows(etl) == before
    conflicting = tmp_path / "conflicting" / SOURCE_NAME
    conflicting.parent.mkdir()
    conflicting.write_bytes(ASSET_PATH.read_bytes())
    with nc.Dataset(conflicting, "r+") as ds:
        ds.set_auto_maskandscale(False)
        ds["time"][
            0
        ] = 27029.00005  # New key first: a later conflict must roll it back.
        ds["sla_filtered"][1] = 999  # Same row-1 key, conflicting scientific value.
    with pytest.raises(UniqueViolation):
        etl.process_along_track_file(conflicting, batch_size=ROW_COUNT)
    assert _stored_rows(etl) == before  # No arrival-order winner or additional row.
