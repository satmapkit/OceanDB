from collections.abc import Mapping
from pathlib import Path

import netCDF4 as nc
import numpy as np
import pytest

from OceanDB.config import Config
from OceanDB.data_access.base_query import BaseReadQuery
from OceanDB.etl.along_track_etl import AlongTrackETL
from OceanDB.etl.base_etl import batch
from OceanDB.schemas.along_track_schema import (along_track_columns,
                                                along_track_columns_schema)

pytestmark = pytest.mark.unit

SCIENTIFIC_FIELDS: tuple[along_track_columns, ...] = (
    "sla_unfiltered",
    "sla_filtered",
    "dac",
    "ocean_tide",
    "internal_tide",
    "lwe",
    "mdt",
    "tpa_correction",
)


@pytest.fixture(params=["_FillValue", "missing_value"])
def extracted_rows(
    tmp_path: Path, request: pytest.FixtureRequest
) -> batch[along_track_columns]:
    file = tmp_path / "required_underscores_j2_20130101.nc"
    with nc.Dataset(file, "w") as ds:
        ds.createDimension("obs", 4)
        for name, values in {
            "latitude": [10, 11, 12, 13],
            "longitude": [20, 21, 22, 23],
            "time": [0, 1, 2, 3],
            "track": [1, 2, 3, 4],
            "cycle": [10, 11, 12, 13],
        }.items():
            ds.createVariable(name, "f8", ("obs",))[:] = values
        ds.variables["time"].units = "days since 2013-01-01 00:00:00"
        for name in SCIENTIFIC_FIELDS:
            var = ds.createVariable(
                name,
                "i2",
                () if name == "tpa_correction" else ("obs",),
                fill_value=32767 if request.param == "_FillValue" else False,
            )
            if request.param == "missing_value":
                var.missing_value = np.int16(32767)
            var.scale_factor = 0.001
            var.add_offset = 0.0
            # Write packed integers without NetCDF applying packing again.
            var.set_auto_maskandscale(False)
            var[:] = 25 if name == "tpa_correction" else [123, 32767, -456, 789]

    etl = AlongTrackETL(config=Config.model_construct())
    with nc.Dataset(file) as ds:
        batches = list(etl.extract_data_from_netcdf(ds, batch_size=2, file=file))
    assert [len(chunk) for chunk in batches] == [2, 2]
    return [row for chunk in batches for row in chunk]


def test_extraction_masks_missing_values_without_scaling_or_dropping_rows(
    extracted_rows: batch[along_track_columns],
) -> None:
    assert len(extracted_rows) == 4
    assert [row["latitude"] for row in extracted_rows] == [10, 11, 12, 13]
    assert [row["track"] for row in extracted_rows] == [1, 2, 3, 4]
    for name in SCIENTIFIC_FIELDS[:-1]:
        field = along_track_columns_schema[name]
        for row, expected in zip(extracted_rows, [123, None, -456, 789], strict=True):
            value = row[name]
            if expected is None:
                assert np.ma.is_masked(value)
                assert AlongTrackETL._copy_value(field, value) is None
            else:
                assert not np.ma.is_masked(value)
                assert value == expected
                assert AlongTrackETL._copy_value(field, value) == expected
    assert [row["tpa_correction"] for row in extracted_rows] == [25] * 4


def test_dataset_exposes_scaled_values_and_nan_after_copy_adaptation(
    extracted_rows: batch[along_track_columns],
) -> None:
    rows: list[Mapping[str, object]] = [
        {
            name: AlongTrackETL._copy_value(field, row[name])
            for name, field in along_track_columns_schema.items()
        }
        for row in extracted_rows
    ]
    dataset = BaseReadQuery(config=Config.model_construct())._build_dataset(
        schema=along_track_columns_schema, rows=rows
    )
    assert dataset.row_count == 4
    assert all(len(dataset[name]) == 4 for name in dataset)
    for name in SCIENTIFIC_FIELDS[:-1]:
        np.testing.assert_allclose(
            dataset[name], [0.123, np.nan, -0.456, 0.789], equal_nan=True
        )
    np.testing.assert_allclose(dataset["tpa_correction"], [0.025] * 4)
    np.testing.assert_array_equal(dataset["track"], [1, 2, 3, 4])
