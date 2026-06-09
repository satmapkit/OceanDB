from typing import Any, cast

import pytest

from OceanDB.config import Config
from OceanDB.etl.base_etl import OceanDBETL
from OceanDB.ocean_data.ocean_data import ColumnField

pytestmark = pytest.mark.unit


class FakeVariable:
    def __init__(self, *, scale_factor=None, add_offset=None):
        if scale_factor is not None:
            self.scale_factor = scale_factor
        if add_offset is not None:
            self.add_offset = add_offset


class FakeDataset:
    def __init__(self, variables):
        self.variables = variables


def test_verify_netcdf_variable_scaling_logs_nothing_on_match():
    etl = OceanDBETL(config=Config(), debug=True)
    messages = []
    etl.debug_log = lambda message: messages.append(message)
    field = ColumnField(
        export_name="latitude",
        postgres_table_name="dummy",
        postgres_column_name="latitude",
        python_type=float,
        scaling=1e-06,
    )
    ds = FakeDataset({"latitude": FakeVariable(scale_factor=1e-06, add_offset=0.0)})

    etl.verify_netcdf_variable_scaling(ds=cast(Any, ds), field=field, context="[TEST]")

    assert messages == []


def test_verify_netcdf_variable_scaling_logs_warning_on_mismatch():
    etl = OceanDBETL(config=Config(), debug=True)
    messages = []
    etl.debug_log = lambda message: messages.append(message)
    field = ColumnField(
        export_name="latitude",
        postgres_table_name="dummy",
        postgres_column_name="latitude",
        python_type=float,
        scaling=1e-06,
        offset=0.0,
    )
    ds = FakeDataset({"latitude": FakeVariable(scale_factor=0.01, add_offset=180.0)})

    etl.verify_netcdf_variable_scaling(ds=cast(Any, ds), field=field, context="[TEST]")

    assert len(messages) == 1
    assert "variable=latitude" in messages[0]
    assert "schema_scaling=1e-06" in messages[0]
    assert "dataset_scaling=0.01" in messages[0]
    assert "schema_offset=0.0" in messages[0]
    assert "dataset_offset=180.0" in messages[0]


def test_verify_netcdf_variable_scaling_uses_default_dataset_values_when_attrs_missing():
    etl = OceanDBETL(config=Config(), debug=True)
    messages = []
    etl.debug_log = lambda message: messages.append(message)
    field = ColumnField(
        export_name="track",
        postgres_table_name="dummy",
        postgres_column_name="track",
        python_type=int,
    )
    ds = FakeDataset({"track": FakeVariable()})

    etl.verify_netcdf_variable_scaling(ds=cast(Any, ds), field=field, context="[TEST]")

    assert messages == []
