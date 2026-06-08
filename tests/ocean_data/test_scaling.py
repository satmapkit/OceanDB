from datetime import datetime

import numpy as np
import pytest

from OceanDB.ocean_data.dataset import Dataset
from OceanDB.ocean_data.ocean_data import ColumnField, DerivedField

pytestmark = pytest.mark.unit


def test_column_field_defaults_scaling_and_offset():
    field = ColumnField(
        export_name="x",
        postgres_table_name="dummy",
        postgres_column_name="x",
        python_type=float,
    )

    assert field.scaling == 1
    assert field.offset == 0


def test_derived_field_accepts_explicit_scaling_and_offset():
    field = DerivedField(
        export_name="distance",
        expression="1",
        python_type=float,
        scaling=0.5,
        offset=2,
    )

    assert field.scaling == 0.5
    assert field.offset == 2


def test_dataset_getitem_applies_scaling_and_offset():
    field = ColumnField(
        export_name="x",
        postgres_table_name="dummy",
        postgres_column_name="x",
        python_type=np.float64,
        scaling=0.1,
        offset=3,
    )
    dataset = Dataset(
        name="scaled",
        data={"x": np.array([10.0, 20.0])},
        dtypes={"x": np.float64},
        schema={"x": field},
    )

    np.testing.assert_allclose(dataset["x"], np.array([4.0, 5.0]))


def test_dataset_getitem_leaves_default_scaled_values_unchanged():
    field = ColumnField(
        export_name="x",
        postgres_table_name="dummy",
        postgres_column_name="x",
        python_type=np.float64,
    )
    raw = np.array([1.0, 2.0])
    dataset = Dataset(
        name="identity",
        data={"x": raw},
        dtypes={"x": np.float64},
        schema={"x": field},
    )

    assert dataset["x"] is raw


def test_dataset_getitem_does_not_scale_datetimes():
    field = ColumnField(
        export_name="when",
        postgres_table_name="dummy",
        postgres_column_name="when",
        python_type=datetime,
        scaling=2,
        offset=5,
    )
    raw = np.array([datetime(2020, 1, 1), datetime(2020, 1, 2)], dtype=object)
    dataset = Dataset(
        name="dates",
        data={"when": raw},
        dtypes={"when": datetime},
        schema={"when": field},
    )

    assert dataset["when"] is raw
