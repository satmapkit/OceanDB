from datetime import datetime
from typing import Any, Mapping

import pytest

from OceanDB.data_access.base_query import BaseReadQuery
from OceanDB.ocean_data.ocean_data import ColumnField

from tests.database.fixtures import *


def test_build_dataset_skips_missing_fields(config):
    base_query = BaseReadQuery(config=config)
    schema = {
        "x": ColumnField(
            export_name="x_value",
            postgres_table_name="dummy",
            postgres_column_name="x_value",
            python_type=float,
        ),
        "y": ColumnField(
            export_name="y_value",
            postgres_table_name="dummy",
            postgres_column_name="y_value",
            python_type=float,
        ),
    }

    rows: list[Mapping[str, Any]] = [
        {"x_value": 1.0},
        {"x_value": 2.0},
    ]

    result = base_query._build_dataset(
        schema=schema,
        rows=rows,
    )

    assert "x" in result
    assert "y" not in result
    assert list(result["x"]) == [1.0, 2.0]


def test_build_dataset_uses_export_name_for_lookup(config):
    base_query = BaseReadQuery(config=config)
    schema = {
        "observed_at": ColumnField(
            export_name="db_time",
            postgres_table_name="dummy",
            postgres_column_name="db_time",
            python_type=datetime,
        )
    }

    rows: list[Mapping[str, Any]] = [
        {"db_time": datetime(2020, 1, 1, 12, 0)},
        {"db_time": datetime(2020, 1, 2, 12, 0)},
    ]

    result = base_query._build_dataset(
        schema=schema,
        rows=rows,
    )

    assert "observed_at" in result
    assert list(result["observed_at"]) == [
        datetime(2020, 1, 1, 12, 0),
        datetime(2020, 1, 2, 12, 0),
    ]


def test_build_dataset_parses_datetime_values(config):
    base_query = BaseReadQuery(config=config)
    schema = {
        "when": ColumnField(
            export_name="when",
            postgres_table_name="dummy",
            postgres_column_name="when",
            python_type=datetime,
        )
    }

    rows: list[Mapping[str, Any]] = [
        {"when": datetime(2013, 1, 1, 0, 0)},
        {"when": datetime(2013, 1, 2, 0, 0)},
    ]

    result = base_query._build_dataset(
        schema=schema,
        rows=rows,
    )

    assert "when" in result
    assert list(result["when"]) == [
        datetime(2013, 1, 1, 0, 0),
        datetime(2013, 1, 2, 0, 0),
    ]


def test_build_dataset_raises_on_empty_rows(config):
    base_query = BaseReadQuery(config=config)

    with pytest.raises(ValueError, match="rows must be nonempty"):
        base_query._build_dataset(schema={}, rows=[])
