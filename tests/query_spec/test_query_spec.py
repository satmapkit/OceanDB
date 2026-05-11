import OceanDB.query_spec as query_spec_module
import pytest
from psycopg import sql
from typing import Any, cast

from OceanDB.ocean_data.ocean_data import ColumnField, DerivedField
from OceanDB.query_spec import QuerySpec

pytestmark = pytest.mark.unit


def test_sql_projection_compiler_with_no_fields():
    query = QuerySpec(
        sql_template="SELECT {fields} FROM dummy_table dummy",
        schema={
            "x": ColumnField(
                export_name="x_value",
                postgres_table_name="dummy",
                postgres_column_name="x_value",
                python_type=float,
            )
        },
    )

    compiled = query.sql_projection_compiler([])

    assert compiled.as_string(None) == "SELECT  FROM dummy_table dummy"


def test_sql_projection_compiler_with_one_column_field():
    query = QuerySpec(
        sql_template="SELECT {fields} FROM dummy_table dummy",
        schema={
            "x": ColumnField(
                export_name="x_value",
                postgres_table_name="dummy",
                postgres_column_name="x_value",
                python_type=float,
            )
        },
    )

    compiled = query.sql_projection_compiler(["x"])

    assert (
        compiled.as_string(None)
        == 'SELECT "dummy"."x_value" AS "x_value" FROM dummy_table dummy'
    )


def test_sql_projection_compiler_with_one_derived_field():
    query = QuerySpec(
        sql_template="SELECT {fields} FROM dummy_table dummy",
        schema={
            "distance": DerivedField(
                export_name="distance",
                expression=(
                    "ST_Distance(ST_MakePoint(%(longitude)s, %(latitude)s), dummy.geom)"
                ),
                python_type=float,
            )
        },
    )

    compiled = query.sql_projection_compiler(["distance"])
    sql_string = compiled.as_string(None)

    assert sql_string == (
        "SELECT ST_Distance(ST_MakePoint(%(longitude)s, %(latitude)s), "
        'dummy.geom) AS "distance" FROM dummy_table dummy'
    )
    assert "%(longitude)s" in sql_string
    assert "%(latitude)s" in sql_string


def test_sql_projection_compiler_with_only_mandatory_field_requested():
    query = QuerySpec(
        sql_template=(
            "SELECT {fields}"
            "dummy.geom <-> ST_MakePoint(%(longitude)s, %(latitude)s) AS distance "
            "FROM dummy_table dummy"
        ),
        schema={
            "distance": DerivedField(
                export_name="distance",
                expression="dummy.geom <-> ST_MakePoint(%(longitude)s, %(latitude)s)",
                python_type=float,
            )
        },
        mandatory_fields=["distance"],
    )

    compiled = query.sql_projection_compiler(["distance"])
    sql_string = compiled.as_string(None)

    assert sql_string == (
        "SELECT dummy.geom <-> ST_MakePoint(%(longitude)s, %(latitude)s) "
        "AS distance FROM dummy_table dummy"
    )
    assert 'AS "distance"' not in sql_string


def test_sql_projection_compiler_with_mandatory_and_extra_field():
    query = QuerySpec(
        sql_template=(
            "SELECT {fields}"
            "dummy.geom <-> ST_MakePoint(%(longitude)s, %(latitude)s) AS distance "
            "FROM dummy_table dummy"
        ),
        schema={
            "x": ColumnField(
                export_name="x_value",
                postgres_table_name="dummy",
                postgres_column_name="x_value",
                python_type=float,
            ),
            "distance": DerivedField(
                export_name="distance",
                expression="dummy.geom <-> ST_MakePoint(%(longitude)s, %(latitude)s)",
                python_type=float,
            ),
        },
        mandatory_fields=["distance"],
    )

    compiled = query.sql_projection_compiler(["x", "distance"])

    assert compiled.as_string(None) == (
        'SELECT "dummy"."x_value" AS "x_value",dummy.geom <-> '
        "ST_MakePoint(%(longitude)s, %(latitude)s) AS distance FROM dummy_table dummy"
    )


def test_sql_projection_compiler_preserves_field_order():
    query = QuerySpec(
        sql_template="SELECT {fields} FROM dummy_table dummy",
        schema={
            "y": ColumnField(
                export_name="y_value",
                postgres_table_name="dummy",
                postgres_column_name="y_value",
                python_type=float,
            ),
            "distance": DerivedField(
                export_name="distance",
                expression="dummy.geom <-> ST_MakePoint(%(longitude)s, %(latitude)s)",
                python_type=float,
            ),
            "x": ColumnField(
                export_name="x_value",
                postgres_table_name="dummy",
                postgres_column_name="x_value",
                python_type=float,
            ),
        },
    )

    compiled = query.sql_projection_compiler(["y", "distance", "x"])
    sql_string = compiled.as_string(None)

    assert sql_string.index('AS "y_value"') < sql_string.index('AS "distance"')
    assert sql_string.index('AS "distance"') < sql_string.index('AS "x_value"')


def test_sql_projection_compiler_aliases_with_export_name():
    query = QuerySpec(
        sql_template="SELECT {fields} FROM dummy_table dummy",
        schema={
            "observed_at": ColumnField(
                export_name="db_time",
                postgres_table_name="dummy",
                postgres_column_name="time_col",
                python_type=object,
            )
        },
    )

    compiled = query.sql_projection_compiler(["observed_at"])
    sql_string = compiled.as_string(None)

    assert sql_string == 'SELECT "dummy"."time_col" AS "db_time" FROM dummy_table dummy'
    assert 'AS "observed_at"' not in sql_string


def test_sql_projection_compiler_raises_for_unknown_field():
    query = QuerySpec(
        sql_template="SELECT {fields} FROM dummy_table",
        schema={},
    )

    with pytest.raises(KeyError):
        query.sql_projection_compiler(["not_in_schema"])


def test_log_query_logs_rendered_sql_for_client_cursor(monkeypatch, caplog):
    class FakeClientCursor:
        def mogrify(self, query, params):
            assert query.as_string(None) == "SELECT * FROM dummy WHERE id = %(id)s"
            assert params == {"id": 3}
            return "OUTPUT"

    monkeypatch.setattr(query_spec_module.pg, "ClientCursor", FakeClientCursor)

    query = cast(Any, sql.SQL("SELECT * FROM dummy WHERE id = %(id)s"))

    with caplog.at_level("INFO", logger="oceandb"):
        query_spec_module.log_query(
            conn=cast(Any, None),
            cursor=cast(Any, FakeClientCursor()),
            query=query,
            params={"id": 3},
        )

    assert "--- SQL QUERY ---" in caplog.text
    assert "OUTPUT" in caplog.text
    assert "--- PARAMS ---" not in caplog.text


def test_log_query_logs_sql_and_params_for_non_client_cursor(monkeypatch, caplog):
    class FakeClientCursor:
        pass

    class FakeCursor:
        pass

    monkeypatch.setattr(query_spec_module.pg, "ClientCursor", FakeClientCursor)

    query = cast(Any, sql.SQL("SELECT * FROM dummy WHERE id = %(id)s"))

    with caplog.at_level("INFO", logger="oceandb"):
        query_spec_module.log_query(
            conn=cast(Any, None),
            cursor=cast(Any, FakeCursor()),
            query=query,
            params={"id": 3},
        )

    assert "--- SQL QUERY ---" in caplog.text
    assert "SELECT * FROM dummy WHERE id = %(id)s" in caplog.text
    assert "--- PARAMS ---" in caplog.text
    assert "{'id': 3}" in caplog.text
