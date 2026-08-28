import psycopg as pg
import pytest
from psycopg import sql

from OceanDB.OceanDB_Initializer import (OceanDBInit, eddy_tables,
                                         table_definitions)
from OceanDB.schemas.eddy_schema import eddy_columns_schema
from tests.database.fixtures import *

pytestmark = pytest.mark.uses_database


def test_create_database(db_with_db):
    with pg.connect(db_with_db.config.postgres_dsn_admin) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = %(db_name)s);",
                {"db_name": "ocean"},
            )
            res = cur.fetchone()
            assert res is not None
            assert len(res) > 0
            exists = res[0]
            assert exists


def test_create_database_when_database_exists(db_with_db, capsys):
    created = db_with_db.create_database()

    assert created is False
    captured = capsys.readouterr()
    assert f"Database '{db_with_db.db_name}' already exists." in captured.out
    assert f"Database '{db_with_db.db_name}' POSTGIS enabled." in captured.out


def test_create_tables(db_with_db):
    for table in [*table_definitions, *eddy_tables]:
        assert not db_with_db.table_exists(table["name"])
    db_with_db.create_tables()
    db_with_db.create_eddy_tables()
    for table in [*table_definitions, *eddy_tables]:
        assert db_with_db.table_exists(table["name"])

    query = sql.SQL("SELECT {field} FROM eddy LIMIT 1;")
    with pg.connect(db_with_db.connection_string) as conn:
        with conn.cursor() as cur:
            for field in eddy_columns_schema.values():
                formatted = query.format(field=field.sql_expression())
                cur.execute(formatted)
            with pytest.raises(pg.errors.UndefinedColumn):
                cur.execute(query.format(field=sql.Identifier("fakefield")))


def test_indices(db_with_indices):
    inventory = db_with_indices.inventory_indexes()
    managed_names = db_with_indices.managed_indices.managed_index_names

    assert managed_names <= {index.index_name for index in inventory}
    assert all(index.index_definition.startswith("CREATE") for index in inventory)
    assert all(index.is_valid and index.is_ready for index in inventory)
    assert any(index.is_constraint_owned for index in inventory)

    attached_indices = [
        index for index in inventory if index.parent_index_name in managed_names
    ]
    assert attached_indices
    assert all(index.is_attached_partition_index for index in attached_indices)
    assert all(index.parent_table_name == "along_track" for index in attached_indices)

    partition_index_name_map = db_with_indices.partition_index_name_map
    assert partition_index_name_map == {
        index.index_name: index.parent_index_name for index in attached_indices
    }
