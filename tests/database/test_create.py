import psycopg as pg

from OceanDB.OceanDB_Initializer import OceanDBInit, table_definitions, eddy_tables

from .fixtures import *


def test_basic(config):
    ocean_db_init = OceanDBInit(config=config)
    assert ocean_db_init.username == "postgres"
    assert ocean_db_init.password == "postgres"
    assert ocean_db_init.port == 5433
    assert (
        ocean_db_init.connection_string
        == "host=localhost dbname=ocean port=5433 user=postgres password=postgres"
    )


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


def test_create_tables(db_with_db):
    for table in [*table_definitions, *eddy_tables]:
        assert not db_with_db.table_exists(table["name"])
    db_with_db.create_tables()
    db_with_db.create_eddy_tables()
    for table in [*table_definitions, *eddy_tables]:
        assert db_with_db.table_exists(table["name"])


def test_indices(db_with_indices):
    # TODO: verify that indices are there
    # TODO: remove indices?
    pass
