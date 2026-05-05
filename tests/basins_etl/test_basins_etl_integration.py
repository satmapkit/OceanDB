import psycopg as pg
import pytest

from tests.database.fixtures import *

pytestmark = pytest.mark.uses_database


def test_insert_basin_data(db_with_basin_data):
    with pg.connect(db_with_basin_data.connection_string) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM basin;")
            res = cur.fetchone()
            assert res
            assert res[0] > 0

            cur.execute("SELECT COUNT(*) FROM basin_connections;")
            res = cur.fetchone()
            assert res
            assert res[0] > 0
