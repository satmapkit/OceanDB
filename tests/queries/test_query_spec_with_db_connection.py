from psycopg import ClientCursor, connect, sql

import OceanDB.query_spec as query_spec_module
from tests.database.fixtures import *


def test_render_query(db_with_db):
    with connect(db_with_db.connection_string, cursor_factory=ClientCursor) as conn:
        with conn.cursor() as cur:
            rendered_query = query_spec_module.render_query(
                conn=conn,
                cursor=cur,
                query=sql.Composed([sql.SQL("SELECT %(value)s::int AS value")]),
                params={"value": 3},
            )

    assert rendered_query == "SELECT 3::int AS value"
