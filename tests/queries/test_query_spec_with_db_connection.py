from psycopg import ClientCursor, connect, sql

import OceanDB.query_spec as query_spec_module
from tests.database.fixtures import *


def test_log_query(db_with_db, caplog):
    with connect(db_with_db.connection_string, cursor_factory=ClientCursor) as conn:
        with conn.cursor() as cur:
            with caplog.at_level("INFO", logger="oceandb"):
                query_spec_module.log_query(
                    conn=conn,
                    cursor=cur,
                    query=sql.Composed([sql.SQL("SELECT %(value)s::int AS value")]),
                    params={"value": 3},
                )

    assert "--- SQL QUERY ---" in caplog.text
    assert "SELECT 3::int AS value" in caplog.text
    assert "--- PARAMS ---" not in caplog.text
