from contextlib import contextmanager

import pytest
from psycopg import sql

from OceanDB.OceanDB import OceanDB

pytestmark = pytest.mark.unit


def test_vacuum_analyze_accepts_database_or_table_scope():
    queries = []
    db = OceanDB()

    @contextmanager
    def fake_cursor(**kwargs):
        class Cursor:
            def execute(self, query):
                queries.append((kwargs, query))

        yield Cursor()

    db.cursor = fake_cursor
    db.vacuum_analyze()
    db.vacuum_analyze("basin")

    assert queries[0][0] == {"autocommit": True}
    assert queries[0][1] == sql.SQL("VACUUM ANALYZE")
    assert queries[1][1].as_string() == 'VACUUM ANALYZE "public"."basin"'
