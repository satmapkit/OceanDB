from contextlib import contextmanager

from OceanDB.config import Config
from OceanDB.etl.base_etl import OceanDBETL
from OceanDB.ocean_data.ocean_data import ColumnField


class FakeCopy:
    def __init__(self):
        self.rows = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def write_row(self, row):
        self.rows.append(row)


class FakeCursor:
    def __init__(self):
        self.executed = []
        self.executemany_calls = []
        self.copy_contexts = []

    def execute(self, query, params=None):
        self.executed.append((query, params))

    def executemany(self, query, rows):
        self.executemany_calls.append((query, list(rows)))

    def copy(self, query):
        context = FakeCopy()
        self.copy_contexts.append((query, context))
        return context


def test_config_reads_oceandb_ingest_mode_env(monkeypatch):
    monkeypatch.setenv("OCEANDB_INGEST_MODE", "copy")
    config = Config()
    assert config.ingest_mode == "copy"


def test_import_schema_rows_uses_insert_mode():
    schema = {
        "id": ColumnField(
            export_name="id",
            python_type=int,
            postgres_table_name="sample",
            postgres_column_name="id",
            postgres_type="integer",
        ),
        "name": ColumnField(
            export_name="name",
            python_type=str,
            postgres_table_name="sample",
            postgres_column_name="name",
            postgres_type="text",
        ),
    }
    cursor = FakeCursor()
    etl = OceanDBETL(config=Config(ingest_mode="insert"))

    @contextmanager
    def fake_cursor(**kwargs):
        assert kwargs == {"commit": True}
        yield cursor

    etl.cursor = fake_cursor

    etl.import_schema_rows_to_postgresql(
        table_name="sample",
        schema=schema,
        data=[{"id": 1, "name": "alpha"}],
    )

    assert len(cursor.executemany_calls) == 1
    assert cursor.executemany_calls[0][1] == [(1, "alpha")]
    assert cursor.executed == []
    assert cursor.copy_contexts == []


def test_import_schema_rows_uses_copy_mode_and_value_adapter():
    schema = {
        "id": ColumnField(
            export_name="id",
            python_type=int,
            postgres_table_name="sample",
            postgres_column_name="id",
            postgres_type="integer",
        ),
        "name": ColumnField(
            export_name="name",
            python_type=str,
            postgres_table_name="sample",
            postgres_column_name="name",
            postgres_type="text",
        ),
    }
    cursor = FakeCursor()
    etl = OceanDBETL(config=Config(ingest_mode="copy"))

    @contextmanager
    def fake_cursor(**kwargs):
        assert kwargs == {"commit": True}
        yield cursor

    etl.cursor = fake_cursor

    etl.import_schema_rows_to_postgresql(
        table_name="sample",
        schema=schema,
        data=[{"id": 1, "name": "alpha"}],
        value_adapter=lambda field, value: str(value).upper()
        if field.postgres_column_name == "name"
        else value,
    )

    assert len(cursor.executed) == 2
    assert cursor.executemany_calls == []
    assert len(cursor.copy_contexts) == 1
    assert cursor.copy_contexts[0][1].rows == [(1, "ALPHA")]
