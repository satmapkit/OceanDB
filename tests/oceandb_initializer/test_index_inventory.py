from contextlib import contextmanager

import pytest

from OceanDB.managed_index_oceandb import DatabaseIndex, ManagedIndexOceanDB

pytestmark = pytest.mark.unit


def database_index(**overrides) -> DatabaseIndex:
    values = {
        "schema_name": "public",
        "table_name": "along_track",
        "index_name": "along_track_point_idx",
        "index_definition": (
            "CREATE INDEX along_track_point_idx ON public.along_track USING gist (point)"
        ),
        "access_method": "gist",
        "index_kind": "I",
        "table_kind": "p",
        "is_unique": False,
        "is_primary": False,
        "is_valid": True,
        "is_ready": True,
        "constraint_name": None,
        "constraint_type": None,
        "parent_index_name": None,
        "parent_table_name": None,
    }
    values.update(overrides)
    return DatabaseIndex(**values)


def test_database_index_describes_partition_and_constraint_roles():
    parent = database_index()
    attached = database_index(
        table_name="along_track_2024_01",
        index_name="along_track_2024_01_point_idx",
        index_kind="i",
        table_kind="r",
        parent_index_name="along_track_point_idx",
        parent_table_name="along_track",
    )
    standalone = database_index(
        table_name="along_track_2024_01",
        index_name="custom_partition_idx",
        index_kind="i",
        table_kind="r",
        parent_table_name="along_track",
    )
    primary = database_index(
        index_name="along_track_pkey",
        constraint_name="along_track_pkey",
        constraint_type="p",
        is_unique=True,
        is_primary=True,
    )

    assert parent.is_partitioned_parent is True
    assert attached.is_attached_partition_index is True
    assert attached.is_standalone_partition_index is False
    assert standalone.is_standalone_partition_index is True
    assert primary.is_constraint_owned is True


def test_inventory_indexes_returns_structured_catalog_rows(monkeypatch):
    ocean_db = ManagedIndexOceanDB()
    expected = database_index()
    executed = []

    class FakeCursor:
        def execute(self, query, params):
            executed.append((query, params))

        def fetchall(self):
            return [expected]

    @contextmanager
    def cursor(**kwargs):
        assert "row_factory" in kwargs
        yield FakeCursor()

    monkeypatch.setattr(ocean_db, "cursor", cursor)

    result = ocean_db.inventory_indexes(schema_name="benchmark")

    assert result == (expected,)
    assert executed[0][1] == {"schema_name": "benchmark"}
    assert "pg_get_indexdef" in executed[0][0]
    assert "owning_constraint" in executed[0][0]
    assert "parent_index_name" in executed[0][0]


def test_get_index_size_returns_physical_size_in_bytes(monkeypatch):
    ocean_db = ManagedIndexOceanDB()
    executed = []

    class FakeCursor:
        def execute(self, query, params):
            executed.append((query, params))

        def fetchone(self):
            return (32768,)

    @contextmanager
    def cursor():
        yield FakeCursor()

    monkeypatch.setattr(ocean_db, "cursor", cursor)

    result = ocean_db.get_index_size('point"idx', schema_name="benchmark")

    assert result == 32768
    assert executed == [
        (
            "SELECT pg_relation_size(%(index_name)s::regclass)",
            {"index_name": '"benchmark"."point""idx"'},
        )
    ]


def test_drop_indexes_executes_managed_roots_and_standalone_partitions(monkeypatch):
    ocean_db = ManagedIndexOceanDB()
    parent = database_index()
    standalone = database_index(
        index_name="along_track_point_idx_2024_01",
        parent_table_name="along_track",
        index_kind="i",
    )
    attached = database_index(parent_index_name=parent.index_name)
    constraint = database_index(constraint_name="managed_constraint")
    unmanaged = database_index(index_name="external_idx")
    queries = []
    monkeypatch.setattr(
        ocean_db,
        "inventory_indexes",
        lambda schema_name: (parent, attached, constraint, unmanaged, standalone),
    )
    monkeypatch.setattr(ocean_db, "execute_write_query", queries.append)
    ocean_db.__dict__["partition_index_name_map"] = {"child": "parent"}

    ocean_db.drop_indexes("benchmark")

    assert [query.query_string.as_string(None) for query in queries] == [
        'DROP INDEX IF EXISTS "benchmark"."along_track_point_idx_2024_01"',
        'DROP INDEX IF EXISTS "benchmark"."along_track_point_idx"',
    ]
    assert "partition_index_name_map" not in ocean_db.__dict__


def test_list_indices_preserves_existing_dictionary_api(monkeypatch):
    ocean_db = ManagedIndexOceanDB()
    managed = database_index()
    unmanaged = database_index(
        index_name="external_idx",
        index_definition="CREATE INDEX external_idx ON public.along_track (mission)",
    )
    schema_names = []

    def inventory_indexes(schema_name="public"):
        schema_names.append(schema_name)
        return managed, unmanaged

    monkeypatch.setattr(ocean_db, "inventory_indexes", inventory_indexes)

    assert ocean_db.list_indices(schema_name="benchmark") == [
        {
            "schema_name": managed.schema_name,
            "table_name": managed.table_name,
            "index_name": managed.index_name,
            "index_definition": managed.index_definition,
        }
    ]
    assert len(ocean_db.list_indices(managed_only=False)) == 2
    assert schema_names == ["benchmark", "public"]


def test_partition_index_map_uses_attached_managed_children(monkeypatch):
    ocean_db = ManagedIndexOceanDB()
    attached = database_index(
        index_name="along_track_2024_01_point_idx",
        parent_index_name="along_track_point_idx",
        parent_table_name="along_track",
    )
    unmanaged = database_index(
        index_name="external_child_idx",
        parent_index_name="external_parent_idx",
        parent_table_name="along_track",
    )
    monkeypatch.setattr(
        ocean_db,
        "inventory_indexes",
        lambda: (attached, unmanaged),
    )

    assert ocean_db._load_partition_index_name_map() == {
        attached.index_name: attached.parent_index_name
    }
