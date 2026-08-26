import pytest

from OceanDB.managed_index_oceandb import ManagedIndexOceanDB
from OceanDB.managed_indices import IndexDefinition
from tests.database.fixtures import *


@pytest.mark.unit
def test_create_indexes_executes_canonical_definitions(monkeypatch):
    ocean_db = ManagedIndexOceanDB()
    definition = IndexDefinition(
        name="mission_idx",
        table="along_track",
        create_sql="CREATE INDEX mission_idx ON along_track (mission)",
    )
    queries = []

    def execute_write_query(query_spec, **_kwargs):
        queries.append(query_spec)

    monkeypatch.setattr(ocean_db, "execute_write_query", execute_write_query)
    ocean_db.__dict__["partition_index_name_map"] = {"child": "parent"}

    ocean_db.create_indexes((definition,))

    assert len(queries) == 1
    assert queries[0].query_string.as_string(None) == definition.create_sql
    assert "partition_index_name_map" not in ocean_db.__dict__


@pytest.mark.uses_database
def test_create_indexes_after_basic_database_initialization(db_with_tables):
    definition = next(
        definition
        for definition in db_with_tables.managed_indices.index_definitions
        if definition.name == "basin_id_idx"
    )

    db_with_tables.create_indexes((definition,))

    created = {
        index.index_name: index
        for index in db_with_tables.inventory_indexes()
        if index.index_name == definition.name
    }
    assert created[definition.name].table_name == definition.table
    assert created[definition.name].is_valid is True
    assert created[definition.name].is_ready is True


@pytest.mark.uses_database
def test_drop_indexes_preserves_constraint_indexes(db_with_indices):
    before = db_with_indices.inventory_indexes()
    constraint_names = {
        index.index_name for index in before if index.is_constraint_owned
    }

    db_with_indices.drop_indexes()

    remaining = {index.index_name for index in db_with_indices.inventory_indexes()}
    assert constraint_names <= remaining
    assert not any(
        db_with_indices._is_managed_index_name(index_name) for index_name in remaining
    )
