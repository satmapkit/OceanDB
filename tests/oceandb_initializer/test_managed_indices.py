import pytest

from OceanDB.managed_index_oceandb import IndexDefinition, ManagedIndices
from OceanDB.OceanDB_Initializer import OceanDBInit

pytestmark = pytest.mark.unit


def test_builtin_index_definitions_are_loaded_from_one_catalog():
    managed_indices = ManagedIndices()

    assert len(managed_indices.index_resources) == 14
    assert len(managed_indices.index_definitions) == 14
    assert len(managed_indices.definitions_for_tables("along_track")) == 10
    assert (
        len(managed_indices.definitions_for_tables("basin", "basin_connections")) == 2
    )
    assert len(managed_indices.definitions_for_tables("eddy")) == 2


def test_dictionary_view_preserves_existing_catalog_shape():
    managed_indices = ManagedIndices()
    definition = next(
        definition
        for definition in managed_indices.index_definitions
        if definition.name == "along_track_point_idx"
    )
    row = next(
        row
        for row in managed_indices.definitions
        if row["logical_name"] == definition.name
    )

    assert row == {
        "logical_name": definition.name,
        "table_name": definition.table,
        "index_name": definition.name,
        "index_definition": " ".join(definition.create_sql.split()),
        "index_definition_multiline": definition.create_sql.strip(),
        "filepath": "indices/along_track/create_along_track_index_point.sql",
    }


def test_catalog_rejects_unparseable_sql(monkeypatch):
    managed_indices = ManagedIndices(
        index_resources=["invalid.sql"], default_indices=set()
    )
    monkeypatch.setattr(
        managed_indices,
        "load_sql_file",
        lambda _filepath: "SELECT 1;",
    )

    with pytest.raises(ValueError, match="Unable to parse index SQL resource"):
        _ = managed_indices.index_definitions


def test_initializer_creates_canonical_definition():
    definition = IndexDefinition(
        name="mission_idx",
        table="along_track",
        create_sql="CREATE INDEX IF NOT EXISTS mission_idx ON along_track (mission);",
    )
    ocean_db_init = OceanDBInit()
    queries = []
    ocean_db_init.execute_write_query = queries.append

    ocean_db_init.create_indexes([definition])

    assert len(queries) == 1
    assert queries[0].query_string.as_string(None) == definition.create_sql
