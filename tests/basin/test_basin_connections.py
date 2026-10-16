"""Packaged basin connectivity must be usable without PostgreSQL."""

import psycopg
import pytest

from OceanDB.ocean_data.basins import BasinConnections, BasinMask

pytestmark = pytest.mark.unit


def test_connections_follow_packaged_table_without_database(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail_connect(*args: object, **kwargs: object) -> None:
        pytest.fail("Basin connectivity must not open a PostgreSQL connection")

    monkeypatch.setattr(psycopg, "connect", fail_connect)
    connections = BasinConnections().connection_map
    assert connections[1][:3] == [1, 14, 28]
    assert connections[5][:3] == [5, 4, 10]
    assert connections[0] == [0]
    assert connections[1001] == [1001, 1002]
    assert connections[1002] == [1002, 1001]
    assert all(int(basin_id) in connections for basin_id in BasinMask().basin_ids())
    assert all(basin_id in linked for basin_id, linked in connections.items())
