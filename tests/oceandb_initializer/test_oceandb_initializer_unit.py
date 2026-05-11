import pytest

from OceanDB.OceanDB_Initializer import OceanDBInit
from tests.database.fixtures import *

pytestmark = pytest.mark.unit


def test_basic(config):
    ocean_db_init = OceanDBInit(config=config)
    assert ocean_db_init.username == "postgres"
    assert ocean_db_init.password == "postgres"
    assert ocean_db_init.port == 5433
    assert (
        ocean_db_init.connection_string
        == "host=localhost dbname=ocean port=5433 user=postgres password=postgres"
    )
