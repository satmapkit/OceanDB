import pytest

from tests.database.fixtures import *

pytestmark = pytest.mark.uses_database


def test_ingest_alongtrack(db_with_alongtrack_data):
    assert True
