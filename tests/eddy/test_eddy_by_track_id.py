import pytest

from OceanDB.data_access.eddy import Eddy
from OceanDB.schemas.eddy_schema import eddy_columns_schema
from tests.database.fixtures import *

pytestmark = pytest.mark.uses_database


def test_eddy_by_track_id(db_with_cyclonic_eddy_data):
    eddy = Eddy(config=db_with_cyclonic_eddy_data.config)

    along_track_eddy_output = eddy.eddy_with_track_id(
        fields=list(eddy_columns_schema.keys()),
        track_id=-1,
    )

    assert along_track_eddy_output is not None
