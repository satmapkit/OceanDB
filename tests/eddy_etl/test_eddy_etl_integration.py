import datetime
import pytest

from OceanDB.data_access.eddy import Eddy
from OceanDB.schemas.eddy_schema import eddy_columns_schema

from tests.database.fixtures import *

pytestmark = pytest.mark.uses_database


def test_ingest_cyclonic_eddy(db_with_cyclonic_eddy_data):
    eddy = Eddy(db_with_cyclonic_eddy_data.config)
    ids = eddy.get_eddy_tracks_from_times(
        datetime.datetime(1900, 1, 1), datetime.datetime(2100, 1, 1)
    )
    assert tuple(sorted(ids)) == (-2, -1, 0)

    fields = list(eddy_columns_schema.keys())

    for i in ids:
        res = eddy.eddy_with_track_id(fields=fields, track_id=i)
        assert res is not None
