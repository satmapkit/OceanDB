from typing import get_args

from OceanDB.data_access.eddy import Eddy
from OceanDB.schemas.along_track_schema import along_track_fields

from tests.database.fixtures import *


def test_along_track_points_near_eddy(db_with_eddy_and_alongtrack_data):
    eddy = Eddy(config=db_with_eddy_and_alongtrack_data.config)

    fields = get_args(along_track_fields)

    result = eddy.along_track_points_near_eddy(
        fields=fields,
        track_id=-1,
    )

    assert result is not None
