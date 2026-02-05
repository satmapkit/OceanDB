from typing import get_args

from OceanDB.data_access.eddy import Eddy
from OceanDB.schemas.along_track_schema import along_track_fields


def test_along_track_points_near_eddy():
    eddy = Eddy()

    fields = get_args(along_track_fields)

    result = eddy.along_track_points_near_eddy(
        fields=fields,
        track_id=-4,
    )

    assert result is not None
