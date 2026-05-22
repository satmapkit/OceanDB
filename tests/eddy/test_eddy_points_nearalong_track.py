from typing import get_args

import numpy as np
import pytest

from OceanDB.data_access.eddy import Eddy
from OceanDB.schemas.along_track_schema import along_track_fields
from tests.database.fixtures import *

pytestmark = pytest.mark.uses_database


def test_along_track_points_near_eddy(db_with_eddy_and_alongtrack_data):
    eddy = Eddy(config=db_with_eddy_and_alongtrack_data.config)

    fields = [f for l in get_args(along_track_fields) for f in get_args(l)]

    result = eddy.along_track_points_near_eddy(
        fields=fields,
        track_id=-1,
    )

    assert result is not None


def test_along_track_points_near_eddy_batch_matches_single_queries(
    db_with_eddy_and_alongtrack_data,
):
    eddy = Eddy(config=db_with_eddy_and_alongtrack_data.config)
    fields = [f for l in get_args(along_track_fields) for f in get_args(l)]
    track_ids = [-1]

    expected = [
        eddy.along_track_points_near_eddy(fields=fields, track_id=track_id)
        for track_id in track_ids
    ]

    result = list(
        eddy.along_track_points_near_eddy_batch(fields=fields, track_ids=track_ids)
    )

    assert len(result) == len(expected)

    for batch_item, single_item in zip(result, expected, strict=True):
        assert (batch_item is None) is (single_item is None)
        if batch_item is None or single_item is None:
            continue

        for field in fields:
            assert field in batch_item
            assert np.array_equal(batch_item[field], single_item[field])
