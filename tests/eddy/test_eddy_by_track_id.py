import numpy as np
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


def test_eddy_with_track_id_batch_matches_single_queries(db_with_cyclonic_eddy_data):
    eddy = Eddy(config=db_with_cyclonic_eddy_data.config)
    fields = list(eddy_columns_schema.keys())
    track_ids = [-1, -999999]

    expected = [
        eddy.eddy_with_track_id(fields=fields, track_id=track_id)
        for track_id in track_ids
    ]

    result = list(eddy.eddy_with_track_id_batch(fields=fields, track_ids=track_ids))

    assert len(result) == len(expected)

    for batch_item, single_item in zip(result, expected, strict=True):
        assert (batch_item is None) is (single_item is None)
        if batch_item is None or single_item is None:
            continue

        for field in fields:
            assert field in batch_item
            assert np.array_equal(batch_item[field], single_item[field])
