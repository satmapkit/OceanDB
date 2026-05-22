from typing import get_args

import numpy as np
import pytest

from OceanDB.data_access.eddy import Eddy, envelope_fields
from tests.database.fixtures import *

pytestmark = pytest.mark.uses_database


def test_eddy_envelope_basic(db_with_cyclonic_eddy_data):
    eddy = Eddy(config=db_with_cyclonic_eddy_data.config)

    fields = get_args(envelope_fields)

    result = eddy.eddy_envelope_query(track_id=-1)
    assert result is not None

    for field in fields:
        assert field in result

    assert np.all(result["min_date"] <= result["max_date"])
    assert all(len(ids) > 0 for ids in result["basin_ids"])


def test_eddy_envelope_query_batch_matches_single_queries(db_with_cyclonic_eddy_data):
    eddy = Eddy(config=db_with_cyclonic_eddy_data.config)
    track_ids = [-1, -999999]
    fields = get_args(envelope_fields)

    expected = [eddy.eddy_envelope_query(track_id=track_id) for track_id in track_ids]
    result = list(eddy.eddy_envelope_query_batch(track_ids=track_ids))

    assert len(result) == len(expected)

    for batch_item, single_item in zip(result, expected, strict=True):
        assert (batch_item is None) is (single_item is None)
        if batch_item is None or single_item is None:
            continue

        for field in fields:
            assert field in batch_item

        assert np.array_equal(batch_item["min_date"], single_item["min_date"])
        assert np.array_equal(batch_item["max_date"], single_item["max_date"])
        assert batch_item["basin_ids"].tolist() == single_item["basin_ids"].tolist()
