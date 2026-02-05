from typing import get_args
import numpy as np

from OceanDB.data_access.eddy import Eddy, envelope_fields

def test_eddy_envelope_basic():
    eddy = Eddy()

    fields = get_args(envelope_fields)

    result = eddy.eddy_envelope_query(
        track_id=-4
    )
    assert result is not None

    for field in fields:
        assert field in result

    assert np.all(result["min_date"] <= result["max_date"])
    assert all(len(ids) > 0 for ids in result["basin_ids"])
