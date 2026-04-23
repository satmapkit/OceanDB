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
