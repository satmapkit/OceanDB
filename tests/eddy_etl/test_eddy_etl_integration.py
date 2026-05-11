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


def test_get_eddy_tracks_from_times_batch_matches_single_queries(
    db_with_cyclonic_eddy_data,
):
    eddy = Eddy(db_with_cyclonic_eddy_data.config)

    start_dates = [
        datetime.datetime(1900, 1, 1),
        datetime.datetime(2100, 1, 1),
    ]
    end_dates = [
        datetime.datetime(2100, 1, 1),
        datetime.datetime(2101, 1, 1),
    ]

    expected = [
        eddy.get_eddy_tracks_from_times(start_date, end_date)
        for start_date, end_date in zip(start_dates, end_dates, strict=True)
    ]

    result = list(eddy.get_eddy_tracks_from_times_batch(start_dates, end_dates))

    assert len(result) == len(expected)
    assert result == expected
    assert tuple(sorted(result[0])) == (-2, -1, 0)
    assert result[1] == []
