from OceanDB.data_access.eddy import Eddy

from tests.database.fixtures import *


def test_eddy_by_track_id(db_with_cyclonic_eddy_data):
    eddy = Eddy(config=db_with_cyclonic_eddy_data.config)

    along_track_eddy_output = eddy.eddy_with_track_id(
        fields=["latitude", "cyclonic_type"], track_id=-1
    )

    assert along_track_eddy_output is not None
