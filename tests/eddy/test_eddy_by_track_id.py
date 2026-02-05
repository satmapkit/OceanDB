from OceanDB.data_access.eddy import Eddy


def test_eddy_by_track_id():
    eddy = Eddy()

    along_track_eddy_output = eddy.eddy_with_track_id(
        fields=["latitude", "cyclonic_type"], track_id=-4
    )

    assert along_track_eddy_output is not None
