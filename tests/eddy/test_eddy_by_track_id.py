from datetime import datetime

from OceanDB.data_access import Eddy

eddy = Eddy()


along_track_result_iterator = eddy.eddy_envelope_query(track_id=4)
print(along_track_result_iterator._data)


along_track_eddy_output = eddy.along_track_points_near_eddy(
    fields=["latitude", "ocean_tide", "delta_t"],
    track_id=4
)