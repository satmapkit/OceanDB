from datetime import datetime

from OceanDB.data_access.schema.eddy_schema import eddy_fields
from OceanDB.data_access import Eddy

eddy = Eddy()

output = eddy.eddy_with_track_id(
    track_id=4,
    fields=eddy_fields
)


# # eddy.eddy_with_track_id(track_id=4)
#
# tracks = eddy.get_eddy_tracks_from_times(
#     start_date=datetime(2013, 3, 1), end_date=datetime(2013, 4, 1)
# )




#
# # print(tracks)
# # print(f"number of tracks {len(tracks)}")
#
#
# output = eddy.eddy_with_track_id(track_id=4)
#
# along_track_eddy_output = eddy.along_track_points_near_eddy(track_id=9844)
#
