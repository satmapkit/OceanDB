from OceanDB.data_access import Eddy

eddy = Eddy()

eddy.along_track_points_near_eddy(
    track_id=4,
    fields=['latitude']
)