
from OceanDB.data_access import Eddy

eddy = Eddy()

# along_track_result_iterator = eddy.eddy_envelope_query(track_id=4)
# print(along_track_result_iterator._data)

result = eddy.eddy_envelope_query(
    track_id=4
)

min_date = result["min_date"][0]
max_date = result["max_date"][0]
basin_ids = result["basin_ids"]

print(min_date)
print(max_date)
print(basin_ids)