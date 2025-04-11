import time
import matplotlib.pyplot as plt
from CheltonEddy import CheltonEddy

chelton_eddy_id = -41
chelton_eddy_id = 527413
# chelton_eddy_id = 700000

if chelton_eddy_id > 0:
    filename = f"chelton_eddy_+{abs(chelton_eddy_id)}.nc"
else:
    filename = f"chelton_eddy_-{abs(chelton_eddy_id)}.nc"

eddy_db = CheltonEddy(db_name='ocean')

# start = time.time()
# eddy_db.along_track_points_near_eddy_old(chelton_eddy_id)
# end = time.time()
# print(f"Finished. Total time: {end - start}")

start = time.time()
eddy_db.along_track_points_near_chelton_eddy(chelton_eddy_id)
end = time.time()
print(eddy_db.along_track_points_near_chelton_eddy(chelton_eddy_id))
print(f"Finished. Total time: {end - start}")

[eddy, eddy_encoding] = eddy_db.chelton_eddy_with_id_as_xarray(chelton_eddy_id)
[along_track, along_encoding] = eddy_db.along_track_points_near_chelton_eddy_as_xarray(chelton_eddy_id)



# Helpful discussion here
#https://github.com/pydata/xarray/issues/3739
eddy.to_netcdf(filename, "w", group="eddy", encoding=eddy_encoding, format="NETCDF4")
along_track.to_netcdf(filename, "a", group="alongtrack", encoding=along_encoding, format="NETCDF4")
print(f"Export complete: {filename}")
# plt.figure()
# plt.scatter(eddy["longitude"], eddy["latitude"], c=eddy["amplitude"])
# plt.scatter(along_track["longitude"], along_track["latitude"], c=along_track["sla_filtered"])
# plt.show()

# eddy_data_xarray.to_netcdf('output.nc')

# tokenized_query = eddy_db.sql_query_with_name("eddy_with_id.sql")
# values = {"track": track,
#           "cyclonic_type": cyclonic_type
#           }
#
# with pg.connect(eddy_db.connect_string()) as connection:
#     with connection.cursor() as cursor:
#         cursor.execute(tokenized_query, values)
#         data = cursor.fetchall()
#
# with pg.connect(eddy_db.connect_string()) as connection:
#     cur = pg.ClientCursor(connection)
#     query = cur.mogrify(tokenized_query, values)
#
# sync_engine = create_engine("postgresql+psycopg://"+eddy_db.username+":"+eddy_db.password+"@"+eddy_db.host+"/"+eddy_db.db_name)
# ds = xr.Dataset.from_dataframe(pd.read_sql(query,sync_engine))
# ds.to_netcdf('output.nc')

