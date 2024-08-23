from OceanDB.Eddy import Eddy
import matplotlib.pyplot as plt
import numpy as np
import psycopg as pg
from sqlalchemy import create_engine
from psycopg import sql
import os.path
import pandas as pd
import xarray as xr

track = 41
cyclonic_type = -1

# track = 527413
# cyclonic_type = 1

if cyclonic_type == 1:
    filename = f"eddy_+{track}.nc"
else:
    filename = f"eddy_-{track}.nc"

eddy_db = Eddy(db_name='ocean')

[eddy, eddy_encoding] = eddy_db.eddy_with_id_as_xarray(track, cyclonic_type)
[along_track, along_encoding] = eddy_db.along_track_points_near_eddy_as_xarray(track, cyclonic_type)

eddy.to_netcdf(filename, "w", group="eddy", encoding=eddy_encoding, format="NETCDF4")
along_track.to_netcdf(filename, "a", group="alongtrack", encoding=along_encoding, format="NETCDF4")

plt.figure()
plt.scatter(eddy["longitude"], eddy["latitude"], c=eddy["amplitude"])
plt.scatter(along_track["longitude"], along_track["latitude"], c=along_track["sla_filtered"])
plt.show()

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

