from OceanDB.index_experiment import run_index_performance_test,  Index
from OceanDB.OceanDB_Initializer import OceanDBInit
from OceanDB.data_access.along_track import AlongTrack
from OceanDB.query_analysis import QueryScenario, BaseQueryScenario
from OceanDB.schemas.along_track_schema import along_track_schema
from OceanDB.query_analysis import QueryAnalysisRunner

from datetime import datetime, timedelta
from itertools import permutations
import time
import numpy as np
import matplotlib.pyplot as plt
import pickle

filename = "times_by_lat_lon_rad_limit.pickle"
# filename = "times_by_lat_lon.pickle"


big_resolution = 10
small_resolution = 2
lats = np.arange(-80,80-big_resolution,big_resolution)
lons = np.arange(-180,180-big_resolution,big_resolution)
# lats = [70, -30]
# lons = [-147, -146, -145 -21]
lons_grid,lats_grid = np.meshgrid(lons,lats)
dates = [datetime(2013,7,26) for _ in range(lons_grid.size)]
orig_shape = lons_grid.shape
lons_grid = lons_grid.reshape(-1)
lats_grid = lats_grid.reshape(-1)
alongTrack = AlongTrack()
alongTrack.start_debug(lambda x,y,z: print(z))


# funcs = [
#     (alongTrack.geographic_nearest_neighbors_batch, {}, "NN old"),
#     (alongTrack.geographic_point_in_r_dt_batch, {}, "r dt"),
#         ]
funcs = [
    (alongTrack.geographic_nearest_neighbors_batch, {}, "NN (max rad 500,000)"),
    (alongTrack.geographic_nearest_neighbors_batch, {"max_radius":None}, "NN (no max rad)"),
    (alongTrack.geographic_point_in_r_dt_batch, {}, "r dt"),
        ]
times_by_method = [np.zeros(lons_grid.shape) for _ in range(len(funcs))]
n_iterations = 1

# for _ in range(n_iterations):
#     for point_i,(lat,lon,date) in enumerate(zip(lats_grid,lons_grid,dates)):
#         lat_lim = (lat, lat+big_resolution)
#         lon_lim = (lon, lon+big_resolution)
#         lon_dim = np.arange(lon_lim[0], lon_lim[1], small_resolution) + small_resolution / 2
#         lat_dim = np.arange(lat_lim[0], lat_lim[1], small_resolution) + small_resolution / 2
#         grid = np.meshgrid(lon_dim, lat_dim)
#         lons = grid[0].flatten()
#         lats = grid[1].flatten()
#         this_dates = [date for _ in range(lons.size)]
#
#         for method_i,(func, kwargs,name)  in enumerate(funcs):
#             print(f"sampling {name} at ({lat},{lon},{date})", end=" ")
#             t1 = time.time()
#             out = list(func(["distance", "latitude", "longitude"], lats, lons, this_dates, **kwargs))
#             t2 = time.time() - t1
#             times_by_method[method_i][point_i] += t2
#             print("done. Took", t2)
# times_by_method = [x / n_iterations for x in times_by_method]
# with open(filename, "wb") as file:
#     pickle.dump((times_by_method, [(str(x),y,z) for x,y,z in funcs]), file)
with open(filename, "rb") as file:
    times_by_method, funcs = pickle.load(file)

# plot time it took for each point
lons_grid = lons_grid.reshape(orig_shape)
lats_grid = lats_grid.reshape(orig_shape)
fig, axs = plt.subplots(len(times_by_method), 1, figsize=(10, 8))
for ax,ts,label in zip(axs, times_by_method, funcs):
    ts = ts.reshape(orig_shape)
    co = ax.pcolormesh(lons_grid, lats_grid, ts, vmin=0, vmax=1.26)
    fig.colorbar(co, ax=ax)
    ax.set_ylabel("Latitude")
    ax.set_title(f"{label[2]} (s)")
if len(axs) > 0:
    axs[-1].set_xlabel("Longitude")
plt.tight_layout()
plt.show()



# scenarios : list[BaseQueryScenario]= [
#     QueryScenario(
#         query_class=AlongTrack,
#         method_name="geographic_nearest_neighbors",
#         kwargs={
#             "fields": list(along_track_schema.keys()),
#             "latitude": lat,
#             "longitude": lon,
#             "date": date,
#             "time_window": timedelta(days=10),
#             "missions": ["s6a", "j3n"]
#         },
#     )
#     for lat,lon,date in zip(lats_grid, lons_grid, dates)
# ]
# runner = QueryAnalysisRunner(scenarios=scenarios)
# out = runner.analyze_queries()
# for i,result in enumerate(out):
#     print(f'============= Scenario {i} ==============')
#     print(result.explain_result_str)
#     print(result.used_indices)
#
# def make_indexes(indexes):
#     return [Index(
#                     name=f"along_track_{'_'.join(fields)}",
#                     table="along_track",
#                     fields=fields,
#                     spec=along_track_index,
#                 )
#             for fields in indexes]

#
# scenarios : list[BaseQueryScenario]= [
#     BatchQueryScenario(
#         query_class=AlongTrack,
#         method_name="geographic_nearest_neighbors_batch",
#         kwargs={
#             "fields": list(along_track_schema.keys()),
#             "latitudes": [-35],
#             "longitudes": [120],
#             "dates": [datetime(2022, 10, 15)],
#             "time_window": timedelta(days=10),
#             "missions": ["s6a", "j3n"]
#         },
#     )
#
#         ]
#
#
#
oceanDBInit = OceanDBInit()
# out = []
#
# # indexes_to_test = ([x] for x in permutations(["along_track_point", "date_time", "mission","basin_id"]))
# indexes_to_test = [
#     [["along_track_point"], ["date_time"], ["mission"],["basin_id"]],
#     [["along_track_point"], ["date_time"], ["mission", "basin_id"]],
#     [["along_track_point"], ["date_time"], ["mission"],["basin_id"], ["along_track_point", "date_time"], ["mission", "basin_id"]],
#     [["along_track_point"], ["date_time"], ["mission"], ["basin_id"], ["along_track_point", "date_time", "mission","basin_id"]],
# ]
# for indexes in indexes_to_test:
#     print("running performance test for fields", indexes)
#     t1 = time.time()
#     result = run_index_performance_test(make_indexes(indexes), oceanDBInit, scenarios)
#     a = result[0]
#     print("done in", time.time() - t1, "query took", result[0].total_time, "sql took", result[0].single_query_sql_time, "index used were", result[0].used_indices)
#     out.append((indexes, result))
