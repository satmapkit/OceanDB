from OceanDB.OceanDB_ETL import OceanDBETL
import pandas as pd

oceandbETL = OceanDBETL()

filepath = "/app/data/eddy/META3.2_DT_allsat_Anticyclonic_long_19930101_20220209.nc"
filepath = "/Users/mddarr/delat/azath/ocean/OceanDB/data/eddy/META3.2_DT_allsat_Anticyclonic_long_19930101_20220209.nc"
eddy=oceandbETL.extract_eddy_data(file=filepath)


df = oceandbETL.import_eddy_data_to_postgresql(eddy,1)

df.to_sql(
    name="eddy",
    con=self.get_engine(),
    schema="public",
    if_exists="append",
    index=False,
    chunksize=1000
)

# df = pd.DataFrame({
#     "amplitude": eddy.amplitude.astype("float"),  # pandas handles downcast
#     "cost_association": eddy.cost_association,
#     "effective_area": eddy.effective_area,
#     "effective_contour_height": eddy.effective_contour_height,
#
#     # 2D arrays → choose representative element (vertex 0)
#     "effective_contour_latitude": eddy.effective_contour_latitude[:, 0],
#     "effective_contour_longitude": eddy.effective_contour_longitude[:, 0],
#
#     "effective_contour_shape_error": eddy.effective_contour_shape_error,
#     "effective_radius": eddy.effective_radius,
#     "inner_contour_height": eddy.inner_contour_height,
#
#     "latitude": eddy.latitude,
#     "latitude_max": eddy.latitude_max,
#     "longitude": eddy.longitude,
#     "longitude_max": eddy.longitude_max,
#
#     "num_contours": eddy.num_contours,
#     "num_point_e": eddy.num_point_e,
#     "num_point_s": eddy.num_point_s,
#
#     "observation_flag": eddy.observation_flag.astype(bool),
#     "observation_number": eddy.observation_number,
#
#     "speed_area": eddy.speed_area,
#     "speed_average": eddy.speed_average,
#     "speed_contour_height": eddy.speed_contour_height,
#
#     # also 2D arrays
#     # "speed_contour_latitude": eddy.speed_contour_latitude[:, 0],
#     # "speed_contour_longitude": eddy.speed_contour_longitude[:, 0],
#
#     "speed_contour_shape_error": eddy.speed_contour_shape_error,
#     "speed_radius": eddy.speed_radius,
#
#     "date_time": eddy.date_time,
#     "track": eddy.track,
#
#     "cyclonic_type": 1,
#
#     # leave NULL → PostgreSQL computes eddy_point automatically
#     # "speed_contour_shape": None,
# })




