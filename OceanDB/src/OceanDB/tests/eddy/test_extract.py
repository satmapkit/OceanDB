import netCDF4 as nc
import psycopg as pg
from psycopg import sql

from OceanDB.OceanDB import OceanDB
from OceanDB.config import Config

config = Config()


cyclonic_filepath = f"{config.eddy_data_directory}/META3.2_DT_allsat_Cyclonic_long_19930101_20220209.nc"
anticyclonic_filepath = f"{config.eddy_data_directory}/META3.2_DT_allsat_Antiyclonic_long_19930101_20220209.nc"

class Eddy(OceanDB):

    def import_data_tuple_to_postgresql(self, data, filename, cyclonic_type):
        COLUMNS = [
            "amplitude",
            "cost_association",
            "effective_area",
            "effective_contour_height",
            "effective_contour_shape_error",
            "effective_radius",
            "inner_contour_height",
            "latitude",
            "latitude_max",
            "longitude",
            "longitude_max",
            "num_contours",
            "num_point_e",
            "num_point_s",
            "observation_flag",
            "observation_number",
            "speed_area",
            "speed_average",
            "speed_contour_height",
            "speed_contour_shape_error",
            "speed_radius",
            "date_time",
            "track",
            "cyclonic_type",
        ]

        copy_query = sql.SQL("COPY {} ({}) FROM STDIN").format(
            sql.Identifier("public", "eddy"),
            sql.SQL(", ").join(map(sql.Identifier, COLUMNS)),
        )

        def py(val):
            # convert numpy → python
            return val.item() if hasattr(val, "item") else val

        try:
            with pg.connect(self.config.postgres_dsn) as connection:
                with connection.cursor() as cursor:
                    with cursor.copy(copy_query) as copy:
                        for i in range(len(data["observation_number"])):
                            copy.write_row([
                                py(data["amplitude"][i]),
                                py(data["cost_association"][i]),
                                py(data["effective_area"][i]),
                                py(data["effective_contour_height"][i]),
                                py(data["effective_contour_shape_error"][i]),
                                py(data["effective_radius"][i]),
                                py(data["inner_contour_height"][i]),
                                py(data["latitude"][i]),
                                py(data["latitude_max"][i]),
                                py(data["longitude"][i]),
                                py(data["longitude_max"][i]),
                                py(data["num_contours"][i]),
                                py(data["num_point_e"][i]),
                                py(data["num_point_s"][i]),
                                py(data["observation_flag"][i]),
                                py(data["observation_number"][i]),
                                py(data["speed_area"][i]),
                                py(data["speed_average"][i]),
                                py(data["speed_contour_height"][i]),
                                py(data["speed_contour_shape_error"][i]),
                                py(data["speed_radius"][i]),
                                py(data["date_time"][i]),
                                py(data["track"][i]),
                                cyclonic_type,
                            ])
        except Exception as e:
            print("COPY FAILED:", e)
            raise

    # def import_data_tuple_to_postgresql(self, data, filename, cyclonic_type):
    #     COLUMNS = [
    #         "amplitude",
    #         "cost_association",
    #         "effective_area",
    #         "effective_contour_height",
    #         "effective_contour_shape_error",
    #         "effective_radius",
    #         "inner_contour_height",
    #         "latitude",
    #         "latitude_max",
    #         "longitude",
    #         "longitude_max",
    #         "num_contours",
    #         "num_point_e",
    #         "num_point_s",
    #         "observation_flag",
    #         "observation_number",
    #         "speed_area",
    #         "speed_average",
    #         "speed_contour_height",
    #         "speed_contour_shape_error",
    #         "speed_radius",
    #         "date_time",
    #         "track",
    #         "cyclonic_type",
    #     ]
    #
    #     copy_query = sql.SQL("COPY {} ({}) FROM STDIN").format(
    #         sql.Identifier("public", "eddy"),  # explicit default schema
    #         sql.SQL(", ").join(map(sql.Identifier, COLUMNS)),
    #     )
    #
    #     with pg.connect(self.config.postgres_dsn) as connection:
    #         with connection.cursor() as cursor:
    #             with cursor.copy(copy_query) as copy:
    #                 for i in range(len(data['observation_number'])):
    #
    #
    #                     try:
    #                         with cursor.copy(copy_query) as copy:
    #                             for i in range(len(data["observation_number"])):
    #                                 print(f"inserting {i}")
    #                                 copy.write_row([...])
    #                     except Exception as e:
    #                         print("COPY FAILED:", e)
    #                         raise



                        # print(f"inserting {i}")
                        # copy.write_row([
                        #     data['amplitude'][i],
                        #     data['cost_association'][i],
                        #     data['effective_area'][i],
                        #     data['effective_contour_height'][i],
                        #     # data['effective_contour_latitude'][i],
                        #     # data['effective_contour_longitude'][i],
                        #     data['effective_contour_shape_error'][i],
                        #     data['effective_radius'][i],
                        #     data['inner_contour_height'][i],
                        #     data['latitude'][i],
                        #     data['latitude_max'][i],
                        #     data['longitude'][i],
                        #     data['longitude_max'][i],
                        #     data['num_contours'][i],
                        #     data['num_point_e'][i],
                        #     data['num_point_s'][i],
                        #     data['observation_flag'][i],
                        #     data['observation_number'][i],
                        #     data['speed_area'][i],
                        #     data['speed_average'][i],
                        #     data['speed_contour_height'][i],
                        #     # data['speed_contour_shape'][i]
                        #     data['speed_contour_shape_error'][i],
                        #     data['speed_radius'][i],
                        #     data['date_time'][i],
                        #     data['track'][i],
                        #     cyclonic_type,
                        # ])


    def extract_data_tuple_from_netcdf(self, file_path, num_points=10):
        # Open the NetCDF file
        try:
            ds = nc.Dataset(file_path, 'r')
            ds.set_auto_mask(False)
            eddy_data = []
            date_time = ds.variables['time']  # Extract dates from the dataset and convert them to standard datetime

            n_total = ds.variables["observation_number"].shape[0]

            if num_points is None:
                sl = slice(None)  # equivalent to [:]
            else:
                sl = slice(0, num_points)

            time_data = nc.num2date(date_time[sl], date_time.units, only_use_cftime_datetimes=False,
                                    only_use_python_datetimes=False)
            ds.set_auto_maskandscale(False)

            eddy_data = {
                'amplitude': ds.variables['amplitude'][sl],
                'cost_association': ds.variables['cost_association'][sl],
                'effective_area': ds.variables['effective_area'][sl],
                'effective_contour_height': ds.variables['effective_contour_height'][sl],
                'effective_contour_latitude': ds.variables['effective_contour_latitude'][sl],
                'effective_contour_longitude': ds.variables['effective_contour_longitude'][sl],
                'effective_contour_shape_error': ds.variables['effective_contour_shape_error'][sl],
                'effective_radius': ds.variables['effective_radius'][sl],
                'inner_contour_height': ds.variables['inner_contour_height'][sl],
                'latitude': ds.variables['latitude'][sl],
                'latitude_max': ds.variables['latitude_max'][sl],
                'longitude': ds.variables['longitude'][sl],
                'longitude_max': ds.variables['longitude_max'][sl],
                'num_contours': ds.variables['num_contours'][sl],
                'num_point_e': ds.variables['num_point_e'][sl],
                'num_point_s': ds.variables['num_point_s'][sl],
                'observation_flag': ds.variables['observation_flag'][sl],
                'observation_number': ds.variables['observation_number'][sl],
                'speed_area': ds.variables['speed_area'][sl],
                'speed_average': ds.variables['speed_average'][sl],
                'speed_contour_height': ds.variables['speed_contour_height'][sl],
                'speed_contour_latitude': ds.variables['speed_contour_latitude'][sl],
                'speed_contour_longitude': ds.variables['speed_contour_longitude'][sl],
                'speed_contour_shape_error': ds.variables['speed_contour_shape_error'][sl],
                'speed_radius': ds.variables['speed_radius'][sl],
                'date_time': time_data[sl],
                'track': ds.variables['track'][sl],
            }
            ds.close()

            # eddy_data['longitude'][:] = (data['longitude'][:] + 180) % 360 - 180
            return eddy_data
        except FileNotFoundError:
            print("File '{}' not found".format(file_path))


eddy = Eddy()
output = eddy.extract_data_tuple_from_netcdf(cyclonic_filepath)
print("Inserting")
eddy.import_data_tuple_to_postgresql(data=output, filename="a", cyclonic_type=1)

