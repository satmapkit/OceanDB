import netCDF4 as nc
import psycopg as pg
from psycopg import sql
import glob
import time
from datetime import timedelta
import os
import yaml
import numpy as np


class Eddy:
    db_name: str
    eddy_table_name: str = 'eddy'
    eddy_metadata_table_name: str = 'eddy_metadata'
    eddies_file_path: str

    ######################################################
    #
    # Initialization
    #
    ######################################################

    def __init__(self, host="", username="", password="", port=5432, db_name='ocean', nc_files_path=""):
        with open(os.path.join(os.path.dirname(__file__), '../config.yaml'), 'r') as param_file:
            along_params = yaml.full_load(param_file)
            if 'db_connect' in along_params:
                if 'host' in along_params['db_connect']:
                    self.host = along_params['db_connect']['host']
                if 'username' in along_params['db_connect']:
                    self.username = along_params['db_connect']['username']
                if 'password' in along_params['db_connect']:
                    self.password = along_params['db_connect']['password']
                if 'port' in along_params['db_connect']:
                    self.port = along_params['db_connect']['port']
                if 'db_name' in along_params['db_connect']:
                    self.db_name = along_params['db_connect']['db_name']
            if 'copernicus_marine' in along_params:
                if 'eddies_file_path' in along_params['copernicus_marine']:
                    self.eddies_file_path = along_params['copernicus_marine']['eddies_file_path']

        # locally defined variable override the settings in the config file
        if host:
            self.host = host
        if username:
            self.username = username
        if password:
            self.password = password
        if port:
            self.port = port
        if db_name:
            self.db_name = db_name
        try:
            eddies_file_path
        except Exception:
            pass
        else:
            self.eddies_file_path = eddies_file_path

    ######################################################
    #
    # Database creation/destruction
    #
    ######################################################

    def create_database(self):
        with pg.connect(f'host={self.host} port={self.port} user={self.username} password={self.password}') as pg_conn:
            pg_conn.autocommit = True  # Enable autocommit to execute CREATE DATABASE command

            # SQL module usage: https://www.psycopg.org/docs/sql.html#module-psycopg2.sql
            with pg_conn.cursor() as cur:
                cur.execute(sql.SQL("SELECT 1 FROM pg_database WHERE datname = %s"), (self.db_name,))
                exists = cur.fetchone()
                if exists is not None:
                    print(f"Database '{self.db_name}' already exists.")
                else:
                    # Create the new database
                    cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(self.db_name)))

                    # Now create a connection to the new db, and add postgis (autocommit set to true!)
                    with pg.connect(self.connect_string()) as atdb_conn:
                        with atdb_conn.cursor() as atdb_cur:
                            atdb_cur.execute(sql.SQL("CREATE EXTENSION IF NOT EXISTS plpgsql;"))
                            atdb_cur.execute(sql.SQL("CREATE EXTENSION IF NOT EXISTS postgis;"))
                            atdb_cur.execute(sql.SQL("CREATE EXTENSION IF NOT EXISTS btree_gist;"))
                            atdb_conn.commit()
                    print(f"Database '{self.db_name}' created successfully.")

    def drop_database(self):
        with pg.connect(f'host={self.host} port={self.port} user={self.username} password={self.password}') as conn:
            conn.autocommit = True  # Enable autocommit to execute CREATE DATABASE command
            with conn.cursor() as cur:
                cur.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(self.db_name)))

        print(f"Database '{self.db_name}' dropped.")

    ######################################################
    #
    # Generic database actions
    #
    ######################################################

    def connect_string(self):
        conn_str = f'''
            host={self.host} 
            dbname={self.db_name}
            port={self.port} 
            user={self.username} 
            password={self.password}'''
        return conn_str

    def drop_table(self, name):
        query_drop_table = sql.SQL("""DROP TABLE IF EXISTS public.{table_name}""").format(
            table_name=sql.Identifier(name)
        )

        with pg.connect(self.connect_string()) as conn:
            with conn.cursor() as cur:
                cur.execute(query_drop_table)
                conn.commit()

        print(f"Table '{name} dropped from database.'{self.db_name}'.")

    def truncate_table(self, name):
        query_truncate_table = sql.SQL("""TRUNCATE public.{table_name}""").format(
            table_name=sql.Identifier(name)
        )

        with pg.connect(self.connect_string()) as conn:
            with conn.cursor() as cur:
                cur.execute(query_truncate_table)
                conn.commit()

        print(f"All data removed from table '{name} in database.'{self.db_name}'.")

    def sql_query_with_name(self, name):
        sql_folder_path = os.path.join(os.path.dirname(__file__), '../../sql/')
        with open(os.path.join(sql_folder_path, name), 'r') as file:
            tokenized_query = file.read()
        return tokenized_query

    def data_file_path_with_name(self, name):
        sql_folder_path = os.path.join(os.path.dirname(__file__), '../../data/')
        path = os.path.join(sql_folder_path, name)
        return path

    ######################################################
    #
    # eddy table creation/destruction
    #
    ######################################################

    def create_eddy_table(self):
        tokenized_query = self.sql_query_with_name('create_eddy_table.sql')
        query = sql.SQL(tokenized_query).format(table_name=sql.Identifier(self.eddy_table_name))

        with pg.connect(self.connect_string()) as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                conn.commit()
        print(f"Table '{self.eddy_table_name} added to database (if it did not previously exist) '{self.db_name}'.")

    def drop_eddy_table(self):
        self.drop_table(self.eddy_table_name)

    def truncate_eddy_table(self):
        self.truncate_table(self.eddy_table_name)

    # def create_eddy_indices(self):
        # tokenized_query = self.sql_query_with_name('create_eddy_index_point.sql')
        # query_create_point_index = sql.SQL(tokenized_query).format(table_name=sql.Identifier(self.eddy_table_name))



        # with pg.connect(self.connect_string()) as conn:
        #     with conn.cursor() as cur:
        #         cur.execute(query_create_point_index)
        #         cur.execute(query_create_point_geom_index)
        #         cur.execute(query_create_date_index)
        #         cur.execute(query_create_filename_index)
        #         cur.execute(create_eddy_index_time)
        #         cur.execute(create_eddy_index_mission)
        #         conn.commit()

    # def drop_eddy_indices(self):
        # tokenized_query = self.sql_query_with_name('drop_eddy_index_point.sql')
        # query_drop_point_index = sql.SQL(tokenized_query).format(table_name=sql.Identifier(self.eddy_table_name))


        # with pg.connect(self.connect_string()) as conn:
        #     with conn.cursor() as cur:
        #         cur.execute(query_drop_point_index)
        #         cur.execute(query_drop_point_geom_index)
        #         cur.execute(query_drop_date_index)
        #         cur.execute(query_drop_filename_index)
        #         cur.execute(drop_eddy_index_time)
        #         cur.execute(drop_eddy_index_mission)
        #         conn.commit()

    def extract_data_tuple_from_netcdf(self, file_path, fname):
        # Open the NetCDF file
        try:
            ds = nc.Dataset(file_path, 'r')
            ds.set_auto_mask(False)

            eddy_data = []
            date_time = ds.variables['time']  # Extract dates from the dataset and convert them to standard datetime

            time_data = nc.num2date(date_time[:], date_time.units, only_use_cftime_datetimes=False,
                                    only_use_python_datetimes=False)
            eddy_data = {
                'amplitude': ds.variables['amplitude'][:],
                'cost_association': ds.variables['cost_association'][:],
                'effective_area': ds.variables['effective_area'][:],
                'effective_contour_height': ds.variables['effective_contour_height'][:],
                'effective_contour_latitude': ds.variables['effective_contour_latitude'][:],
                'effective_contour_longitude': ds.variables['effective_contour_longitude'][:],
                'effective_contour_shape_error': ds.variables['effective_contour_shape_error'][:],
                'effective_radius': ds.variables['effective_radius'][:],
                'inner_contour_height': ds.variables['inner_contour_height'][:],
                'latitude': ds.variables['latitude'][:],
                'latitude_max': ds.variables['latitude_max'][:],
                'longitude': ds.variables['longitude'][:],
                'longitude_max': ds.variables['longitude_max'][:],
                'num_contours': ds.variables['num_contours'][:],
                'num_point_e': ds.variables['num_point_e'][:],
                'num_point_s': ds.variables['num_point_s'][:],
                'observation_flag': ds.variables['observation_flag'][:],
                'observation_number': ds.variables['observation_number'][:],
                'speed_area': ds.variables['speed_area'][:],
                'speed_average': ds.variables['speed_average'][:],
                'speed_contour_height': ds.variables['speed_contour_height'][:],
                'speed_contour_latitude': ds.variables['speed_contour_latitude'][:],
                'speed_contour_longitude': ds.variables['speed_contour_longitude'][:],
                'speed_contour_shape_error': ds.variables['speed_contour_shape_error'][:],
                'speed_radius': ds.variables['speed_radius'][:],
                'date_time': time_data,
                'track': ds.variables['track'][:],
            }
            ds.close()

            # eddy_data['longitude'][:] = (data['longitude'][:] + 180) % 360 - 180
            return eddy_data
        except FileNotFoundError:
            print("File '{}' not found".format(file_path))

    def import_data_tuple_to_postgresql(self, data, filename, cyclonic_type):
        copy_query = sql.SQL(
            """COPY {eddy_tbl_nme} ( 
                amplitude, 
                cost_association, 
                effective_area, 
                effective_contour_height, 
                effective_contour_shape_error, 
                effective_radius, 
                inner_contour_height, 
                latitude, 
                latitude_max, 
                longitude, 
                longitude_max, 
                num_contours, 
                num_point_e, 
                num_point_s, 
                observation_flag, 
                observation_number, 
                speed_area, 
                speed_average, 
                speed_contour_height, 
                speed_contour_shape_error, 
                speed_radius, 
                date_time, 
                track, 
                cyclonic_type) FROM STDIN""").format(eddy_tbl_nme=sql.Identifier(self.eddy_table_name))

        with pg.connect(self.connect_string()) as connection:
            with connection.cursor() as cursor:
                with cursor.copy(copy_query) as copy:
                    for i in range(len(data['observation_number'])):
                        copy.write_row([
                            data['amplitude'][i],
                            data['cost_association'][i],
                            data['effective_area'][i],
                            data['effective_contour_height'][i],
                            # data['effective_contour_latitude'][i],
                            # data['effective_contour_longitude'][i],
                            data['effective_contour_shape_error'][i],
                            data['effective_radius'][i],
                            data['inner_contour_height'][i],
                            data['latitude'][i],
                            data['latitude_max'][i],
                            data['longitude'][i],
                            data['longitude_max'][i],
                            data['num_contours'][i],
                            data['num_point_e'][i],
                            data['num_point_s'][i],
                            data['observation_flag'][i],
                            data['observation_number'][i],
                            data['speed_area'][i],
                            data['speed_average'][i],
                            data['speed_contour_height'][i],
                            # data['speed_contour_shape'][i]
                            data['speed_contour_shape_error'][i],
                            data['speed_radius'][i],
                            data['date_time'][i],
                            data['track'][i],
                            cyclonic_type,
                        ])

    # Jeffrey's version... specify specific mission directory.
    # def insert_eddy_data_from_netcdf_with_tuples(self, directory):
    #     start = time.time()
    #     for file_path in glob.glob(directory + '/*.nc'):
    #         names = [os.path.basename(x) for x in glob.glob(file_path)]
    #         filename = names[0]  # filename will be used to link data to metadata
    #         data = self.extract_data_tuple_from_netcdf(file_path, filename)
    #         import_start = time.time()
    #         self.import_data_tuple_to_postgresql(data, filename)
    #         import_end = time.time()
    #         print(f"{filename} import time: {import_end - import_start}")
    #     end = time.time()
    #     print(f"Script end. Total time: {end - start}")

    # Cim's version... specify missions get directory from config.yaml
    def insert_eddy_data_from_netcdf_with_tuples(self):
        start = time.time()
        directory = self.eddies_file_path

        for file_path in glob.glob(directory + '/*.nc'):
            filenames = [os.path.basename(x) for x in glob.glob(file_path)]
            cyclonic_type = 1
            print(filenames[0])
            if filenames[0] == 'META3.2_DT_allsat_Anticyclonic_long_19930101_20220209.nc' or filenames[0] == 'META3.2_DT_allsat_Cyclonic_long_19930101_20220209.nc':
                data = self.extract_data_tuple_from_netcdf(file_path, filenames[0])
                if filenames[0] == 'META3.2_DT_allsat_Cyclonic_long_19930101_20220209.nc':
                    cyclonic_type = -1
                import_start = time.time()
                self.import_data_tuple_to_postgresql(data, filenames[0], cyclonic_type)
                del data
                import_end = time.time()
                print(f"{filenames[0]} import time: {import_end - import_start}")
        end = time.time()
        print(f"Script end. Total time: {end - start}")

    ######################################################
    #
    # simple queries
    #
    ######################################################

    def geographic_points_in_spatialtemporal_window(self, latitude, longitude, date, distance=500000, time_window=timedelta(seconds=856710), should_basin_mask=1):
        tokenized_query = self.sql_query_with_name('geographic_points_in_spatialtemporal_window.sql')
        values = {"longitude": longitude,
                  "latitude": latitude,
                  "central_date_time": date,
                  "distance": distance,
                  "time_delta": time_window/2}

        with pg.connect(self.connect_string()) as connection:
            with connection.cursor() as cursor:
                cursor.execute(tokenized_query,values)
                data = cursor.fetchall()

        return data

    def projected_points_in_spatialtemporal_window(self, latitude: float, longitude: float, date, Lx: float = 500000., Ly: float = 500000., time_window=timedelta(seconds=856710), should_basin_mask=1):
        if should_basin_mask == 1:
            tokenized_query = self.sql_query_with_name("geographic_points_in_spatialtemporal_projected_window.sql")
        else:
            tokenized_query = self.sql_query_with_name("geographic_points_in_spatialtemporal_projected_window_nomask.sql")

        [x0, y0, minLat, minLon, maxLat, maxLon] = AlongTrack.latitude_longitude_bounds_for_transverse_mercator_box(latitude, longitude, 2*Lx, 2*Ly)

        values = {"longitude": longitude,
                  "latitude": latitude,
                  "xmin": minLon,
                  "ymin": minLat,
                  "xmax": maxLon,
                  "ymax": maxLat,
                  "central_date_time": date,
                  "time_delta": time_window / 2}

        with pg.connect(self.connect_string()) as connection:
            with connection.cursor() as cursor:
                cursor.execute(tokenized_query, values)
                data = cursor.fetchall()

        lon = np.array([data_i[0] for data_i in data])
        lat = np.array([data_i[1] for data_i in data])
        sla = np.array([data_i[2] for data_i in data])
        t = np.array([data_i[3] for data_i in data])

        [x, y] = AlongTrack.latitude_longitude_to_spherical_transverse_mercator(lat, lon, longitude)
        out_of_bounds = (x < x0 - Lx) | (x > x0 + Lx) | (y < y0 - Ly) | (y > y0 + Ly)
        x = x[~out_of_bounds]
        y = y[~out_of_bounds]
        sla = sla[~out_of_bounds]
        t = t[~out_of_bounds]

        x = x - x0
        y = y - y0

        return x, y, sla, t

    @staticmethod
    def latitude_longitude_bounds_for_transverse_mercator_box(lat0: float, lon0: float, Lx: float, Ly: float):
        [x0, y0] = AlongTrack.latitude_longitude_to_spherical_transverse_mercator(lat0, lon0, lon0=lon0)
        x = np.zeros(6)
        y = np.zeros(6)

        x[1] = x0 - Lx / 2
        y[1] = y0 - Ly / 2

        x[2] = x0 - Lx / 2
        y[2] = y0 + Ly / 2

        x[3] = x0
        y[3] = y0 + Ly / 2

        x[4] = x0
        y[4] = y0 - Ly / 2

        x[5] = x0 + Lx / 2
        y[5] = y0 - Ly / 2

        x[0] = x0 + Lx / 2
        y[0] = y0 + Ly / 2

        [lats, lons] = AlongTrack.spherical_transverse_mercator_to_latitude_longitude(x, y, lon0)
        minLat = min(lats)
        maxLat = max(lats)
        minLon = min(lons)
        maxLon = max(lons)

        return x0, y0, minLat, minLon, maxLat, maxLon

    @staticmethod
    def latitude_longitude_to_spherical_transverse_mercator(lat: float, lon: float, lon0: float):
        k0 = 0.9996
        WGS84a = 6378137.
        R = k0 * WGS84a

        phi = np.array(lat) * np.pi / 180
        deltaLambda = (np.array(lon) - np.array(lon0)) * np.pi / 180
        sinLambdaCosPhi = np.sin(deltaLambda) * np.cos(phi)
        x = (R / 2) * np.log((1 + sinLambdaCosPhi) / (1 - sinLambdaCosPhi))
        y = R * np.arctan(np.tan(phi) / np.cos(deltaLambda))

        return x, y

    @staticmethod
    def spherical_transverse_mercator_to_latitude_longitude(x: float, y: float, lon0: float):
        k0 = 0.9996
        WGS84a = 6378137
        R = k0 * WGS84a

        lon = (180 / np.pi) * np.arctan(np.sinh(x / R) / np.cos(y / R)) + lon0
        lat = (180 / np.pi) * np.arcsin(np.sin(y / R) / np.cosh(x / R))

        return lat, lon