import netCDF4 as nc
import psycopg as pg
from psycopg import sql
import glob
import time
import os
import yaml
from OceanDB import OceanDB


class Eddy(OceanDB):
    eddy_table_name: str = 'eddy'
    eddy_metadata_table_name: str = 'eddy_metadata'
    eddies_file_path: str

    ######################################################
    #
    # Initialization
    #
    ######################################################

    def __init__(self, host="", username="", password="", port=5432, db_name='ocean', eddies_file_path=""):
        super().__init__(host=host, username=username, password=password, port=port, db_name=db_name)
        with open(os.path.join(os.path.dirname(__file__), '../config.yaml'), 'r') as param_file:
            along_params = yaml.full_load(param_file)
            if 'copernicus_marine' in along_params:
                if 'eddies_file_path' in along_params['copernicus_marine']:
                    self.eddies_file_path = along_params['copernicus_marine']['eddies_file_path']

        # locally defined variable override the settings in the config file
        if eddies_file_path:
            self.eddies_file_path = eddies_file_path

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

    def create_eddy_indices(self):
        tokenized_query = self.sql_query_with_name('create_eddy_index_point.sql')
        query_create_point_index = sql.SQL(tokenized_query).format(table_name=sql.Identifier(self.eddy_table_name))
        with pg.connect(self.connect_string()) as conn:
            with conn.cursor() as cur:
                cur.execute(query_create_point_index)
                conn.commit()

    def drop_eddy_indices(self):
        tokenized_query = self.sql_query_with_name('drop_eddy_index_point.sql')
        query_drop_point_index = sql.SQL(tokenized_query).format(table_name=sql.Identifier(self.eddy_table_name))

        with pg.connect(self.connect_string()) as conn:
            with conn.cursor() as cur:
                cur.execute(query_drop_point_index)
                conn.commit()

    ######################################################
    #
    # import
    #
    ######################################################

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

    def eddy_with_id(self, track, cyclonic_type):
        tokenized_query = self.sql_query_with_name("eddy_with_id.sql")
        values = {"track": track,
                  "cyclonic_type": cyclonic_type}

        with pg.connect(self.connect_string()) as connection:
            with connection.cursor() as cursor:
                cursor.execute(tokenized_query, values)
                data = cursor.fetchall()

        return data

    def eddy_with_id_as_xarray(self, track, cyclonic_type):
        data = self.eddy_with_id(track, cyclonic_type)
        header_array = ['track', 'cyclonic_type', 'time', 'latitude', 'longitude', 'observation_number', 'speed_radius', 'amplitude']
        row_meta = [
            {'var_name': 'amplitude',
                'comment': "Magnitude of the height difference between the extremum of SSH within the eddy and the SSH around the effective contour defining the eddy edge",
                'long_name': "Amplitude",
                'units': "m",
                'scale_factor': 0.0001,
                'add_offset': 0},
            {'var_name': 'cost_association',
                'comment': "Cost value to associate one eddy with the next observation",
                'long_name': "Cost association between two eddies"},
            {'var_name': 'effective_area',
                'comment': "Area enclosed by the effective contour in m^2",
                'long_name': "Effective area",
                'units': "m^2"},
            {'var_name': 'effective_contour_height',
                'comment': "SSH filtered height for effective contour",
                'long_name': "Effective Contour Height",
                'units': "m"},
            {'var_name': 'effective_contour_latitude',
                'axis': "X",
                'comment': "Latitudes of effective contour",
                'long_name': "Effective Contour Latitudes",
                'units': "degrees_east",
                'scale_factor': 0.01,
                'add_offset': 0},
            {'var_name': 'effective_contour_longitude',
                'axis': "X",
                'comment': "Longitudes of the effective contour",
                'long_name': "Effective Contour Longitudes",
                'units': "degrees_east",
                'scale_factor': 0.01,
                'add_offset': 180.},
            {'var_name': 'effective_contour_shape_error',
                'comment': "Error criterion between the effective contour and its best fit circle",
                'long_name': "Effective Contour Shape Error",
                'units': "%",
                'scale_factor': 0.5,
                'add_offset': 0},
            {'var_name': 'effective_radius',
                'comment': "Radius of the best fit circle corresponding to the effective contour",
                'long_name': "Effective Radius",
                'units': "m",
                'scale_factor': 50.,
                'add_offset': 0},
            {'var_name': 'inner_contour_height',
                'comment': "SSH filtered height for the smallest detected contour",
                'long_name': "Inner Contour Height",
                'units': "m"},
            {'var_name': 'latitude',
                'axis': "Y",
                'comment': "Latitude center of the best fit circle",
                'long_name': "Eddy Center Latitude",
                'standard_name': "latitude",
                'units': "degrees_north"},
            {'var_name': 'latitude_max',
                'axis': "Y",
                'comment': "Latitude of the inner contour",
                'long_name': "Latitude of the SSH maximum",
                'standard_name': "latitude",
                'units': "degrees_north"},
            {'var_name': 'longitude',
                'axis': "X",
                'comment': "Longitude center of the best fit circle",
                'long_name': "Eddy Center Longitude",
                'standard_name': "longitude",
                'units': "degrees_east"},
            {'var_name': 'longitude_max',
                'axis': "X",
                'comment': "Longitude of the inner contour",
                'long_name': "Longitude of the SSH maximum",
                'standard_name': "longitude",
                'units': "degrees_east"},
            {'var_name': 'num_contours',
                'comment': "Number of contours selected for this eddy",
                'long_name': "Number of contours"},
            {'var_name': 'num_point_e',
                'description': "Number of points for effective contour before resampling",
                'long_name': "number of points for effective contour",
                'units': "ordinal"},
            {'var_name': 'num_point_s',
                'description': "Number of points for speed contour before resampling",
                'long_name': "number of points for speed contour",
                'units': "ordinal"},
            {'var_name': 'observation_flag',
                'comment': "Flag indicating if the value is interpolated between two observations or not (0: observed eddy, 1: interpolated eddy)",
                'long_name': "Virtual Eddy Position"},
            {'var_name': 'observation_number',
                'comment': "Observation sequence number, days starting at the eddy first detection",
                'long_name': "Eddy temporal index in a trajectory"},
            {'var_name': 'speed_area',
                'comment': "Area enclosed by the speed contour in m^2",
                'long_name': "Speed area",
                'units': "m^2"},
            {'var_name': 'speed_average',
                'comment': "Average speed of the contour defining the radius scale speed_radius",
                'long_name': "Maximum circum-averaged Speed",
                'units': "m/s",
                'scale_factor': 0.0001,
                'add_offset': 0},
            {'var_name': 'speed_contour_height',
                'comment': "SSH filtered height for speed contour",
                'long_name': "Speed Contour Height",
                'units': "m"},
            {'var_name': 'speed_contour_latitude',
                'axis': "X",
                'comment': "Latitudes of speed contour",
                'long_name': "Speed Contour Latitudes",
                'units': "degrees_east",
                'scale_factor': 0.01,
                'add_offset': 0},
            {'var_name': 'speed_contour_longitude',
                'axis': "X",
                'comment': "Longitudes of speed contour",
                'long_name': "Speed Contour Longitudes",
                'units': "degrees_east",
                'scale_factor': 0.01,
                'add_offset': 180.},
            {'var_name': 'speed_contour_shape_error',
                'comment': "Error criterion between the speed contour and its best fit circle",
                'long_name': "Speed Contour Shape Error",
                'units': "%",
                'scale_factor': 0.5,
                'add_offset': 0},
            {'var_name': 'speed_radius',
                'comment': "Radius of the best fit circle corresponding to the contour of maximum circum-average speed",
                'long_name': "Speed Radius",
                'units': "m",
                'scale_factor': 50.,
                'add_offset': 0},
            {'var_name': 'time',
                'axis': "T",
                'calendar': "proleptic_gregorian",
                'comment': "Date of this observation",
                'long_name': "Time",
                'standard_name': "time",
                'units': "days since 1950-01-01 00:00:00",
                'scale_factor': 1.15740740740741e-05,
                'add_offset': 0},
            {'var_name': 'track',
                'comment': "Trajectory identification number",
                'long_name': "Trajectory number"},
            {'var_name': 'uavg_profile',
                'comment': "Speed averaged values from the effective contour inwards to the smallest contour, evenly spaced points",
                'long_name': "Radial Speed Profile",
                'units': "m/s",
                'scale_factor': 0.0001,
                'add_offset': 0}]
        [xrdata, encoding] = self.data_as_xarray(data, header_array, row_meta)
        return xrdata, encoding

    def along_track_points_near_eddy(self, eddy_track, cyclonic_type):
        tokenized_query = self.sql_query_with_name('along_near_eddy.sql')
        values = {"track": eddy_track,
                  "cyclonic_type": cyclonic_type,}

        with pg.connect(self.connect_string()) as connection:
            with connection.cursor() as cursor:
                cursor.execute(tokenized_query, values)
                data = cursor.fetchall()

        return data

    def along_track_points_near_eddy_as_xarray(self, track, cyclonic_type):
        data = self.along_track_points_near_eddy(track, cyclonic_type)
        header_array = ['along_file_name', 'track', 'cycle', 'latitude', 'longitude', 'sla_unfiltered', 'sla_filtered',
                        'time', 'dac', 'ocean_tide', 'internal_tide', 'lwe', 'mdt', 'tpa_correction']
        row_meta = [
            {'var_name': 'sla_unfiltered',
             'comment': 'The sea level anomaly is the sea surface height above mean sea surface height; the uncorrected sla can be computed as follows: [uncorrected sla]=[sla from product]+[dac]+[ocean_tide]+[internal_tide]-[lwe]; see the product user manual for details',
             'long_name': 'Sea level anomaly not-filtered not-subsampled with dac, ocean_tide and lwe correction applied',
             'scale_factor': 0.001,
             'standard_name': 'sea_surface_height_above_sea_level',
             'units': 'm',
             'dtype': 'int16'},
            {'var_name': 'sla_filtered',
             'comment': 'The sea level anomaly is the sea surface height above mean sea surface height; the uncorrected sla can be computed as follows: [uncorrected sla]=[sla from product]+[dac]+[ocean_tide]+[internal_tide]-[lwe]; see the product user manual for details',
             'long_name': 'Sea level anomaly filtered not-subsampled with dac, ocean_tide and lwe correction applied',
             'scale_factor': 0.001,
             'add_offset': 0.,
             '_FillValue': 32767,
             'standard_name': 'sea_surface_height_above_sea_level',
             'units': 'm',
             'dtype': 'int16'},
            {'var_name': 'dac',
             'comment': 'The sla in this file is already corrected for the dac; the uncorrected sla can be computed as follows: [uncorrected sla]=[sla from product]+[dac]; see the product user manual for details',
             'long_name': 'Dynamic Atmospheric Correction', 'scale_factor': 0.001, 'standard_name': None,
             'units': 'm'},
            {'var_name': 'time', 'comment': '', 'long_name': 'Time of measurement', 'scale_factor': None,
             'standard_name': 'time', 'units': 'days since 1950-01-01 00:00:00'},
            {'var_name': 'track', 'comment': '', 'long_name': 'Track in cycle the measurement belongs to',
             'scale_factor': None, 'standard_name': None, 'units': '1\n'},
            {'var_name': 'cycle', 'comment': '', 'long_name': 'Cycle the measurement belongs to',
             'scale_factor': None, 'standard_name': None, 'units': '1'},
            {'var_name': 'ocean_tide',
              'comment': 'The sla in this file is already corrected for the ocean_tide; the uncorrected sla can be computed as follows: [uncorrected sla]=[sla from product]+[ocean_tide]; see the product user manual for details',
              'long_name': 'Ocean tide model',
              'scale_factor': 0.001,
              'standard_name': None, 'units': 'm'},
            {'var_name': 'internal_tide',
             'comment': 'The sla in this file is already corrected for the internal_tide; the uncorrected sla can be computed as follows: [uncorrected sla]=[sla from product]+[internal_tide]; see the product user manual for details',
             'long_name': 'Internal tide correction', 'scale_factor': 0.001, 'standard_name': None,
             'units': 'm'},
            {'var_name': 'lwe',
             'comment': 'The sla in this file is already corrected for the lwe; the uncorrected sla can be computed as follows: [uncorrected sla]=[sla from product]-[lwe]; see the product user manual for details',
             'long_name': 'Long wavelength error', 'scale_factor': 0.001, 'standard_name': None,
             'units': 'm'},
            {'var_name': 'mdt',
             'comment': 'The mean dynamic topography is the sea surface height above geoid; it is used to compute the absolute dynamic tyopography adt=sla+mdt',
             'long_name': 'Mean dynamic topography', 'scale_factor': 0.001,
             'standard_name': 'sea_surface_height_above_geoid', 'units': 'm'}]

        [xrdata, encoding] = self.data_as_xarray(data, header_array, row_meta)
        return xrdata, encoding

    def eddy_speed_radii_json(self, track, cyclonic_type):
        geojson_query = '''SELECT
        		ST_AsGeoJSON(CASE
        			WHEN
        				abs(min(eddy.longitude)-max(eddy.longitude)) > 180
        			THEN
        				ST_ShiftLongitude(ST_Collect(cast(ST_Buffer(eddy.eddy_point, eddy.speed_radius) as geometry)))
        			ELSE
        				ST_Collect(ST_Buffer(eddy.eddy_point, eddy.speed_radius)::geometry)
        			END, 6) AS speed_radius_buffer,
        			max(eddy.longitude) as max_longitude,
                    min(eddy.longitude) as min_longitude,
                    max(eddy.latitude) as max_latitude,
                    min(eddy.latitude) as min_latitude
        	FROM eddy
        	WHERE eddy.track = %(track)s AND cyclonic_type = %(cyclonic_type)s
        	GROUP BY track, cyclonic_type'''

        values = {"track": track,
                  "cyclonic_type": cyclonic_type,}

        with pg.connect(self.connect_string()) as connection:
            with connection.cursor() as cursor:
                cursor.execute(geojson_query, values)
                json_data = cursor.fetchall()

        return json_data