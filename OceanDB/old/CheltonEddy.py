import netCDF4 as nc
import psycopg as pg
from psycopg import sql
import glob
import time
import os
import yaml
from OceanDB.OceanDB import OceanDB
from AlongTrack import AlongTrack

class CheltonEddy(OceanDB):
    eddy_table_name: str = 'chelton_eddy'
    eddy_metadata_table_name: str = 'chelton_eddy_metadata'
    chelton_eddies_file_path: str = ''
    eddy_variable_metadata: dict = dict()
    variable_scale_factor: dict = dict()
    variable_add_offset: dict = dict()

    ######################################################
    #
    # Initialization
    #
    ######################################################

    def __init__(self, host="", username="", password="", port=5432, db_name='ocean', chelton_eddies_file_path=""):
        super().__init__(host=host, username=username, password=password, port=port, db_name=db_name)
        with open(os.path.join(os.path.dirname(__file__), '../config.yaml'), 'r') as param_file:
            chelton_params = yaml.full_load(param_file)
            if 'copernicus_marine' in chelton_params:
                if 'chelton_eddies_file_path' in chelton_params['copernicus_marine']:
                    self.chelton_eddies_file_path = chelton_params['copernicus_marine']['chelton_eddies_file_path']

        # locally defined variable override the settings in the config file
        if chelton_eddies_file_path:
            self.chelton_eddies_file_path = chelton_eddies_file_path

        self.init_variable_metadata()
        for metadata in self.eddy_variable_metadata:
            if 'scale_factor' in metadata:
                self.variable_scale_factor[metadata['var_name']] = metadata['scale_factor']
            if 'add_offset' in metadata:
                self.variable_add_offset[metadata['var_name']] = metadata['add_offset']


    def init_variable_metadata(self):
        self.eddy_variable_metadata = [
            {'var_name': 'amplitude',
                'comment': "Magnitude of the height difference between the extremum of SSH within the eddy and the SSH around the effective contour defining the eddy edge",
                'long_name': "Amplitude",
                'units': "m",
                'scale_factor': 0.001,
                'add_offset': 0,
                'dtype': 'uint16'},
            {'var_name': 'cyclonic_type',
             'comment': "Cyclonic -1; Anti-cyclonic +1",
             'long_name': "Rotating sense of the eddy",
             'dtype': 'int8'},
            {'var_name': 'latitude',
                'axis': "Y",
                'comment': "Latitude center of the fitted circle",
                'long_name': "Eddy Center Latitude",
                'standard_name': "latitude",
                'units': "degrees_north",
                'dtype': 'float32'},
            {'var_name': 'longitude',
                'axis': "X",
                'comment': "Longitude center of the fitted circle",
                'long_name': "Eddy Center Longitude",
                'standard_name': "longitude",
                'units': "degrees_east",
                'dtype': 'float32'},
            {'var_name': 'observation_flag',
                'comment': "Flag indicating if the value is interpolated between two observations or not (0: observed eddy, 1: interpolated eddy)",
                'long_name': "Virtual Eddy Position",
                'dtype': 'int8'},
            {'var_name': 'observation_number',
                'comment': "Observation sequence number, days starting at the eddy first detection",
                'long_name': "Eddy temporal index in a trajectory",
                'dtype': 'uint16'},
            {'var_name': 'speed_average',
                'comment': "Average speed of the contour defining the radius scale speed_radius",
                'long_name': "Maximum circum-averaged Speed",
                'units': "m/s",
                'scale_factor': 0.0001,
                'add_offset': 0,
                'dtype': 'uint16'},
            {'var_name': 'speed_radius',
                'comment': "Radius of the best fit circle corresponding to the contour of maximum circum-average speed",
                'long_name': "Speed Radius",
                'units': "m",
                'scale_factor': 50.,
                'add_offset': 0,
                'dtype': 'uint16'},
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
                'long_name': "Trajectory number",
                'dtype': 'uint32'},]

    ######################################################
    #
    # eddy table creation/destruction
    #
    ######################################################

    def create_chelton_eddy_table(self):
        tokenized_query = self.sql_query_with_name('create_chelton_eddy_table.sql')
        query = sql.SQL(tokenized_query).format(table_name=sql.Identifier(self.eddy_table_name))

        with pg.connect(self.connect_string()) as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                conn.commit()
        print(f"Table '{self.eddy_table_name} added to database (if it did not previously exist) '{self.db_name}'.")

    def drop_chelton_eddy_table(self):
        self.drop_table(self.eddy_table_name)

    def truncate_chelton_eddy_table(self):
        self.truncate_table(self.eddy_table_name)

    def create_chelton_eddy_indices(self):
        query_create_point_index = sql.SQL(self.sql_query_with_name('create_chelton_eddy_index_point.sql')).format(table_name=sql.Identifier(self.eddy_table_name))
        query_create_track_times_cyclonic_type_idx = sql.SQL(self.sql_query_with_name('create_chelton_eddy_index_track_cyclonic_type.sql')).format(table_name=sql.Identifier(self.eddy_table_name))

        with pg.connect(self.connect_string()) as conn:
            with conn.cursor() as cur:
                cur.execute(query_create_point_index)
                cur.execute(query_create_track_times_cyclonic_type_idx)
                conn.commit()

    def drop_chelton_eddy_indices(self):
        query_drop_point_index = sql.SQL(self.sql_query_with_name('drop_chelton_eddy_index_point.sql')).format(table_name=sql.Identifier(self.eddy_table_name))

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
            ds.set_auto_maskandscale(False)
            eddy_data = {
                'amplitude': ds.variables['amplitude'][:],
                'cyclonic_type': ds.variables['cyclonic_type'][:],
                'latitude': ds.variables['latitude'][:],
                'longitude': ds.variables['longitude'][:],
                'observation_flag': ds.variables['observation_flag'][:],
                'observation_number': ds.variables['observation_number'][:],
                'speed_average': ds.variables['speed_average'][:],
                'speed_radius': ds.variables['speed_radius'][:],
                'date_time': time_data,
                'track': ds.variables['track'][:],
            }
            ds.close()

            # eddy_data['longitude'][:] = (data['longitude'][:] + 180) % 360 - 180
            return eddy_data
        except FileNotFoundError:
            print("File '{}' not found".format(file_path))

    def import_data_tuple_to_postgresql(self, data, filename):
        copy_query = sql.SQL(
            """COPY {eddy_tbl_nme} ( 
                amplitude, 
                cyclonic_type, 
                latitude, 
                longitude, 
                observation_flag, 
                observation_number, 
                speed_average, 
                speed_radius, 
                date_time, 
                track) FROM STDIN""").format(eddy_tbl_nme=sql.Identifier(self.eddy_table_name))

        with pg.connect(self.connect_string()) as connection:
            with connection.cursor() as cursor:
                with cursor.copy(copy_query) as copy:
                    for i in range(len(data['observation_number'])):
                        copy.write_row([
                            data['amplitude'][i],
                            data['cyclonic_type'][i],
                            data['latitude'][i],
                            data['longitude'][i],
                            data['observation_flag'][i],
                            data['observation_number'][i],
                            data['speed_average'][i],
                            data['speed_radius'][i],
                            data['date_time'][i],
                            data['track'][i],
                        ])

    def insert_chelton_eddy_data_from_netcdf_with_tuples(self):
        start = time.time()
        directory = self.chelton_eddies_file_path
        for file_path in glob.glob(directory + '/Eddy Trajectory DT 2.0 Jan 1 1993 to Mar 7 2020.nc'):
            filename = 'Eddy Trajectory DT 2.0 Jan 1 1993 to Mar 7 2020'
            print(filename)
            data = self.extract_data_tuple_from_netcdf(file_path, filename)
            import_start = time.time()
            self.import_data_tuple_to_postgresql(data, filename)
            del data
            import_end = time.time()
            print(f"{filename} import time: {import_end - import_start}")
        end = time.time()
        print(f"Script end. Total time: {end - start}")

    ######################################################
    #
    # simple queries
    #
    ######################################################

    def chelton_eddy_with_id(self, eddy_id):
        tokenized_query = self.sql_query_with_name("chelton_eddy_with_id.sql")
        values = {"track_cyclonic_type": eddy_id}

        with pg.connect(self.connect_string()) as connection:
            with connection.cursor() as cursor:
                cursor.execute(tokenized_query, values)
                data = cursor.fetchall()

        return data

    def chelton_eddy_with_id_as_xarray(self, eddy_id):
        data = self.chelton_eddy_with_id(eddy_id)
        header_array = ['track', 'cyclonic_type', 'date_time', 'latitude', 'longitude', 'observation_number', 'speed_radius', 'amplitude']
        [xrdata, encoding] = self.data_as_xarray(data, header_array, self.eddy_variable_metadata)
        return xrdata, encoding

    def along_track_points_near_chelton_eddy_old(self, eddy_id):
        tokenized_query = self.sql_query_with_name('along_near_chelton_eddy.sql').format(speed_radius_scale_factor=self.variable_scale_factor["speed_radius"])
        values = {"eddy_id": eddy_id}

        with pg.connect(self.connect_string()) as connection:
            with connection.cursor() as cursor:
                cursor.execute(tokenized_query, values)
                data = cursor.fetchall()

        return data

    def along_track_points_near_chelton_eddy(self, eddy_id):
        eddy_query = """SELECT 
                MIN(date_time), 
                MAX(date_time), 
                array_agg(distinct connected_id) || array_agg(distinct basin.id)
            FROM chelton_eddy as ceddy 
            LEFT JOIN basin ON ST_Intersects(basin.basin_geog, ceddy.chelton_eddy_point)
            LEFT JOIN basin_connection ON basin_connection.basin_id = basin.id
            WHERE ceddy.track = %(eddy_id)s
            GROUP BY track, cyclonic_type;"""
        along_query = """SELECT atk.file_name, atk.track, atk.cycle, atk.latitude, atk.longitude, atk.sla_unfiltered, atk.sla_filtered, atk.date_time as time, atk.dac, atk.ocean_tide, atk.internal_tide, atk.lwe, atk.mdt, atk.tpa_correction
                   FROM chelton_eddy as ceddy 
                   INNER JOIN along_track atk ON atk.date_time BETWEEN ceddy.date_time AND (ceddy.date_time + interval '1 day')
	               AND st_dwithin(atk.along_track_point, ceddy.chelton_eddy_point, (ceddy.speed_radius * {speed_radius_scale_factor} * 2.0)::double precision)
                   WHERE ceddy.track = %(eddy_id)s
                   AND atk.date_time BETWEEN '{min_date}'::timestamp AND '{max_date}'::timestamp
                   AND basin_id = ANY( ARRAY[{connected_basin_ids}] );"""
        values = {"eddy_id": eddy_id}

        with pg.connect(self.connect_string()) as connection:
            with connection.cursor() as cursor:
                cursor.execute(eddy_query, values)
                data = cursor.fetchall()
                # values["min_date"] = data[0][0]
                # values["max_date"] = data[0][1]
                print(data)
                along_query = along_query.format(speed_radius_scale_factor=2*self.variable_scale_factor["speed_radius"],
                                                 min_date=data[0][0],
                                                 max_date=data[0][1],
                                                 connected_basin_ids=data[0][2])
                cursor.execute(along_query, values)
                data = cursor.fetchall()
        return data

    def along_track_points_near_chelton_eddy_as_xarray(self, eddy_id):
        data = self.along_track_points_near_chelton_eddy(eddy_id)
        header_array = ['along_file_name', 'track', 'cycle', 'latitude', 'longitude', 'sla_unfiltered', 'sla_filtered',
                        'time', 'dac', 'ocean_tide', 'internal_tide', 'lwe', 'mdt', 'tpa_correction']

        [xrdata, encoding] = self.data_as_xarray(data, header_array, AlongTrack.along_track_variable_metadata())
        return xrdata, encoding

    def chelton_eddy_speed_radii_json(self, eddy_id):
        sql.SQL(self.sql_query_with_name('create_chelton_eddy_index_point.sql')).format(
            table_name=sql.Identifier(self.eddy_table_name))
        geojson_query = sql.SQL('''SELECT ST_AsGeoJSON(CASE
            WHEN
                abs(min(ceddy.longitude) - max(ceddy.longitude)) > 180
                THEN
                ST_ShiftLongitude(ST_Collect(cast(ST_Buffer(ceddy.chelton_eddy_point, ceddy.speed_radius *{scale_factor}) as geometry)))
            ELSE
                ST_Collect(ST_Buffer(ceddy.chelton_eddy_point, ceddy.speed_radius *{scale_factor})::geometry)
            END, 6) AS speed_radius_buffer,
            max(ceddy.longitude)      as max_longitude,
            min(ceddy.longitude)      as min_longitude,
            max(ceddy.latitude)       as max_latitude,
            min(ceddy.latitude)       as min_latitude
            FROM chelton_eddy as ceddy
            WHERE ceddy.track = %(eddy_id)s
            GROUP BY track, cyclonic_type''').format(scale_factor=self.variable_scale_factor["speed_radius"])

        values = {"eddy_id": eddy_id}

        with pg.connect(self.connect_string()) as connection:
            with connection.cursor() as cursor:
                cursor.execute(geojson_query, values)
                json_data = cursor.fetchall()

        return json_data