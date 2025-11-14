from typing import Generator, List, Callable
import psycopg as pg
from datetime import timedelta, datetime
import numpy as np
import numpy.typing as npt
from dataclasses import dataclass

from OceanDB.OceanDB import OceanDB
from OceanDB.utils.projections import spherical_transverse_mercator_to_latitude_longitude, latitude_longitude_to_spherical_transverse_mercator, latitude_longitude_bounds_for_transverse_mercator_box


@dataclass
class SLA_Geographic:
    """
    Make SLA data type explict
    longitude,
     latitude,
	 sla_filtered,
	 EXTRACT(EPOCH FROM ({central_date_time} - date_time)) AS time_difference_secs

    """
    latitude: npt.NDArray
    longitude: npt.NDArray
    sla_filtered: npt.NDArray
    distance: npt.NDArray
    delta_t: npt.NDArray

    def __repr__(self):
        return f"""
        latitude: {self.latitude[:10]}...
        longitude: {self.longitude[:10]} ...
        delta_t: {self.delta_t[:10]} ...
        sla_filtered: {self.sla_filtered[:10]} ...
        """



    @classmethod
    def from_rows(cls,
                 rows: list,
                 variable_scale_factor
                 ):
        return cls(
            latitude = np.array([data_i[0] for data_i in rows]),
            longitude = np.array([data_i[1] for data_i in rows]),
            sla_filtered = variable_scale_factor * np.array([data_i[2] for data_i in rows]),
            distance = np.array([data_i[3] for data_i in rows]),
            delta_t = np.array([data_i[4] for data_i in rows], dtype=np.float64)
        )

    def to_dict(self):
        return {
            'longitude': self.longitude,
            'latitude': self.latitude,
            'sla_filtered': self.sla_filtered,
            'delta_t': self.delta_t
        }

@dataclass
class SLA_Projected(SLA_Geographic):
    delta_x: npt.NDArray
    delta_y: npt.NDArray



    def __init__(self,
                 sla_geographic: SLA_Geographic,
                 latitude: float,
                 longitude: float,
                 projection_function: Callable = latitude_longitude_to_spherical_transverse_mercator):
        super().__init__(**sla_geographic.to_dict())
        x0, y0 = projection_function(
            lat=latitude,
            lon=longitude,
            lon0=longitude
        )
        x, y = projection_function(
            lat=sla_geographic.latitude,
            lon = sla_geographic.longitude,
            lon0=longitude
        )



class AlongTrack(OceanDB):
    along_track_table_name: str = 'along_track'
    along_track_metadata_table_name: str = 'along_track_metadata'
    ocean_basin_table_name: str = 'basin'
    ocean_basins_connections_table_name: str = 'basin_connection'
    variable_scale_factor: dict = dict()
    variable_add_offset: dict = dict()
    missions = ['al', 'alg', 'c2', 'c2n', 'e1g', 'e1', 'e2', 'en', 'enn', 'g2', 'h2a', 'h2b', 'j1g', 'j1', 'j1n', 'j2g',
                'j2', 'j2n', 'j3', 'j3n', 's3a', 's3b', 's6a', 'tp', 'tpn']


    nearest_neighbor_query = 'queries/geographic_nearest_neighbor.sql'
    geo_spatiotemporal_query = 'queries/geographic_points_in_spatialtemporal_window.sql'
    projected_spatio_temporal_query_mask = 'queries/geographic_points_in_spatialtemporal_projected_window_nomask.sql'
    projected_spatio_temporal_query_no_mask = 'queries/geographic_points_in_spatialtemporal_window.sql'


    def __init__(self, host="", username="", password="", port=5432, db_name='ocean', nc_files_path=""):
        super().__init__(host=host, username=username, password=password, port=port, db_name=db_name)
        aList = AlongTrack.along_track_variable_metadata()
        for metadata in aList:
            if 'scale_factor' in metadata:
                self.variable_scale_factor[metadata['var_name']] = metadata['scale_factor']
            if 'add_offset' in metadata:
                self.variable_add_offset[metadata['var_name']] = metadata['add_offset']

    @staticmethod
    def along_track_variable_metadata():
        along_track_variable_metadata = [
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
             'units': 'm',
             'dtype': 'int16'},
            {'var_name': 'time',
             'comment': '',
             'long_name': 'Time of measurement',
             'scale_factor': None,
             'standard_name': 'time',
             'units': 'days since 1950-01-01 00:00:00',
             'calendar': 'gregorian'},
            {'var_name': 'track',
             'comment': '',
             'long_name': 'Track in cycle the measurement belongs to',
             'scale_factor': None,
             'standard_name': None,
             'units': '1\n',
             'dtype': 'int16'},
            {'var_name': 'cycle',
             'comment': '',
             'long_name': 'Cycle the measurement belongs to',
             'scale_factor': None,
             'standard_name': None,
             'units': '1',
             'dtype': 'int16'},
            {'var_name': 'ocean_tide',
              'comment': 'The sla in this file is already corrected for the ocean_tide; the uncorrected sla can be computed as follows: [uncorrected sla]=[sla from product]+[ocean_tide]; see the product user manual for details',
              'long_name': 'Ocean tide model',
              'scale_factor': 0.001,
              'standard_name': None,
              'units': 'm',
             'dtype': 'int16'},
            {'var_name': 'internal_tide',
             'comment': 'The sla in this file is already corrected for the internal_tide; the uncorrected sla can be computed as follows: [uncorrected sla]=[sla from product]+[internal_tide]; see the product user manual for details',
             'long_name': 'Internal tide correction',
             'scale_factor': 0.001,
             'standard_name': None,
             'units': 'm',
             'dtype': 'int16'},
            {'var_name': 'lwe',
             'comment': 'The sla in this file is already corrected for the lwe; the uncorrected sla can be computed as follows: [uncorrected sla]=[sla from product]-[lwe]; see the product user manual for details',
             'long_name': 'Long wavelength error',
             'scale_factor': 0.001,
             'standard_name': None,
             'units': 'm',
             'dtype': 'int16'},
            {'var_name': 'mdt',
             'comment': 'The mean dynamic topography is the sea surface height above geoid; it is used to compute the absolute dynamic tyopography adt=sla+mdt',
             'long_name': 'Mean dynamic topography',
             'scale_factor': 0.001,
             'standard_name': 'sea_surface_height_above_geoid',
             'units': 'm',
             'dtype': 'int16'}]
        return along_track_variable_metadata

    def geographic_nearest_neighbors_dt(self,
                                    latitude: float,
                                    longitude: float,
                                    date: datetime,
                                    time_window=timedelta(seconds=856710),
                                    missions=None
                                    ) -> SLA_Geographic|None:
        """
        Given a spatiotemporal point returns the THREE closest data points
        distance: in meters
        """

        if missions is None:
            missions = self.missions

        query = self.load_sql_file(self.nearest_neighbor_query)

        basin_id = self.basin_mask(latitude, longitude)
        connected_basin_id = self.basin_connection_map[basin_id]

        params = {
            "longitude": longitude,
            "latitude": latitude,
            "central_date_time": date,
            "time_delta": str(time_window / 2),
            "connected_basin_ids": connected_basin_id,
            "missions": missions
        }

        with pg.connect(self.connect_string()) as connection:
            with connection.cursor() as cursor:
                cursor.execute(query,params)
                rows = cursor.fetchall()
                if not rows:
                    return None
                return SLA_Geographic.from_rows(rows, self.variable_scale_factor["sla_filtered"])

    def geographic_nearest_neighbors(self,
                                     latitudes: npt.NDArray[np.floating],
                                     longitudes: npt.NDArray[np.floating],
                                     dates: List[datetime],
                                     time_window=timedelta(seconds=856710),
                                     missions=None
                                     ) -> Generator[SLA_Geographic|None, None, None]:
        """
        Given a spatiotemporal point returns the THREE closest data points
        distance: in meters
        """

        query = self.load_sql_file(self.nearest_neighbor_query)

        if missions is None:
            missions = self.missions

        basin_ids = self.basin_mask(latitudes, longitudes)
        connected_basin_ids = list( map(self.basin_connection_map.get, basin_ids) )
        params = [
            {
                "latitude": latitude,
                "longitude": longitude,
                "central_date_time": date,
                "connected_basin_ids": connected_basin_ids,
                "time_delta": str(time_window / 2),
                "missions": missions
             }
            for latitude, longitude, date, connected_basin_ids in zip(latitudes, longitudes, dates, connected_basin_ids)]

        with pg.connect(self.connect_string()) as connection:
            with connection.cursor() as cursor:
                cursor.executemany(query, params, returning=True)
                while True:
                    rows = cursor.fetchall()
                    if not rows:
                        yield None
                    else:
                        data = SLA_Geographic.from_rows(rows, self.variable_scale_factor["sla_filtered"])
                        yield data
                    if not cursor.nextset():
                        break

    def geographic_points_in_spatialtemporal_window(self,
                                                    latitude: float,
                                                    longitude: float,
                                                    date: datetime,
                                                    distance=500000,
                                                    time_window=timedelta(seconds=856710),
                                                    missions:List[str]|None=None
                                                    ) -> SLA_Geographic | None:

        if missions is None:
            missions = self.missions

        query = self.load_sql_file(self.geo_spatiotemporal_query)
        basin_id = self.basin_mask(latitude, longitude)
        if basin_id == 0:
            return None

        if basin_id in self.basin_connection_map:
            connected_basin_id = self.basin_connection_map[basin_id]
        else:
            connected_basin_id = []

        connected_basin_id.append(3)

        params = {
            "longitude": longitude,
            "latitude": latitude,
            "distance": distance,
            "central_date_time": date,
            "time_delta": time_window,
            "connected_basin_ids": connected_basin_id,
            "missions": [missions]
        }

        with pg.connect(self.connect_string()) as connection:
            with connection.cursor() as cursor:
                cursor.execute(query, params)
                rows = cursor.fetchall()
                if not rows:
                    return None
                sla_geographic = SLA_Geographic.from_rows(rows, self.variable_scale_factor["sla_filtered"])
                return sla_geographic

    def geographic_points_in_spatialtemporal_windows(self,
                                                     latitudes: npt.NDArray[np.floating],
                                                     longitudes: npt.NDArray[np.floating],
                                                     dates: List[datetime],
                                                     distances=500000,
                                                     time_window=timedelta(seconds=856710),
                                                     missions=None
                                                     ) -> Generator[SLA_Geographic|None, None, None]:
        """
        Runs the geographic_points_in_spatialtemporal_window query for every point in the latitudes and longitudes arrays and dates list.

        latitudes: n-array
        longitudes: n-array
        dates: n-list
        """
        query = self.load_sql_file(self.geo_spatiotemporal_query)

        if missions is None:
            missions = self.missions

        if not isinstance(distances, list) and not isinstance(distances,np.ndarray):
            distances = [distances]*len(latitudes)


        basin_ids = self.basin_mask(latitudes, longitudes)
        connected_basin_ids = list( map(self.basin_connection_map.get, basin_ids) )

        params = [
            {
                "longitude": longitude,
                "latitude": latitude,
                "distance": distance,
                "central_date_time": date,
                "time_delta": time_window,
                "connected_basin_ids": connected_basins,
                "missions": [missions]
            }
            for latitude, longitude, date, connected_basins, distance in zip(latitudes, longitudes, dates, connected_basin_ids, distances)
        ]


        with pg.connect(self.connect_string()) as connection:
            with connection.cursor() as cursor:
                cursor.executemany(query, params, returning=True)
                while True:
                    rows = cursor.fetchall()
                    if not rows:
                        yield None
                    else:
                        data = SLA_Geographic.from_rows(rows, self.variable_scale_factor["sla_filtered"])
                        yield data
                    if not cursor.nextset():
                        break

                # cursor.executemany(query, params, returning=True)
                # i = 0
                # while True:
                #     rows = cursor.fetchall()
                #     if not rows:
                #         data = {"longitude": np.full(shape=1, fill_value=np.nan),
                #                 "latitude": np.full(shape=1, fill_value=np.nan),
                #                 "sla_filtered": np.full(shape=1, fill_value=np.nan),
                #                 "delta_t": np.full(shape=1, fill_value=np.nan)}
                #     else:
                #         data = {"longitude": np.array([data_i[0] for data_i in rows]),
                #                 "latitude": np.array([data_i[1] for data_i in rows]),
                #                 "sla_filtered": self.variable_scale_factor["sla_filtered"] * np.array([data_i[2] for data_i in rows]),
                #                 "delta_t": np.array(np.array([data_i[3] for data_i in rows]), dtype=np.float64)}
                #     yield data
                #     i = i + 1
                #     if not cursor.nextset():
                #         break


    def projected_points_in_dx_dy_dt(self,
                                                   latitude: float,
                                                   longitude: float,
                                                   date: datetime,
                                                   Lx: float = 500000.,
                                                   Ly: float = 500000.,
                                                   time_window=timedelta(seconds=856710),
                                                   should_basin_mask: bool = True
                                                   ) -> SLA_Projected | None:

        """
        Returns all points in a Geographic distance,
        Removes all points outside of the Projected box

        Length 1 Array

        should_basin_mask: ->
        NO MASK -> if should_basin_mask = True, only return points in the basin or connected basin.

        Panama Example
        should_basin_mask = False ->  returns points on BOTH sides of the Panama,
        should_basin_mask = True -> Returns only data in connected basin

        """
        if should_basin_mask:
            tokenized_query = self.load_sql_file("queries/geographic_points_in_spatialtemporal_projected_window.sql")
        else:
            tokenized_query = self.load_sql_file("queries/geographic_points_in_spatialtemporal_projected_window_nomask.sql")

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

        # lon = np.array([data_i[0] for data_i in data])
        # lat = np.array([data_i[1] for data_i in data])
        # sla = np.array([data_i[2] for data_i in data])
        # t = np.array([data_i[3] for data_i in data])
        #
        # [x, y] = AlongTrack.latitude_longitude_to_spherical_transverse_mercator(lat, lon, longitude)
        # out_of_bounds = (x < x0 - Lx) | (x > x0 + Lx) | (y < y0 - Ly) | (y > y0 + Ly)
        # x = x[~out_of_bounds]
        # y = y[~out_of_bounds]
        # sla = sla[~out_of_bounds]
        # t = t[~out_of_bounds]
        #
        # x = x - x0
        # y = y - y0
        #
        # return x, y, sla, t

    def projected_points_in_r_dt(self,
                                                               latitudes: List[float],
                                                               longitudes: List[float],
                                                               dates: List[datetime],
                                                               distance=500000,
                                                               time_window=timedelta(seconds=856710),
                                                               missions=None
                                                               ) -> Generator[SLA_Projected | None, None, None]:
        """
        Calls the geographic_points_in_spatialtemporal_windows() method
        """
        # i = 0

        sla_geographic_data_points = self.geographic_points_in_spatialtemporal_windows(
            latitudes=latitudes,
            longitudes=longitudes,
            dates = dates,
            distances=distance,
            time_window=time_window,
            missions=missions
        )

        for sla_geographic in sla_geographic_data_points:
            if not sla_geographic:
                yield None
            else:
                sla_projected = SLA_Projected(sla_geographic)
                yield sla_projected



        # for sla_geographic in sla_geographic_data_points:
        #     [x0, y0] = AlongTrack.latitude_longitude_to_spherical_transverse_mercator(latitude, longitude, longitude)
        #     [x, y] = AlongTrack.latitude_longitude_to_spherical_transverse_mercator(latitude, longitude,
        #                                                                             longitudes[i])
        #     data["delta_x"] = x - x0
        #     data["delta_y"] = y - y0
        #     i = i + 1
        #     yield data

        # for latitude, longitude, date in self.geographic_points_in_spatialtemporal_windows():
        #     [x0, y0] = AlongTrack.latitude_longitude_to_spherical_transverse_mercator(latitude, longitude, longitude)
        #     [x, y] = AlongTrack.latitude_longitude_to_spherical_transverse_mercator(latitude, longitude,
        #                                                                             longitudes[i])
        #     data["delta_x"] = x - x0
        #     data["delta_y"] = y - y0
        #     i = i + 1
        #     yield data
