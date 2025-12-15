from dataclasses import dataclass
from dataclasses import asdict
import netCDF4 as nc
import pandas as pd
import psycopg
import psycopg as pg
from psycopg import sql
import glob
import time
import os
import numpy as np
from OceanDB.OceanDB import OceanDB
from functools import cached_property
from typing import List, Tuple, Any, Iterable, Optional, Union
from datetime import datetime, timedelta
from pathlib import Path
from OceanDB.utils.postgres_upsert import upsert_ignore

@dataclass
class AlongTrackData:
    """Structured container for extracted along-track variables."""
    file_name: np.ndarray
    mission: np.ndarray
    time: np.ndarray
    latitude: np.ndarray
    longitude: np.ndarray
    cycle: np.ndarray
    track: np.ndarray
    sla_unfiltered: np.ndarray
    sla_filtered: np.ndarray
    dac: np.ndarray
    ocean_tide: np.ndarray
    internal_tide: np.ndarray
    lwe: np.ndarray
    mdt: np.ndarray
    tpa_correction: np.ndarray
    basin_id: np.ndarray

# filepath = "data/copernicus/SEALEVEL_GLO_PHY_L3_MY_008_062/cmems_obs-sl_glo_phy-ssh_my_al-l3-duacs_PT1S_202411/2013/03/dt_global_al_phy_l3_1hz_20130331_20240205.nc"
@dataclass
class AlongTrackMetaData:
    """Structured representation of NetCDF global metadata."""
    file_name: str
    conventions: Optional[str] = None
    metadata_conventions: Optional[str] = None
    cdm_data_type: Optional[str] = None
    comment: Optional[str] = None
    contact: Optional[str] = None
    creator_email: Optional[str] = None
    creator_name: Optional[str] = None
    creator_url: Optional[str] = None
    date_created: Optional[str] = None
    date_issued: Optional[str] = None
    date_modified: Optional[str] = None
    history: Optional[str] = None
    institution: Optional[str] = None
    keywords: Optional[str] = None
    license: Optional[str] = None
    platform: Optional[str] = None
    processing_level: Optional[str] = None
    product_version: Optional[str] = None
    project: Optional[str] = None
    references: Optional[str] = None
    software_version: Optional[str] = None
    source: Optional[str] = None
    ssalto_duacs_comment: Optional[str] = None
    summary: Optional[str] = None
    title: Optional[str] = None

    def to_dict(self):
        return asdict(self)


    @classmethod
    def from_netcdf(cls, ds: nc.Dataset, file_name: str) -> "AlongTrackMetaData":
        """Create AlongTrackMetaData from a NetCDF4 dataset."""

        if not isinstance(ds, nc.Dataset):
            raise TypeError("AlongTrackMetaData requires a NetCDF Dataset")

        def get(attr: str):
            return getattr(ds, attr, None)
        # print("from netcdf")
        # print(getattr(ds, "Conventions", None))
        # print(ds)

        conventions = getattr(ds, 'Conventions', None)

        print(f"CONVENTIOSNS {conventions}")
        return cls(
            file_name=file_name,
            conventions=get("Conventions"),
            metadata_conventions=get("Metadata_Conventions"),
            cdm_data_type=get("cdm_data_type"),
            comment=get("comment"),
            contact=get("contact"),
            creator_email=get("creator_email"),
            creator_name=get("creator_name"),
            creator_url=get("creator_url"),
            date_created=get("date_created"),
            date_issued=get("date_issued"),
            date_modified=get("date_modified"),
            history=get("history"),
            institution=get("institution"),
            keywords=get("keywords"),
            license=get("license"),
            platform=get("platform"),
            processing_level=get("processing_level"),
            product_version=get("product_version"),
            project=get("project"),
            references=get("references"),
            software_version=get("software_version"),
            source=get("source"),
            ssalto_duacs_comment=get("ssalto_duacs_comment"),
            summary=get("summary"),
            title=get("title"),
        )


@dataclass
class EddyData:
    amplitude: np.ndarray
    cost_association: np.ndarray
    effective_area: np.ndarray
    effective_contour_height: np.ndarray
    effective_contour_latitude: np.ndarray
    effective_contour_longitude: np.ndarray
    effective_contour_shape_error: np.ndarray
    effective_radius: np.ndarray
    inner_contour_height: np.ndarray

    latitude: np.ndarray
    latitude_max: np.ndarray
    longitude: np.ndarray
    longitude_max: np.ndarray

    num_contours: np.ndarray
    num_point_e: np.ndarray
    num_point_s: np.ndarray

    observation_flag: np.ndarray
    observation_number: np.ndarray

    speed_area: np.ndarray
    speed_average: np.ndarray
    speed_contour_height: np.ndarray
    # speed_contour_latitude: np.ndarray
    # speed_contour_longitude: np.ndarray
    speed_contour_shape_error: np.ndarray
    speed_radius: np.ndarray
    date_time: np.ndarray
    track: np.ndarray







class OceanDBETL(OceanDB):
    ocean_basin_table_name: str = 'basin'
    ocean_basins_connections_table_name: str = 'basin_connection'
    along_track_table_name: str = 'along_track'
    along_track_metadata_table_name: str = 'along_track_metadata'


    variable_scale_factor: dict = dict()
    variable_add_offset: dict = dict()
    missions = ['al', 'alg', 'c2', 'c2n', 'e1g', 'e1', 'e2', 'en', 'enn', 'g2', 'h2a', 'h2b', 'j1g', 'j1', 'j1n', 'j2g',
                'j2', 'j2n', 'j3', 'j3n', 's3a', 's3b', 's6a', 'tp', 'tpn']

    def __init__(self):
        super().__init__()

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

    def load_netcdf(self, file: Union[str, Path]) -> nc.Dataset:
        """
        Load a NetCDF file from either a string path or a Path object.
        """
        file = Path(file)  # normalize to Path
        return nc.Dataset(str(file), "r")

    def extract_dataset_metadata(self, ds: nc.Dataset, file: Path) -> AlongTrackMetaData:
        return AlongTrackMetaData.from_netcdf(ds, file_name=file.name)

    def extract_along_track_from_netcdf(self, ds: nc.Dataset, file: Path) -> AlongTrackData:
        """
        Parse & transform NetCDF file
        """
        mission = file.name.split('_')[2]
        try:
            # ds = nc.Dataset(file_path, 'r')
            # Don't scale most variables, so they are stored more efficiently in db
            ds.variables['sla_unfiltered'].set_auto_maskandscale(False)
            ds.variables['sla_filtered'].set_auto_maskandscale(False)
            ds.variables['ocean_tide'].set_auto_maskandscale(False)
            ds.variables['internal_tide'].set_auto_maskandscale(False)
            ds.variables['lwe'].set_auto_maskandscale(False)
            ds.variables['mdt'].set_auto_maskandscale(False)
            ds.variables['dac'].set_auto_maskandscale(False)
            ds.variables['tpa_correction'].set_auto_maskandscale(False)

            time_data = ds.variables['time']  # Extract dates from the dataset and convert them to standard datetime
            time_data = nc.num2date(time_data[:], time_data.units, only_use_cftime_datetimes=False,
                                    only_use_python_datetimes=False)
            time_data = nc.date2num(time_data[:],
                                    "microseconds since 2000-01-01 00:00:00")  # Convert the standard date back to the 8-byte integer PSQL uses

            basin_id = self.basin_mask(ds.variables['latitude'][:], ds.variables['longitude'][:])

            data = AlongTrackData(
                time=time_data,
                latitude=ds.variables["latitude"][:],
                longitude=ds.variables["longitude"][:],
                cycle=ds.variables["cycle"][:],
                track=ds.variables["track"][:],
                sla_unfiltered=ds.variables["sla_unfiltered"][:],
                sla_filtered=ds.variables["sla_filtered"][:],
                dac=ds.variables["dac"][:],
                ocean_tide=ds.variables["ocean_tide"][:],
                internal_tide=ds.variables["internal_tide"][:],
                lwe=ds.variables["lwe"][:],
                mdt=ds.variables["mdt"][:],
                tpa_correction=ds.variables["tpa_correction"][:],
                basin_id=basin_id,
                mission=mission,
                file_name=file.name
            )

            ds.close()
            return data
            #return time_data, lat_data, lon_data, cycle_data, track_data, sla_un_data, sla_f_data, dac_data, o_tide_data, i_tide_data, lwe_data, mdt_data, tpa_corr_data

        except Exception as ex:
            print(ex)

        # except FileNotFoundError:
        #     print("File '{}' not found".format(file_path))


    # def insert_along_track_data_from_netcdf(self, directory):
    #     """
    #     Iterate over all files in direcotory,
    #     """
    #     start = time.time()
    #     for file_path in glob.glob(directory + '/*.nc'):
    #         names = [os.path.basename(x) for x in glob.glob(file_path)]
    #         fname = names[0]  # filename will be used to link data to metadata
    #         along_track: AlongTrackData = self.extract_data_from_netcdf(file_path)
    #         import_start = time.time()
    #         self.import_along_track_data_to_postgresql(fname, along_track)
    #         import_end = time.time()
    #         print(f"{fname} import time: {import_end - import_start}")
    #     end = time.time()
    #     print(f"Script end. Total time: {end - start}")


    def insert_basins_data(self):
        with self.load_module_file(module="OceanDB.data", filename="basins/ocean_basins.csv", mode="r") as f:
            df = pd.read_csv(f)

        df.rename(columns={"geom": "basin_geog"}, inplace=True)

        columns = list(df.columns)
        query = sql.SQL("INSERT INTO {table} ({fields}) VALUES ({placeholders})").format(
            table=sql.Identifier("basin"),
            fields=sql.SQL(", ").join(map(sql.Identifier, columns)),
            placeholders=sql.SQL(", ").join(sql.Placeholder() * len(columns)),
        )

        data = df.to_records(index=False).tolist()

        with psycopg.connect(self.config.postgres_dsn) as conn:
            with conn.cursor() as cur:
                cur.executemany(query.as_string(conn), data)
                conn.commit()

        print(f"Inserted {len(df)} rows in to the basins table")


    def insert_basin_connections_data(self):
        with self.load_module_file(module="OceanDB.data", filename="basins/ocean_basin_connections.csv", mode="r") as f:
            df = pd.read_csv(f)
        df.rename(columns={"basinid": "basin_id", "connected_basin": "connected_id"}, inplace=True)
        print(df.columns)
        columns = list(df.columns)

        query = sql.SQL("INSERT INTO {table} ({fields}) VALUES ({placeholders})").format(
            table=sql.Identifier("basin_connections"),
            fields=sql.SQL(", ").join(map(sql.Identifier, columns)),
            placeholders=sql.SQL(", ").join(sql.Placeholder() * len(columns)),
        )

        data = df.to_records(index=False).tolist()

        with psycopg.connect(self.config.postgres_dsn) as conn:
            with conn.cursor() as cur:
                cur.executemany(query.as_string(conn), data)
                conn.commit()

        print(f"Inserted {len(df)} rows in to the basins table")


    @cached_property
    def basin_mask_data(self):
        """
        Load the basin mask NetCDF file packaged with the module.
        Returns the 'basinmask' variable as a NumPy array.
        """
        # Open resource file via importlib.resources
        with self.load_module_file("OceanDB.data", "basin_masks/new_basin_mask.nc", mode="rb") as f:
            ds = nc.Dataset("inmemory.nc", memory=f.read())  # load from memory buffer
            ds.set_auto_mask(False)
            basin_mask = ds.variables["basinmask"][:]
            ds.close()
            return basin_mask

    def basin_mask(self, latitude, longitude):
        onesixth = 1 / 6
        i = np.floor((latitude + 90) / onesixth).astype(int)
        j = np.floor((longitude % 360) / onesixth).astype(int)
        mask_data = self.basin_mask_data
        basin_mask = mask_data[i, j]
        return basin_mask

    def import_along_track_data_to_postgresql(self, along_track_data: AlongTrackData):
        """
        Cast the AlongTrackData to a Pandas DataFrame
        """

        EPOCH = datetime(2000, 1, 1)

        date_times = [EPOCH + timedelta(microseconds=int(t)) for t in along_track_data.time]

        df = pd.DataFrame({
            "file_name": along_track_data.file_name,
            "mission": along_track_data.mission,
            "track": along_track_data.track,
            "cycle": along_track_data.cycle,
            "latitude": along_track_data.latitude,
            "longitude": along_track_data.longitude,
            "sla_unfiltered": along_track_data.sla_unfiltered,
            "sla_filtered": along_track_data.sla_filtered,
            "date_time": date_times,
            "dac": along_track_data.dac,
            "ocean_tide": along_track_data.ocean_tide,
            "internal_tide": along_track_data.internal_tide,
            "lwe": along_track_data.lwe,
            "mdt": along_track_data.mdt,
            "tpa_correction": along_track_data.tpa_correction,
            "basin_id": along_track_data.basin_id,
        })

        df.to_sql(
            name=self.along_track_table_name,
            con=self.get_engine(),
            schema="public",
            if_exists="append",
            index=False,
            chunksize=1000,
            method=upsert_ignore
        )

    def import_metadata_to_psql(self, metadata: AlongTrackMetaData) -> None:
        """Insert metadata into along_track_metadata table, ignoring duplicates."""
        fields = [
            "file_name", "conventions", "metadata_conventions", "cdm_data_type",
            "comment", "contact", "creator_email", "creator_name", "creator_url",
            "date_created", "date_issued", "date_modified", "history", "institution",
            "keywords", "license", "platform", "processing_level", "product_version",
            "project", "references",  # reserved keyword — will be safely quoted
            "software_version", "source", "ssalto_duacs_comment",
            "summary", "title",
        ]

        for k,v in metadata.to_dict().items():
            print(f"k: {k} v: {v}")


        query = sql.SQL("""
            INSERT INTO {table} ({fields})
            VALUES ({placeholders})
            ON CONFLICT (file_name) DO NOTHING;
        """).format(
            table=sql.Identifier(self.along_track_metadata_table_name),
            fields=sql.SQL(', ').join(sql.Identifier(f) for f in fields),
            placeholders=sql.SQL(', ').join(sql.Placeholder() * len(fields)),
        )

        with pg.connect(self.connection_string) as conn:
            with conn.cursor() as cur:
                cur.execute(query, tuple(metadata.__dict__.values()))
            conn.commit()
        print(f"Inserted Metadata for {metadata.file_name}")

    def query_metadata(self):
        query = "SELECT * FROM along_track_metadata;"
        with pg.connect(self.connection_string) as connection:
            with connection.cursor(row_factory=pg.rows.dict_row) as cursor:
                cursor.execute(query)
                rows = cursor.fetchall()
        return set([metadata['file_name'] for metadata in rows])


    def extract_along_track_file(self, file: Path) -> (AlongTrackData, AlongTrackMetaData):
        dataset: nc.Dataset = self.load_netcdf(file)
        along_track_data: AlongTrackData = self.extract_along_track_from_netcdf(ds=dataset, file=file)
        along_track_metadata: AlongTrackMetaData = self.extract_dataset_metadata(
             ds=dataset,
             file=file
        )
        return along_track_data, along_track_metadata

    def extract_eddy_data_from_netcdf(self):
        pass

    def ingest_along_track_file(self, along_track_data: AlongTrackData, along_track_metadata: AlongTrackMetaData):
        self.import_along_track_data_to_postgresql(
             along_track_data=along_track_data
        )
        self.import_metadata_to_psql(
            metadata=along_track_metadata
        )


    def extract_eddy_data(self, file: Path) -> EddyData:
        ds: nc.Dataset = self.load_netcdf(file)

        date_time = ds.variables['time']  # Extract dates from the dataset and convert them to standard datetime

        time_data = nc.num2date(date_time[:], date_time.units, only_use_cftime_datetimes=False,
                                only_use_python_datetimes=False)
        eddy_data = EddyData(
            amplitude=ds.variables['amplitude'][:],
            cost_association=ds.variables['cost_association'][:],
            effective_area= ds.variables['effective_area'][:],
            effective_contour_height= ds.variables['effective_contour_height'][:],
            effective_contour_latitude=ds.variables['effective_contour_latitude'][:],
            effective_contour_longitude=ds.variables['effective_contour_longitude'][:],
            effective_contour_shape_error=ds.variables['effective_contour_shape_error'][:],
            effective_radius=ds.variables['effective_radius'][:],
            inner_contour_height=ds.variables['inner_contour_height'][:],
            latitude=ds.variables['latitude'][:],
            latitude_max=ds.variables['latitude_max'][:],
            longitude= ds.variables['longitude'][:],
            longitude_max=ds.variables['longitude_max'][:],
            num_contours=ds.variables['num_contours'][:],
            num_point_e=ds.variables['num_point_e'][:],
            num_point_s=ds.variables['num_point_s'][:],
            observation_flag=ds.variables['observation_flag'][:],
            observation_number=ds.variables['observation_number'][:],
            speed_area=ds.variables['speed_area'][:],
            speed_average=ds.variables['speed_average'][:],
            speed_contour_height=ds.variables['speed_contour_height'][:],
            # speed_contour_latitude=ds.variables['speed_contour_latitude'][:],
            # speed_contour_longitude=ds.variables['speed_contour_longitude'][:],
            speed_contour_shape_error=ds.variables['speed_contour_shape_error'][:],
            speed_radius=ds.variables['speed_radius'][:],
            date_time=time_data,
            track=ds.variables['track'][:]
            # track=ds.variables['track'][:],
        )
        return eddy_data

    def import_eddy_data_to_postgresql(self, eddy: EddyData, cyclonic_type: int):
        """
        Insert EddyData (columnar arrays) into the PostgreSQL eddy table.
        """

        df = pd.DataFrame({
            "amplitude": eddy.amplitude.astype("float"),  # pandas handles downcast
            "cost_association": eddy.cost_association,
            "effective_area": eddy.effective_area,
            "effective_contour_height": eddy.effective_contour_height,

            # 2D arrays → choose representative element (vertex 0)
            "effective_contour_latitude": eddy.effective_contour_latitude[:, 0],
            "effective_contour_longitude": eddy.effective_contour_longitude[:, 0],

            "effective_contour_shape_error": eddy.effective_contour_shape_error,
            "effective_radius": eddy.effective_radius,
            "inner_contour_height": eddy.inner_contour_height,

            "latitude": eddy.latitude,
            "latitude_max": eddy.latitude_max,
            "longitude": eddy.longitude,
            "longitude_max": eddy.longitude_max,

            "num_contours": eddy.num_contours,
            "num_point_e": eddy.num_point_e,
            "num_point_s": eddy.num_point_s,

            "observation_flag": eddy.observation_flag.astype(bool),
            "observation_number": eddy.observation_number,

            "speed_area": eddy.speed_area,
            "speed_average": eddy.speed_average,
            "speed_contour_height": eddy.speed_contour_height,

            # also 2D arrays
            # "speed_contour_latitude": eddy.speed_contour_latitude[:, 0],
            # "speed_contour_longitude": eddy.speed_contour_longitude[:, 0],

            "speed_contour_shape_error": eddy.speed_contour_shape_error,
            "speed_radius": eddy.speed_radius,

            "date_time": eddy.date_time,
            "track": eddy.track,

            "cyclonic_type": cyclonic_type,

            # leave NULL → PostgreSQL computes eddy_point automatically
            # "speed_contour_shape": None,
        })
        return df
        # df.to_sql(
        #     name="eddy",
        #     con=self.get_engine(),
        #     schema="public",
        #     if_exists="append",
        #     index=False,
        #     chunksize=1000
        # )
