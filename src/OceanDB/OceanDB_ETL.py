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
from typing import List, Tuple, Any, Iterable, Optional
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


class OceanDBETl(OceanDB):
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

    def load_netcdf(self, file: Path) -> nc.Dataset:
        ds = nc.Dataset(file, 'r')
        return ds

    def extract_dataset_metadata(self, ds: nc.Dataset, file: Path) -> AlongTrackMetaData:
        return AlongTrackMetaData.from_netcdf(ds, file_name=file.name)

    def extract_data_from_netcdf(self, ds: nc.Dataset, file: Path) -> AlongTrackData:
        """
        Parse & transform NetCDF file
        """
        mission = file.name.split('_')[2]
        try:
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

        except Exception as ex:
            print(ex)

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

        copy_query = sql.SQL(
            "COPY {along_tbl_nme} (  file_name, mission, track, cycle, latitude, longitude, sla_unfiltered, sla_filtered, date_time, dac, ocean_tide, internal_tide, lwe, mdt, basin_id) FROM STDIN").format(
            along_tbl_nme=sql.Identifier(self.along_track_table_name))

        with pg.connect(self.config.postgres_dsn) as connection:
            with connection.cursor() as cursor:
                with cursor.copy(copy_query) as copy:
                    for i in range(len(along_track_data.time)):
                        copy.write_row([

                            along_track_data.file_name,
                            along_track_data.mission,
                            along_track_data.track[i],
                            along_track_data.cycle[i],
                            along_track_data.latitude[i],
                            along_track_data.longitude[i],
                            along_track_data.sla_unfiltered[i],
                            along_track_data.sla_filtered[i],
                            date_times[i],
                            along_track_data.dac[i],
                            along_track_data.ocean_tide[i],
                            along_track_data.internal_tide[i],
                            along_track_data.lwe[i],
                            along_track_data.mdt[i],
                            along_track_data.basin_id[i]
                        ])


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
        along_track_data: AlongTrackData = self.extract_data_from_netcdf(ds=dataset, file=file)
        along_track_metadata: AlongTrackMetaData = self.extract_dataset_metadata(
             ds=dataset,
             file=file
        )
        return along_track_data, along_track_metadata

    def ingest_along_track_file(self, along_track_data: AlongTrackData, along_track_metadata: AlongTrackMetaData):
        self.import_along_track_data_to_postgresql(
             along_track_data=along_track_data
        )
        self.import_metadata_to_psql(
            metadata=along_track_metadata
        )
