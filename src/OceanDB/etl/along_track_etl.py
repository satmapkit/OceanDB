from dataclasses import dataclass
from dataclasses import asdict
import netCDF4 as nc
import pandas as pd
import psycopg
import psycopg as pg
from psycopg import sql
import time
import numpy as np
from functools import cached_property
from typing import Optional
from datetime import datetime, timedelta
from pathlib import Path

from OceanDB.etl.base_etl import BaseETL


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

        conventions = getattr(ds, "Conventions", None)

        print(f"CONVENTIONS {conventions}")
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


class AlongTrackETL(BaseETL):
    ocean_basin_table_name: str = "basin"
    ocean_basins_connections_table_name: str = "basin_connection"
    along_track_table_name: str = "along_track"
    along_track_metadata_table_name: str = "along_track_metadata"

    variable_add_offset: dict = dict()
    missions = [
        "al",
        "alg",
        "c2",
        "c2n",
        "e1g",
        "e1",
        "e2",
        "en",
        "enn",
        "g2",
        "h2a",
        "h2b",
        "j1g",
        "j1",
        "j1n",
        "j2g",
        "j2",
        "j2n",
        "j3",
        "j3n",
        "s3a",
        "s3b",
        "s6a",
        "tp",
        "tpn",
    ]

    def extract_dataset_metadata(
        self, ds: nc.Dataset, file: Path
    ) -> AlongTrackMetaData:
        return AlongTrackMetaData.from_netcdf(ds, file_name=file.name)

    def extract_data_from_netcdf(self, ds: nc.Dataset, file: Path) -> AlongTrackData:
        """
        Parse & transform NetCDF file
        """
        mission = file.name.split("_")[2]
        try:
            ds.variables["sla_unfiltered"].set_auto_scale(False)
            ds.variables["sla_filtered"].set_auto_scale(False)
            ds.variables["ocean_tide"].set_auto_scale(False)
            ds.variables["internal_tide"].set_auto_scale(False)
            ds.variables["lwe"].set_auto_scale(False)
            ds.variables["mdt"].set_auto_scale(False)
            ds.variables["dac"].set_auto_scale(False)
            ds.variables["tpa_correction"].set_auto_scale(False)

            time_data = ds.variables[
                "time"
            ]  # Extract dates from the dataset and convert them to standard datetime
            time_data = nc.num2date(
                time_data[:],
                time_data.units,
                only_use_cftime_datetimes=False,
                only_use_python_datetimes=False,
            )
            time_data = nc.date2num(
                time_data[:], "microseconds since 2000-01-01 00:00:00"
            )  # Convert the standard date back to the 8-byte integer PSQL uses

            basin_id = self.basin_mask(
                ds.variables["latitude"][:], ds.variables["longitude"][:]
            )

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
                file_name=file.name,
            )
            ds.close()
            return data

        except Exception as ex:
            print(ex)

    def insert_basins_data(self):
        with self.load_module_file(
            module="OceanDB.data", filename="basins/ocean_basins.csv", mode="r"
        ) as f:
            df = pd.read_csv(f)

        df.rename(columns={"geom": "basin_geog"}, inplace=True)

        columns = list(df.columns)
        query = sql.SQL(
            "INSERT INTO {table} ({fields}) VALUES ({placeholders})"
        ).format(
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
        with self.load_module_file(
            module="OceanDB.data",
            filename="basins/ocean_basin_connections.csv",
            mode="r",
        ) as f:
            df = pd.read_csv(f)
        df.rename(
            columns={"basinid": "basin_id", "connected_basin": "connected_id"},
            inplace=True,
        )
        print(df.columns)
        columns = list(df.columns)

        query = sql.SQL(
            "INSERT INTO {table} ({fields}) VALUES ({placeholders})"
        ).format(
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
        with self.load_module_file(
            "OceanDB.data", "basin_masks/new_basin_mask.nc", mode="rb"
        ) as f:
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
        date_times = [
            EPOCH + timedelta(microseconds=int(t)) for t in along_track_data.time
        ]

        # 1. Define the INSERT query
        insert_query = sql.SQL(
            """
                               INSERT INTO {table} (file_name, mission, track, cycle, latitude, longitude,
                                                    sla_unfiltered, sla_filtered, date_time, dac,
                                                    ocean_tide, internal_tide, lwe, mdt, basin_id)
                               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                               """
        ).format(table=sql.Identifier(self.along_track_table_name))

        # 2. Prepare the list of data tuples
        # Using .item() is still recommended to ensure native Python types
        data_to_insert = []
        for i in range(len(along_track_data.time)):
            data_to_insert.append(
                (
                    along_track_data.file_name,
                    along_track_data.mission,
                    along_track_data.track[i].item(),
                    along_track_data.cycle[i].item(),
                    along_track_data.latitude[i].item(),
                    along_track_data.longitude[i].item(),
                    along_track_data.sla_unfiltered[i].item(),
                    along_track_data.sla_filtered[i].item(),
                    date_times[i],
                    along_track_data.dac[i].item(),
                    along_track_data.ocean_tide[i].item(),
                    along_track_data.internal_tide[i].item(),
                    along_track_data.lwe[i].item(),
                    along_track_data.mdt[i].item(),
                    along_track_data.basin_id[i].item(),
                )
            )

        # 3. Execute the batch insert
        with pg.connect(self.config.postgres_dsn) as connection:
            with connection.cursor() as cursor:
                print(f"Starting batch insert of {len(data_to_insert)} rows...")
                cursor.executemany(insert_query, data_to_insert)
            connection.commit()
            print("Successfully inserted all rows.")

    def import_metadata_to_psql(self, metadata: AlongTrackMetaData) -> None:
        """Insert metadata into along_track_metadata table, ignoring duplicates."""
        fields = [
            "file_name",
            "conventions",
            "metadata_conventions",
            "cdm_data_type",
            "comment",
            "contact",
            "creator_email",
            "creator_name",
            "creator_url",
            "date_created",
            "date_issued",
            "date_modified",
            "history",
            "institution",
            "keywords",
            "license",
            "platform",
            "processing_level",
            "product_version",
            "project",
            "references",  # reserved keyword — will be safely quoted
            "software_version",
            "source",
            "ssalto_duacs_comment",
            "summary",
            "title",
        ]

        query = sql.SQL(
            """
            INSERT INTO {table} ({fields})
            VALUES ({placeholders})
            ON CONFLICT (file_name) DO NOTHING;
        """
        ).format(
            table=sql.Identifier(self.along_track_metadata_table_name),
            fields=sql.SQL(", ").join(sql.Identifier(f) for f in fields),
            placeholders=sql.SQL(", ").join(sql.Placeholder() * len(fields)),
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
        return set([metadata["file_name"] for metadata in rows])

    def process_along_track_file(self, file: Path):
        """
        Processes an along track netcdf file & inserts into Postgres
        """
        start = time.perf_counter()

        dataset: nc.Dataset = self.load_netcdf(file)
        along_track_data: AlongTrackData = self.extract_data_from_netcdf(
            ds=dataset, file=file
        )
        along_track_metadata: AlongTrackMetaData = self.extract_dataset_metadata(
            ds=dataset, file=file
        )
        self.import_along_track_data_to_postgresql(along_track_data=along_track_data)
        self.import_metadata_to_psql(metadata=along_track_metadata)
        duration = time.perf_counter() - start
        size_mb = file.stat().st_size / (1024 * 1024)
        print(f"✅ {file.name} | {size_mb:.2f} MB | {duration:.2f} seconds")
