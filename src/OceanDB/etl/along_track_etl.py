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
from typing import Any, Optional, Iterator, TypeVar, Sequence, Mapping, cast
from pathlib import Path

from OceanDB.etl.base_etl import BaseETL
from OceanDB.ocean_data.ocean_data import ColumnField
from OceanDB.schemas.along_track_schema import (
    along_track_columns,
    along_track_columns_schema,
)

K = TypeVar("K", bound=str)
batch = Sequence[Mapping[K, Any]]


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

    def extract_data_from_netcdf(
        self,
        ds: nc.Dataset,
        batch_size: int,
        file: Path,
    ) -> Iterator[batch[along_track_columns]]:
        """
        Parse & transform NetCDF file
        """

        # Mask, but don't scale
        ds.set_auto_mask(True)
        ds.set_auto_maskandscale(False)

        # pull out mission
        file_parts = file.name.split("_")
        if len(file_parts) < 3:
            raise ValueError(f"Could not parse mission from file name: {file.name}")

        mission = file_parts[2]
        if mission not in self.missions:
            raise ValueError(f"Unsupported mission '{mission}' in file: {file.name}")

        # extract fields that are both expected and in the dataset
        derived_fields = {"file_name", "mission", "basin_id"}
        required_fields: list[tuple[along_track_columns, ColumnField]] = [
            (name, field)
            for name, field in along_track_columns_schema.items()
            if name not in derived_fields
        ]

        missing_variables = [
            field.netcdf_name
            for _, field in required_fields
            if field.netcdf_name not in ds.variables
        ]
        if missing_variables:
            missing = ", ".join(sorted(missing_variables))
            raise ValueError(f"Missing required variables in {file.name}: {missing}")

        fields_in_ds: list[tuple[along_track_columns, ColumnField]] = [
            (name, field)
            for name, field in required_fields
            if field.netcdf_name in ds.variables
        ]

        # certain fields have to be scaled
        # TODO:
        ds["latitude"].set_auto_scale(True)
        ds["longitude"].set_auto_scale(True)

        n_total = len(ds["latitude"])
        for start in range(0, n_total, batch_size):
            stop = min(start + batch_size, n_total)
            vars_slice: dict[along_track_columns, Any] = {
                name: field.from_netcdf(ds, slice(start, stop)) for name, field in fields_in_ds
            }

            # file_name, mission, and basin_id are part of the DB schema but derived here
            # rather than read directly from the NetCDF dataset.
            vars_slice["file_name"] = [file.name] * len(vars_slice["latitude"])
            vars_slice["mission"] = [mission] * len(vars_slice["latitude"])
            latitude = vars_slice["latitude"]
            longitude = vars_slice["longitude"]
            vars_slice["basin_id"] = self.basin_mask(latitude, longitude)

            yield [
                {name: values[i] for name, values in vars_slice.items()}
                for i in range(stop - start)
            ]

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
                cur.executemany(query, data)
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
                cur.executemany(query, data)
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

    def import_along_track_data_to_postgresql(
        self, along_track_data: batch[along_track_columns]
    ):
        """
        Insert along-track records into PostgreSQL using INSERT statements.
        """

        columns = [
            field.postgres_column_name for field in along_track_columns_schema.values()
        ]

        insert_query = sql.SQL("""
               INSERT INTO {} ({})
               VALUES ({})
               ON CONFLICT DO NOTHING
           """).format(
            sql.Identifier("public", self.along_track_table_name),
            sql.SQL(", ").join(map(sql.Identifier, columns)),
            sql.SQL(", ").join(map(sql.Placeholder, columns)),
        )

        data = cast(list[Mapping[str, Any]], along_track_data)

        try:
            with pg.connect(self.config.postgres_dsn) as conn:
                with conn.cursor() as cur:
                    cur.executemany(insert_query, data)
        except Exception as e:
            print("INSERT FAILED:", e)
            raise

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

        query = sql.SQL("""
            INSERT INTO {table} ({fields})
            VALUES ({placeholders})
            ON CONFLICT (file_name) DO NOTHING;
        """).format(
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

    def process_along_track_file(self, file: Path, batch_size: int = 500000):
        """
        Processes an along track netcdf file & inserts into Postgres
        """
        start = time.perf_counter()

        dataset: nc.Dataset = self.load_netcdf(file)
        along_track_metadata: AlongTrackMetaData = self.extract_dataset_metadata(
            ds=dataset, file=file
        )
        for data_batch in self.extract_data_from_netcdf(
            ds=dataset, file=file, batch_size=batch_size
        ):
            self.import_along_track_data_to_postgresql(data_batch)
        self.import_metadata_to_psql(metadata=along_track_metadata)
        duration = time.perf_counter() - start
        size_mb = file.stat().st_size / (1024 * 1024)
        print(f"✅ {file.name} | {size_mb:.2f} MB | {duration:.2f} seconds")
