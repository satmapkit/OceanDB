import netCDF4 as nc

import psycopg as pg
from psycopg import sql
import time
from typing import Iterator, Any
from pathlib import Path

from OceanDB.etl import BaseETL, batch
from OceanDB.ocean_data.ocean_data import ColumnField
from OceanDB.schemas.eddy_schema import eddy_columns, eddy_columns_schema


class EddyETL(BaseETL):
    def ingest_eddy_data_file(self, file: Path, cyclonic_type):
        """
        Processes & Ingests Eddy Data NetCDF file
        """
        dataset = self.load_netcdf(file)
        for eddy_data_batch in self.extract_eddy_data_batches_from_netcdf(
            dataset, batch_size=500000
        ):
            start = time.perf_counter()
            self.import_eddy_data_to_postgresql(
                eddy_data=eddy_data_batch, cyclonic_type=cyclonic_type
            )
            duration = time.perf_counter() - start
            print(f"✅ Ingested Eddy Data Points took {duration:.2f} seconds")

    def extract_eddy_data_batches_from_netcdf(
        self,
        ds: nc.Dataset,
        batch_size: int,
    ) -> Iterator[batch[eddy_columns]]:
        """
        Yield batches of eddy data from a NetCDF dataset.

        This function assumes the eddy time variable is stored as
        Unix seconds (uint32), despite metadata claiming
        'days since 1950-01-01'.

        :param ds:
            Open NetCDF dataset
        :param batch_size:
            Number of points per batch

        :returns:
            For each batch, yields a list of dictionaries of data from the netcdf,
            filtered to only expected output data and processed
        """
        ds.set_auto_mask(True)
        ds.set_auto_maskandscale(False)

        obs_var = ds.variables["observation_number"]
        n_total = obs_var.shape[0]

        fields_in_ds: list[tuple[eddy_columns, ColumnField]] = [
            (name, field)
            for name, field in eddy_columns_schema.items()
            if field.netcdf_name in ds.variables
        ]

        for start in range(0, n_total, batch_size):
            stop = min(start + batch_size, n_total)

            vars_slice: dict[eddy_columns, Any] = {
                name: field.from_netcdf(ds, slice(start, stop))
                for name, field in fields_in_ds
            }

            # get expected data from the netcdf. Certain fields
            # that are expected in the database (e.g. date_time)
            # may not exist in the netcdf, and thus will not be returned.
            yield [
                {name: values[i] for name, values in vars_slice.items()}
                for i in range(start, stop)
            ]

    def import_eddy_data_to_postgresql(
        self, eddy_data: batch[eddy_columns], cyclonic_type: int
    ):
        """
        Insert eddy records into PostgreSQL using INSERT statements.

        COPY implicitly casts 0/1 -> boolean
        INSERT Strict typing - smallint != boolean

        ON CONFLICT DO NOTHING // Ignores rows that already exist

        """

        columns = [field.postgres_column_name for field in eddy_columns_schema.values()]

        insert_query = sql.SQL("""
               INSERT INTO {} ({})
               VALUES ({})
               ON CONFLICT DO NOTHING
           """).format(
            sql.Identifier("public", "eddy"),
            sql.SQL(", ").join(map(sql.Identifier, columns)),
            sql.SQL(", ").join(map(sql.Placeholder, columns)),
        )

        data = [
            {
                **row,
                eddy_columns_schema[
                    "cyclonic_type"
                ].postgres_column_name: cyclonic_type,
            }
            for row in eddy_data
        ]

        try:
            with pg.connect(self.config.postgres_dsn) as conn:
                with conn.cursor() as cur:
                    cur.executemany(insert_query, data)
        except Exception as e:
            print("INSERT FAILED:", e)
            raise
