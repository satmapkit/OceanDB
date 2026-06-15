import time
from pathlib import Path
from typing import Any, Iterator

import netCDF4 as nc

from OceanDB.etl.base_etl import OceanDBETL, batch
from OceanDB.ocean_data.ocean_data import ColumnField
from OceanDB.schemas.eddy_schema import eddy_columns, eddy_columns_schema


class EddyETL(OceanDBETL):
    def ingest_eddy_data_file(
        self, file: Path, cyclonic_type: int, batch_size: int = 500000, offset: int = 0
    ):
        """
        Processes & Ingests Eddy Data NetCDF file
        """
        dataset = self.load_netcdf(file)
        for eddy_data_batch in self.extract_eddy_data_batches_from_netcdf(
            dataset,
            cyclonic_type=cyclonic_type,
            batch_size=batch_size,
            offset=offset,
        ):
            start = time.perf_counter()
            self.import_eddy_data_to_postgresql(eddy_data=eddy_data_batch)
            duration = time.perf_counter() - start
            print(f"✅ Ingested Eddy Data Points took {duration:.2f} seconds")

    def extract_eddy_data_batches_from_netcdf(
        self, ds: nc.Dataset, cyclonic_type: int, batch_size: int, offset: int
    ) -> Iterator[batch[eddy_columns]]:
        """
        Yield batches of eddy data from a NetCDF dataset.

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

        self.verify_netcdf_schema_scaling(
            ds=ds,
            schema=eddy_columns_schema,
            context="[EDDY]",
        )

        fields_in_ds: list[tuple[eddy_columns, ColumnField]] = [
            (name, field)
            for name, field in eddy_columns_schema.items()
            if field.in_netcdf(ds)
        ]

        # certain fields have to be scaled
        ds["latitude"].set_auto_scale(True)
        ds["longitude"].set_auto_scale(True)
        ds["effective_contour_latitude"].set_auto_scale(True)
        ds["effective_contour_longitude"].set_auto_scale(True)
        ds["speed_contour_latitude"].set_auto_scale(True)
        ds["speed_contour_longitude"].set_auto_scale(True)

        for start in range(offset, n_total, batch_size):
            stop = min(start + batch_size, n_total)

            vars_slice: dict[eddy_columns, Any] = {
                name: field.from_netcdf(ds, slice(start, stop))
                for name, field in fields_in_ds
            }
            vars_slice["cyclonic_type"] = [cyclonic_type] * (stop - start)

            # get expected data from the netcdf. Certain fields
            # that are expected in the database (e.g. date_time)
            # may not exist in the netcdf, and thus will not be returned.
            yield [
                {name: values[i] for name, values in vars_slice.items()}
                for i in range(stop - start)
            ]

    def import_eddy_data_to_postgresql(self, eddy_data: batch[eddy_columns]):
        """
        Insert eddy records into PostgreSQL using INSERT statements.

        COPY implicitly casts 0/1 -> boolean
        INSERT Strict typing - smallint != boolean

        ON CONFLICT DO NOTHING // Ignores rows that already exist

        """

        self.import_schema_rows_to_postgresql(
            table_name="eddy",
            schema=eddy_columns_schema,
            data=eddy_data,
        )
