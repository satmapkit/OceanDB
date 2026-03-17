import netCDF4 as nc
import pandas as pd

from psycopg import sql, Cursor

from OceanDB.OceanDB import OceanDB, connect_to_db
from pathlib import Path

from typing import Any


class BaseETL(OceanDB):

    def load_netcdf(self, file: Path) -> nc.Dataset:
        ds = nc.Dataset(file, "r")
        return ds
    
    def connection_string(self):
        return super().connection_string()

    @connect_to_db(connection_string, commit=True)
    def execute_write_query(self, cur: Cursor, query: sql.Composed, data: list[dict[str,Any]]):
        cur.executemany(query, data)

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

        self.execute_write_query(query, data)
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

        self.execute_write_query(query, data)
        print(f"Inserted {len(df)} rows in to the basins table")
