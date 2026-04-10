import netCDF4 as nc
import pandas as pd
from psycopg import sql
from typing import Any, TypeVar, Mapping, Sequence, cast
from pathlib import Path

from OceanDB.OceanDB import OceanDB
from OceanDB.ocean_data.ocean_data import ColumnField

K = TypeVar("K", bound=str)
batch = list[dict[K, Any]]


class BaseETL(OceanDB):

    def _table_has_rows(self, table_name: str) -> bool:
        query = sql.SQL("SELECT EXISTS (SELECT 1 FROM {table} LIMIT 1)").format(
            table=sql.Identifier(table_name)
        )
        with self.cursor() as cur:
            cur.execute(query)
            result = cur.fetchone()
        return bool(result and result[0])

    def load_netcdf(self, file: Path) -> nc.Dataset:
        ds = nc.Dataset(file, "r")
        return ds

    def import_schema_rows_to_postgresql(
        self,
        *,
        table_name: str,
        schema: Mapping[K, ColumnField],
        data: Sequence[Mapping[K, Any]],
    ) -> None:
        columns = [field.postgres_column_name for field in schema.values()]
        rows = cast(Sequence[Mapping[str, Any]], data)

        insert_query = sql.SQL("""
               INSERT INTO {} ({})
               VALUES ({})
               ON CONFLICT DO NOTHING
           """).format(
            sql.Identifier("public", table_name),
            sql.SQL(", ").join(map(sql.Identifier, columns)),
            sql.SQL(", ").join(map(sql.Placeholder, columns)),
        )

        try:
            with self.cursor(commit=True) as cur:
                cur.executemany(insert_query, rows)
        except Exception as e:
            print("INSERT FAILED:", e)
            raise

    def insert_basins_data(self):
        if self._table_has_rows("basin"):
            print("Skipping basin seed data: basin table already contains rows")
            return

        with self.load_module_file(
            module="OceanDB.data", filename="basins/ocean_basins.csv", mode="r"
        ) as f:
            df = pd.read_csv(f)

        df.rename(columns={"geom": "basin_geog"}, inplace=True)

        columns = list(df.columns)
        query: sql.Composed = sql.SQL(
            "INSERT INTO {table} ({fields}) VALUES ({placeholders})"
        ).format(
            table=sql.Identifier("basin"),
            fields=sql.SQL(", ").join(map(sql.Identifier, columns)),
            placeholders=sql.SQL(", ").join(sql.Placeholder() * len(columns)),
        )

        data = df.to_records(index=False).tolist()

        with self.cursor(commit=True) as cur:
            cur.executemany(query, data)

        print(f"Inserted {len(df)} rows in to the basins table")

    def insert_basin_connections_data(self):
        if self._table_has_rows("basin_connections"):
            print(
                "Skipping basin connection seed data: basin_connections table already contains rows"
            )
            return

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

        query: sql.Composed = sql.SQL(
            "INSERT INTO {table} ({fields}) VALUES ({placeholders})"
        ).format(
            table=sql.Identifier("basin_connections"),
            fields=sql.SQL(", ").join(map(sql.Identifier, columns)),
            placeholders=sql.SQL(", ").join(sql.Placeholder() * len(columns)),
        )

        data = df.to_records(index=False).tolist()

        with self.cursor(commit=True) as cur:
            cur.executemany(query, data)

        print(f"Inserted {len(df)} rows in to the basins table")
