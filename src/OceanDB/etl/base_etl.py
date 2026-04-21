from pathlib import Path
from typing import Any, Mapping, Sequence, TypeVar, cast

import netCDF4 as nc
from psycopg import sql

from OceanDB.ocean_data.ocean_data import ColumnField
from OceanDB.OceanDB import OceanDB

K = TypeVar("K", bound=str)
batch = list[dict[K, Any]]


class OceanDBETL(OceanDB):

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
