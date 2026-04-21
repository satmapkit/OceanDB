import pandas as pd
from psycopg import sql

from OceanDB.etl.base_etl import OceanDBETL


class BasinsETL(OceanDBETL):
    basin_table_name: str = "basin"
    basin_connections_table_name: str = "basin_connections"

    def _insert_csv(
        self,
        *,
        module: str,
        filename: str,
        table_name: str,
        rename_map: dict[str, str] | None = None,
    ) -> int:
        with self.load_module_file(module=module, filename=filename, mode="r") as f:
            df = pd.read_csv(f)

        if rename_map:
            df.rename(columns=rename_map, inplace=True)

        columns = list(df.columns)
        query: sql.Composed = sql.SQL(
            "INSERT INTO {table} ({fields}) VALUES ({placeholders})"
        ).format(
            table=sql.Identifier(table_name),
            fields=sql.SQL(", ").join(map(sql.Identifier, columns)),
            placeholders=sql.SQL(", ").join(sql.Placeholder() * len(columns)),
        )

        data = df.to_records(index=False).tolist()

        with self.cursor(commit=True) as cur:
            cur.executemany(query, data)

        return len(df)

    def insert_basins_data(self):
        if self._table_has_rows(self.basin_table_name):
            print("Skipping basin seed data: basin table already contains rows")
            return

        row_count = self._insert_csv(
            module="OceanDB.data",
            filename="basins/ocean_basins.csv",
            table_name=self.basin_table_name,
            rename_map={"geom": "basin_geog"},
        )
        print(f"Inserted {row_count} rows in to the basins table")

    def insert_basin_connections_data(self):
        if self._table_has_rows(self.basin_connections_table_name):
            print(
                "Skipping basin connection seed data: basin_connections table already contains rows"
            )
            return

        row_count = self._insert_csv(
            module="OceanDB.data",
            filename="basins/ocean_basin_connections.csv",
            table_name=self.basin_connections_table_name,
            rename_map={"basinid": "basin_id", "connected_basin": "connected_id"},
        )
        print(f"Inserted {row_count} rows in to the basin connections table")
