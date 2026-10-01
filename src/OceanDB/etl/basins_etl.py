from typing import Generator

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

    def _read_basin_connections_txt(
        self,
        *,
        module: str,
        filename: str,
        header_prefix: str = "HDR",
        key_sep: str = ":",
        value_sep: str = ",",
    ) -> Generator[tuple[int, int], None, None]:
        with self.load_module_file(module=module, filename=filename, mode="r") as f:
            for line in f:
                if line.startswith(header_prefix):
                    continue
                try:
                    key_str, values_str = line.split(key_sep)
                    key = int(key_str)
                    values = [int(x) for x in values_str.split(value_sep)]
                except ValueError:
                    raise ValueError(
                        "Error parsing basin connections table: "
                        + f'expected format "1{key_sep}1{value_sep}2{value_sep}3", '
                        + f'instead got "{line}"'
                    )

                for value in values:
                    yield (key, value)

    def _insert_basin_connections_txt(
        self,
        *,
        module: str,
        filename: str,
        table_name: str,
        key_name: str,
        value_name: str,
        header_prefix: str = "HDR",
        key_sep: str = ":",
        value_sep: str = ",",
        batch_size: int = 1000,
    ) -> int:
        if batch_size < 1:
            raise ValueError("batch_size must be positive")

        columns = (key_name, value_name)

        query: sql.Composed = sql.SQL(
            "INSERT INTO {table} ({fields}) VALUES ({placeholders})"
        ).format(
            table=sql.Identifier(table_name),
            fields=sql.SQL(", ").join(map(sql.Identifier, columns)),
            placeholders=sql.SQL(", ").join(sql.Placeholder() * len(columns)),
        )

        data = self._read_basin_connections_txt(
            module=module,
            filename=filename,
            header_prefix=header_prefix,
            key_sep=key_sep,
            value_sep=value_sep,
        )

        row_count = 0

        while True:
            batch = [x for x, _ in zip(data, range(batch_size))]
            if not batch:
                break
            row_count += len(batch)

            with self.cursor(commit=True) as cur:
                cur.executemany(query, batch)

        return row_count

    def insert_basins_data(self):
        if self._table_has_rows(self.basin_table_name):
            self.logger.info(
                "Skipping basin seed data: basin table already contains rows"
            )
            return

        row_count = self._insert_csv(
            module="OceanDB.data",
            filename="basins/ocean_basins.csv",
            table_name=self.basin_table_name,
            rename_map={"geom": "basin_geog"},
        )
        self.vacuum_analyze(self.basin_table_name)
        self.logger.info(f"Inserted {row_count} rows in to the basins table")

    def insert_basin_connections_data(self):
        if self._table_has_rows(self.basin_connections_table_name):
            self.logger.info(
                "Skipping basin connection seed data: basin_connections table already contains rows"
            )
            return

        row_count = self._insert_basin_connections_txt(
            module="OceanDB.data",
            filename="basins/basin_connection_table.txt",
            table_name=self.basin_connections_table_name,
            key_name="basin_id",
            value_name="connected_id",
        )
        self.vacuum_analyze(self.basin_connections_table_name)
        self.logger.info(f"Inserted {row_count} rows in to the basin connections table")
