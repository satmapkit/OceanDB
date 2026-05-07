from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence, TypeVar

import click
import netCDF4 as nc
import numpy as np
from psycopg import sql
from pathlib import Path

from OceanDB.OceanDB import OceanDB
from OceanDB.cli_utils import format_status_line, style_value
from OceanDB.ocean_data.ocean_data import ColumnField

K = TypeVar("K", bound=str)
batch = list[dict[K, Any]]
ValueAdapter = Callable[[ColumnField, Any], Any]


class OceanDBETL(OceanDB):
    def __init__(self, *args, debug: bool = False, **kwargs):
        super().__init__(*args, **kwargs)
        self.debug = debug

    def set_debug(self, enabled: bool) -> None:
        self.debug = enabled

    def debug_log(self, message: str) -> None:
        if self.debug:
            timestamp = style_value(
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                fg="bright_black",
            )
            label = "DEBUG"
            body = message

            if message.startswith("[") and "]" in message:
                closing_bracket = message.find("]")
                label = message[1:closing_bracket]
                body = message[closing_bracket + 1 :].strip()

            click.echo(
                format_status_line(
                    label,
                    f"{timestamp} {body}",
                    label_color="yellow",
                ),
                color=True,
            )

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
        value_adapter: ValueAdapter | None = None,
        ignore_conflicts: bool = True,
    ) -> None:
        if not data:
            return

        columns = list(schema.values())
        normalized_rows = self._normalize_schema_rows(
            schema=schema,
            data=data,
            value_adapter=value_adapter,
        )

        if self.config.ingest_mode == "copy":
            self._copy_schema_rows_to_postgresql(
                table_name=table_name,
                columns=columns,
                rows=normalized_rows,
                ignore_conflicts=ignore_conflicts,
            )
            return

        self._insert_schema_rows_to_postgresql(
            table_name=table_name,
            columns=columns,
            rows=normalized_rows,
            ignore_conflicts=ignore_conflicts,
        )

    @staticmethod
    def _normalize_value(value: Any) -> Any:
        if np.ma.is_masked(value):
            return None

        if isinstance(value, np.generic):
            value = value.item()

        if value is None:
            return None

        if isinstance(value, float) and np.isnan(value):
            return None

        return value

    def _normalize_schema_rows(
        self,
        *,
        schema: Mapping[K, ColumnField],
        data: Sequence[Mapping[K, Any]],
        value_adapter: ValueAdapter | None = None,
    ) -> list[tuple[Any, ...]]:
        adapter = value_adapter or (lambda field, value: self._normalize_value(value))
        return [
            tuple(adapter(field, row[field_name]) for field_name, field in schema.items())
            for row in data
        ]

    def _insert_schema_rows_to_postgresql(
        self,
        *,
        table_name: str,
        columns: Sequence[ColumnField],
        rows: Sequence[tuple[Any, ...]],
        ignore_conflicts: bool,
    ) -> None:
        insert_query = sql.SQL("""
            INSERT INTO {table} ({columns})
            VALUES ({placeholders})
        """).format(
            table=sql.Identifier("public", table_name),
            columns=sql.SQL(", ").join(
                sql.Identifier(field.postgres_column_name) for field in columns
            ),
            placeholders=sql.SQL(", ").join(
                sql.Placeholder() for _ in columns
            ),
        )
        if ignore_conflicts:
            insert_query += sql.SQL(" ON CONFLICT DO NOTHING")

        with self.cursor(commit=True) as cur:
            cur.executemany(insert_query, rows)

    def _copy_schema_rows_to_postgresql(
        self,
        *,
        table_name: str,
        columns: Sequence[ColumnField],
        rows: Sequence[tuple[Any, ...]],
        ignore_conflicts: bool,
    ) -> None:
        temp_table_name = f"temp_{table_name}_ingest"
        target_table = sql.Identifier("public", table_name)
        temp_table = sql.Identifier(temp_table_name)
        column_list = sql.SQL(", ").join(
            sql.Identifier(field.postgres_column_name) for field in columns
        )

        create_temp_query = sql.SQL("""
            CREATE TEMP TABLE {temp_table} AS
            SELECT {columns}
            FROM {target_table}
            WHERE FALSE
        """).format(
            temp_table=temp_table,
            columns=column_list,
            target_table=target_table,
        )
        copy_query = sql.SQL("""
            COPY {temp_table} ({columns}) FROM STDIN
        """).format(
            temp_table=temp_table,
            columns=column_list,
        )
        merge_query = sql.SQL("""
            INSERT INTO {target_table} ({columns})
            SELECT {columns}
            FROM {temp_table}
        """).format(
            target_table=target_table,
            columns=column_list,
            temp_table=temp_table,
        )
        if ignore_conflicts:
            merge_query += sql.SQL(" ON CONFLICT DO NOTHING")

        with self.cursor(commit=True) as cur:
            cur.execute(create_temp_query)
            with cur.copy(copy_query) as copy:
                for row in rows:
                    copy.write_row(row)
            cur.execute(merge_query)
