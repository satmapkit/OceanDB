from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Iterable, Mapping, Generic, LiteralString

import numpy as np
import psycopg as pg
from psycopg import sql

from OceanDB.OceanDB import OceanDB
from OceanDB.ocean_data.ocean_data import OceanDataField
from OceanDB.ocean_data.dataset import Dataset, K


@dataclass(frozen=True)
class QuerySpec(Generic[K]):
    """
    Declarative specification for a query.

    :param sql_template:
        See :attr:`sql_template`

    :param schema:
        See :attr:`schema`

    :param mandatory_fields:
        See :attr:`mandatory_fields`
    """

    sql_template: LiteralString
    """
    The template string for the sql query, typically read from a file. The
    template format admits *parameters* and *fields* to be generic. For
    generic fields, add :code:`{fields}` to the select statement, and for
    parameters, each parameter should be included as per psycopg's parsing,
    e.g. :code:`%(basin_ids)s::int[]`.

    For example::

        SELECT {fields}
        FROM eddy
        WHERE eddy.track * eddy.cyclonic_type = %(track_id)s;

    Here, there is only one parameter, :code:`track_id`, but potentially many
    fields (see :attr:`schema`)
    """

    schema: Mapping[K, OceanDataField]
    """
    Schema for field validation, in the form of a mapping from
    field names to an :class:`OceanDataField <OceanDB.ocean_data.ocean_data.OceanDataField>`
    (which contains details on how to extract a field from a column)
    """

    mandatory_fields: list[K] = field(default_factory=list)
    """
    In case a particular query must always SELECT one or more fields, these fields should be
    included in this list. By default, no queries are mandatory.

    For example, in the following query, the :code:`distance` field must always be computed::

        SELECT
            {fields}
            atk.along_track_point <-> ST_SetSRID(ST_MakePoint(%(longitude)s, %(latitude)s), 4326)
            AS distance
        FROM along_track atk
        ORDER BY distance
        LIMIT 3;
    """

    def sql_projection_compiler(self, fields: Iterable[K]) -> sql.Composed:
        extra_fields = filter(lambda field: field not in self.mandatory_fields, fields)
        field_sql = sql.SQL(", ").join(
            self.schema[field].to_sql_query() for field in extra_fields
        )
        if self.mandatory_fields and extra_fields:
            field_sql += sql.SQL(",")

        return sql.SQL(self.sql_template).format(fields=field_sql)


def _normalize_sql(q: SqlLike) -> sql.Composable:
    """
    Ensure we always pass a psycopg composable to cursor.execute().
    """
    return q if isinstance(q, sql.Composable) else sql.SQL(q)


def log_query(conn: pg.Connection, query: sql.Composable, params: Any) -> None:
    print("\n--- SQL QUERY ---")
    print(query.as_string(conn))
    print("--- PARAMS ---")
    print(params)
    print("----------------\n")


class BaseQuery(OceanDB):
    """
    Base class for read-only query services.

    Supports:
    (A) curated, first-class schemas (AlongTrackSchema, EddySchema, etc.)
    (B) ad-hoc user-supplied queries + schemas via QuerySpec

    The core contract is:
    - SQL aliases == schema keys
    - build_dataset hydrates a columnar Dataset from dict_row results
    """

    def execute_query(
        self,
        query_spec: QuerySpec,
        # query: sql.Composable,
        *,
        fields: Iterable[K],
        params: Mapping[str, Any],
        dataset_name: str = "query_result",
        debug_sql: bool = False,
    ) -> Dataset[K, Any] | None:
        """
        Execute a single query and return a Dataset, or None if empty.
        """

        sql_query = query_spec.sql_projection_compiler(fields)

        print(sql_query)

        with pg.connect(self.config.postgres_dsn) as conn:
            if debug_sql:
                log_query(conn=conn, query=sql_query, params=params)

            with conn.cursor(row_factory=pg.rows.dict_row) as cur:
                cur.execute(sql_query, params)
                rows: list[Mapping[str, Any]] = cur.fetchall()

        if not rows:
            return None

        return self.build_dataset(
            schema=query_spec.schema,
            rows=rows,
            dataset_name=dataset_name,
        )

    def build_dataset(
        self,
        *,
        schema: Mapping[K, OceanDataField],
        rows: list[Mapping[str, Any]],
        dataset_name: str = "query_result",
    ) -> Dataset[K, Any]:
        """
        Given a schema and a nonempty list of dict rows, construct a Dataset.

        Notes:
        - Row keys are expected to match schema keys (SQL aliases).
        - Missing keys are skipped.
        """
        if not rows:
            raise ValueError("rows must be nonempty")

        data: dict[K, np.ndarray] = {}
        dtypes: dict[K, type] = {}

        row0 = rows[0]

        for name, field in schema.items():
            if field.name not in row0:
                continue

            values = [row[field.name] for row in rows]

            if field.python_type is not None:
                arr = np.asarray(values, dtype=field.python_type)
            else:
                arr = np.asarray(values)

            data[name] = arr
            if field.python_type is not None:
                dtypes[name] = field.python_type

        return Dataset(
            name=dataset_name,
            data=data,
            dtypes=dtypes,
            schema=schema,
        )
