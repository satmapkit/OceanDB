from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Iterable, Mapping, Union, Generic

import numpy as np
import psycopg as pg
from psycopg import sql

from OceanDB.OceanDB import OceanDB
from OceanDB.data_access.metadata import METADATA_REGISTRY
from OceanDB.ocean_data.ocean_data import OceanDataField
from OceanDB.ocean_data.dataset import Dataset, K, T


SqlLike = Union[str, sql.Composable]


@dataclass(frozen=True)
class QuerySpec(Generic[K]):
    """
    Declarative specification for a query.

    Contract:
    - The SQL must SELECT expressions aliased to match keys in `schema`.
      (i.e. SELECT ... AS <field_name>)
    - The schema describes how to type/hydrate returned columns.
    """
    sql_template: SqlLike
    schema: Mapping[K, OceanDataField]
    mandatory_fields : list[K] = field(default_factory=list)

    def sql_projection_compiler(self,
                                fields: Iterable[K]
                                ) -> sql.SQL:
        extra_fields = filter(lambda field: field not in self.mandatory_fields, fields)
        field_sql = sql.SQL(", ").join(
                self.schema[field].to_sql_query()
                for field in extra_fields
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

    METADATA = METADATA_REGISTRY

    def execute_query(
        self,
        query_spec: QuerySpec,
        # query: sql.Composable,
        *,
        fields: Iterable[K] ,
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
            log_query(
                conn=conn,
                query=sql_query,
                params=params
            )

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
            if name not in row0:
                continue

            values = [row[name] for row in rows]

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




    # def run_query(
    #     self,
    #     *,
    #     query_spec: QuerySpec,
    #     params: Mapping[str, Any] | None = None,
    #     dataset_name: str = "query_result",
    #     debug_sql: bool = False,
    #     require_all_fields: bool = True,
    # ) -> Dataset[str, Any] | None:
    #     """
    #     Execute a user-defined QuerySpec.
    #
    #     Returns:
    #         Dataset if rows were returned, else None.
    #
    #     Args:
    #         require_all_fields:
    #             If True, ensures every schema key exists in the first returned row.
    #             If False, silently skips missing fields (your original behavior).
    #     """
    #
    #     compiler = ProjectionCompiler(schema=query_spec.schema)
    #     return self.execute_query(
    #         _normalize_sql(query_spec.sql),
    #         # schema=query_spec.schema,
    #         params=params or {},
    #         dataset_name=dataset_name,
    #         debug_sql=debug_sql,
    #         require_all_fields=require_all_fields,
    #     )

    # def run_batch(
    #     self,
    #     *,
    #     query: QuerySpec,
    #     params_list: Iterable[Mapping[str, Any]],
    #     dataset_name: str = "query_result",
    #     debug_sql: bool = False,
    #     require_all_fields: bool = True,
    # ) -> Iterable[Dataset[str, Any] | None]:
    #     """
    #     Execute the same QuerySpec repeatedly for a batch of parameter sets.
    #
    #     Note: this is intentionally implemented as a Python-side loop.
    #     It's reliable across psycopg3 usage and keeps dataset hydration simple.
    #     """
    #     q = _normalize_sql(query.sql)
    #     for params in params_list:
    #         yield self.execute_query(
    #             q,
    #             schema=query.schema,
    #             params=params,
    #             dataset_name=dataset_name,
    #             debug_sql=debug_sql,
    #             require_all_fields=require_all_fields,
    #         )


    # def execute_query(
    #     self,
    #     query_spec: QuerySpec,
    #     # query: sql.Composable,
    #     *,
    #     params: Mapping[str, Any],
    #     dataset_name: str = "query_result",
    #     debug_sql: bool = False,
    #     require_all_fields: bool = True,
    # ) -> Dataset[K, Any] | None:
    #     """
    #     Execute a single query and return a Dataset, or None if empty.
    #     """
    #     with pg.connect(self.config.postgres_dsn) as conn:
    #         if debug_sql:
    #             log_query(conn=conn, query=query_spec.sql, params=params)
    #
    #         with conn.cursor(row_factory=pg.rows.dict_row) as cur:
    #             cur.execute(query_spec.sql, params)
    #             rows: list[dict[str, Any]] = cur.fetchall()
    #
    #     if not rows:
    #         return None
    #
    #     if require_all_fields:
    #         missing = [k for k in query_spec.schema.keys() if k not in rows[0]]
    #         if missing:
    #             raise ValueError(
    #                 f"Query did not return expected fields: {missing}. "
    #                 "Make sure your SELECT expressions are aliased to match schema keys."
    #             )
    #
    #     return self.build_dataset(
    #         schema=query_spec.schema,
    #         rows=rows,
    #         dataset_name=dataset_name,
    #     )
    #
    # def build_dataset(
    #     self,
    #     *,
    #     schema: Mapping[K, OceanDataField],
    #     rows: list[Mapping[str, Any]],
    #     dataset_name: str = "query_result",
    # ) -> Dataset[K, Any]:
    #     """
    #     Given a schema and a nonempty list of dict rows, construct a Dataset.
    #
    #     Notes:
    #     - Row keys are expected to match schema keys (SQL aliases).
    #     - Missing keys are skipped unless `require_all_fields=True` is enforced upstream.
    #     """
    #     if not rows:
    #         raise ValueError("rows must be nonempty")
    #
    #     data: dict[K, np.ndarray] = {}
    #     dtypes: dict[K, type | None] = {}
    #
    #     row0 = rows[0]
    #
    #     for name, field in schema.items():
    #         if name not in row0:
    #             continue
    #
    #         values = [row[name] for row in rows]
    #
    #         if field.python_type is not None:
    #             arr = np.asarray(values, dtype=field.python_type)
    #         else:
    #             arr = np.asarray(values)
    #
    #         data[name] = arr
    #         dtypes[name] = field.python_type
    #
    #     return Dataset(
    #         name=dataset_name,
    #         data=data,
    #         dtypes=dtypes,
    #         schema=schema,
    #     )
