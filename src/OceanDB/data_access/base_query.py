from __future__ import annotations

from typing import Any, Iterable, Mapping

import numpy as np
from psycopg.rows import dict_row

from OceanDB.ocean_data.dataset import Dataset, K
from OceanDB.ocean_data.ocean_data import OceanDataField
from OceanDB.OceanDB import OceanDB
from OceanDB.query_spec import QuerySpec, log_query


class BaseReadQuery(OceanDB):
    """
    Base class for read-only query services.

    Supports:

    * ad-hoc user-supplied queries + schemas via QuerySpec
    * output processed into arbitrary schemas

    The core contract is:

    * SQL aliases == schema keys
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.debug = False

    def execute_read_query(
        self,
        query_spec: QuerySpec,
        *,
        fields: Iterable[K],
        params: Mapping[str, Any],
        dataset_name: str = "query_result",
    ) -> Dataset[K, Any] | None:
        """
        Execute a single query and return a Dataset, or None if empty.

        :param query_spec:
            The specification for the query to be made

        :param fields:
            Set of fields to be extracted from the query

        :param params:
            Set of parameters to be passed to the query

        :param dataset_name:
            Name to give to the resulting dataset
            (this is mostly used for output to NETCDF)
        """

        sql_query = query_spec.sql_projection_compiler(fields)

        with self.cursor(row_factory=dict_row, debug=self.debug) as cur:
            if self.debug:
                log_query(conn=cur.connection, cursor=cur, query=sql_query, params=params)

            cur.execute(sql_query, params)
            rows: list[Mapping[str, Any]] = cur.fetchall()

        if not rows:
            return None

        return self._build_dataset(
            schema=query_spec.schema,
            rows=rows,
            dataset_name=dataset_name,
        )

    def _build_dataset(
        self,
        *,
        schema: Mapping[K, OceanDataField],
        rows: list[Mapping[str, Any]],
        dataset_name: str = "query_result",
    ) -> Dataset[K, Any]:
        """
        Given a schema and a nonempty list of dict rows, construct a Dataset.

        Notes:
        - Missing keys are skipped.

        :param schema:
            Schema to be used when parsing the input data (rows) into the output dataset

        :param rows:
            Input data, as retrieved from:

            .. code-block:: python

                with self.cursor(row_factory=dict_row) as cur:
                    cur.execute(sql_query, params)
                    rows = cur.fetchall()


        """
        if not rows:
            raise ValueError("rows must be nonempty")

        data: dict[K, np.ndarray] = {}
        dtypes: dict[K, type] = {}

        row0 = rows[0]

        for name, field in schema.items():
            if field.export_name not in row0:
                continue

            values = [row[field.export_name] for row in rows]

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
