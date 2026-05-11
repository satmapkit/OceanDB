from __future__ import annotations

from typing import Any, Callable, Generator, Iterable, Mapping

import numpy as np
from psycopg import sql
from psycopg.rows import dict_row

from OceanDB.ocean_data.dataset import Dataset, K
from OceanDB.ocean_data.ocean_data import OceanDataField
from OceanDB.OceanDB import OceanDB
from OceanDB.query_spec import QuerySpec, render_query

QueryObserver = Callable[[sql.Composed, Mapping[str, Any], str], None]


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
        self.query_observer: QueryObserver | None = None

    def start_debug(self, query_observer: QueryObserver):
        """
        Enable SQL rendering callbacks for subsequent read queries.

        When active, each executed query is rendered with bound parameters and
        passed to ``query_observer`` before execution. This is intended for
        lightweight debugging/profiling workflows such as capturing SQL for
        ``EXPLAIN ANALYZE``.

        Example:

        .. code-block:: python

            along_track.start_debug(lambda x,y,z: print(z))
            along_track.geographic_nearest_neighbors(...)

        :param query_observer:
            Callback invoked with the fully rendered SQL string.
        """
        self.query_observer = query_observer

    def stop_debug(self):
        """
        Disable SQL rendering callbacks for subsequent read queries.

        After this is called, queries execute normally without rendering SQL for
        observation.
        """
        self.query_observer = None

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
        should_render_query = self.query_observer is not None

        with self.cursor(row_factory=dict_row, debug=should_render_query) as cur:
            if self.query_observer is not None:
                rendered_query = render_query(
                    conn=cur.connection,
                    cursor=cur,
                    query=sql_query,
                    params=params,
                )
                self.query_observer(
                    sql_query,
                    params,
                    rendered_query,
                )

            cur.execute(sql_query, params)
            rows: list[Mapping[str, Any]] = cur.fetchall()

        if not rows:
            return None

        return self._build_dataset(
            schema=query_spec.schema,
            rows=rows,
            dataset_name=dataset_name,
        )

    def execute_batch_read_query(
        self,
        query_spec: QuerySpec,
        *,
        fields: Iterable[K],
        params_batch: Iterable[Mapping[str, Any]],
        dataset_name: str = "query_result",
    ) -> Generator[Dataset[K, Any] | None, None, None]:
        """
        Execute a prepared batch query and return one Dataset per parameter set.

        Each batch item produces its own result set via psycopg3
        ``executemany(..., returning=True, prepare=True)``.
        """

        sql_query = query_spec.sql_projection_compiler(fields)
        params_batch_list = list(params_batch)

        if not params_batch_list:
            return

        with self.cursor(
            row_factory=dict_row, debug=self.query_observer is not None
        ) as cur:
            if self.query_observer is not None:
                rendered_query = render_query(
                    conn=cur.connection,
                    cursor=cur,
                    query=sql_query,
                    params=params_batch_list[0],
                )
                self.query_observer(
                    sql_query,
                    params_batch_list[0],
                    rendered_query,
                )

            cur.executemany(
                sql_query,
                params_batch_list,
                returning=True,
            )
            while True:
                rows: list[Mapping[str, Any]] = cur.fetchall()

                if not rows:
                    yield None
                else:
                    yield self._build_dataset(
                        schema=query_spec.schema,
                        rows=rows,
                        dataset_name=dataset_name,
                    )

                if not cur.nextset():
                    break

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
