from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from queue import Empty, Queue
from typing import Any, Callable, Generator, Iterable, Mapping

import numpy as np
from psycopg import sql
from psycopg.rows import dict_row

from OceanDB.ocean_data.dataset import Dataset, K
from OceanDB.ocean_data.ocean_data import OceanDataField
from OceanDB.OceanDB import OceanDB
from OceanDB.query_spec import QuerySpec, render_query

QueryObserver = Callable[[sql.Composed, Mapping[str, Any], str], None]


@dataclass(frozen=True)
class QueryPlan:
    """Assignment of every input query to a chunk shared by the workers."""

    chunks: dict[int, int]  # index -> chunk
    n_jobs: int

    def validate(self, n_queries: int) -> None:
        if self.n_jobs < 1:
            raise ValueError("Invalid query plan: n_jobs must be at least 1")
        if not set(self.chunks.keys()) == set(range(n_queries)):
            raise ValueError(
                "Invalid query plan: all indices must be allocated to a chunk"
            )

    def __len__(self) -> int:
        """Number of workers needed for this plan."""
        return min(self.n_jobs, len(set(self.chunks.values())))

    def chunk_indices(self) -> list[list[int]]:
        chunks: dict[int, list[int]] = {}
        for index, chunk in self.chunks.items():
            chunks.setdefault(chunk, []).append(index)
        return list(chunks.values())


@dataclass(frozen=True)
class _QueryResult:
    index: int
    result: Any


@dataclass(frozen=True)
class _WorkerFinished:
    error: Exception | None = None


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
    ) -> Dataset[K] | None:
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

        with self.cursor(
            row_factory=dict_row, debug=should_render_query, use_geometry=True
        ) as cur:
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
    ) -> Generator[Dataset[K] | None, None, None]:
        """
        Execute the same query over many different parameters.
        For each result, yield a Dataset, or None if empty.

        :param query_spec:
            The specification for the query to be made

        :param fields:
            Set of fields to be extracted from the query

        :param params:
            Iterable of desired query params, one for each query.

        :param dataset_name:
            Name to give to the resulting dataset
            (this is mostly used for output to NETCDF)
        """

        sql_query = query_spec.sql_projection_compiler(fields)
        params_batch_list = list(params_batch)

        if not params_batch_list:
            return

        with self.cursor(
            row_factory=dict_row,
            debug=self.query_observer is not None,
            use_geometry=True,
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

    def execute_batch_read_query_stream(
        self,
        query_spec: QuerySpec,
        *,
        fields: Iterable[K],
        params_batch: Iterable[Mapping[str, Any]],
        plan: QueryPlan,
        dataset_name: str = "query_result",
    ) -> Generator[tuple[int, Dataset[K] | None], None, None]:
        """Execute a planned batch concurrently and yield indexed results.

        Workers pull chunks from a shared queue, executing one batch query
        per chunk. Results are placed on an internal thread-safe queue and
        yielded in completion order; the index preserves their input identity.
        """
        params = list(params_batch)
        plan.validate(len(params))

        selected_fields = tuple(fields)
        chunks: Queue[list[int]] = Queue()
        for chunk in plan.chunk_indices():
            chunks.put(chunk)
        results: Queue[_QueryResult | _WorkerFinished] = Queue()

        def run_worker() -> None:
            try:
                while True:
                    try:
                        chunk = chunks.get_nowait()
                    except Empty:
                        break
                    chunk_params = [params[index] for index in chunk]
                    chunk_results = self.execute_batch_read_query(
                        query_spec=query_spec,
                        fields=selected_fields,
                        params_batch=chunk_params,
                        dataset_name=dataset_name,
                    )
                    for index, result in zip(chunk, chunk_results, strict=True):
                        results.put(_QueryResult(index, result))
            except Exception as error:
                results.put(_WorkerFinished(error))
            else:
                results.put(_WorkerFinished())

        if not len(plan):
            return

        with ThreadPoolExecutor(max_workers=len(plan)) as executor:
            for _ in range(len(plan)):
                executor.submit(run_worker)

            finished = 0
            while finished < len(plan):
                result = results.get()
                if isinstance(result, _WorkerFinished):
                    finished += 1
                    if result.error is not None:
                        raise result.error
                else:
                    yield result.index, result.result

    def _build_dataset(
        self,
        *,
        schema: Mapping[K, OceanDataField],
        rows: list[Mapping[str, Any]],
        dataset_name: str = "query_result",
    ) -> Dataset[K]:
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
