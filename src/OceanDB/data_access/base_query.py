from OceanDB.OceanDB import OceanDB
from OceanDB.data_access.metadata import METADATA_REGISTRY
from OceanDB.ocean_data.ocean_data import OceanDataField
import numpy as np
import psycopg as pg

from typing import Iterable, Any, Mapping, TypeVar

from OceanDB.ocean_data.dataset import Dataset

from typing import TypeVar

K = TypeVar("K", bound=str)
T = TypeVar("T")

class BaseQuery(OceanDB):
    """
    Base class for read-only query services.

    Provides shared functionality for executing SQL queries and
    constructing typed, schema-backed datasets from query result rows.
    """

    METADATA = METADATA_REGISTRY

    def build_dataset(
        self,
            *,
            schema: Mapping[K, OceanDataField],
            rows: list[dict[str, T]]
    ) -> Dataset[K, T]:
        """
        Given a schema and a nonempty list of rows, return a dataset.
        """
        data = {}
        dtypes = {}

        if len(rows) == 0:
            raise ValueError("rows must be nonempty")

        for name, field in schema.items():
            fname = field.postgres_column_or_query_name
            if not fname in rows[0]:
                continue
            values = [row[fname] for row in rows]
            data[name] = np.asarray(values, dtype=field.python_type)
            dtypes[name] = field.python_type

        return Dataset[K, T](
            name="along_track_spatiotemporal",
            data=data, dtypes=dtypes, schema=schema
        )

    def execute_query(
            self,
            query: str,
            schema: dict[K, OceanDataField],
            params: dict[str, Any]
    ) -> Dataset[K, T]:
        """

        """
        with pg.connect(self.config.postgres_dsn) as conn:
            with conn.cursor(row_factory=pg.rows.dict_row) as cur:
                cur.execute(query, params)

                rows : list[dict[str, T]]= cur.fetchall()
                if not rows:
                    return None
                else:
                    dataset = self.build_dataset(schema=schema, rows=rows)
                    return dataset

    def execute_batch_query(
        self,
        query: str,
        schema: dict[K, OceanDataField],
        params: list[dict[str, Any]],
    ) -> Iterable[Dataset[K, T] | None]:
        """

        """
        with pg.connect(self.config.postgres_dsn) as conn:
            with conn.cursor(row_factory=pg.rows.dict_row) as cur:
                cur.executemany(query, params, returning=True)

                while True:
                    rows : list[dict[str, T]]= cur.fetchall()
                    if not rows:
                        yield None
                    else:
                        dataset = self.build_dataset(schema=schema, rows=rows)
                        yield dataset
                    if not cur.nextset():
                        break
