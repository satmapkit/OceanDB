from typing import Iterable, Any, Mapping, TypeVar
import numpy as np
import psycopg as pg

from OceanDB.OceanDB import OceanDB
from OceanDB.data_access.metadata import METADATA_REGISTRY
from OceanDB.ocean_data.ocean_data import OceanDataField
from OceanDB.ocean_data.dataset import Dataset


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
        rows: list[dict[str, T]],
        dataset_name: str = "query_result",
    ) -> Dataset[K, T]:
        """
        Given a schema and a nonempty list of rows, return a Dataset.
        """

        if not rows:
            raise ValueError("rows must be nonempty")

        data: dict[K, np.ndarray] = {}
        dtypes: dict[K, type] = {}

        row0 = rows[0]

        for name, field in schema.items():
            # IMPORTANT: row keys are the SQL aliases → field.name
            if name not in row0:
                continue

            values = [row[name] for row in rows]

            if field.python_type is not None:
                arr = np.asarray(values, dtype=field.python_type)
            else:
                arr = np.asarray(values)

            data[name] = arr
            dtypes[name] = field.python_type

        return Dataset[K, T](
            name=dataset_name,
            data=data,
            dtypes=dtypes,
            schema=schema,
        )

    def execute_query(
            self,
            query,  # <- note: this is a psycopg.sql.SQL, not a str anymore
            *,
            schema: Mapping[K, OceanDataField],
            params: Mapping[str, Any],
            dataset_name: str = "query_result",
    ) -> Dataset[K, T] | None:
        """
        Execute a single query and return a Dataset, or None if empty.
        """

        with pg.connect(self.config.postgres_dsn) as conn:
            # 👇 THIS is the correct place to render SQL
            print("\n--- SQL QUERY ---")
            print(query.as_string(conn))
            print("--- PARAMS ---")
            print(params)
            print("----------------\n")

            with conn.cursor(row_factory=pg.rows.dict_row) as cur:
                cur.execute(query, params)
                rows: list[dict[str, T]] = cur.fetchall()

                if not rows:
                    return None

                return self.build_dataset(
                    schema=schema,
                    rows=rows,
                    dataset_name=dataset_name,
                )


    # def execute_query(
    #     self,
    #     query: str,
    #     *,
    #     schema: Mapping[K, OceanDataField],
    #     params: Mapping[str, Any],
    #     dataset_name: str = "query_result",
    # ) -> Dataset[K, T] | None:
    #     """
    #     Execute a single query and return a Dataset, or None if empty.
    #     """
    #
    #     with pg.connect(self.config.postgres_dsn) as conn:
    #         with conn.cursor(row_factory=pg.rows.dict_row) as cur:
    #             cur.execute(query, params)
    #             rows: list[dict[str, T]] = cur.fetchall()
    #
    #             if not rows:
    #                 return None
    #
    #             return self.build_dataset(
    #                 schema=schema,
    #                 rows=rows,
    #                 dataset_name=dataset_name,
    #             )

    def execute_batch_query(
        self,
        query: str,
        *,
        schema: Mapping[K, OceanDataField],
        params: list[Mapping[str, Any]],
        dataset_name: str = "query_result",
    ) -> Iterable[Dataset[K, T] | None]:
        """
        Execute a batch query and yield Datasets per result set.
        """

        with pg.connect(self.config.postgres_dsn) as conn:
            with conn.cursor(row_factory=pg.rows.dict_row) as cur:
                cur.executemany(query, params, returning=True)

                while True:
                    rows: list[dict[str, T]] = cur.fetchall()

                    if not rows:
                        yield None
                    else:
                        yield self.build_dataset(
                            schema=schema,
                            rows=rows,
                            dataset_name=dataset_name,
                        )

                    if not cur.nextset():
                        break



# from OceanDB.OceanDB import OceanDB
# from OceanDB.data_access.metadata import METADATA_REGISTRY
# from OceanDB.ocean_data.ocean_data import OceanDataField
# import numpy as np
# import psycopg as pg
#
# from typing import Iterable, Any, Mapping, TypeVar
#
# from OceanDB.ocean_data.dataset import Dataset
#
# from typing import TypeVar
#
# K = TypeVar("K", bound=str)
# T = TypeVar("T")
#
# class BaseQuery(OceanDB):
#     """
#     Base class for read-only query services.
#
#     Provides shared functionality for executing SQL queries and
#     constructing typed, schema-backed datasets from query result rows.
#     """
#
#     METADATA = METADATA_REGISTRY
#
#     def build_dataset(
#         self,
#             *,
#             schema: Mapping[K, OceanDataField],
#             rows: list[dict[str, T]]
#     ) -> Dataset[K, T]:
#         """
#         Given a schema and a nonempty list of rows, return a dataset.
#         """
#         data = {}
#         dtypes = {}
#
#         if len(rows) == 0:
#             raise ValueError("rows must be nonempty")
#
#         for name, field in schema.items():
#             fname =
#             # fname = field.postgres_column_or_query_name
#             if not fname in rows[0]:
#                 continue
#             values = [row[fname] for row in rows]
#             data[name] = np.asarray(values, dtype=field.python_type)
#             dtypes[name] = field.python_type
#
#         return Dataset[K, T](
#             name="along_track_spatiotemporal",
#             data=data, dtypes=dtypes, schema=schema
#         )
#
#     def execute_query(
#             self,
#             query: str,
#             schema: dict[K, OceanDataField],
#             params: dict[str, Any]
#     ) -> Dataset[K, T]:
#         """
#
#         """
#         with pg.connect(self.config.postgres_dsn) as conn:
#             with conn.cursor(row_factory=pg.rows.dict_row) as cur:
#                 cur.execute(query, params)
#
#                 rows : list[dict[str, T]]= cur.fetchall()
#                 if not rows:
#                     return None
#                 else:
#                     dataset = self.build_dataset(schema=schema, rows=rows)
#                     return dataset
#
#     def execute_batch_query(
#         self,
#         query: str,
#         schema: dict[K, OceanDataField],
#         params: list[dict[str, Any]],
#     ) -> Iterable[Dataset[K, T] | None]:
#         """
#
#         """
#         with pg.connect(self.config.postgres_dsn) as conn:
#             with conn.cursor(row_factory=pg.rows.dict_row) as cur:
#                 cur.executemany(query, params, returning=True)
#
#                 while True:
#                     rows : list[dict[str, T]]= cur.fetchall()
#                     if not rows:
#                         yield None
#                     else:
#                         dataset = self.build_dataset(schema=schema, rows=rows)
#                         yield dataset
#                     if not cur.nextset():
#                         break
