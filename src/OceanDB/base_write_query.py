from typing import Any, Iterable, Mapping

import psycopg as pg

from OceanDB.OceanDB import OceanDB
from OceanDB.ocean_data.dataset import K
from OceanDB.query_spec import QuerySpec, log_query


class BaseWriteQuery(OceanDB):
    """
    Base class for write-only query services.
    """

    def execute_write_query(
        self,
        query_spec: QuerySpec,
        *,
        fields: Iterable[K] = [],
        params: Mapping[str, Any] = {},
        debug_sql: bool = False,
    ) -> None:
        """
        Execute a single query and return a Dataset, or None if empty.

        :param query_spec:
            The specification for the query to be made

        :param fields:
            Set of fields for writing to the query

        :param params:
            Set of parameters to be passed to the query
        """

        sql_query = query_spec.sql_projection_compiler(fields)

        with pg.connect(self.config.postgres_dsn) as conn:
            if debug_sql:
                log_query(conn=conn, query=sql_query, params=params)

            with conn.cursor(row_factory=pg.rows.dict_row) as cur:
                cur.execute(sql_query, params)
                conn.commit()
