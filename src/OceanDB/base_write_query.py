from typing import Any, Iterable, Mapping

import psycopg as pg
from psycopg.rows import dict_row, DictRow

from OceanDB.OceanDB import OceanDB, connect_to_db
from OceanDB.ocean_data.dataset import K
from OceanDB.query_spec import QuerySpec

class BaseWriteQuery(OceanDB):
    """
    Base class for write-only query services.
    """

    def connection_string(self):
        return super().connection_string()

    @connect_to_db(connection_string, commit=True, row_factory=dict_row)
    def execute_write_query(
        self,
        cur: pg.Cursor[DictRow],
        query_spec: QuerySpec,
        *,
        fields: Iterable[K] = [],
        params: Mapping[str, Any] = {},
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

        cur.execute(sql_query, params)
