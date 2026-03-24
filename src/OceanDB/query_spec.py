from dataclasses import dataclass, field
from typing import Any, Iterable, Mapping, Generic, LiteralString
import numpy as np
import psycopg as pg
from psycopg import sql

from OceanDB.ocean_data.ocean_data import OceanDataField
from OceanDB.ocean_data.dataset import K


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
        """
        Given a specific set of fields, project down the SQL query template
        to an sql query template missing only parameters.

        For example, if this is the main query template::

            SELECT {fields}
            FROM eddy
            WHERE eddy.track * eddy.cyclonic_type = %(track_id)s;

        Then invoking this function with fields :code:`['field1', 'field2']` might give
        a query that looks like::

            SELECT
                eddy.field1 as field1,
                eddy.field2 as field2
            FROM eddy
            WHERE eddy.track * eddy.cyclonic_type = %(track_id)s;

        :param fields:
            The requested fields

        :return: SQL query template with all patterns removed other than parameters
        """
        extra_fields = filter(lambda field: field not in self.mandatory_fields, fields)
        field_sql = sql.SQL(", ").join(
            self.schema[field].to_sql_query() for field in extra_fields
        )
        if self.mandatory_fields and extra_fields:
            field_sql += sql.SQL(",")

        return sql.SQL(self.sql_template).format(fields=field_sql)


class RawSpec(QuerySpec):
    def __init__(self, query_string: sql.Composed):
        self.query_string = query_string

    def sql_projection_compiler(self, fields):
        return self.query_string


def log_query(conn: pg.Connection, query: sql.Composable, params: Any) -> None:
    print("\n--- SQL QUERY ---")
    print(query.as_string(conn))
    print("--- PARAMS ---")
    print(params)
    print("----------------\n")
