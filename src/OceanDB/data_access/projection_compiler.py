from typing import Iterable
import psycopg.sql as sql


class ProjectionCompiler:
    """
    Compiles a domain field projection into a psycopg-safe SQL query.

    This class is intentionally dumb:
    - no domain logic
    - no database execution
    - no knowledge of AlongTrack vs Eddy
    """

    def __init__(self, *, schema):
        self.schema = schema

    def compile(
        self,
        *,
        sql_template: str,
        fields: Iterable[str],
    ) -> sql.SQL:
        """
        Parameters
        ----------
        sql_template
            SQL string containing a `{fields}` placeholder
        fields
            Iterable of domain field names

        Returns
        -------
        psycopg.sql.SQL
            Fully formatted SQL query
        """
        field_sql = sql.SQL(", ").join(
            self.schema[field].to_sql_query()
            for field in fields
        )

        return sql.SQL(sql_template).format(fields=field_sql)