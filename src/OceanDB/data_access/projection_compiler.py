




# from typing import Iterable
# import psycopg.sql as sql
#
#
# class ProjectionCompiler:
#     """
#     Compile a schema-backed field projection into a SQL SELECT clause.
#
#     The ProjectionCompiler is responsible for translating a list of
#     domain field names into a psycopg-safe SQL projection using the
#     corresponding OceanDataField definitions.
#
#     Responsibilities
#     ----------------
#     - Expand logical field names into SQL expressions (including
#       custom calculations).
#     - Apply stable SQL aliases that match Dataset column names.
#     - Remain agnostic to query shape (joins, aliases, WHERE clauses).
#     - Perform no database execution or parameter binding.
#
#     Non-Responsibilities
#     --------------------
#     - It does NOT know about query execution (single vs batch).
#     - It does NOT validate or inspect concrete parameter values.
#     - It does NOT introduce or assume SQL table aliases.
#     - It does NOT contain domain-specific query logic.
#
#     Design Notes
#     ------------
#     - Table aliases are defined exclusively by SQL templates, not schemas.
#     - Schema definitions must reference only real database tables.
#     - This separation allows the same schema and compiler to be reused
#       across multiple query shapes safely.
#
#     The output of this compiler is a psycopg.sql.SQL object suitable for
#     execution by BaseQuery methods.
#     """
#
#     def __init__(self, *, schema):
#         self.schema = schema
#
#     def compile(
#         self,
#         *,
#         # sql_template: str,
#         fields: Iterable[str],
#     ) -> sql.SQL:
#         """
#         Parameters
#         ----------
#         sql_template
#             SQL string containing a `{fields}` placeholder
#         fields
#             Iterable of domain field names
#
#         Returns
#         -------
#         psycopg.sql.SQL
#             Fully formatted SQL query
#         """
#         field_sql = sql.SQL(", ").join(
#             self.schema[field].to_sql_query()
#             for field in fields
#         )
#
#         return sql.SQL(sql_template).format(fields=field_sql)