from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional
from psycopg import sql


@dataclass(frozen=True)
class OceanDataField(ABC):
    """
    Abstract projection definition.

    A ProjectionField represents a single SELECT expression
    that produces a named output column.
    """

    name: str

    @abstractmethod
    def sql_expression(self) -> sql.Composable:
        """
        Return the SQL expression *without* aliasing.
        """
        ...

    def to_sql_query(self) -> sql.Composed:
        """
        Produce a SELECT expression with a stable alias.
        """
        return sql.SQL("{expr} AS {alias}").format(
            expr=self.sql_expression(),
            alias=sql.Identifier(self.name),
        )

@dataclass(frozen=True)
class ColumnField(OceanDataField):
    postgres_table_name: str
    postgres_column_name: str

    python_type: Optional[type] = None
    postgres_type: Optional[str] = None

    def sql_expression(self) -> sql.Composable:
        return sql.Identifier(
            self.postgres_table_name,
            self.postgres_column_name,
        )

@dataclass(frozen=True)
class DerivedField(OceanDataField):
    """
    Projection backed by a custom SQL expression.
    """

    expression: str

    python_type: Optional[type] = None
    postgres_type: Optional[str] = None

    def sql_expression(self) -> sql.Composable:
        return sql.SQL(self.expression)





#
# @dataclass
# class OceanDataField:
#     # ---- Domain / output identity ----
#     name: str
#
#     # ---- SQL projection ----
#     custom_calculation: Optional[str] = None
#     postgres_table_name: Optional[str] = None
#     postgres_column_name: Optional[str] = None
#
#     # ---- Typing / storage metadata (optional) ----
#     python_type: Optional[type] = None
#     postgres_type: Optional[str] = None
#
#     # ---- NetCDF metadata (optional) ----
#     nc_name: Optional[str] = None
#     nc_scale: Optional[int] = None
#     nc_offset: Optional[int] = None
#
#     def to_sql_query(self) -> sql.Composed:
#         """
#         Produce a SELECT expression with a stable alias.
#         """
#
#         alias = sql.Identifier(self.name)
#
#         if self.custom_calculation:
#             expr = sql.SQL(self.custom_calculation)
#
#         else:
#             if not self.postgres_table_name or not self.postgres_column_name:
#                 raise ValueError(
#                     f"Field '{self.name}' is missing table/column metadata"
#                 )
#
#             expr = sql.Identifier(
#                 self.postgres_table_name,
#                 self.postgres_column_name,
#             )
#
#         return sql.SQL("{expr} AS {alias}").format(
#             expr=expr,
#             alias=alias,
#         )
#
#



# @dataclass
# class OceanDataField:
#     nc_name: str
#     nc_scale: int
#     nc_offset: int
#     python_type: type
#     postgres_type: str
#     postgres_column_or_query_name: str
#     postgres_table_name: str
#     custom_calculation: str | None = None
#
#     def to_sql_query(self):
#         output_name = sql.Identifier(self.postgres_column_or_query_name)
#         if self.custom_calculation:
#             postgres_calc = sql.SQL(self.custom_calculation)
#         else:
#             postgres_calc = sql.Identifier(self.postgres_table_name, self.postgres_column_or_query_name)
#
#         return sql.SQL("{postgres_calc} AS {output_name}").format(
#                 postgres_calc=postgres_calc,
#                 output_name=output_name
#             )
