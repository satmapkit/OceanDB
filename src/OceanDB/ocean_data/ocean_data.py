from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional, Any
from psycopg import sql
import numpy as np


@dataclass(frozen=True)
class OceanDataField(ABC):
    """
    Abstract projection definition.

    A ProjectionField represents a single SELECT expression
    that produces a named output column.
    """

    name: str
    python_type: Optional[type]

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

    def from_sql_query(self, values: list[Any]) -> Any:
        """
        Convert to a python type, most often a NDArray[np.float64].
        """
        if self.python_type is None:
            return np.array(values)
        return np.array(values, dtype=self.python_type)


@dataclass(frozen=True)
class ColumnField(OceanDataField):
    postgres_table_name: str
    postgres_column_name: str

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

    postgres_type: Optional[str] = None

    def sql_expression(self) -> sql.Composable:
        return sql.SQL(self.expression)
