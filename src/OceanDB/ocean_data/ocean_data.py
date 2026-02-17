from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional, Any
from psycopg import sql
import numpy as np


@dataclass(frozen=True)
class OceanDataField(ABC):
    """
    The base dataclass for all ocean fields supported by this library.

    :param name:
        See :attr:`name`

    :param python_type:
        See :attr:`python_type`
    """

    export_name: str
    """
    Unique name for this field, used in exporting
    """

    python_type: Optional[type]
    """
    Numpy dtype to use when casting this field to a numpy array
    """

    @abstractmethod
    def sql_expression(self) -> sql.Composable:
        """
        :return: the SQL expression for this field *without* aliasing.
        """
        ...

    def to_sql_query(self) -> sql.Composed:
        """
        :return: a SELECT expression for this field with a stable alias.
        """
        return sql.SQL("{expr} AS {alias}").format(
            expr=self.sql_expression(),
            alias=sql.Identifier(self.export_name),
        )

    def from_sql_query(self, values: list[Any]) -> Any:
        """
        Convert to a NDArray[self.python_type] from a list of values.

        :param values:
            List of values to convert

        :return: values, cast to a numpy array of the expected type.
        """
        if self.python_type is None:
            return np.array(values)
        return np.array(values, dtype=self.python_type)


@dataclass(frozen=True)
class ColumnField(OceanDataField):
    """
    A field retrieved directly from the column of a table.

    :param name:
        See :attr:`OceanDataField.name`

    :param python_type:
        See :attr:`OceanDataField.python_type`

    :param postgres_table_name:
        See :attr:`postgres_table_name`

    :param postgres_column_name:
        See :attr:`postgres_column_name`

    :param postgres_type:
        See :attr:`postgres_type`
    """

    postgres_table_name: str
    """
    Name (or alias) of the source table for this column
    """

    postgres_column_name: str
    """
    Name of the column containing this field in a database,
    i.e. :code:`name` in::
        
        SELECT field AS name FROM ... WHERE ...
                        ^
    """

    postgres_type: Optional[str] = None
    """
    Postgres type of this column (UNUSED)
    """

    def sql_expression(self) -> sql.Composable:
        return sql.Identifier(
            self.postgres_table_name,
            self.postgres_column_name,
        )


@dataclass(frozen=True)
class DerivedField(OceanDataField):
    """
    Projection backed by a custom SQL expression.

    :param name:
        See :attr:`OceanDataField.name`

    :param python_type:
        See :attr:`OceanDataField.python_type`

    :param expression:
        See :attr:`expression`

    :param postgres_type:
        See :attr:`postgres_type`
    """

    expression: str
    """
    Expression string for computing this field in postgres, e.g.::

        EXTRACT(EPOCH FROM (%(central_date_time)s - along_track.date_time))
    """

    postgres_type: Optional[str] = None
    """
    Postgres type of this column (UNUSED)
    """

    def sql_expression(self) -> sql.Composable:
        return sql.SQL(self.expression)
