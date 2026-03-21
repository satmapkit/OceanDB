from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional, Any, Callable, overload
from psycopg import sql
import numpy as np
import netCDF4 as nc


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

    netcdf_unique_name: Optional[str] = None
    """
    Name of the parameter when importing from netcdf.
    Defaults to same as postgres column name
    """

    process_from_netcdf: Optional[Callable[[Any], Any]] = None
    """
    When loading from netcdf, additional post-processing to perform
    """

    @property
    def netcdf_name(self) -> str:
        if self.netcdf_unique_name is None:
            return self.postgres_column_name
        return self.netcdf_unique_name

    def sql_expression(self) -> sql.Composable:
        return sql.Identifier(
            self.postgres_table_name,
            self.postgres_column_name,
        )

    @overload
    def from_netcdf(self, ds: nc.Dataset, rows: int) -> Any: ...
    @overload
    def from_netcdf(self, ds: nc.Dataset, rows: slice) -> Any: ...
    def from_netcdf(self, ds, rows):
        """
        Read the value of this field from NetCDF

        :param ds:
            NetCDF dataset to read

        :param rows:
            Slice or index of rows to read

        :returns:
            The value corresponding to this field, as type :attr:`OceanDataField.python_type`
        """

        if not self.netcdf_name in ds.variables:
            raise ValueError(f"Field not found in dataset: {self.netcdf_name}")

        var = ds.variables[self.netcdf_name][rows]

        if self.process_from_netcdf is None:
            return var.astype(self.python_type)
        return self.process_from_netcdf(var)


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
