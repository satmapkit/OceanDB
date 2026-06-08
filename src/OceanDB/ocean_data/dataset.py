from typing import Any, Mapping, TypeVar

from ..ocean_data.ocean_data import OceanDataField

K = TypeVar("K", bound=str)


class Dataset(Mapping[K, Any]):
    """
    Immutable, column-oriented dataset.

    Represents the result of a query, ingestion step, or transformation.

    Is used like a dictionary, e.g.

    .. code-block:: python

        if 'field' in my_dataset:
            print(my_dataset['field'])
    """

    def __init__(
        self,
        *,
        name: str,
        data: Mapping[K, Any],
        dtypes: Mapping[K, type],
        schema: Mapping[K, OceanDataField],
    ):
        self.name = name
        self._data = dict(data)
        self._dtypes = dict(dtypes)
        self.schema = schema
        self._scaled_data: dict[K, Any] = {}

    def __repr__(self) -> str:
        cols = ", ".join(self._data.keys())
        return f"Dataset(name='{self.name}', columns=[{cols}])"

    def __getitem__(self, key: K) -> Any:
        # fetch from cached data if possible
        if key in self._scaled_data:
            return self._scaled_data[key]

        values = self._data[key]
        field = self.schema[key]
        scaled_values = field.apply_scaling(values)
        # cache scaled data
        self._scaled_data[key] = scaled_values
        return scaled_values

    def __contains__(self, key) -> bool:
        return key in self._data

    def __iter__(self):
        return iter(self._data)

    def __len__(self) -> int:
        # number of columns, not rows
        return len(self._data)

    @property
    def row_count(self) -> int:
        """
        Number of rows represented by this dataset.

        Since Dataset is column-oriented, this is derived from the length of any
        returned column. All returned columns are expected to have the same row count.
        """
        if not self._data:
            return 0

        first_column = next(iter(self._data.values()))
        return len(first_column)

    def to_xarray(self):
        raise NotImplementedError()

    def to_netcdf(self):
        raise NotImplementedError()
