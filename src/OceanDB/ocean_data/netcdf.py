from __future__ import annotations

from typing import Any
import numpy as np
import netCDF4 as nc


def _infer_len(dataset: Any) -> int:
    """
    Infer the length of the dataset along its primary dimension.
    Assumes all variables are 1D and share the same length.
    """
    if not dataset.data:
        return 0
    first = next(iter(dataset.data.values()))
    return int(len(first))


def write_dataset_to_group(
    grp: nc.Group,
    dataset: Any,
    *,
    dim_name: str = "obs",
) -> None:
    """
    Serialize a Dataset into an existing NetCDF group.

    Current implementation:
    - Assumes 1D arrays of equal length
    - Writes each field as a variable over the 'obs' dimension
    """

    n = _infer_len(dataset)

    # Create dimension (unlimited could be nice later; for now fixed length)
    if dim_name not in grp.dimensions:
        grp.createDimension(dim_name, n)

    for field, arr in dataset.data.items():
        # Normalize to numpy array
        arr_np = np.asarray(arr)

        # NetCDF4 supports numpy dtypes directly in most cases
        var = grp.createVariable(
            field,
            arr_np.dtype,
            (dim_name,),
        )
        var[:] = arr_np
