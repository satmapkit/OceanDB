from datetime import datetime
from typing import Any

import netCDF4 as nc
import numpy as np

from OceanDB.ocean_data.ocean_data import ColumnField, DerivedField
from OceanDB.utils.date_time_conversion import compute_date_time

# -----------------
# Core coordinates & identity
# -----------------

atk_alias = "atk"

latitude = ColumnField(
    export_name="latitude",
    postgres_table_name=atk_alias,
    postgres_column_name="latitude",
    python_type=np.float64,
    postgres_type="double precision",
)

longitude = ColumnField(
    export_name="longitude",
    postgres_table_name=atk_alias,
    postgres_column_name="longitude",
    python_type=np.float64,
    postgres_type="double precision",
)

date_time = ColumnField(
    export_name="date_time",
    postgres_table_name=atk_alias,
    postgres_column_name="date_time",
    python_type=datetime,
    postgres_type="timestamp",
    netcdf_unique_name="time",
    process_from_netcdf=compute_date_time,
)

file_name = ColumnField(
    export_name="file_name",
    postgres_table_name=atk_alias,
    postgres_column_name="file_name",
    python_type=str,
    postgres_type="text",
)

mission = ColumnField(
    export_name="mission",
    postgres_table_name=atk_alias,
    postgres_column_name="mission",
    python_type=str,
    postgres_type="text",
)

track = ColumnField(
    export_name="track",
    postgres_table_name=atk_alias,
    postgres_column_name="track",
    python_type=int,
    postgres_type="smallint",
)

cycle = ColumnField(
    export_name="cycle",
    postgres_table_name=atk_alias,
    postgres_column_name="cycle",
    python_type=int,
    postgres_type="smallint",
)

basin_id = ColumnField(
    export_name="basin_id",
    postgres_table_name=atk_alias,
    postgres_column_name="basin_id",
    python_type=int,
    postgres_type="smallint",
)

# -----------------
# Altimetry signals
# -----------------

sla_unfiltered = ColumnField(
    export_name="sla_unfiltered",
    postgres_table_name=atk_alias,
    postgres_column_name="sla_unfiltered",
    python_type=np.float64,
    postgres_type="smallint",
    netcdf_unique_name="sla_unfiltered",
)

sla_filtered = ColumnField(
    export_name="sla_filtered",
    postgres_table_name=atk_alias,
    postgres_column_name="sla_filtered",
    python_type=np.float64,
    postgres_type="smallint",
    netcdf_unique_name="sla_filtered",
)

dac = ColumnField(
    export_name="dac",
    postgres_table_name=atk_alias,
    postgres_column_name="dac",
    python_type=np.float64,
    postgres_type="smallint",
    netcdf_unique_name="dac",
)

ocean_tide = ColumnField(
    export_name="ocean_tide",
    postgres_table_name=atk_alias,
    postgres_column_name="ocean_tide",
    python_type=np.float64,
    postgres_type="smallint",
    netcdf_unique_name="ocean_tide",
)

internal_tide = ColumnField(
    export_name="internal_tide",
    postgres_table_name=atk_alias,
    postgres_column_name="internal_tide",
    python_type=np.float64,
    postgres_type="smallint",
    netcdf_unique_name="internal_tide",
)

lwe = ColumnField(
    export_name="lwe",
    postgres_table_name=atk_alias,
    postgres_column_name="lwe",
    python_type=np.float64,
    postgres_type="smallint",
    netcdf_unique_name="lwe",
)

mdt = ColumnField(
    export_name="mdt",
    postgres_table_name=atk_alias,
    postgres_column_name="mdt",
    python_type=np.float64,
    postgres_type="smallint",
    netcdf_unique_name="mdt",
)


def _expand_if_low_dim(var: nc.Variable, rows: slice) -> Any:
    """
    Given a datetime variable in a netcdf dataset
    which may not have the desired dimension,
    duplicate the variable to be the right shape.

    :param var:
        The netcdf variable to convert

    :param rows:
        expected output slice

    :returns:
        numpy array of the var, with the requested size
    """
    if len(var.shape) == 0 and rows.stop is not None:
        return np.array([var[:] for _ in range(*rows.indices(rows.stop))])
    return var[rows]


tpa_correction = ColumnField(
    export_name="tpa_correction",
    postgres_table_name=atk_alias,
    postgres_column_name="tpa_correction",
    python_type=np.float64,
    postgres_type="smallint",
    netcdf_unique_name="tpa_correction",
    process_from_netcdf=_expand_if_low_dim,
)

# Derived / query-only fields

distance = DerivedField(
    export_name="distance",
    expression=f"""
        ST_Distance(
            ST_MakePoint(%(longitude)s, %(latitude)s),
            {atk_alias}.along_track_point
        )
    """,
    python_type=np.float64,
    postgres_type="double precision",
)

delta_t = DerivedField(
    export_name="delta_t",
    expression=f"""
        EXTRACT(EPOCH FROM (%(central_date_time)s - {atk_alias}.date_time))
    """,
    python_type=np.float64,
    postgres_type="double precision",
)
