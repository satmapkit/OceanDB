import numpy as np
from datetime import datetime

from OceanDB.ocean_data.ocean_data import ColumnField, DerivedField

# -----------------
# Core coordinates & identity
# -----------------

atk_alias = "atk"

latitude = ColumnField(
    name="latitude",
    postgres_table_name=atk_alias,
    postgres_column_name="latitude",
    python_type=np.float64,
    postgres_type="double precision",
)

longitude = ColumnField(
    name="longitude",
    postgres_table_name=atk_alias,
    postgres_column_name="longitude",
    python_type=np.float64,
    postgres_type="double precision",
)

date_time = ColumnField(
    name="date_time",
    postgres_table_name=atk_alias,
    postgres_column_name="date_time",
    python_type=datetime,
    postgres_type="timestamp",
)

file_name = ColumnField(
    name="file_name",
    postgres_table_name=atk_alias,
    postgres_column_name="file_name",
    python_type=str,
    postgres_type="text",
)

mission = ColumnField(
    name="mission",
    postgres_table_name=atk_alias,
    postgres_column_name="mission",
    python_type=str,
    postgres_type="text",
)

track = ColumnField(
    name="track",
    postgres_table_name=atk_alias,
    postgres_column_name="track",
    python_type=int,
    postgres_type="smallint",
)

cycle = ColumnField(
    name="cycle",
    postgres_table_name=atk_alias,
    postgres_column_name="cycle",
    python_type=int,
    postgres_type="smallint",
)

basin_id = ColumnField(
    name="basin_id",
    postgres_table_name=atk_alias,
    postgres_column_name="basin_id",
    python_type=int,
    postgres_type="smallint",
)

# -----------------
# Altimetry signals
# -----------------

sla_unfiltered = ColumnField(
    name="sla_unfiltered",
    postgres_table_name=atk_alias,
    postgres_column_name="sla_unfiltered",
    python_type=np.float64,
    postgres_type="smallint",
)

sla_filtered = ColumnField(
    name="sla_filtered",
    postgres_table_name=atk_alias,
    postgres_column_name="sla_filtered",
    python_type=np.float64,
    postgres_type="smallint",
)

dac = ColumnField(
    name="dac",
    postgres_table_name=atk_alias,
    postgres_column_name="dac",
    python_type=np.float64,
    postgres_type="smallint",
)

ocean_tide = ColumnField(
    name="ocean_tide",
    postgres_table_name=atk_alias,
    postgres_column_name="ocean_tide",
    python_type=np.float64,
    postgres_type="smallint",
)

internal_tide = ColumnField(
    name="internal_tide",
    postgres_table_name=atk_alias,
    postgres_column_name="internal_tide",
    python_type=np.float64,
    postgres_type="smallint",
)

lwe = ColumnField(
    name="lwe",
    postgres_table_name=atk_alias,
    postgres_column_name="lwe",
    python_type=np.float64,
    postgres_type="smallint",
)

mdt = ColumnField(
    name="mdt",
    postgres_table_name=atk_alias,
    postgres_column_name="mdt",
    python_type=np.float64,
    postgres_type="smallint",
)

tpa_correction = ColumnField(
    name="tpa_correction",
    postgres_table_name=atk_alias,
    postgres_column_name="tpa_correction",
    python_type=np.float64,
    postgres_type="smallint",
)

# Derived / query-only fields

distance = DerivedField(
    name="distance",
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
    name="delta_t",
    expression=f"""
        EXTRACT(EPOCH FROM (%(central_date_time)s - {atk_alias}.date_time))
    """,
    python_type=np.float64,
    postgres_type="double precision",
)
