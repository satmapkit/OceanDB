import numpy as np
from datetime import datetime
from OceanDB.ocean_data.ocean_data import OceanDataField


# -----------------
# Core coordinates & identity
# -----------------

latitude = OceanDataField(
    name="latitude",
    postgres_table_name="along_track",
    postgres_column_name="latitude",
    python_type=np.float64,
    postgres_type="double precision",
    nc_name="latitude",
    nc_scale=1,
    nc_offset=0,
)

longitude = OceanDataField(
    name="longitude",
    postgres_table_name="along_track",
    postgres_column_name="longitude",
    python_type=np.float64,
    postgres_type="double precision",
    nc_name="longitude",
    nc_scale=1,
    nc_offset=0,
)

date_time = OceanDataField(
    name="time",
    postgres_table_name="along_track",
    postgres_column_name="date_time",
    python_type=datetime,
    postgres_type="timestamp",
    nc_name="time",
    nc_scale=1,
    nc_offset=0,
)

file_name = OceanDataField(
    name="file_name",
    postgres_table_name="along_track",
    postgres_column_name="file_name",
    python_type=str,
    postgres_type="text",
    nc_name="file_name",
    nc_scale=1,
    nc_offset=0,
)

mission = OceanDataField(
    name="mission",
    postgres_table_name="along_track",
    postgres_column_name="mission",
    python_type=str,
    postgres_type="text",
    nc_name="mission",
    nc_scale=1,
    nc_offset=0,
)

track = OceanDataField(
    name="track",
    postgres_table_name="along_track",
    postgres_column_name="track",
    python_type=int,
    postgres_type="smallint",
    nc_name="track",
    nc_scale=1,
    nc_offset=0,
)

cycle = OceanDataField(
    name="cycle",
    postgres_table_name="along_track",
    postgres_column_name="cycle",
    python_type=int,
    postgres_type="smallint",
    nc_name="cycle",
    nc_scale=1,
    nc_offset=0,
)

basin_id = OceanDataField(
    name="basin_id",
    postgres_table_name="along_track",
    postgres_column_name="basin_id",
    python_type=int,
    postgres_type="smallint",
    nc_name="basin_id",
    nc_scale=1,
    nc_offset=0,
)

# -----------------
# Altimetry signals
# -----------------

sla_unfiltered = OceanDataField(
    name="sla_unfiltered",
    postgres_table_name="along_track",
    postgres_column_name="sla_unfiltered",
    python_type=np.float64,
    postgres_type="smallint",
    nc_name="sla_unfiltered",
    nc_scale=1000,  # meters → millimeters
    nc_offset=0,
)

sla_filtered = OceanDataField(
    name="sla_filtered",
    postgres_table_name="along_track",
    postgres_column_name="sla_filtered",
    python_type=np.float64,
    postgres_type="smallint",
    nc_name="sla_filtered",
    nc_scale=1000,
    nc_offset=0,
)

dac = OceanDataField(
    name="dac",
    postgres_table_name="along_track",
    postgres_column_name="dac",
    python_type=np.float64,
    postgres_type="smallint",
    nc_name="dac",
    nc_scale=1000,
    nc_offset=0,
)

ocean_tide = OceanDataField(
    name="ocean_tide",
    postgres_table_name="along_track",
    postgres_column_name="ocean_tide",
    python_type=np.float64,
    postgres_type="smallint",
    nc_name="ocean_tide",
    nc_scale=1000,
    nc_offset=0,
)

internal_tide = OceanDataField(
    name="internal_tide",
    postgres_table_name="along_track",
    postgres_column_name="internal_tide",
    python_type=np.float64,
    postgres_type="smallint",
    nc_name="internal_tide",
    nc_scale=1000,
    nc_offset=0,
)

lwe = OceanDataField(
    name="lwe",
    postgres_table_name="along_track",
    postgres_column_name="lwe",
    python_type=np.float64,
    postgres_type="smallint",
    nc_name="lwe",
    nc_scale=1000,
    nc_offset=0,
)

mdt = OceanDataField(
    name="mdt",
    postgres_table_name="along_track",
    postgres_column_name="mdt",
    python_type=np.float64,
    postgres_type="smallint",
    nc_name="mdt",
    nc_scale=1000,
    nc_offset=0,
)

tpa_correction = OceanDataField(
    name="tpa_correction",
    postgres_table_name="along_track",
    postgres_column_name="tpa_correction",
    python_type=np.float64,
    postgres_type="smallint",
    nc_name="tpa_correction",
    nc_scale=1000,
    nc_offset=0,
)

# -----------------
# Derived / query-only fields
# -----------------

distance = OceanDataField(
    name="distance",
    custom_calculation="""
        ST_Distance(
            ST_MakePoint(%(longitude)s, %(latitude)s),
            along_track_point
        )
    """,
    python_type=np.float64,
    postgres_type="double precision",
)

delta_t = OceanDataField(
    name="delta_t",
    custom_calculation="""
        EXTRACT(EPOCH FROM (%(central_date_time)s - along_track.date_time))
    """,
    python_type=np.float64,
    postgres_type="double precision",
)