import numpy as np
from datetime import datetime
from OceanDB.ocean_data.ocean_data import OceanDataField


# -----------------
# Core identity & position
# -----------------

latitude = OceanDataField(
    name="latitude",
    postgres_table_name="eddy",
    postgres_column_name="latitude",
    python_type=np.floating,
    postgres_type="real",
    nc_name="latitude",
    nc_scale=1,
    nc_offset=0,
)

longitude = OceanDataField(
    name="longitude",
    postgres_table_name="eddy",
    postgres_column_name="longitude",
    python_type=np.floating,
    postgres_type="real",
    nc_name="longitude",
    nc_scale=1,
    nc_offset=0,
)

date_time = OceanDataField(
    name="time",
    postgres_table_name="eddy",
    postgres_column_name="date_time",
    python_type=datetime,
    postgres_type="timestamp",
    nc_name="time",
    nc_scale=1,
    nc_offset=0,
)

track = OceanDataField(
    name="track",
    postgres_table_name="eddy",
    postgres_column_name="track",
    python_type=int,
    postgres_type="integer",
    nc_name="track",
    nc_scale=1,
    nc_offset=0,
)

cyclonic_type = OceanDataField(
    name="cyclonic_type",
    postgres_table_name="eddy",
    postgres_column_name="cyclonic_type",
    python_type=int,
    postgres_type="smallint",
    nc_name="cyclonic_type",
    nc_scale=1,
    nc_offset=0,
)

# -----------------
# Physical properties
# -----------------

amplitude = OceanDataField(
    name="amplitude",
    postgres_table_name="eddy",
    postgres_column_name="amplitude",
    python_type=np.floating,
    postgres_type="smallint",
    nc_name="amplitude",
    nc_scale=1,
    nc_offset=0,
)

effective_radius = OceanDataField(
    name="effective_radius",
    postgres_table_name="eddy",
    postgres_column_name="effective_radius",
    python_type=np.floating,
    postgres_type="smallint",
    nc_name="effective_radius",
    nc_scale=1,
    nc_offset=0,
)

effective_area = OceanDataField(
    name="effective_area",
    postgres_table_name="eddy",
    postgres_column_name="effective_area",
    python_type=np.floating,
    postgres_type="real",
    nc_name="effective_area",
    nc_scale=1,
    nc_offset=0,
)

cost_association = OceanDataField(
    name="cost_association",
    postgres_table_name="eddy",
    postgres_column_name="cost_association",
    python_type=np.floating,
    postgres_type="real",
    nc_name="cost_association",
    nc_scale=1,
    nc_offset=0,
)

# -----------------
# Observation metadata
# -----------------

observation_flag = OceanDataField(
    name="observation_flag",
    postgres_table_name="eddy",
    postgres_column_name="observation_flag",
    python_type=bool,
    postgres_type="boolean",
    nc_name="observation_flag",
    nc_scale=1,
    nc_offset=0,
)

observation_number = OceanDataField(
    name="observation_number",
    postgres_table_name="eddy",
    postgres_column_name="observation_number",
    python_type=int,
    postgres_type="smallint",
    nc_name="observation_number",
    nc_scale=1,
    nc_offset=0,
)

num_contours = OceanDataField(
    name="num_contours",
    postgres_table_name="eddy",
    postgres_column_name="num_contours",
    python_type=int,
    postgres_type="smallint",
)

# -----------------
# Derived / query-only fields
# -----------------

distance = OceanDataField(
    name="distance",
    custom_calculation="""
        ST_Distance(
            ST_MakePoint(%(longitude)s, %(latitude)s),
            eddy.eddy_point
        )
    """,
    python_type=np.floating,
    postgres_type="double precision",
)

delta_t = OceanDataField(
    name="delta_t",
    custom_calculation="""
        EXTRACT(EPOCH FROM (%(central_date_time)s - eddy.date_time))
    """,
    python_type=np.floating,
    postgres_type="double precision",
)

speed_average = OceanDataField(
    name="speed_average",
    postgres_table_name="eddy",
    postgres_column_name="speed_average",
    python_type=int,
    postgres_type="integer",
    nc_name="speed_average",
    nc_scale=1,
    nc_offset=0,
)

speed_radius = OceanDataField(
    name="speed_radius",
    postgres_table_name="eddy",
    postgres_column_name="speed_radius",
    python_type=int,
    postgres_type="smallint",
    nc_name="speed_radius",
    nc_scale=1,
    nc_offset=0,
)

speed_area = OceanDataField(
    name="speed_area",
    postgres_table_name="eddy",
    postgres_column_name="speed_area",
    python_type=np.floating,
    postgres_type="real",
    nc_name="speed_area",
    nc_scale=1,
    nc_offset=0,
)

# -----------------
# Aggregates (eddy lifecycle)
# -----------------

min_date_time = OceanDataField(
    name="min_date_time",
    custom_calculation="MIN(eddy.date_time)",
    python_type=datetime,
)

max_date_time = OceanDataField(
    name="max_date_time",
    custom_calculation="MAX(eddy.date_time)",
    python_type=datetime,
)