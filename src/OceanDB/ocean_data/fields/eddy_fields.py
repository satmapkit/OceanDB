from datetime import datetime, timedelta, timezone
from typing import Any

import numpy as np

from OceanDB.ocean_data.ocean_data import ColumnField, DerivedField
from OceanDB.utils.date_time_conversion import compute_date_time

atk_alias = "atk"
eddy_alias = "eddy"
# -----------------
# Core identity & position
# -----------------

latitude = ColumnField(
    export_name="latitude",
    postgres_table_name="eddy",
    postgres_column_name="latitude",
    python_type=np.float64,
    postgres_type="real",
)

latitude_max = ColumnField(
    export_name="latitude_max",
    postgres_table_name="eddy",
    postgres_column_name="latitude_max",
    python_type=np.float64,
    postgres_type="float4",
)


longitude = ColumnField(
    export_name="longitude",
    postgres_table_name="eddy",
    postgres_column_name="longitude",
    python_type=np.float64,
    postgres_type="real",
)

longitude_max = ColumnField(
    export_name="longitude_max",
    postgres_table_name="eddy",
    postgres_column_name="longitude_max",
    python_type=np.float64,
    postgres_type="float4",
)

date_time = ColumnField(
    export_name="time",
    postgres_table_name="eddy",
    postgres_column_name="date_time",
    python_type=datetime,
    postgres_type="timestamp",
    netcdf_unique_name="time",
    process_from_netcdf=compute_date_time,
)

track = ColumnField(
    export_name="track",
    postgres_table_name="eddy",
    postgres_column_name="track",
    python_type=int,
    postgres_type="integer",
)

cyclonic_type = ColumnField(
    export_name="cyclonic_type",
    postgres_table_name="eddy",
    postgres_column_name="cyclonic_type",
    python_type=int,
    postgres_type="smallint",
)

# -----------------
# Physical properties
# -----------------

amplitude = ColumnField(
    export_name="amplitude",
    postgres_table_name="eddy",
    postgres_column_name="amplitude",
    python_type=np.float64,
    postgres_type="smallint",
)

effective_radius = ColumnField(
    export_name="effective_radius",
    postgres_table_name="eddy",
    postgres_column_name="effective_radius",
    python_type=np.float64,
    postgres_type="smallint",
)

inner_contour_height = ColumnField(
    export_name="inner_contour_height",
    postgres_table_name="eddy",
    postgres_column_name="inner_contour_height",
    python_type=np.float64,
    postgres_type="float4",
)


effective_area = ColumnField(
    export_name="effective_area",
    postgres_table_name="eddy",
    postgres_column_name="effective_area",
    python_type=np.float64,
    postgres_type="real",
)

effective_contour_height = ColumnField(
    export_name="effective_contour_height",
    postgres_table_name="eddy",
    postgres_column_name="effective_contour_height",
    python_type=np.float64,
    postgres_type="float4",
)

effective_contour_latitude = ColumnField(
    export_name="effective_contour_latitude",
    postgres_table_name="eddy",
    postgres_column_name="effective_contour_latitude",
    python_type=np.int32,
    postgres_type="int2",
)

effective_contour_longitude = ColumnField(
    export_name="effective_contour_longitude",
    postgres_table_name="eddy",
    postgres_column_name="effective_contour_longitude",
    python_type=np.int32,
    postgres_type="int2",
)

effective_contour_shape_error = ColumnField(
    export_name="effective_contour_shape_error",
    postgres_table_name="eddy",
    postgres_column_name="effective_contour_shape_error",
    python_type=np.int32,
    postgres_type="int2",
)

cost_association = ColumnField(
    export_name="cost_association",
    postgres_table_name="eddy",
    postgres_column_name="cost_association",
    python_type=np.float64,
    postgres_type="real",
)

# -----------------
# Observation metadata
# -----------------

observation_flag = ColumnField(
    export_name="observation_flag",
    postgres_table_name="eddy",
    postgres_column_name="observation_flag",
    python_type=bool,
    postgres_type="boolean",
)

observation_number = ColumnField(
    export_name="observation_number",
    postgres_table_name="eddy",
    postgres_column_name="observation_number",
    python_type=int,
    postgres_type="smallint",
)

num_contours = ColumnField(
    export_name="num_contours",
    postgres_table_name="eddy",
    postgres_column_name="num_contours",
    python_type=int,
    postgres_type="smallint",
)

num_point_e = ColumnField(
    export_name="num_point_e",
    postgres_table_name="eddy",
    postgres_column_name="num_point_e",
    python_type=int,
    postgres_type="int2",
)

num_point_s = ColumnField(
    export_name="num_point_s",
    postgres_table_name="eddy",
    postgres_column_name="num_point_s",
    python_type=int,
    postgres_type="int2",
)


# -----------------
# Derived / query-only fields
# -----------------

speed_average = ColumnField(
    export_name="speed_average",
    postgres_table_name="eddy",
    postgres_column_name="speed_average",
    python_type=int,
    postgres_type="integer",
)

speed_radius = ColumnField(
    export_name="speed_radius",
    postgres_table_name="eddy",
    postgres_column_name="speed_radius",
    python_type=int,
    postgres_type="smallint",
)

speed_area = ColumnField(
    export_name="speed_area",
    postgres_table_name="eddy",
    postgres_column_name="speed_area",
    python_type=np.float64,
    postgres_type="real",
)

speed_contour_height = ColumnField(
    export_name="speed_contour_height",
    postgres_table_name="eddy",
    postgres_column_name="speed_contour_height",
    python_type=np.float64,
    postgres_type="real",
)

speed_contour_shape = ColumnField(
    export_name="speed_contour_shape",
    postgres_table_name="eddy",
    postgres_column_name="speed_contour_shape",
    python_type=np.float64,
    postgres_type="real",
)

speed_contour_shape_error = ColumnField(
    export_name="speed_contour_shape_error",
    postgres_table_name="eddy",
    postgres_column_name="speed_contour_shape_error",
    python_type=np.float64,
    postgres_type="int2",
)

# -----------------
# Aggregates (eddy lifecycle)
# -----------------

min_date_time = DerivedField(
    export_name="min_date_time",
    expression="MIN(eddy.date_time)",
    python_type=datetime,
)

max_date_time = DerivedField(
    export_name="max_date_time",
    expression="MAX(eddy.date_time)",
    python_type=datetime,
)

basin_ids = DerivedField(
    export_name="basin_ids",
    expression="""
        array_agg(DISTINCT basin_connections.connected_id)
        || array_agg(DISTINCT basin.id)
    """,
    python_type=list,  # important
    postgres_type="integer[]",  # optional but useful
)

# -----------------
# alongtrack computations
# -----------------

distance = DerivedField(
    export_name="distance",
    expression=f"""
        ST_Distance(
            {eddy_alias}.eddy_point,
            {atk_alias}.along_track_point
        )
    """,
    python_type=np.float64,
    postgres_type="double precision",
)

delta_t = DerivedField(
    export_name="delta_t",
    expression=f"""
        EXTRACT(EPOCH FROM ({eddy_alias}.date_time - {atk_alias}.date_time))
    """,
    python_type=np.float64,
    postgres_type="double precision",
)
