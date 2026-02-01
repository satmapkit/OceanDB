import numpy as np
from datetime import datetime
from OceanDB.ocean_data.ocean_data import OceanDataField


latitude = OceanDataField(
    nc_name="latitude",
    nc_scale=1,
    nc_offset=0,
    python_type=np.floating,
    postgres_type="real",
    postgres_column_or_query_name="latitude",
    postgres_table_name="eddy",
)

longitude = OceanDataField(
    nc_name="longitude",
    nc_scale=1,
    nc_offset=0,
    python_type=np.floating,
    postgres_type="real",
    postgres_column_or_query_name="longitude",
    postgres_table_name="eddy",
)

date_time = OceanDataField(
    nc_name="time",
    nc_scale=1,
    nc_offset=0,
    python_type=datetime,
    postgres_type="timestamp",
    postgres_column_or_query_name="date_time",
    postgres_table_name="eddy",
)

track = OceanDataField(
    nc_name="track",
    nc_scale=1,
    nc_offset=0,
    python_type=int,
    postgres_type="integer",
    postgres_column_or_query_name="track",
    postgres_table_name="eddy",
)

cyclonic_type = OceanDataField(
    nc_name="cyclonic_type",
    nc_scale=1,
    nc_offset=0,
    python_type=int,
    postgres_type="smallint",
    postgres_column_or_query_name="cyclonic_type",
    postgres_table_name="eddy",
)

amplitude = OceanDataField(
    nc_name="amplitude",
    nc_scale=1,
    nc_offset=0,
    python_type=np.floating,
    postgres_type="smallint",
    postgres_column_or_query_name="amplitude",
    postgres_table_name="eddy",
)

effective_radius = OceanDataField(
    nc_name="effective_radius",
    nc_scale=1,
    nc_offset=0,
    python_type=np.floating,
    postgres_type="smallint",
    postgres_column_or_query_name="effective_radius",
    postgres_table_name="eddy",
)

effective_area = OceanDataField(
    nc_name="effective_area",
    nc_scale=1,
    nc_offset=0,
    python_type=np.floating,
    postgres_type="real",
    postgres_column_or_query_name="effective_area",
    postgres_table_name="eddy",
)


cost_association = OceanDataField(
    nc_name="cost_association",
    nc_scale=1,
    nc_offset=0,
    python_type=np.floating,
    postgres_type="real",
    postgres_column_or_query_name="cost_association",
    postgres_table_name="eddy",
)

observation_flag = OceanDataField(
    nc_name="observation_flag",
    nc_scale=1,
    nc_offset=0,
    python_type=bool,
    postgres_type="boolean",
    postgres_column_or_query_name="observation_flag",
    postgres_table_name="eddy",
)

observation_number = OceanDataField(
    nc_name="observation_number",
    nc_scale=1,
    nc_offset=0,
    python_type=int,
    postgres_type="smallint",
    postgres_column_or_query_name="observation_number",
    postgres_table_name="eddy",
)

distance = OceanDataField(
    nc_name="distance",
    nc_scale=1,
    nc_offset=0,
    python_type=np.floating,
    postgres_type="double precision",
    postgres_column_or_query_name="distance",
    custom_calculation="""
        ST_Distance(
            ST_MakePoint(%(longitude)s, %(latitude)s),
            eddy_point
        )
    """,
    postgres_table_name="eddy",
)

delta_t = OceanDataField(
    nc_name="delta_t",
    nc_scale=1,
    nc_offset=0,
    python_type=np.floating,
    postgres_type="double precision",
    postgres_column_or_query_name="delta_t",
    custom_calculation="""
        EXTRACT(EPOCH FROM (%(central_date_time)s - date_time))
    """,
    postgres_table_name="eddy",
)

speed_average = OceanDataField(
    nc_name="speed_average",
    nc_scale=1,
    nc_offset=0,
    python_type=int,
    postgres_type="integer",
    postgres_column_or_query_name="speed_average",
    postgres_table_name="eddy",
)

speed_radius = OceanDataField(
    nc_name="speed_radius",
    nc_scale=1,
    nc_offset=0,
    python_type=int,
    postgres_type="smallint",
    postgres_column_or_query_name="speed_radius",
    postgres_table_name="eddy",
)

speed_area = OceanDataField(
    nc_name="speed_area",
    nc_scale=1,
    nc_offset=0,
    python_type=np.floating,
    postgres_type="real",
    postgres_column_or_query_name="speed_area",
    postgres_table_name="eddy",
)

num_contours = OceanDataField(
    nc_name="None",
    postgres_table_name="eddy",
    postgres_column_or_query_name="num_contours",
    nc_scale=1,
    nc_offset=0,
    python_type=int,
    postgres_type="smallint"
)



