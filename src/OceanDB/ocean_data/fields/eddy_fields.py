import numpy as np
from datetime import datetime
from OceanDB.ocean_data.ocean_data import ColumnField, DerivedField

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

longitude = ColumnField(
    export_name="longitude",
    postgres_table_name="eddy",
    postgres_column_name="longitude",
    python_type=np.float64,
    postgres_type="real",
)

date_time = ColumnField(
    export_name="time",
    postgres_table_name="eddy",
    postgres_column_name="date_time",
    python_type=datetime,
    postgres_type="timestamp",
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

effective_area = ColumnField(
    export_name="effective_area",
    postgres_table_name="eddy",
    postgres_column_name="effective_area",
    python_type=np.float64,
    postgres_type="real",
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

# -----------------
# Derived / query-only fields
# -----------------

distance = DerivedField(
    export_name="distance",
    expression="""
        ST_Distance(
            ST_MakePoint(%(longitude)s, %(latitude)s),
            eddy.eddy_point
        )
    """,
    python_type=np.float64,
    postgres_type="double precision",
)

delta_t = DerivedField(
    export_name="delta_t",
    expression="""
        EXTRACT(EPOCH FROM (%(central_date_time)s - eddy.date_time))
    """,
    python_type=np.float64,
    postgres_type="double precision",
)

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

# -----------------
# Aggregates (eddy lifecycle)
# -----------------

min_date_time = DerivedField(
    export_name="min_date",
    expression="MIN(eddy.date_time)",
    python_type=datetime,
)

max_date_time = DerivedField(
    export_name="max_date",
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
            ST_MakePoint({eddy_alias}.latitude,{eddy_alias}.longitude),
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
