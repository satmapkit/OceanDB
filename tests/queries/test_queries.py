from OceanDB.data_access.base_query import QuerySpec
from OceanDB.ocean_data.ocean_data import ColumnField, DerivedField

query = QuerySpec(
    sql="""
        SELECT
            atk.latitude   AS latitude,
            atk.longitude  AS longitude,
            ST_Distance(
                ST_MakePoint(%(lon)s, %(lat)s),
                atk.along_track_point
            ) AS distance
        FROM along_track atk
        WHERE atk.date_time BETWEEN %(t0)s AND %(t1)s
    """,
    schema={
        "latitude": ColumnField(
            name="latitude",
            postgres_table_name="atk",
            postgres_column_name="latitude",
            python_type=float,
        ),
        "longitude": ColumnField(
            name="longitude",
            postgres_table_name="atk",
            postgres_column_name="longitude",
            python_type=float,
        ),
        "distance": DerivedField(
            name="distance",
            expression="ST_Distance(...)",
            python_type=float,
        ),
    },
)

