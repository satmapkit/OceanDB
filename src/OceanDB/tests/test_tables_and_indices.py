from sqlalchemy import text
from OceanDB.get_engine import get_engine

EXPECTED_TABLE_INDEXES = {
    "along_track": {
        "along_track_basin_idx",
        "along_track_date_idx",
        "along_track_file_name_idx",
        "along_track_mission_idx",
        "along_track_point_idx",
        "along_track_point_date_idx",
        "along_track_point_date_mission_idx",
        "along_track_point_date_mission_basin_idx",
        "along_track_point_geom_idx",
        "along_track_time_idx",
    },
    "basin": {
        "basin_geog_idx",
    },
    "basin_connection": {
        "basin_id_idx",
    },
    "chelton_eddy": {
        "chelton_eddy_point_idx",
        "chelton_track_times_cyclonic_type_idx",
    },
    "eddy": {
        "eddy_point_idx",
        "track_times_cyclonic_type_idx",
    },
}
engine = get_engine()

for table_name, expected_indices in EXPECTED_TABLE_INDEXES.items():
    schema_name = "public"  # change if you use another schema

    with engine.connect() as conn:
        rows = conn.execute(
            text("""
                SELECT indexname, indexdef
                FROM pg_indexes
                WHERE schemaname = :schema AND tablename = :table
                ORDER BY indexname
            """),
            {"schema": schema_name, "table": table_name},
        ).fetchall()
        actual_indexes = {row[0] for row in rows}
        missing = expected_indices - actual_indexes
        print(f"MISSING INDICES {missing}")

    # missing = expected_indices - actual_indexes

