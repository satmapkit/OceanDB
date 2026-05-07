from typing import Literal

from OceanDB.ocean_data.fields import along_track_fields as atk_fields
from OceanDB.ocean_data.ocean_data import ColumnField, OceanDataField

along_track_columns = Literal[
    "latitude",
    "longitude",
    "date_time",
    "file_name",
    "mission",
    "track",
    "cycle",
    "basin_id",
    "sla_unfiltered",
    "sla_filtered",
    "dac",
    "ocean_tide",
    "internal_tide",
    "lwe",
    "mdt",
    "tpa_correction",
]

along_track_columns_schema: dict[along_track_columns, ColumnField] = {
    "latitude": atk_fields.latitude,
    "longitude": atk_fields.longitude,
    "date_time": atk_fields.date_time,
    "file_name": atk_fields.file_name,
    "mission": atk_fields.mission,
    "track": atk_fields.track,
    "cycle": atk_fields.cycle,
    "basin_id": atk_fields.basin_id,
    "sla_unfiltered": atk_fields.sla_unfiltered,
    "sla_filtered": atk_fields.sla_filtered,
    "dac": atk_fields.dac,
    "ocean_tide": atk_fields.ocean_tide,
    "internal_tide": atk_fields.internal_tide,
    "lwe": atk_fields.lwe,
    "mdt": atk_fields.mdt,
    "tpa_correction": atk_fields.tpa_correction,
}

along_track_fields = (
    along_track_columns
    | Literal[
        "distance",
        "delta_t",
    ]
)

along_track_schema: dict[along_track_fields, OceanDataField] = {
    **along_track_columns_schema,
    "distance": atk_fields.distance,
    "delta_t": atk_fields.delta_t,
}
