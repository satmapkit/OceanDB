from typing import Literal
from OceanDB.ocean_data.fields import eddy_fields as edy_fields
from OceanDB.ocean_data.ocean_data import OceanDataField

eddy_fields = Literal[
    "latitude",
    "longitude",
    "date_time",
    "track",
    "cyclonic_type",
    "amplitude",
    "effective_radius",
    "effective_area",
    "speed_radius",
    "speed_average",
    "num_contours",
    "observation_flag",
    "max_date",
    "min_date",
    "basin_ids"
]

eddy_schema: dict[eddy_fields, OceanDataField] = {
    "latitude": edy_fields.latitude,
    "longitude": edy_fields.longitude,
    "date_time": edy_fields.date_time,
    "track": edy_fields.track,
    "cyclonic_type": edy_fields.cyclonic_type,
    "amplitude": edy_fields.amplitude,
    "effective_radius": edy_fields.effective_radius,
    "effective_area": edy_fields.effective_area,
    "speed_radius": edy_fields.speed_radius,
    "speed_average": edy_fields.speed_average,
    "num_contours": edy_fields.num_contours,
    "observation_flag": edy_fields.observation_flag,

    "max_date": edy_fields.max_date_time,
    "min_date": edy_fields.min_date_time,
    "basin_ids": edy_fields.basin_ids,
}
