from typing import Literal

import OceanDB.ocean_data.fields.eddy_fields as efields
from OceanDB.ocean_data.ocean_data import ColumnField, OceanDataField
from OceanDB.schemas.along_track_schema import (along_track_fields,
                                                along_track_schema)

eddy_columns = Literal[
    "amplitude",
    "cost_association",
    "effective_area",
    "effective_contour_height",
    "effective_contour_latitude",
    "effective_contour_longitude",
    "effective_contour_shape_error",
    "effective_radius",
    "inner_contour_height",
    "latitude",
    "latitude_max",
    "longitude",
    "longitude_max",
    "num_contours",
    "num_point_e",
    "num_point_s",
    "observation_flag",
    "observation_number",
    "speed_area",
    "speed_average",
    "speed_contour_height",
    "speed_contour_shape",
    "speed_contour_shape_error",
    "speed_radius",
    "date_time",
    "track",
    "cyclonic_type",
]

eddy_columns_schema: dict[eddy_columns, ColumnField] = {
    "amplitude": efields.amplitude,
    "cost_association": efields.cost_association,
    "effective_area": efields.effective_area,
    "effective_contour_height": efields.effective_contour_height,
    # commented out since these use geometry
    # "effective_contour_latitude": edy_fields.effective_contour_latitude,
    # "effective_contour_longitude": edy_fields.effective_contour_longitude,
    "effective_contour_shape_error": efields.effective_contour_shape_error,
    "effective_radius": efields.effective_radius,
    "inner_contour_height": efields.inner_contour_height,
    "latitude": efields.latitude,
    "latitude_max": efields.latitude_max,
    "longitude": efields.longitude,
    "longitude_max": efields.longitude_max,
    "num_contours": efields.num_contours,
    "num_point_e": efields.num_point_e,
    "num_point_s": efields.num_point_s,
    "observation_flag": efields.observation_flag,
    "observation_number": efields.observation_number,
    "speed_area": efields.speed_area,
    "speed_average": efields.speed_average,
    "speed_contour_height": efields.speed_contour_height,
    # commented out since these use geometry
    # "speed_contour_shape": edy_fields.speed_contour_shape,
    "speed_contour_shape_error": efields.speed_contour_shape_error,
    "speed_radius": efields.speed_radius,
    "date_time": efields.date_time,
    "track": efields.track,
    "cyclonic_type": efields.cyclonic_type,
}

eddy_fields = eddy_columns | Literal["max_date", "min_date", "basin_ids"]

eddy_schema: dict[eddy_fields, OceanDataField] = {
    **eddy_columns_schema,
    "max_date": efields.max_date_time,
    "min_date": efields.min_date_time,
    "basin_ids": efields.basin_ids,
}

along_track_eddy_schema: dict[along_track_fields, OceanDataField] = {
    **along_track_schema,
    "distance": efields.distance,
    "delta_t": efields.distance,
}
