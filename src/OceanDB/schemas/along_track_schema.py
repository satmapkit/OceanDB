from typing import ClassVar, Mapping
import numpy as np
from datetime import datetime

from OceanDB.ocean_data.ocean_data import ColumnField, DerivedField, OceanDataField

atk_alias = "atk"

class AlongTrackSchema:
    """
    Declarative schema for along-track projections.

    - Defines all available fields
    - Provides a single source of truth for valid field names
    - Exposes helpers for query compilation
    """
    ALL_FIELDS: ClassVar[Mapping[str, OceanDataField]] = {
        name: value
        for name, value in vars().items()
        if isinstance(value, OceanDataField)
    }


    latitude = ColumnField(
        name="latitude",
        postgres_table_name=atk_alias,
        postgres_column_name="latitude",
        python_type=np.float64,
        postgres_type="double precision",
        nc_name="latitude",
        nc_scale=1,
        nc_offset=0,
    )

    longitude = ColumnField(
        name="longitude",
        postgres_table_name=atk_alias,
        postgres_column_name="longitude",
        python_type=np.float64,
        postgres_type="double precision",
        nc_name="longitude",
        nc_scale=1,
        nc_offset=0,
    )

    date_time = ColumnField(
        name="date_time",
        postgres_table_name=atk_alias,
        postgres_column_name="date_time",
        python_type=datetime,
        postgres_type="timestamp",
        nc_name="time",
        nc_scale=1,
        nc_offset=0,
    )

    file_name = ColumnField(
        name="file_name",
        postgres_table_name=atk_alias,
        postgres_column_name="file_name",
        python_type=str,
        postgres_type="text",
        nc_name="file_name",
    )

    mission = ColumnField(
        name="mission",
        postgres_table_name=atk_alias,
        postgres_column_name="mission",
        python_type=str,
        postgres_type="text",
        nc_name="mission",
    )

    track = ColumnField(
        name="track",
        postgres_table_name=atk_alias,
        postgres_column_name="track",
        python_type=int,
        postgres_type="smallint",
        nc_name="track",
    )

    cycle = ColumnField(
        name="cycle",
        postgres_table_name=atk_alias,
        postgres_column_name="cycle",
        python_type=int,
        postgres_type="smallint",
        nc_name="cycle",
    )

    basin_id = ColumnField(
        name="basin_id",
        postgres_table_name=atk_alias,
        postgres_column_name="basin_id",
        python_type=int,
        postgres_type="smallint",
        nc_name="basin_id",
    )

    # -----------------
    # Altimetry signals
    # -----------------

    sla_unfiltered = ColumnField(
        name="sla_unfiltered",
        postgres_table_name=atk_alias,
        postgres_column_name="sla_unfiltered",
        python_type=np.float64,
        postgres_type="smallint",
        nc_name="sla_unfiltered",
        nc_scale=1000,
    )

    sla_filtered = ColumnField(
        name="sla_filtered",
        postgres_table_name=atk_alias,
        postgres_column_name="sla_filtered",
        python_type=np.float64,
        postgres_type="smallint",
        nc_name="sla_filtered",
        nc_scale=1000,
    )

    dac = ColumnField(
        name="dac",
        postgres_table_name=atk_alias,
        postgres_column_name="dac",
        python_type=np.float64,
        postgres_type="smallint",
        nc_name="dac",
        nc_scale=1000,
    )

    ocean_tide = ColumnField(
        name="ocean_tide",
        postgres_table_name=atk_alias,
        postgres_column_name="ocean_tide",
        python_type=np.float64,
        postgres_type="smallint",
        nc_name="ocean_tide",
        nc_scale=1000,
    )

    internal_tide = ColumnField(
        name="internal_tide",
        postgres_table_name=atk_alias,
        postgres_column_name="internal_tide",
        python_type=np.float64,
        postgres_type="smallint",
        nc_name="internal_tide",
        nc_scale=1000,
    )

    lwe = ColumnField(
        name="lwe",
        postgres_table_name=atk_alias,
        postgres_column_name="lwe",
        python_type=np.float64,
        postgres_type="smallint",
        nc_name="lwe",
        nc_scale=1000,
    )

    mdt = ColumnField(
        name="mdt",
        postgres_table_name=atk_alias,
        postgres_column_name="mdt",
        python_type=np.float64,
        postgres_type="smallint",
        nc_name="mdt",
        nc_scale=1000,
    )

    tpa_correction = ColumnField(
        name="tpa_correction",
        postgres_table_name=atk_alias,
        postgres_column_name="tpa_correction",
        python_type=np.float64,
        postgres_type="smallint",
        nc_name="tpa_correction",
        nc_scale=1000,
    )

    # -----------------
    # Derived / query-only fields
    # -----------------

    distance = DerivedField(
        name="distance",
        expression=f"""
            ST_Distance(
                ST_MakePoint(%(longitude)s, %(latitude)s),
                {atk_alias}.along_track_point
            )
        """,
        python_type=np.float64,
        postgres_type="double precision",
    )

    delta_t = DerivedField(
        name="delta_t",
        expression=f"""
            EXTRACT(EPOCH FROM (%(central_date_time)s - {atk_alias}.date_time))
        """,
        python_type=np.float64,
        postgres_type="double precision",
    )









# from typing import Literal
#
# import numpy as np
# from datetime import datetime
# from OceanDB.ocean_data.ocean_data import OceanDataField
#
#
# # -----------------
# # Core coordinates & identity
# # -----------------
#
# atk_alias = "atk"
#
#
# latitude = OceanDataField(
#     name="latitude",
#     postgres_table_name=atk_alias,  # ✅ alias
#     postgres_column_name="latitude",
#     python_type=np.float64,
#     postgres_type="double precision",
#     nc_name="latitude",
#     nc_scale=1,
#     nc_offset=0,
# )
#
# longitude = OceanDataField(
#     name="longitude",
#     postgres_table_name=atk_alias,
#     postgres_column_name="longitude",
#     python_type=np.float64,
#     postgres_type="double precision",
#     nc_name="longitude",
#     nc_scale=1,
#     nc_offset=0,
# )
#
# date_time = OceanDataField(
#     name="date_time",
#     postgres_table_name=atk_alias,
#     postgres_column_name="date_time",
#     python_type=datetime,
#     postgres_type="timestamp",
#     nc_name="time",
#     nc_scale=1,
#     nc_offset=0,
# )
#
# file_name = OceanDataField(
#     name="file_name",
#     postgres_table_name=atk_alias,
#     postgres_column_name="file_name",
#     python_type=str,
#     postgres_type="text",
#     nc_name="file_name",
#     nc_scale=1,
#     nc_offset=0,
# )
#
# mission = OceanDataField(
#     name="mission",
#     postgres_table_name=atk_alias,
#     postgres_column_name="mission",
#     python_type=str,
#     postgres_type="text",
#     nc_name="mission",
#     nc_scale=1,
#     nc_offset=0,
# )
#
# track = OceanDataField(
#     name="track",
#     postgres_table_name=atk_alias,
#     postgres_column_name="track",
#     python_type=int,
#     postgres_type="smallint",
#     nc_name="track",
#     nc_scale=1,
#     nc_offset=0,
# )
#
# cycle = OceanDataField(
#     name="cycle",
#     postgres_table_name=atk_alias,
#     postgres_column_name="cycle",
#     python_type=int,
#     postgres_type="smallint",
#     nc_name="cycle",
#     nc_scale=1,
#     nc_offset=0,
# )
#
# basin_id = OceanDataField(
#     name="basin_id",
#     postgres_table_name=atk_alias,
#     postgres_column_name="basin_id",
#     python_type=int,
#     postgres_type="smallint",
#     nc_name="basin_id",
#     nc_scale=1,
#     nc_offset=0,
# )
#
# # -----------------
# # Altimetry signals
# # -----------------
#
# sla_unfiltered = OceanDataField(
#     name="sla_unfiltered",
#     postgres_table_name=atk_alias,
#     postgres_column_name="sla_unfiltered",
#     python_type=np.float64,
#     postgres_type="smallint",
#     nc_name="sla_unfiltered",
#     nc_scale=1000,
#     nc_offset=0,
# )
#
# sla_filtered = OceanDataField(
#     name="sla_filtered",
#     postgres_table_name=atk_alias,
#     postgres_column_name="sla_filtered",
#     python_type=np.float64,
#     postgres_type="smallint",
#     nc_name="sla_filtered",
#     nc_scale=1000,
#     nc_offset=0,
# )
#
# dac = OceanDataField(
#     name="dac",
#     postgres_table_name=atk_alias,
#     postgres_column_name="dac",
#     python_type=np.float64,
#     postgres_type="smallint",
#     nc_name="dac",
#     nc_scale=1000,
#     nc_offset=0,
# )
#
# ocean_tide = OceanDataField(
#     name="ocean_tide",
#     postgres_table_name=atk_alias,
#     postgres_column_name="ocean_tide",
#     python_type=np.float64,
#     postgres_type="smallint",
#     nc_name="ocean_tide",
#     nc_scale=1000,
#     nc_offset=0,
# )
#
# internal_tide = OceanDataField(
#     name="internal_tide",
#     postgres_table_name=atk_alias,
#     postgres_column_name="internal_tide",
#     python_type=np.float64,
#     postgres_type="smallint",
#     nc_name="internal_tide",
#     nc_scale=1000,
#     nc_offset=0,
# )
#
# lwe = OceanDataField(
#     name="lwe",
#     postgres_table_name=atk_alias,
#     postgres_column_name="lwe",
#     python_type=np.float64,
#     postgres_type="smallint",
#     nc_name="lwe",
#     nc_scale=1000,
#     nc_offset=0,
# )
#
# mdt = OceanDataField(
#     name="mdt",
#     postgres_table_name=atk_alias,
#     postgres_column_name="mdt",
#     python_type=np.float64,
#     postgres_type="smallint",
#     nc_name="mdt",
#     nc_scale=1000,
#     nc_offset=0,
# )
#
# tpa_correction = OceanDataField(
#     name="tpa_correction",
#     postgres_table_name=atk_alias,
#     postgres_column_name="tpa_correction",
#     python_type=np.float64,
#     postgres_type="smallint",
#     nc_name="tpa_correction",
#     nc_scale=1000,
#     nc_offset=0,
# )
#
# # -----------------
# # Derived / query-only fields
# # -----------------
#
# distance = OceanDataField(
#     name="distance",
#     custom_calculation=f"""
#         ST_Distance(
#             ST_MakePoint(%(longitude)s, %(latitude)s),
#             {atk_alias}.along_track_point
#         )
#     """,
#     python_type=np.float64,
#     postgres_type="double precision",
# )
#
# delta_t = OceanDataField(
#     name="delta_t",
#     custom_calculation=f"""
#         EXTRACT(EPOCH FROM (%(central_date_time)s - {atk_alias}.date_time))
#     """,
#     python_type=np.float64,
#     postgres_type="double precision",
# )
#
# along_track_fields = Literal[
#     "latitude",
#     "longitude",
#     "date_time",
#     "file_name",
#     "mission",
#     "track",
#     "cycle",
#     "basin_id",
#     "sla_unfiltered",
#     "sla_filtered",
#     "dac",
#     "ocean_tide",
#     "internal_tide",
#     "lwe",
#     "mdt",
#     "tpa_correction",
#     "distance",
#     "delta_t",
# ]
#
# along_track_schema: dict[along_track_fields, OceanDataField] = {
#     "latitude": latitude,
#     "longitude": atk_fields.longitude,
#     "date_time": atk_fields.date_time,
#     "file_name": atk_fields.file_name,
#     "mission": atk_fields.mission,
#     "track": atk_fields.track,
#     "cycle": atk_fields.cycle,
#     "basin_id": atk_fields.basin_id,
#     "sla_unfiltered": atk_fields.sla_unfiltered,
#     "sla_filtered": atk_fields.sla_filtered,
#     "dac": atk_fields.dac,
#     "ocean_tide": atk_fields.ocean_tide,
#     "internal_tide": atk_fields.internal_tide,
#     "lwe": atk_fields.lwe,
#     "mdt": atk_fields.mdt,
#     "tpa_correction": atk_fields.tpa_correction,
#     "distance": atk_fields.distance,
#     "delta_t": atk_fields.delta_t,
# }
