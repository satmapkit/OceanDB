from datetime import datetime, timedelta
from typing import Iterable, List, Literal, get_args
import psycopg as pg
import numpy.typing as npt
import numpy as np

from OceanDB.data_access.base_query import BaseQuery
from OceanDB.data_access.schema.along_track_schema import along_track_fields, along_track_schema
from OceanDB.ocean_data.dataset import Dataset
from OceanDB.ocean_data.fields.eddy_fields import eddy_schema


class Eddy(BaseQuery):
    along_track_near_eddy_query = "queries/eddy/along_near_eddy.sql"
    eddy_with_id_query = "queries/eddy/eddy_from_track_id.sql"

    def __init__(self):
        super().__init__()

    def eddy_with_track_id(
        self,
        track_id: int,
    ) -> Dataset[along_track_fields, npt.NDArray[np.floating]]:
        """
        Retrieve all observations for a single eddy track.

        Returns
        -------
        OceanData[EddyDataset] | None
        """
        query = self.load_sql_file(self.eddy_with_id_query)
        params = {"track_id": track_id}

        return self.execute_query(query, eddy_schema, params)


    def get_eddy_tracks_from_times(
        self,
        start_date: datetime,
        end_date: datetime,
    ) -> list[int]:
        """
        Retrieve distinct eddy track identifiers observed within a given time range.

        This is executing a query -> but not going through our base execute query method quite yet,

        """
        query = """
        SELECT DISTINCT track
        FROM eddy
        WHERE date_time >= %(start_date)s
          AND date_time <  %(end_date)s
        ORDER BY track;
        """

        params = {
            "start_date": start_date,
            "end_date": end_date,
        }

        with pg.connect(self.config.postgres_dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                return [row[0] for row in cur.fetchall()]


    def along_track_points_near_eddy(self, track_id):
        """
        Retrieve along-track altimetry points spatially and temporally associated
        with a given eddy track.

        This method performs a two-stage query:

        1. It first determines the temporal extent of the specified eddy track
           (minimum and maximum `date_time`) and collects all basin identifiers
           associated with the eddy, including directly intersecting basins and
           their connected basins.

        2. It then queries the `along_track` table for altimetry observations that:
           - Occur within the eddy's lifetime (with an additional 1-day tolerance),
           - Lie within a distance threshold of the eddy center
             (`speed_radius * scale_factor * 2.0`),
           - Belong to one of the basins connected to the eddy.

        Parameters
        ----------
        track_id : int
            Signed eddy track identifier. The sign encodes cyclonic polarity and
            is matched against `eddy.track * eddy.cyclonic_type`.

        Returns
        -------
        list[tuple]
            A list of rows from the `along_track` table containing altimetry
            measurements near the eddy. Each row includes spatial coordinates,
            sea level anomaly values, timing information, and geophysical
            correction terms.

        Notes
        -----
        - Temporal filtering is based on `TIMESTAMP WITHOUT TIME ZONE` columns;
          all timestamps are assumed to be naive and expressed in a consistent
          reference time (typically UTC).
        - Spatial filtering uses PostGIS geography types and `ST_DWithin`, with
          distances interpreted in meters.
        - The spatial search radius is derived from the eddy `speed_radius` and
          scaled using `self.variable_scale_factor["speed_radius"]`.
        - Basin connectivity is resolved via the `basin_connections` table.
        - This method assumes the eddy track exists; no explicit guard is
          performed for empty result sets.


        """

        # eddy_query = """SELECT MIN(date_time), MAX(date_time), array_agg(distinct connected_id) || array_agg(distinct basin.id)
        #                     FROM eddy
        #                     LEFT JOIN basin ON ST_Intersects(basin.basin_geog, eddy.eddy_point)
        #                     LEFT JOIN basin_connections ON basin_connections.basin_id = basin.id
        #                     WHERE eddy.track * eddy.cyclonic_type=%(track_id)s
        #                     GROUP BY track, cyclonic_type;"""
        #
        # along_query = """SELECT atk.file_name, atk.track, atk.cycle, atk.latitude, atk.longitude, atk.sla_unfiltered, atk.sla_filtered, atk.date_time as time, atk.dac, atk.ocean_tide, atk.internal_tide, atk.lwe, atk.mdt, atk.tpa_correction
        #                FROM eddy
        #                INNER JOIN along_track atk ON atk.date_time BETWEEN eddy.date_time AND (eddy.date_time + interval '1 day')
    	#                AND st_dwithin(atk.along_track_point, eddy.eddy_point, (eddy.speed_radius * {speed_radius_scale_factor} * 2.0)::double precision)
        #                WHERE eddy.track * eddy.cyclonic_type=%(track_id)s
        #                AND atk.date_time BETWEEN '{min_date}'::timestamp AND '{max_date}'::timestamp
        #                AND basin_id = ANY( ARRAY[{connected_basin_ids}] );"""
        # values = {"track_id": track_id}
        #
        # with pg.connect(self.config.postgres_dsn) as connection:
        #     with connection.cursor() as cursor:
        #         cursor.execute(eddy_query, values)
        #         data = cursor.fetchall()
        #
        #         values["min_date"] = data[0][0]
        #         values["max_date"] = data[0][1]
        #
        #         print(data)
        #
        #         along_query = along_query.format(
        #             # speed_radius_scale_factor=self.variable_scale_factor["speed_radius"],
        #             speed_radius_scale_factor=100,
        #             min_date=data[0][0],
        #             max_date=data[0][1],
        #             connected_basin_ids=data[0][2],
        #         )
        #         cursor.execute(along_query, values)
        #         data = cursor.fetchall()

        eddy_query = """
                SELECT
                    MIN(date_time) AS min_date,
                    MAX(date_time) AS max_date,
                    array_agg(DISTINCT connected_id)
                        || array_agg(DISTINCT basin.id) AS basin_ids
                FROM eddy
                LEFT JOIN basin
                    ON ST_Intersects(basin.basin_geog, eddy.eddy_point)
                LEFT JOIN basin_connections
                    ON basin_connections.basin_id = basin.id
                WHERE eddy.track * eddy.cyclonic_type = %(track_id)s
                GROUP BY track, cyclonic_type;
                """

        fields: list[along_track_fields]
        query = pg.sql.SQL(eddy_query).format(
            fields=pg.sql.SQL(', ').join([
                along_track_schema[field].to_sql_query() for field in fields
        ]))
        # self.execute_query(
        #     query=eddy_query,
        #     params={
        #         "track_id": track_id
        #     },
        #     schema=
        # )



        # along_query = """SELECT atk.file_name, atk.track, atk.cycle, atk.latitude, atk.longitude, atk.sla_unfiltered, atk.sla_filtered, atk.date_time as time, atk.dac, atk.ocean_tide, atk.internal_tide, atk.lwe, atk.mdt, atk.tpa_correction
        #                        FROM eddy
        #                        INNER JOIN along_track atk ON atk.date_time BETWEEN eddy.date_time AND (eddy.date_time + interval '1 day')
        #     	               AND st_dwithin(atk.along_track_point, eddy.eddy_point, (eddy.speed_radius * {speed_radius_scale_factor} * 2.0)::double precision)
        #                        WHERE eddy.track * eddy.cyclonic_type=%(track_id)s
        #                        AND atk.date_time BETWEEN '{min_date}'::timestamp AND '{max_date}'::timestamp
        #                        AND basin_id = ANY( ARRAY[{connected_basin_ids}] );"""
        # values = {"track_id": track_id}
        #
        #
        #
        #
        # with pg.connect(self.config.postgres_dsn) as connection:
        #     with connection.cursor(row_factory=pg.rows.dict_row) as cursor:
        #         cursor.execute(eddy_query, values)
        #         data = cursor.fetchall()
        #
        #         along_query = along_query.format(
        #             speed_radius_scale_factor=100,
        #             min_date=data[0]['min_date'],
        #             max_date=data[0]['max_date'],
        #             connected_basin_ids=data[0]['basin_ids'],
        #         )
        #         cursor.execute(along_query, values)
        #         data = cursor.fetchall()

        # return data

