from datetime import datetime, timedelta
import psycopg as pg
import numpy.typing as npt
import numpy as np

from OceanDB.data_access.base_query import BaseQuery
from OceanDB.data_access.projection_compiler import ProjectionCompiler
from OceanDB.data_access.schema.along_track_schema import along_track_fields, along_track_schema
from OceanDB.data_access.schema.eddy_schema import eddy_schema, eddy_fields
from OceanDB.ocean_data.dataset import Dataset
# from OceanDB.ocean_data.fields.eddy_fields import eddy_schema


class Eddy(BaseQuery):
    along_track_near_eddy_query = "queries/eddy/along_near_eddy.sql"
    eddy_with_id_query = "queries/eddy/eddy_from_track_id.sql"
    envelope_query = "queries/eddy/eddy_envelope.sql"
    default_along_track_fields = DEFAULT_ALONG_TRACK_FIELDS = [
        "file_name",
        "track",
        "cycle",
        "latitude",
        "longitude",
        "sla_unfiltered",
        "sla_filtered",
        "date_time",
        "dac",
        "ocean_tide",
        "internal_tide",
        "lwe",
        "mdt",
        "tpa_correction",
    ]

    def __init__(self):
        super().__init__()


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


    def eddy_with_track_id(
        self,
        fields: list[eddy_fields],
        track_id: int,
    ) -> Dataset[eddy_fields, npt.NDArray[np.floating]]:
        """
        Retrieve all observations for a single eddy track.
        Returns
        -------
        OceanData[EddyDataset] | None
        """
        compiler = ProjectionCompiler(schema=eddy_schema)
        query_string = compiler.compile(
            sql_template=self.load_sql_file(self.eddy_with_id_query),
            fields=fields,
        )
        params = {"track_id": track_id}

        return self.execute_query(
            query=query_string,
            schema=eddy_schema,
            params=params
        )

    def eddy_envelope_query(self, track_id: int):
        """
        Compute the spatiotemporal envelope for a single eddy track.

        This query aggregates all observations belonging to the given eddy
        (`track * cyclonic_type`) and returns:
        - the minimum and maximum observation timestamps, and
        - the set of basin identifiers intersecting the eddy over its lifetime.

        The result is a single-row dataset used to parameterize downstream
        along-track queries (time window and basin filtering).
        """

        compiler = ProjectionCompiler(schema=eddy_schema)
        query_string = compiler.compile(
            sql_template=self.load_sql_file(self.envelope_query),
            fields=[
                "max_date",
                "min_date",
                "basin_ids"
            ],
        )
        params = {"track_id": track_id}

        return self.execute_query(
            query=query_string,
            schema=eddy_schema,
            params=params
        )

    def along_track_points_near_eddy(
            self,
            *,
            track_id: int,
            fields: list[along_track_fields] | None = None,
    ):
        """
        Retrieve along-track altimetry points spatially and temporally
        associated with a given eddy track.

        The eddy envelope (time range and basin membership) is computed
        first and used to parameterize the along-track query.
        """

        # --- Phase 1: eddy envelope ---
        eddy_track = self.eddy_envelope_query(track_id=track_id)
        min_date = eddy_track["min_date"][0]
        max_date = eddy_track["max_date"][0]
        basin_ids = list(eddy_track["basin_ids"][0])
        print(f"envelope for eddy with track id {track_id} is {min_date} {max_date} {basin_ids}")

        # --- Phase 2: along-track projection ---
        compiler = ProjectionCompiler(schema=along_track_schema)

        raw_query = self.load_sql_file(self.along_track_near_eddy_query)
        print(raw_query)
        query = compiler.compile(
            sql_template=raw_query,
            fields=fields or self.default_along_track_fields,
        )

        params = {
            "track_id": track_id,
            "min_date": min_date,
            "max_date": max_date,
            "basin_ids": basin_ids,
            "speed_radius_scale_factor": 100,
        }

        return self.execute_query(
            query=query,
            schema=along_track_schema,
            params=params,
            dataset_name="along_track_near_eddy",
        )



    # def along_track_points_near_eddy(self,
    #                                 track_id: int,
    #                                 fields: list[along_track_fields]
    #     ):
    #     """
    #     Maybe provide more parameters?
    #     what does "near" mean?
    #
    #     """
    #
    #
    #     eddy_track =  self.eddy_envelope_query(track_id=track_id)
    #     """
    #     min_date = eddy_track["min_date"][0]
    #     max_date = eddy_track["max_date"][0]
    #     basin_ids = eddy_track["basin_ids"]
    #     """
    #     compiler = ProjectionCompiler(schema=along_track_schema)
    #     query = compiler.compile(
    #         sql_template=self.load_sql_file(self.along_track_near_eddy_query),
    #         fields=fields or self.default_along_track_fields,
    #     )
    #
    #     min_date = eddy_track["min_date"][0]
    #     max_date = eddy_track["max_date"][0]
    #     basin_ids = eddy_track["basin_ids"][0]
    #
    #     params = {
    #         "track_id": track_id,
    #         "min_date": min_date,
    #         "max_date": max_date,
    #         "basin_ids": basin_ids,
    #         "speed_radius_scale_factor": 100,
    #     }
    #
    #     return self.execute_query(
    #         query=query,
    #         schema=along_track_schema,
    #         params=params,
    #         dataset_name="along_track_near_eddy",
    #     )


        # along_query = """SELECT atk.file_name, atk.track, atk.cycle, atk.latitude, atk.longitude, atk.sla_unfiltered, atk.sla_filtered, atk.date_time as time, atk.dac, atk.ocean_tide, atk.internal_tide, atk.lwe, atk.mdt, atk.tpa_correction
        #                    FROM eddy
        #                    INNER JOIN along_track atk ON atk.date_time BETWEEN eddy.date_time AND (eddy.date_time + interval '1 day')
        # 	               AND st_dwithin(atk.along_track_point, eddy.eddy_point, (eddy.speed_radius * {speed_radius_scale_factor} * 2.0)::double precision)
        #                    WHERE eddy.track * eddy.cyclonic_type=%(track_id)s
        #                    AND atk.date_time BETWEEN '{min_date}'::timestamp AND '{max_date}'::timestamp
        #                    AND basin_id = ANY( ARRAY[{connected_basin_ids}] );"""


    #
    #     eddy = self.eddy_with_track_id(
    #         fields=[
    #             "max_date",
    #             "min_date",
    #             "basin_ids"
    #         ],
    #         track_id=track_id
    #     )
    #     return eddy
    #
    #
    #     eddy_query = """SELECT MIN(date_time), MAX(date_time), array_agg(distinct connected_id) || array_agg(distinct basin.id)
    #                         FROM eddy
    #                         LEFT JOIN basin ON ST_Intersects(basin.basin_geog, eddy.eddy_point)
    #                         LEFT JOIN basin_connections ON basin_connections.basin_id = basin.id
    #                         WHERE eddy.track * eddy.cyclonic_type=%(track_id)s
    #                         GROUP BY track, cyclonic_type;"""
    #     #
    #     along_query = """SELECT atk.file_name, atk.track, atk.cycle, atk.latitude, atk.longitude, atk.sla_unfiltered, atk.sla_filtered, atk.date_time as time, atk.dac, atk.ocean_tide, atk.internal_tide, atk.lwe, atk.mdt, atk.tpa_correction
    #                    FROM eddy
    #                    INNER JOIN along_track atk ON atk.date_time BETWEEN eddy.date_time AND (eddy.date_time + interval '1 day')
    # 	               AND st_dwithin(atk.along_track_point, eddy.eddy_point, (eddy.speed_radius * {speed_radius_scale_factor} * 2.0)::double precision)
    #                    WHERE eddy.track * eddy.cyclonic_type=%(track_id)s
    #                    AND atk.date_time BETWEEN '{min_date}'::timestamp AND '{max_date}'::timestamp
    #                    AND basin_id = ANY( ARRAY[{connected_basin_ids}] );"""
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

        # eddy_query = """
        #         SELECT
        #             MIN(date_time) AS min_date,
        #             MAX(date_time) AS max_date,
        #             array_agg(DISTINCT connected_id)
        #                 || array_agg(DISTINCT basin.id) AS basin_ids
        #         FROM eddy
        #         LEFT JOIN basin
        #             ON ST_Intersects(basin.basin_geog, eddy.eddy_point)
        #         LEFT JOIN basin_connections
        #             ON basin_connections.basin_id = basin.id
        #         WHERE eddy.track * eddy.cyclonic_type = %(track_id)s
        #         GROUP BY track, cyclonic_type;
        #         """
        #
        # fields: list[along_track_fields]
        # query = pg.sql.SQL(eddy_query).format(
        #     fields=pg.sql.SQL(', ').join([
        #         along_track_schema[field].to_sql_query() for field in fields
        # ]))
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

