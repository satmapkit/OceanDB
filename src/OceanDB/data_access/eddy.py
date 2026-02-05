from datetime import datetime
import psycopg as pg
import numpy.typing as npt
import numpy as np
from typing import Any, Literal, Iterable

from OceanDB.data_access.along_track import BaseQuery
from OceanDB.schemas.along_track_schema import along_track_fields
from OceanDB.schemas.eddy_schema import eddy_schema, eddy_fields, along_track_eddy_schema 
from OceanDB.ocean_data.dataset import Dataset
from OceanDB.data_access.base_query import QuerySpec

envelope_fields = Literal["max_date", "min_date", "basin_ids"]


class Eddy(BaseQuery):
    along_track_near_eddy_query = "queries/eddy/along_near_eddy.sql"
    eddy_with_id_query = "queries/eddy/eddy_from_track_id.sql"
    envelope_query = "queries/eddy/eddy_envelope.sql"
    default_along_track_fields: list[along_track_fields] = [
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
    ) -> Dataset[eddy_fields, npt.NDArray[np.floating]] | None:
        """
        Retrieve all observations for a single eddy track.
        Returns
        -------
        OceanData[EddyDataset] | None
        """
        query_spec = QuerySpec(
            sql_template=self.load_sql_file(self.eddy_with_id_query),
            schema=eddy_schema,
        )
        params = {"track_id": track_id}

        return self.execute_query(
            query_spec=query_spec, fields=fields, params=params, dataset_name="eddy"
        )

    def eddy_envelope_query(
        self, track_id: int
    ) -> Dataset[envelope_fields, Any] | None:
        """
        Compute the spatiotemporal envelope for a single eddy track.

        This query aggregates all observations belonging to the given eddy
        (`track * cyclonic_type`) and returns:
        - the minimum and maximum observation timestamps, and
        - the set of basin identifiers intersecting the eddy over its lifetime.

        The result is a single-row dataset used to parameterize downstream
        along-track queries (time window and basin filtering).
        """

        query_spec = QuerySpec(
            sql_template=self.load_sql_file(self.envelope_query),
            schema=eddy_schema,
        )
        params = {"track_id": track_id}
        fields: list[envelope_fields] = ["max_date", "min_date", "basin_ids"]

        return self.execute_query(
            query_spec=query_spec,
            fields=fields,
            params=params,
            dataset_name="eddy",
        )

    def along_track_points_near_eddy(
        self,
        *,
        track_id: int,
        fields: Iterable[along_track_fields] | None = None,
    ):
        """
        Retrieve along-track altimetry points spatially and temporally
        associated with a given eddy track.

        The eddy envelope (time range and basin membership) is computed
        first and used to parameterize the along-track query.
        """

        if fields is None:
            fields = self.default_along_track_fields

        # --- Phase 1: eddy envelope ---
        eddy_track = self.eddy_envelope_query(track_id=track_id)
        if eddy_track is None:
            raise ValueError("Could not find eddy track")

        min_date = eddy_track["min_date"][0]
        max_date = eddy_track["max_date"][0]
        basin_ids = list(eddy_track["basin_ids"][0])

        # --- Phase 2: along-track projection ---
        query_spec = QuerySpec(
            sql_template=self.load_sql_file(self.along_track_near_eddy_query),
            schema=along_track_eddy_schema,
        )

        params = {
            "track_id": track_id,
            "min_date": min_date,
            "max_date": max_date,
            "basin_ids": basin_ids,
            "speed_radius_scale_factor": 100,
        }

        return self.execute_query(
            query_spec=query_spec,
            fields=fields,
            params=params,
            dataset_name="along_track_near_eddy",
        )

    def old_eddy(self, track_id):

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

        along_query = """SELECT atk.file_name, atk.track, atk.cycle, atk.latitude, atk.longitude, atk.sla_unfiltered, atk.sla_filtered, atk.date_time as time, atk.dac, atk.ocean_tide, atk.internal_tide, atk.lwe, atk.mdt, atk.tpa_correction
                               FROM eddy
                               INNER JOIN along_track atk ON atk.date_time BETWEEN eddy.date_time AND (eddy.date_time + interval '1 day')
            	               AND st_dwithin(atk.along_track_point, eddy.eddy_point, (eddy.speed_radius * {speed_radius_scale_factor} * 2.0)::double precision)
                               WHERE eddy.track * eddy.cyclonic_type=%(track_id)s
                               AND atk.date_time BETWEEN '{min_date}'::timestamp AND '{max_date}'::timestamp
                               AND basin_id = ANY( ARRAY[{connected_basin_ids}] );"""
        values = {"track_id": track_id}

        with pg.connect(self.config.postgres_dsn) as connection:
            with connection.cursor(row_factory=pg.rows.dict_row) as cursor:
                cursor.execute(eddy_query, values)
                data = cursor.fetchall()

                along_query = along_query.format(
                    speed_radius_scale_factor=100,
                    min_date=data[0]["min_date"],
                    max_date=data[0]["max_date"],
                    connected_basin_ids=data[0]["basin_ids"],
                )
                cursor.execute(along_query, values)
                data = cursor.fetchall()

        return data
