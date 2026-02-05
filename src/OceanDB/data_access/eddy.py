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
    _along_track_near_eddy_query = "queries/eddy/along_near_eddy.sql"
    _eddy_with_id_query = "queries/eddy/eddy_from_track_id.sql"
    _envelope_query = "queries/eddy/eddy_envelope.sql"
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
            sql_template=self.load_sql_file(self._eddy_with_id_query),
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
            sql_template=self.load_sql_file(self._envelope_query),
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
    ) -> Dataset[along_track_fields, Any] | None:
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
            sql_template=self.load_sql_file(self._along_track_near_eddy_query),
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

