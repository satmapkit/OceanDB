from datetime import datetime
import numpy.typing as npt
import numpy as np
from typing import Any, Literal, Iterable

from OceanDB.data_access.along_track import BaseReadQuery
from OceanDB.schemas.along_track_schema import along_track_fields
from OceanDB.schemas.eddy_schema import (
    eddy_schema,
    eddy_fields,
    eddy_columns,
    eddy_columns_schema,
    along_track_eddy_schema,
)
from OceanDB.ocean_data.dataset import Dataset
from OceanDB.data_access.base_query import QuerySpec

envelope_fields = Literal["max_date", "min_date", "basin_ids"]


class Eddy(BaseReadQuery):
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

    def get_eddy_tracks_from_times(
        self,
        start_date: datetime,
        end_date: datetime,
    ) -> list[int]:
        """
        Retrieve distinct eddy track identifiers observed within a given time range.

        :return:
            Eddy track ids ( - = cyclonic, + = anticyclonic )
        """
        query = """
        SELECT DISTINCT (track * eddy.cyclonic_type) AS track_id
        FROM eddy
        WHERE date_time >= %(start_date)s
          AND date_time <  %(end_date)s
        ORDER BY track_id;
        """

        params = {
            "start_date": start_date,
            "end_date": end_date,
        }

        with self.cursor() as cur:
            cur.execute(query, params)
            return [row[0] for row in cur.fetchall()]

    def eddy_with_track_id(
        self,
        fields: list[eddy_columns],
        track_id: int,
    ) -> Dataset[eddy_columns, npt.NDArray[np.floating]] | None:
        """
        Retrieve observations for a single eddy track.

        :param fields:
            Requested fields to output

        :param track_id:
            Query eddy track id ( - = cyclonic, + = anticyclonic )

        :return:
            If no eddy found with the specified track id,
            :code:`None` is returned.
            Otherwise, a :class:`Dataset <OceanDB.ocean_data.dataset.Dataset>`
            of eddy data with requested fields is returned.
        """
        query_spec = QuerySpec(
            sql_template=self.load_sql_file(self._eddy_with_id_query),
            schema=eddy_columns_schema,
        )
        params = {"track_id": track_id}

        return self.execute_read_query(
            query_spec=query_spec, fields=fields, params=params, dataset_name="eddy"
        )

    def eddy_envelope_query(
        self, track_id: int
    ) -> Dataset[envelope_fields, Any] | None:
        """
        Compute the spatiotemporal envelope for a single eddy track.
        Specifically, extracts

        - the minimum and maximum observation timestamps surrounding
          the eddy
        - the set of basins intersecting the eddy over its lifetime.

        :param track_id:
            Query eddy track id ( - = cyclonic, + = anticyclonic )

        :return:
            Single-row dataset used to parameterize downstream
            along-track queries (time window and basin filtering).
        """

        query_spec = QuerySpec(
            sql_template=self.load_sql_file(self._envelope_query),
            schema=eddy_schema,
        )
        params = {"track_id": track_id}
        fields: list[envelope_fields] = ["max_date", "min_date", "basin_ids"]

        return self.execute_read_query(
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

        If no eddy of the given id exists, will raise a :code:`ValueError`

        The eddy envelope (time range and basin membership) is computed
        first and used to parameterize the along-track query.

        :param track_id:
            Query eddy track id ( - = cyclonic, + = anticyclonic )

        :param fields:
            Requested along track fields to output.
            If none are specified, defaults to :attr:`default_along_track_fields <Eddy.default_along_track_fields>`

        :return:
            If no along track data is found surrounding the eddy, :code:`None` is returned.
            Otherwise, a :class:`Dataset <OceanDB.ocean_data.dataset.Dataset>`
            of along track data with requested fields is returned.
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

        return self.execute_read_query(
            query_spec=query_spec,
            fields=fields,
            params=params,
            dataset_name="along_track_near_eddy",
        )
