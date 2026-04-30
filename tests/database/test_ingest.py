import datetime

import psycopg as pg

from OceanDB.data_access.eddy import Eddy
from OceanDB.schemas.eddy_schema import eddy_columns_schema

from .fixtures import *


def test_insert_basin_data(db_with_basin_data):
    with pg.connect(db_with_basin_data.connection_string) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM basin;")
            res = cur.fetchone()
            assert res
            # should have at least one entry in basin table
            assert res[0] > 0

            cur.execute("SELECT COUNT(*) FROM basin_connections;")
            res = cur.fetchone()
            assert res
            # should have at least one entry in basin table
            assert res[0] > 0


def test_ingest_cyclonic_eddy(db_with_cyclonic_eddy_data):
    eddy = Eddy(db_with_cyclonic_eddy_data.config)
    ids = eddy.get_eddy_tracks_from_times(
        datetime.datetime(1900, 1, 1), datetime.datetime(2100, 1, 1)
    )
    assert tuple(sorted(ids)) == (-2, -1, 0)

    fields = list(eddy_columns_schema.keys())

    for i in ids:
        res = eddy.eddy_with_track_id(fields=fields, track_id=i)
        assert res is not None

    # print(
    #     "Processing Ingesting META3.2_DT_allsat_Anticyclonic_long_19930101_20220209.nc"
    # )
    # anticyclonic_filepath = Path(
    #     f"{eddy_directory}/META3.2_DT_allsat_AntiCyclonic_long_19930101_20220209.nc"
    # )
    # oceandb_etl.ingest_eddy_data_file(anticyclonic_filepath, cyclonic_type=1)


def test_ingest_alongtrack(db_with_alongtrack_data):
    assert True
