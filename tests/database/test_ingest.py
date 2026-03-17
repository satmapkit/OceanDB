import psycopg as pg
from pathlib import Path

from OceanDB.etl import EddyETL

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


def test_ingest_eddy(db_with_basin_data):
    oceandb_etl = EddyETL(config=db_with_basin_data.config)
    eddy_directory = oceandb_etl.config.eddy_data_directory

    cyclonic_filepath = Path(f"{eddy_directory}/cyclonic.nc")
    oceandb_etl.ingest_eddy_data_file(cyclonic_filepath, cyclonic_type=-1)

    # print(
    #     "Processing Ingesting META3.2_DT_allsat_Anticyclonic_long_19930101_20220209.nc"
    # )
    # anticyclonic_filepath = Path(
    #     f"{eddy_directory}/META3.2_DT_allsat_AntiCyclonic_long_19930101_20220209.nc"
    # )
    # oceandb_etl.ingest_eddy_data_file(anticyclonic_filepath, cyclonic_type=1)
