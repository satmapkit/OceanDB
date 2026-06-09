from pathlib import Path

import netCDF4 as nc
import numpy as np
import pytest

from OceanDB.data_access.along_track import AlongTrack
from OceanDB.data_access.base_query import QuerySpec
from OceanDB.schemas.along_track_schema import along_track_columns_schema
from tests.database.fixtures import *

pytestmark = pytest.mark.uses_database


def test_ingest_alongtrack(db_with_alongtrack_data):
    assert True


def test_along_track_scaling_matches_netcdf(db_with_alongtrack_data):
    alongtrack_directory = db_with_alongtrack_data.config.along_track_data_directory
    file_name = "required_underscores_j2_20130101.nc"
    alongtrack_filepath = Path(f"{alongtrack_directory}/{file_name}")
    file = nc.Dataset(alongtrack_filepath)
    file.set_auto_scale(False)
    file.set_auto_mask(True)
    file["latitude"].set_auto_scale(True)
    file["longitude"].set_auto_scale(True)

    atdb = AlongTrack(db_with_alongtrack_data.config)
    data_from_db = atdb.execute_read_query(
        query_spec=QuerySpec(
            sql_template="""
                SELECT
                    {fields}
                FROM along_track atk
                WHERE atk.file_name = %(file_name)s
            """,
            schema=along_track_columns_schema,
        ),
        fields=list(along_track_columns_schema.keys()),
        params={"file_name": file_name},
        dataset_name="along_track",
    )
    assert data_from_db is not None

    exceptions = (
        "date_time",
        "file_name",
        "mission",
        "basin_id",
        "tpa_correction",
    )
    row_slice = slice(0, len(file.variables["latitude"]))

    for variable, schema in along_track_columns_schema.items():
        if variable in exceptions:
            continue
        var_from_netcdf = schema.from_netcdf(file, row_slice)
        var_from_db = data_from_db.get_unscaled(variable)
        assert len(var_from_netcdf) == len(var_from_db)
        assert np.all(
            np.isclose(np.sort(var_from_netcdf), np.sort(var_from_db), equal_nan=True)
        )
