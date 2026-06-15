import datetime

import netCDF4 as nc
import numpy as np
import pytest
from shapely import Polygon

from OceanDB.data_access.eddy import Eddy
from OceanDB.schemas.eddy_schema import eddy_columns, eddy_columns_schema
from tests.database.fixtures import *

pytestmark = pytest.mark.uses_database


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


def test_get_eddy_tracks_from_times_batch_matches_single_queries(
    db_with_cyclonic_eddy_data,
):
    eddy = Eddy(db_with_cyclonic_eddy_data.config)

    start_dates = [
        datetime.datetime(1900, 1, 1),
        datetime.datetime(2100, 1, 1),
    ]
    end_dates = [
        datetime.datetime(2100, 1, 1),
        datetime.datetime(2101, 1, 1),
    ]

    expected = [
        eddy.get_eddy_tracks_from_times(start_date, end_date)
        for start_date, end_date in zip(start_dates, end_dates, strict=True)
    ]

    result = list(eddy.get_eddy_tracks_from_times_batch(start_dates, end_dates))

    assert len(result) == len(expected)
    assert result == expected
    assert tuple(sorted(result[0])) == (-2, -1, 0)
    assert result[1] == []


def test_eddy_scaling_matches_netcdf(db_with_cyclonic_eddy_data):
    cyclonic_filepath = Path(
        f"{db_with_cyclonic_eddy_data.config.eddy_data_directory}/cyclonic.nc"
    )
    file = nc.Dataset(cyclonic_filepath)
    file.set_auto_scale(False)
    file.set_auto_mask(True)
    netcdf_mask = np.array(file.variables["track"]) == 1
    print(netcdf_mask)

    eddy = Eddy(db_with_cyclonic_eddy_data.config)
    data_from_db = eddy.eddy_with_track_id(list(eddy_columns_schema.keys()), -1)
    assert data_from_db is not None

    exceptions = (
        "date_time",
        "cost_association",
        "cyclonic_type",
        "effective_contour_shape",
        "speed_contour_shape",
    )

    for variable, schema in eddy_columns_schema.items():
        if variable in exceptions:
            continue
        var_from_netcdf = file.variables[schema.netcdf_name][netcdf_mask]
        var_from_db = data_from_db.get_unscaled(variable)
        assert len(var_from_netcdf) == len(var_from_db)
        assert np.all(np.isclose(np.sort(var_from_netcdf), np.sort(var_from_db)))

    assert np.all(
        np.isclose(
            np.sort(file.variables["cost_association"][netcdf_mask]),
            np.sort(data_from_db.get_unscaled("cost_association")),
        )
    )


def test_ingest_cyclonic_eddy_exposes_contour_polygons(db_with_cyclonic_eddy_data):
    eddy = Eddy(db_with_cyclonic_eddy_data.config)
    contour_fields: list[eddy_columns] = [
        "effective_contour_shape",
        "speed_contour_shape",
    ]

    result = eddy.eddy_with_track_id(
        fields=contour_fields,
        track_id=-1,
    )

    assert result is not None

    for field_name in contour_fields:
        assert field_name in result

        polygons = result[field_name]

        assert len(polygons) > 0
        assert all(isinstance(polygon, Polygon) for polygon in polygons)
        assert all(not polygon.is_empty for polygon in polygons)
