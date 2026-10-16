"""Regress WGS84 distance, membership, and nearest-neighbor rank.

Oracle literals were independently generated with GeographicLib 2.1 using
``Geodesic.WGS84.Inverse(0.0, 0.0, latitude, longitude)["s12"]`` (meters;
semi-major axis 6378137 m, flattening 1/298.257223563). GeographicLib is not a
test dependency, and no expected distance is computed through PostGIS.
"""

from datetime import datetime, timedelta

import numpy as np
import pytest
from psycopg import sql

from OceanDB.data_access.along_track import AlongTrack, Mission
from OceanDB.ocean_data.dataset import Dataset
from OceanDB.OceanDB_Initializer import OceanDBInit
from OceanDB.schemas import along_track_schema as schema
from tests.database.fixtures import config as config
from tests.database.fixtures import db_with_db as db_with_db
from tests.database.fixtures import db_with_tables as db_with_tables
from tests.database.fixtures import fresh_db as fresh_db

pytestmark = pytest.mark.uses_database
DATE = datetime(2013, 1, 4, 12)
RADIUS_DATE = DATE + timedelta(days=10)
COINCIDENT_DATE = DATE + timedelta(days=11)
DISTANT_DATE = DATE + timedelta(days=12)
SCIENCE_FIELDS: tuple[schema.along_track_columns, ...] = (
    "sla_unfiltered",
    "sla_filtered",
    "dac",
    "ocean_tide",
    "internal_tide",
    "lwe",
    "mdt",
    "tpa_correction",
)
POINTS: tuple[tuple[float, float, Mission, datetime, int], ...] = (
    (1.0, 0.0, "j2", DATE, 1),
    (0.0, 0.996, "j2", DATE, 1),
    (0.0, 0.997, "j2", DATE, 1),
    (0.0, 0.998, "j2", DATE, 1),
    (0.0, 1.0, "j2", DATE, 1),
    (0.0, 0.999, "j3", DATE, 1),
    (0.0, 0.01, "j2", DATE + timedelta(days=3), 1),
    (0.0, 0.02, "j2", DATE, 2),
)
SOURCE_ROWS: list[dict[schema.along_track_columns, str | int | float | datetime]] = [
    {
        "file_name": f"wgs84-{index}.nc",
        "mission": mission,
        "track": index + 1,
        "cycle": index + 10,
        "date_time": date,
        "latitude": latitude,
        "longitude": longitude,
        "basin_id": basin_id,
        **{
            field: 100 * (index + 1) + offset
            for offset, field in enumerate(SCIENCE_FIELDS)
        },
    }
    for index, (latitude, longitude, mission, date, basin_id) in enumerate(POINTS)
]
DISTANCES = (
    110574.38855779878,
    110874.21283010049,
    110985.53232089376,
    111096.85181168703,
    111319.49079327357,
    111208.1713024803,
)


class _BasinMask:
    def lookup(self, latitude: float, longitude: float) -> int:
        return 1


class _BasinConnections:
    connection_map: dict[int, list[int]] = {1: [1]}


@pytest.fixture
def along_track(db_with_tables: OceanDBInit) -> AlongTrack:
    """Insert synthetic observations directly into the existing test schema.

    :param db_with_tables: Isolated database from the shared test fixtures.
    :returns: Query client with deterministic cached basin collaborators.
    """
    fields = list(schema.along_track_columns_schema)
    query = sql.SQL("INSERT INTO along_track ({fields}) VALUES ({values})").format(
        fields=sql.SQL(", ").join(sql.Identifier(field) for field in fields),
        values=sql.SQL(", ").join(sql.Placeholder(field) for field in fields),
    )
    with db_with_tables.cursor(commit=True) as cursor:
        cursor.executemany(query, SOURCE_ROWS)
    client = AlongTrack(config=db_with_tables.config)
    client.__dict__["basin_mask_lookup"] = _BasinMask()
    client.__dict__["basin_connections"] = _BasinConnections()
    return client


def _assert_rows(
    result: Dataset[schema.along_track_fields] | None,
    indices: list[int],
    *,
    ordered: bool,
) -> None:
    assert result is not None
    assert result.row_count == len(indices)
    assert set(result) == set(schema.along_track_schema)
    order = np.arange(result.row_count) if ordered else np.argsort(result["track"])
    for field in schema.along_track_columns_schema:
        expected = [SOURCE_ROWS[index][field] for index in indices]
        if field in SCIENCE_FIELDS:
            np.testing.assert_allclose(
                result[field][order],
                np.asarray(expected, dtype=np.float64) * 0.001,
                rtol=0,
                atol=1e-12,
            )
        else:
            np.testing.assert_array_equal(result[field][order], np.asarray(expected))
    np.testing.assert_allclose(
        result["distance"][order],
        [DISTANCES[index] for index in indices],
        rtol=0,
        atol=1e-6,
    )
    np.testing.assert_array_equal(result["delta_t"][order], np.zeros(len(indices)))


@pytest.mark.parametrize("missions", [None, ["j2"]])
def test_nearest_ranks_all_eligible_rows_by_wgs84(
    along_track: AlongTrack,
    missions: list[Mission] | None,
) -> None:
    # Spherical top three would exclude north (row 0) for the three east rows.
    scalar = along_track.geographic_nearest_neighbors(
        fields=list(schema.along_track_schema),
        latitude=0.0,
        longitude=0.0,
        date=DATE,
        time_window=timedelta(days=1),
        missions=missions,
    )
    _assert_rows(scalar, [0, 1, 2], ordered=True)
    batch = list(
        along_track.geographic_nearest_neighbors_batch(
            fields=list(schema.along_track_schema),
            latitudes=[0.0, 0.0],
            longitudes=[0.0, 0.0],
            dates=[DATE, DATE],
            time_window=timedelta(days=1),
            missions=missions,
            chunk_size=1,
        )
    )
    assert len(batch) == 2
    for result in batch:
        _assert_rows(result, [0, 1, 2], ordered=True)


@pytest.mark.parametrize("missions", [None, ["j2"]])
@pytest.mark.parametrize("reverse_insertion", [False, True])
@pytest.mark.parametrize(
    "latitude, longitude, tie_points",
    [
        pytest.param(
            20.0,
            20.0,
            (
                (DATE - timedelta(hours=3), 20.0, 20.0),
                (DATE - timedelta(hours=2), 20.0, 20.0),
                (DATE - timedelta(hours=1), 20.0, 20.0),
                (DATE + timedelta(hours=1), 20.0, 20.0),
            ),
            id="chronological",
        ),
        pytest.param(
            0.0,
            0.0,
            (
                (DATE, -0.5, 0.5),
                (DATE, -0.5, 359.5),
                (DATE, 0.5, 0.5),
                (DATE, 0.5, 359.5),
            ),
            id="latitude-canonical-longitude",
        ),
    ],
)
def test_nearest_ties_follow_scientific_key(
    along_track: AlongTrack,
    missions: list[Mission] | None,
    reverse_insertion: bool,
    latitude: float,
    longitude: float,
    tie_points: tuple[tuple[datetime, float, float], ...],
) -> None:
    """Order exact ties by scientific identity, including the three-row cutoff."""
    rows = [SOURCE_ROWS[0].copy() for _ in tie_points]
    for index, (row, (date, point_latitude, point_longitude)) in enumerate(
        zip(rows, tie_points, strict=True)
    ):
        row.update(
            {
                "file_name": f"ties-{100 - index}.nc",
                "track": 100 - index,
                "date_time": date,
                "latitude": point_latitude,
                "longitude": point_longitude,
            }
        )
    fields = list(schema.along_track_columns_schema)
    query = sql.SQL("INSERT INTO along_track ({fields}) VALUES ({values})").format(
        fields=sql.SQL(", ").join(sql.Identifier(field) for field in fields),
        values=sql.SQL(", ").join(sql.Placeholder(field) for field in fields),
    )
    with along_track.cursor(commit=True) as cursor:
        cursor.executemany(query, list(reversed(rows)) if reverse_insertion else rows)

    identity_fields: tuple[schema.along_track_columns, ...] = (
        "date_time",
        "latitude",
        "longitude",
        "file_name",
        "track",
    )
    scalar = along_track.geographic_nearest_neighbors(
        fields=list(identity_fields),
        latitude=latitude,
        longitude=longitude,
        date=DATE,
        time_window=timedelta(days=1),
        missions=missions,
    )
    batch = list(
        along_track.geographic_nearest_neighbors_batch(
            fields=list(identity_fields),
            latitudes=[latitude, latitude],
            longitudes=[longitude, longitude],
            dates=[DATE, DATE],
            time_window=timedelta(days=1),
            missions=missions,
            chunk_size=1,
        )
    )
    assert len(batch) == 2
    for result in [scalar, *batch]:
        assert result is not None
        assert result.row_count == 3
        # Four equidistant observations compete for three slots; do not sort results.
        for field in identity_fields:
            np.testing.assert_array_equal(
                result[field], np.asarray([row[field] for row in rows[:3]])
            )
        np.testing.assert_array_equal(
            result["distance"], np.full(3, result["distance"][0])
        )


@pytest.mark.parametrize("missions", [None, ["j2"]])
@pytest.mark.parametrize("radius", [111_000.0, 111_250.0])
def test_radius_membership_and_distance_use_wgs84(
    along_track: AlongTrack,
    missions: list[Mission] | None,
    radius: float,
) -> None:
    indices = [0, 1, 2] if radius == 111_000.0 else [0, 1, 2, 3]
    if radius == 111_250.0 and missions is None:
        indices.append(5)
    # North (row 0) is inside 111000 m; east at 1 degree (row 4) exceeds 111250 m.
    scalar = along_track.geographic_point_in_r_dt(
        fields=list(schema.along_track_schema),
        latitude=0.0,
        longitude=0.0,
        date=DATE,
        radius=radius,
        time_window=timedelta(days=1),
        missions=missions,
    )
    _assert_rows(scalar, indices, ordered=False)
    batch = list(
        along_track.geographic_point_in_r_dt_batch(
            fields=list(schema.along_track_schema),
            latitudes=[0.0, 0.0],
            longitudes=[0.0, 0.0],
            dates=[DATE, DATE],
            radius=radius,
            time_window=timedelta(days=1),
            missions=missions,
            chunk_size=1,
        )
    )
    assert len(batch) == 2
    for result in batch:
        _assert_rows(result, indices, ordered=False)


@pytest.fixture
def nearest_radius_along_track(along_track: AlongTrack) -> AlongTrack:
    # Isolate each scenario in time; retain a closer, scientifically ineligible
    # basin-2 point so radius filtering cannot replace basin eligibility.
    rows = [SOURCE_ROWS[index].copy() for index in (0, 3, 4, 5, 7)]
    for row in rows:
        row["date_time"] = RADIUS_DATE
        row["file_name"] = f"radius-{row['track']}.nc"
    for date, longitude, track in (
        (COINCIDENT_DATE, 0.0, 101),
        (DISTANT_DATE, 10.0, 102),
    ):
        row = SOURCE_ROWS[0].copy()
        row.update(
            {
                "file_name": f"radius-{track}.nc",
                "date_time": date,
                "latitude": 0.0,
                "longitude": longitude,
                "track": track,
            }
        )
        rows.append(row)
    fields = list(schema.along_track_columns_schema)
    query = sql.SQL("INSERT INTO along_track ({fields}) VALUES ({values})").format(
        fields=sql.SQL(", ").join(sql.Identifier(field) for field in fields),
        values=sql.SQL(", ").join(sql.Placeholder(field) for field in fields),
    )
    with along_track.cursor(commit=True) as cursor:
        cursor.executemany(query, rows)
    return along_track


def _nearest_radius_results(
    along_track: AlongTrack,
    date: datetime,
    missions: list[Mission] | None,
    max_radius: float,
) -> list[Dataset[schema.along_track_fields] | None]:
    scalar = along_track.geographic_nearest_neighbors(
        fields=["track"],
        latitude=0.0,
        longitude=0.0,
        date=date,
        time_window=timedelta(hours=1),
        missions=missions,
        max_radius=max_radius,
    )
    batch = list(
        along_track.geographic_nearest_neighbors_batch(
            fields=["track"],
            latitudes=[0.0],
            longitudes=[0.0],
            dates=[date],
            time_window=timedelta(hours=1),
            missions=missions,
            max_radius=max_radius,
        )
    )
    assert len(batch) == 1
    return [scalar, *batch]


@pytest.mark.parametrize("missions", [None, ["j2"]])
def test_nearest_radius_membership_and_inclusive_boundary(
    nearest_radius_along_track: AlongTrack,
    missions: list[Mission] | None,
) -> None:
    # The exact boundary is a frozen GeographicLib oracle, not a PostGIS result.
    # Use a full millimeter on either side rather than platform-sensitive ULPs.
    boundary = DISTANCES[0]
    cases: tuple[tuple[float, list[int]], ...] = (
        (111_000.0, [0]),
        (111_250.0, [0, 3, 5] if missions is None else [0, 3]),
        (100_000.0, []),
        (0.0, []),
        (boundary, [0]),
        (boundary - 0.001, []),
        (boundary + 0.001, [0]),
    )
    for max_radius, indices in cases:
        for result in _nearest_radius_results(
            nearest_radius_along_track, RADIUS_DATE, missions, max_radius
        ):
            if not indices:
                assert result is None
                continue
            assert result is not None
            assert result.row_count == len(indices)
            np.testing.assert_array_equal(
                result["track"],
                np.asarray([SOURCE_ROWS[index]["track"] for index in indices]),
            )
            np.testing.assert_allclose(
                result["distance"],
                [DISTANCES[index] for index in indices],
                rtol=0,
                atol=1e-6,
            )


@pytest.mark.parametrize("missions", [None, ["j2"]])
def test_nearest_zero_radius_includes_coincident_observation(
    nearest_radius_along_track: AlongTrack,
    missions: list[Mission] | None,
) -> None:
    for result in _nearest_radius_results(
        nearest_radius_along_track, COINCIDENT_DATE, missions, 0.0
    ):
        assert result is not None
        assert result.row_count == 1
        np.testing.assert_array_equal(result["track"], [101])
        np.testing.assert_array_equal(result["distance"], [0.0])


@pytest.mark.parametrize("missions", [None, ["j2"]])
def test_nearest_default_has_no_implicit_500km_cap(
    nearest_radius_along_track: AlongTrack,
    missions: list[Mission] | None,
) -> None:
    scalar = nearest_radius_along_track.geographic_nearest_neighbors(
        fields=["track"],
        latitude=0.0,
        longitude=0.0,
        date=DISTANT_DATE,
        time_window=timedelta(hours=1),
        missions=missions,
    )
    batch = list(
        nearest_radius_along_track.geographic_nearest_neighbors_batch(
            fields=["track"],
            latitudes=[0.0],
            longitudes=[0.0],
            dates=[DISTANT_DATE],
            time_window=timedelta(hours=1),
            missions=missions,
        )
    )
    assert len(batch) == 1
    for result in [scalar, *batch]:
        assert result is not None
        assert result.row_count == 1
        np.testing.assert_array_equal(result["track"], [102])
        assert result["distance"][0] > 500_000.0
    assert _nearest_radius_results(
        nearest_radius_along_track, DISTANT_DATE, missions, 500_000.0
    ) == [None, None]
