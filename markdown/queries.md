# Queries

OceanDB exposes its query API through two data access classes:

- `OceanDB.data_access.along_track.AlongTrack`
- `OceanDB.data_access.eddy.Eddy`

This page documents each query method currently implemented and exercised by the test suite.

## Common Setup

Most query examples follow this pattern:

```python
from datetime import datetime, timedelta

from OceanDB.config import Config
from OceanDB.data_access.along_track import AlongTrack
from OceanDB.data_access.eddy import Eddy
from OceanDB.schemas.along_track_schema import along_track_schema
from OceanDB.schemas.eddy_schema import eddy_columns_schema

config = Config()

along_track = AlongTrack(config=config)
eddy = Eddy(config=config)
```

To request a broad set of fields:

```python
along_track_fields = list(along_track_schema.keys())
eddy_fields = list(eddy_columns_schema.keys())
```

## Along-Track Queries

### `geographic_point_in_r_dt(...)`

Returns along-track observations inside a spatial radius and temporal window around a single point.

Parameters:

- `fields`: along-track fields to return
- `latitude`, `longitude`: query center
- `date`: central datetime
- `radius`: radius in meters, default `500_000.0`
- `time_window`: half-width of the time window, default `timedelta(days=10)`
- `missions`: optional mission filter, defaults to all supported missions

Example:

```python
result = along_track.geographic_point_in_r_dt(
    fields=list(along_track_schema.keys()),
    latitude=-39.1,
    longitude=54.7,
    date=datetime(year=2013, month=1, day=4, hour=23),
    radius=500_000,
    time_window=timedelta(days=10),
)
```

Notes:

- Returns a `Dataset` when rows are found, otherwise `None`.
- This is the main radius-plus-time-window query for along-track data.
- It supports narrow field requests such as `["distance"]`, `["sla_filtered"]`, or `["distance", "sla_filtered"]`.

See [tests/along_track/test_spatiotemporal_queries.py](/Users/mddarr/delat/azath/ocean/OceanDB/tests/along_track/test_spatiotemporal_queries.py).

### `geographic_point_in_r_dt_batch(...)`

Batch version of `geographic_point_in_r_dt(...)`.

Parameters:

- `fields`
- `latitudes`
- `longitudes`
- `dates`
- `radius`
- `time_window`
- `missions`

Example:

```python
results = list(
    along_track.geographic_point_in_r_dt_batch(
        fields=list(along_track_schema.keys()),
        latitudes=[-39.1, 58.9],
        longitudes=[54.7, -65.9],
        dates=[
            datetime(year=2013, month=1, day=4, hour=23),
            datetime(year=2013, month=1, day=4, hour=23),
        ],
        radius=500_000,
        time_window=timedelta(days=10),
    )
)
```

Notes:

- Yields one result per input query point.
- Each yielded value is a `Dataset` or `None`.
- Input list lengths must match. Mismatched lengths raise `ValueError`.
- The batch implementation is tested against the single-query behavior.

See [tests/along_track/test_spatiotemporal_queries.py](/Users/mddarr/delat/azath/ocean/OceanDB/tests/along_track/test_spatiotemporal_queries.py).

### `geographic_nearest_neighbors(...)`

Returns the nearest along-track observations around a point, constrained by time window and basin-aware filtering.

Parameters:

- `fields`
- `latitude`, `longitude`
- `date`
- `time_window`
- `missions`

Example:

```python
result = along_track.geographic_nearest_neighbors(
    fields=list(along_track_schema.keys()),
    latitude=-69,
    longitude=28.1,
    date=datetime(year=2013, month=1, day=4, hour=23),
    time_window=timedelta(days=10),
)
```

Notes:

- Returns a `Dataset` or `None`.
- The `distance` field is included by the query as a mandatory output.
- The test coverage verifies that returned distances are monotonically increasing.
- Use this query when you want ordered nearest-neighbor results rather than all points in a fixed radius.

See [tests/along_track/test_geographic_nearest_neighbor.py](/Users/mddarr/delat/azath/ocean/OceanDB/tests/along_track/test_geographic_nearest_neighbor.py).

### `geographic_nearest_neighbors_batch(...)`

Batch version of `geographic_nearest_neighbors(...)`.

Parameters:

- `fields`
- `latitudes`
- `longitudes`
- `dates`
- `time_window`
- `missions`

Example:

```python
results = list(
    along_track.geographic_nearest_neighbors_batch(
        fields=list(along_track_schema.keys()),
        latitudes=[-69, -39.1],
        longitudes=[28.1, 54.7],
        dates=[
            datetime(year=2013, month=1, day=4, hour=23),
            datetime(year=2013, month=1, day=4, hour=23),
        ],
        time_window=timedelta(days=10),
    )
)
```

Notes:

- Yields one nearest-neighbor result per input point.
- Each yielded value is a `Dataset` or `None`.
- As with the other batch query, input lengths must match.

The query surface is implemented in [src/OceanDB/data_access/along_track.py](/Users/mddarr/delat/azath/ocean/OceanDB/src/OceanDB/data_access/along_track.py).

## Eddy Queries

OceanDB uses a signed `track_id` convention for eddy queries:

- negative values represent cyclonic eddies
- positive values represent anticyclonic eddies

### `get_eddy_tracks_from_times(...)`

Returns the distinct eddy track ids observed in a time window.

Parameters:

- `start_date`
- `end_date`

Example:

```python
track_ids = eddy.get_eddy_tracks_from_times(
    datetime(1900, 1, 1),
    datetime(2100, 1, 1),
)
```

Notes:

- Returns `list[int]`.
- In the test fixture data, a broad time range returns `(-2, -1, 0)` after sorting.
- This is a good discovery query when you want to enumerate available eddy tracks before fetching specific tracks.

See [tests/eddy_etl/test_eddy_etl_integration.py](/Users/mddarr/delat/azath/ocean/OceanDB/tests/eddy_etl/test_eddy_etl_integration.py).

### `get_eddy_tracks_from_times_batch(...)`

Batch version of `get_eddy_tracks_from_times(...)`.

Parameters:

- `start_dates`
- `end_dates`

Example:

```python
results = list(
    eddy.get_eddy_tracks_from_times_batch(
        start_dates=[
            datetime(1900, 1, 1),
            datetime(2100, 1, 1),
        ],
        end_dates=[
            datetime(2100, 1, 1),
            datetime(2101, 1, 1),
        ],
    )
)
```

Notes:

- Yields one `list[int]` per time window.
- A window with no matching eddies returns an empty list.
- Input lengths must match.

See [tests/eddy_etl/test_eddy_etl_integration.py](/Users/mddarr/delat/azath/ocean/OceanDB/tests/eddy_etl/test_eddy_etl_integration.py).

### `eddy_with_track_id(...)`

Returns the full observation set for a single eddy track.

Parameters:

- `fields`: eddy fields to return
- `track_id`

Example:

```python
result = eddy.eddy_with_track_id(
    fields=list(eddy_columns_schema.keys()),
    track_id=-1,
)
```

Notes:

- Returns a `Dataset` when the eddy exists, otherwise `None`.
- Use this query when you want the actual eddy observations for one logical track.

See [tests/eddy/test_eddy_by_track_id.py](/Users/mddarr/delat/azath/ocean/OceanDB/tests/eddy/test_eddy_by_track_id.py).

### `eddy_with_track_id_batch(...)`

Batch version of `eddy_with_track_id(...)`.

Parameters:

- `fields`
- `track_ids`

Example:

```python
results = list(
    eddy.eddy_with_track_id_batch(
        fields=list(eddy_columns_schema.keys()),
        track_ids=[-1, -999999],
    )
)
```

Notes:

- Yields one result per track id.
- Each yielded value is a `Dataset` or `None`.
- The tests explicitly cover a missing track id and verify that batch behavior matches repeated single calls.

See [tests/eddy/test_eddy_by_track_id.py](/Users/mddarr/delat/azath/ocean/OceanDB/tests/eddy/test_eddy_by_track_id.py).

### `eddy_envelope_query(...)`

Returns the spatiotemporal envelope for a single eddy track.

The envelope consists of:

- `min_date`
- `max_date`
- `basin_ids`

Example:

```python
envelope = eddy.eddy_envelope_query(track_id=-1)
```

Notes:

- Returns a single-row `Dataset` or `None`.
- This query is used internally to support the "along-track points near eddy" workflow.
- Tests verify that `min_date <= max_date` and that the returned basin list is non-empty.

See [tests/eddy/test_eddy_envelope.py](/Users/mddarr/delat/azath/ocean/OceanDB/tests/eddy/test_eddy_envelope.py).

### `eddy_envelope_query_batch(...)`

Batch version of `eddy_envelope_query(...)`.

Parameters:

- `track_ids`

Example:

```python
results = list(eddy.eddy_envelope_query_batch(track_ids=[-1, -999999]))
```

Notes:

- Yields one envelope result per track id.
- Each yielded value is a single-row `Dataset` or `None`.
- The tests verify that batch results match repeated single-track queries.

See [tests/eddy/test_eddy_envelope.py](/Users/mddarr/delat/azath/ocean/OceanDB/tests/eddy/test_eddy_envelope.py).

### `along_track_points_near_eddy(...)`

Returns along-track altimetry points associated with a specific eddy track.

This is the main cross-domain query in OceanDB:

1. Find the eddy envelope for the requested track.
2. Use that envelope to query along-track data in the relevant time range and basins.

Parameters:

- `track_id`
- `fields`: optional along-track field list. If omitted, OceanDB uses `Eddy.default_along_track_fields`.

Example:

```python
result = eddy.along_track_points_near_eddy(
    track_id=-1,
    fields=list(along_track_schema.keys()),
)
```

Notes:

- Returns a `Dataset` when matching along-track points are found.
- Returns `None` if the eddy exists but no along-track rows match.
- Raises `ValueError` if the eddy track itself cannot be found.
- This is the query to use for "eddy near along-track" analysis.

See [tests/eddy/test_eddy_points_nearalong_track.py](/Users/mddarr/delat/azath/ocean/OceanDB/tests/eddy/test_eddy_points_nearalong_track.py).

### `along_track_points_near_eddy_batch(...)`

Batch version of `along_track_points_near_eddy(...)`.

Parameters:

- `track_ids`
- `fields`

Example:

```python
results = list(
    eddy.along_track_points_near_eddy_batch(
        track_ids=[-1],
        fields=list(along_track_schema.keys()),
    )
)
```

Notes:

- Yields one result per track id.
- Each yielded value is a `Dataset` or `None`.
- Unlike the single-track method, missing eddy tracks are represented as `None` in the output sequence rather than raising `ValueError`.

See [tests/eddy/test_eddy_points_nearalong_track.py](/Users/mddarr/delat/azath/ocean/OceanDB/tests/eddy/test_eddy_points_nearalong_track.py).

## Field Notes

Along-track query fields come from [src/OceanDB/schemas/along_track_schema.py](/Users/mddarr/delat/azath/ocean/OceanDB/src/OceanDB/schemas/along_track_schema.py). Common fields include:

- `latitude`
- `longitude`
- `date_time`
- `mission`
- `track`
- `cycle`
- `sla_unfiltered`
- `sla_filtered`
- `distance`
- `delta_t`

Eddy query fields come from [src/OceanDB/schemas/eddy_schema.py](/Users/mddarr/delat/azath/ocean/OceanDB/src/OceanDB/schemas/eddy_schema.py). Common fields include:

- `track`
- `cyclonic_type`
- `date_time`
- `latitude`
- `longitude`
- `amplitude`
- `speed_radius`
- `effective_radius`

## Test-Driven Examples

For concrete usage patterns, the best references are:

- [tests/along_track/test_spatiotemporal_queries.py](/Users/mddarr/delat/azath/ocean/OceanDB/tests/along_track/test_spatiotemporal_queries.py)
- [tests/along_track/test_geographic_nearest_neighbor.py](/Users/mddarr/delat/azath/ocean/OceanDB/tests/along_track/test_geographic_nearest_neighbor.py)
- [tests/eddy/test_eddy_by_track_id.py](/Users/mddarr/delat/azath/ocean/OceanDB/tests/eddy/test_eddy_by_track_id.py)
- [tests/eddy/test_eddy_envelope.py](/Users/mddarr/delat/azath/ocean/OceanDB/tests/eddy/test_eddy_envelope.py)
- [tests/eddy/test_eddy_points_nearalong_track.py](/Users/mddarr/delat/azath/ocean/OceanDB/tests/eddy/test_eddy_points_nearalong_track.py)
- [tests/eddy_etl/test_eddy_etl_integration.py](/Users/mddarr/delat/azath/ocean/OceanDB/tests/eddy_etl/test_eddy_etl_integration.py)
