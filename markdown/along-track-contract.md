# Along-track scientific contract (#195)

**Scientific contract and synthetic PostgreSQL regression reference for #195.**
This records the implemented behavior at `79a3dc3` and the available evidence
for #195. There is no formal reference-freeze gate for #195 or #197. #197 covers
shared-reader and versioned-preparation work only.

## Scope and query semantics

The local, windowed, serial reference covers these four `AlongTrack` methods:

| Method | Selection | Default |
| --- | --- | --- |
| `geographic_point_in_r_dt` | Scalar radius window | `radius=500_000.0` m |
| `geographic_point_in_r_dt_batch` | Radius window per center | Same radius; `n_jobs=1` |
| `geographic_nearest_neighbors` | Scalar nearest neighbors | Up to 3; `max_radius=None` |
| `geographic_nearest_neighbors_batch` | Nearest neighbors per center | Same limit/cap; `n_jobs=1` |

- All four default to a ten-day **half-window**: both endpoints of
  `[date - time_window, date + time_window]` are included. A zero half-window
  selects only the exact timestamp.
- Radius membership is inclusive. NN is unbounded by default, including beyond
  500 km. Its optional keyword-only `max_radius` is inclusive and applied before
  ranking/limiting; zero admits coincident observations. Negative, NaN, or infinite
  NN caps raise `ValueError` before database access.
- Time, mission, and connected-basin eligibility all apply. No matches returns
  `None`; batches yield one `Dataset | None` per center in input order. Empty
  latitude/longitude/date lists yield zero results. Lengths must match, and
  `n_jobs` and `chunk_size` must be positive (default chunk size: 16).
- Streams retain their completion-order `(input_index, result)` interface;
  nearest streams accept the same optional radius cap. They are not the
  serial-reference surface, and no parallel-performance parity is claimed.

### One spatial metric and deterministic NN order

WGS84 ellipsoidal geodesic distance in meters governs **all** radius membership,
NN caps, NN ordering, and returned `distance` values. SQL uses geography
`ST_DWithin(..., true)` / `ST_Distance(..., true)`; WGS84 has semi-major axis
6,378,137 m and flattening 1/298.257223563.

NN orders by exact computed `(distance, date_time, latitude, stored longitude)`,
ascending, including at the three-row cutoff. Only **equal computed distances** use
the remaining keys; no comparison tolerance or additional rounding creates ties.
The shared scientific unique key is `(date_time, latitude, longitude)`, not the generated database
sequence `id`, filename, or insertion order.

This intentionally changes the former spherical geography `<->` NN selection.
The corrected PostgreSQL queries rank the full eligible pool, without a spherical
shortlist. Future candidate pruning must prove it cannot discard an eligible
ellipsoidal neighbor; a fixed spherical top-k is insufficient. The current
correctness-first full-pool ranking can increase latency. No measured speedup or
speed ratio is claimed.

## Basin authority and connectivity

The user-approved authority is OceanDB's packaged raster and connection assets,
**not QuickMap's polygon classifier or land-exclusion policy**:

- `src/OceanDB/data/basin_masks/new_basin_mask.nc` is sampled by the existing
  1/6-degree floor lookup: `i=floor((latitude+90)/(1/6))`,
  `j=floor((longitude % 360)/(1/6))`. The current raster profile is
  `latitude in [-90, 90)`; the exact north pole is outside it. No pole handling,
  coordinate validation, or boundary-policy change is introduced here.
- `src/OceanDB/data/basins/basin_connection_table.txt` supplies direct listed
  connections, plus self. There is **no transitive closure**. Raster IDs absent
  from the connection table, including `0` (land) and unlisted lakes, are
  self-only. This is existing behavior, not an ocean-only eligibility rule.
- Preserve the file's explicit Caspian `1001 <-> 1002` links. Do not infer that
  every ID >=1000 is isolated from a header description that contradicts those
  entries. `ocean_basins.csv` is also hashed, but does not replace raster lookup.
- The source reference deliberately retains original row index 4, classified
  as land (`basin_id=0`); it is queryable from a same-basin center.

The generator and these three basin assets are hashed in the
[reference manifest](../tests/data/along_track_reference/manifest.json).
Runtime Python, schema, and query SQL files are not pinned by the fixture.

## Source coordinates, time, and missions

The reference source already has canonical `[0, 360)` longitudes. Preserve
decoded float64 coordinate precision without quantization; do not round the
unique key or normalize it through geography's displayed longitude. Raster
lookup's modulo is not a new runtime canonicalization of stored coordinates.

The demonstrated time profile is UTC-naive, microsecond-resolution Python/SQL
datetimes decoded from CF `days since 1950-01-01 00:00:00`, Gregorian source
metadata. Actual observations span midnight (including microseconds on either
side); decoded times, not filename dates, determine coverage and membership.
Timezone-aware input handling, alternative calendars, and datetime normalization
are not established here and require #197 reconciliation; no new handling is
introduced or claimed by this contract.

Public query `missions` use canonical names from `Mission` in
[`along_track.py`](../src/OceanDB/data_access/along_track.py), matching the
**actual stored mission strings**: `None` means all, `[]` means none. Mission
filters are literal, not aliases. The downloader's dataset/layout naming
`s6a-lr` versus public query `s6a` must be reconciled by #197; the existing
download mapping is not a guarantee of ingest/query alias normalization.
Ingest extracts the filename mission; no silent `s6a-lr` rewrite is added here.

## Scientific values, identity, and population integrity

Eight scientific fields are packed signed int16 millimeters in storage:
`sla_unfiltered`, `sla_filtered`, `dac`, `ocean_tide`, `internal_tide`, `lwe`,
`mdt`, and `tpa_correction`. Public `Dataset` access applies scale `0.0010`
exactly once to expose meters; `get_unscaled` exposes packed values.

Mask `_FillValue` and `missing_value` **before scaling**: scientific missing
values become SQL `NULL` and public floating `NaN`, not scaled sentinels.
The fixture uses `32767` / `-32766`; scalar `tpa_correction` broadcasts its
value **and mask** to every observation. Missing projected science values do
not remove otherwise eligible rows or change NN membership/rank.

Source observation identity is `(source SHA256, original zero-based row index)`,
distinct from file path/basename, database `id`, and the scientific unique key.
The database does **not** yet persist original row offsets. The test reference
can map `(file_name, date_time, latitude, longitude)` back to the unique source
row index; this is test-only provenance, not a persisted production guarantee.

Duplicate `(date_time, latitude, longitude)` keys raise `UniqueViolation` in
both INSERT and COPY ingestion, whether the repeated values are identical or
conflicting. There is no deduplication or arrival-order winner. Transactions
are **per batch, not whole-asset atomic**: a failed batch rolls back, but earlier
successful batches may remain. Validate the complete population and verify that
failed batches left no new or changed rows, not only metadata or a successful
count. Completion metadata follows the batches; normal reruns skip completed basenames.
Existing populated databases are not repaired by corrected SQL/decoder code;
use a fresh, controlled reference ingestion (see [Ingest Data](ingest.md)).

## Projection and comparison: correctness before performance

Every requested field must stay aligned by observation, including metadata
(`file_name`, `mission`, `track`, `cycle`, `basin_id`), coordinates, timestamp,
all science fields, and derived fields. Requested `delta_t` is signed
`center date - observation date_time` in seconds. NN always returns `distance`;
radius returns it only when requested. `len(dataset)` counts columns;
`dataset.row_count` counts rows.

Legacy caveat: radius `fields=[]` can return a zero-column `Dataset` with
`row_count == 0` even when SQL matched rows. It is not equivalent to `None`.
No broad projection/Dataset fix is included. A parity harness must request
identity/mapping fields when needed; public callers are **not** required to
request metadata for their ordinary narrow projections.

Equivalence requires identically selected observations and **all scientific
and source fields**. Coordinate-only or count-only matches are insufficient.

| Comparison | Rule |
| --- | --- |
| Timestamp, decoded coordinates, keys, packed values, masks, text/metadata | Exact; preserve microseconds and row association |
| Physical scientific values (m) | `atol=1e-12`, `rtol=0` |
| Returned WGS84 distance (m) | `atol=1e-4`, `rtol=0`; current tests use stricter `1e-6` |
| `delta_t` | Signed seconds derived from the exact timestamps |

Tolerances compare reported values only: never relax membership, NN rank, or
tie-breaking. Canonically sort **whole rows only for unordered radius results**;
never sort individual columns. Preserve NN rank in comparisons and hashes.
Normalize NaN/missing masks for hashing, not NaN payload bits. Record lightweight
source/query, code revision, runtime, and comparison-setting provenance when
actual #199/#200 comparisons are performed, not as a prerequisite freeze.

## Available evidence and completion boundary

The tiny synthetic source is
`tests/data/along_track_reference/dt_global_j2_phy_l3_1hz_20240101_20240102.nc`:
**2,680 bytes, six literal rows, canonical `j2`**, not upstream observations or
the recent daily benchmark. Its SHA256 is
`29fbd4c7f1731a21f4167033325af279a17fd50a25759b6a09cb94e7457c28c4`.
The adjacent [`generate.py`](../tests/data/along_track_reference/generate.py)
contains independent packed/physical literals, not production-decoder output.
The manifest records source metadata/hash and hashes only the generator and three
basin assets. Its `SYNTHETIC` status identifies a PostgreSQL regression fixture,
not real-data or cross-backend benchmark evidence. The NetCDF's historical
"provisional" title remains unchanged to preserve its byte identity; it is not a
completion gate.

Informational manifest origin base code: `9d4b76e283aa38898a94bf6420b3c1573e0bad32`;
fixture commit: `1589bbb`; inclusive-boundary regressions: `79a3dc3`.
[`test_source_reference.py`](../tests/along_track/test_source_reference.py)
exercises real INSERT/COPY ingestion, all four methods, full fields, exact
packed values/masks, scaling, source-row mapping, and identical/conflicting
duplicate rejection with failed-batch rollback checks.
[`test_ellipsoidal_queries.py`](../tests/along_track/test_ellipsoidal_queries.py)
covers inclusive +/-10-day edges and one microsecond outside, radius/cap
boundaries +/-1 mm, spherical/ellipsoidal selection differences, exact ties,
mission/basin eligibility, complete fields, and signed `delta_t`.

Historical **non-main** `index-performance-analysis-2` at
`a96ec320e64f2359042a939956eb71c153a8ec57` contains
`optimize_geo_points_r_dt.py` and `src/OceanDB/index_experiment.py`.
That experiment uses one center date (`2022-10-15`), +/-10 days, a 1-degree
grid, 50 km radius / unbounded three-neighbor scenarios, and parallel latitude
and basin planners (16 jobs, chunks of 256). It is **not** the future milestone's
+/-5-day daily serial profile, not the unidentified recent DAILY benchmark, and
not corrected-query timings. Historical reports remain unchanged; no performance
ratios are inferred from them.

#195's completion boundary is the scientific contract, WGS84-correct PostgreSQL
queries, stable ties and bounded/unbounded three-neighbor selection, and the
existing synthetic source/PostgreSQL regression fixture. These deliverables are
sufficient for #195; identifying the historical or recent DAILY benchmark is not
a blocker. They establish the contract for #196/#197, not cross-backend parity or
a completed benchmark.

#197 is limited to shared-reader and versioned-preparation work. It does not own
a production source/database/code/runtime freeze or a benchmark-identity gate.
Lightweight run provenance belongs with actual #199/#200 comparisons, not an
additional completion requirement for #195 or #197.

## Reproduce safely

From the repository root, with dependencies already available to `uv`:

```sh
UV_CACHE_DIR=.cache/uv uv run --no-sync python tests/data/along_track_reference/generate.py \
  --output-directory build/along-track-reference-check
cmp tests/data/along_track_reference/manifest.json build/along-track-reference-check/manifest.json
cmp tests/data/along_track_reference/dt_global_j2_phy_l3_1hz_20240101_20240102.nc \
  build/along-track-reference-check/dt_global_j2_phy_l3_1hz_20240101_20240102.nc
```

**Database tests DESTRUCTIVELY DROP the configured database on teardown.**
Use an explicit, unique disposable database name and a test-only container;
never point these fixtures at production or any database to preserve. Confirm
the example container/database names and port are unused; rename for concurrent
runs. This recipe uses a pinned disposable PostGIS image and ephemeral storage:

```sh
docker run --rm --detach --name oceandb-issue195-postgis \
  --publish 127.0.0.1:55433:5432 --env POSTGRES_PASSWORD=postgres \
  --tmpfs /var/lib/postgresql/data:rw \
  postgis/postgis@sha256:44126d872ac91993766c341e369c539e8196614321765d36a6f1bab0419a5fa5
docker exec oceandb-issue195-postgis pg_isready --username postgres --dbname postgres
```

Wait until `pg_isready` reports accepting connections, then run:

```sh
env OCEANDB_TEST_POSTGRES_HOST=127.0.0.1 OCEANDB_TEST_POSTGRES_PORT=55433 \
  OCEANDB_TEST_POSTGRES_DATABASE=oceandb_issue195_test \
  OCEANDB_TEST_POSTGRES_USERNAME=postgres OCEANDB_TEST_POSTGRES_PASSWORD=postgres \
  PYTHONPATH=src:. UV_CACHE_DIR=.cache/uv uv run --no-sync pytest \
  tests/along_track/test_source_reference.py tests/along_track/test_ellipsoidal_queries.py \
  -q --basetemp=build/pytest-reference
docker stop oceandb-issue195-postgis
```

Observed reference runtime (2026-10-16): Python 3.14.5, NumPy 2.0.2,
netCDF4 1.7.4, psycopg 3.2.13, pytest 9.0.3, mypy 2.3.1;
PostgreSQL 16.4, PostGIS 3.4.3, GEOS 3.9.0, PROJ 7.2.1.
This is a recorded environment, **not** a portability guarantee or a required
runtime snapshot. These correctness checks are not a performance benchmark.
