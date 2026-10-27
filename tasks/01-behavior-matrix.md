# Task 01 — Source-derived behavior matrix

Recorded **before test edits** against
`766db427da69f0f682604bd2dfa7e33ae6a9fb95`. This characterizes the existing
indexed backend, not a proposal to make validation uniformly eager. No runtime
behavior or public API changes are intended.

## Four API defaults

`AlongTrackBackend`, `AlongTrack`, and `IndexedAlongTrackBackend` agree on these
defaults (`src/OceanDB/data_access/along_track.py` and
`src/OceanDB/data_access/indexed_along_track.py`):

| API | Spatial argument | Temporal half-window | Missions | Batch controls |
| --- | --- | --- | --- | --- |
| `geographic_point_in_r_dt` | `radius=500_000.0` m | `timedelta(days=10)` | `None` | — |
| `geographic_point_in_r_dt_batch` | `radius=500_000.0` m | `timedelta(days=10)` | `None` | keyword-only `n_jobs=1`, `chunk_size=16` |
| `geographic_nearest_neighbors` | keyword-only `max_radius=None` (unbounded) | `timedelta(days=10)` | `None` | — |
| `geographic_nearest_neighbors_batch` | keyword-only `max_radius=None` (unbounded) | `timedelta(days=10)` | `None` | keyword-only `n_jobs=1`, `chunk_size=16` |

Results are ordinary `Dataset | None`; nearest returns at most three rows and
always adds `distance`. Radius adds `distance` only when requested. Time and
WGS84 spatial boundaries are inclusive. A matched radius `fields=[]` preserves
the legacy zero-column, zero-row-count `Dataset` rather than returning `None`.

## Call versus first iteration

Here “facade” means `AlongTrack(backend=indexed_backend)`, not the PostgreSQL
implementation. Scalar indexed methods consume the singleton batch internally,
so deferred batch failures surface during the scalar call.

| Concern | Direct indexed batch call | Facade batch call | First `next()` on accepted indexed batch |
| --- | --- | --- | --- |
| Worker/chunk limits | Raises `ValueError` unless `n_jobs == 1` and `chunk_size >= 1` | `_validate_batch` raises `ValueError` for limits below one; positive parallel requests raise `AlongTrackCapabilityError` | Already checked |
| Unequal coordinate/date lengths | Returns generator | `_validate_batch` raises `ValueError` without consuming backend | Direct path raises `ValueError` before source work |
| Unknown requested fields | Returns generator | Delegated, not checked by facade | `_batch` raises `ValueError`, including for empty input |
| Negative/nonfinite radius | Returns generator | Radius API delegates, without checking radius | `_batch` raises `ValueError`, including for empty input |
| Negative/nonfinite nearest cap | Returns generator | Nearest API raises `ValueError` without consuming backend | Direct path raises `ValueError`, including for empty input |
| Source inventory | No new work after constructor | No new work | Complete pinned inventory checked once for a nonempty batch, before preparing the first center |
| File loading / window building | No work | No work | Only consumed center prepared; later centers remain unprepared |
| Negative half-window / aware datetime | Returns generator | Delegated | `prepare_window` rejects at the consumed center; empty batches never reach it |
| Valid empty input | Returns generator | Returns generator | Stops with no inventory validation, file loading, or window preparation; field/radius validation still runs |
| Changed/deleted pinned source | Returns generator | Returns generator | Fails closed before first result, even for a previously unselected source |

Facade stream operations, SQL observation, and PostgreSQL-only access reject an
indexed/injected backend at call time with `AlongTrackCapabilityError`; they do
not silently fall back to PostgreSQL. Facade scalar nearest also validates its
cap eagerly; scalar radius simply delegates.

Source trace: `AlongTrack._validate_batch`, its four public query methods and
`_postgresql`; indexed public batch wrappers and `_batch`, `prepare_window`,
`_validate_inventory`, `_load_file`, `_prepare_file`, and `_materialize`.
Within `_batch`, validation order is shape, requested fields, radius, then
inventory (only if nonempty), followed by per-center preparation/selection.

## Reuse and safety boundaries

- Constructor/`refresh()` discovers sources and obtains decoded time coverage;
  tests must capture counters **after construction** when proving batch laziness.
- Inventory validation is once per nonempty batch, **not** the total number of
  `_check_source` calls. `_load_file` checks selected files, a fresh decode checks
  again afterward, and an exact-window hit rechecks its active sources.
- Mutation before first iteration is covered. Source edits during an executing
  batch are unsupported; new files require explicit refresh.
- Centers are processed in input order, including distinct nonmonotonic dates.
  Closing a partially consumed generator does not prepare later centers.
- Window identity uses the exact datetime, half-window, and sorted unique mission
  selection. `None` resolves to the pinned inventory's missions; an equivalent
  explicit full list reuses that window. `[]` selects none and is a distinct key.
- Projection, query coordinates, radius/cap, and radius-versus-nearest operation
  do not change window identity. Overlapping or adjacent but nonidentical temporal
  keys rebuild; only the current window is retained. Decoded files may be reused.

## Evidence scope

Checklist 1–7 is covered by the two existing database-free test modules. The
six-row source asset and temporary multi-mission copies are synthetic; independent
literals, not the production decoder, supply expected scientific results.
Checklist 8–10, performance measurements, and independent audit evidence are
separate work and are not claimed complete by this matrix.
