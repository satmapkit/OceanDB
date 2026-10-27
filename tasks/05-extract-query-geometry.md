# 05 — Extract reusable query geometry

## Status, prerequisites, and navigation

- **Status: proposed; this document does not implement the extraction.**
- Read [README.md](README.md), including common scientific invariants, validation guidance, and the common audit protocol.
- Prerequisites: accepted [01 characterization](01-characterize-behavior-and-performance.md) through [04 prepared windows](04-introduce-prepared-window.md).
- Previous: [04 — Introduce prepared windows](04-introduce-prepared-window.md).
- Next: [06 — Refactor radius selection](06-refactor-radius-selection.md).
- This belongs to the behavior-preserving steps 01–08; [09 snapshots](09-persist-index-snapshots.md) requires a separate opt-in feature decision.

## Goal and concrete starting points

Give cached and direct center geometry one explicit typed representation and one coherent construction path.
Current owners are `IndexedAlongTrackBackend.prepare_grid`, `_grid_geometry`, and the geometry branch inside `_select` in `src/OceanDB/data_access/indexed_along_track.py`.
The cache currently maps exact `(longitude, latitude, radius)` tuples to anonymous `(basin, cells, vector)` tuples.
`cap_cells` and `unit_vectors` live in `indexed_geometry.py`; basin lookup uses `BasinMask.lookup`.
`_nearest` repeatedly calls `_select` with expanding radii; it must not reuse one radius's cell cover for another.
The proposed owner is `src/OceanDB/data_access/indexed_spatial.py`, alongside the task-04 prepared-window boundary, not inside file management.

## Before/after: simplicity, coupling, and readability

| Concern | Before | Proposed after |
| --- | --- | --- |
| Representation | Positional tuple with implicit coordinate/radius identity | Small named `QueryGeometry` record |
| Construction | Preparation and direct queries have separate branches | Shared geometry construction and exact-key lookup |
| Ownership | Backend mixes window state with reusable center data | Spatial component owns geometry independently of windows |
| Reuse | Selection also knows cache details and counter updates | `get_geometry` supplies a complete result |
| Testing | Cache behavior mainly inferred from full queries | Direct/cached equality and budget rules tested explicitly |

Reduced coupling means spatial traversal consumes geometry, not file-cache or adapter internals.
Keep the implementation concrete: no generic cache framework, wrapper shims, eviction strategy abstraction, or speculative vectorization API.

## Scope and non-goals

- Introduce `QueryGeometry` and `get_geometry` near the spatial/cache owner; keep pure mathematical functions in `indexed_geometry.py`.
- Preserve `prepare_grid` as an optional public preparation operation taking paired centers, not Cartesian axes.
- Leave file discovery/validation/decoding, disk caching, and per-file LRU in task-02/03 `indexed_files.py`.
- Do not canonicalize stored source longitude or round center/radius keys; do not change basin assets or cell-size semantics.
- Do not add a north-pole policy: the current basin raster supports latitude `[-90, 90)`, even though `cap_cells` itself accepts 90.
- Leave nearest traversal redesign to [07](07-refactor-nearest-selection.md), adapter completion to [08](08-finish-backend-adapter.md), and persisted snapshots to 09.

## Numbered implementation checklist

1. Define a small typed `QueryGeometry` record containing original query longitude/latitude, radius, center basin, conservative cell IDs, and center unit vector.
   Carry the original coordinates through to geodesic evaluation; do not reconstruct them from the vector or require traversal to reverse-look-up a cache key.
   Keep the exact lookup key explicit alongside this named representation; avoid ambiguous anonymous tuples.
   Mark retained cell/vector arrays read-only and ensure ownership cannot expose writable aliases.
2. Preserve exact current Python key semantics `(longitude, latitude, radius)` without rounding or extra coordinate normalization.
   Equivalent source positions may still have different raw stored longitudes; geometry reuse must not rewrite source values.
   A cache entry for radius R cannot be counted as a hit for radius S, even if both have the same basin/vector.
3. Share construction math between preparation and cache misses while preserving preparation/admission sequencing.
   Today `prepare_grid` validates paired lengths/radius, builds cells/vector, checks byte budget, then looks up basin only for admission.
   Do not turn a formerly skipped basin lookup into an eager error by constructing a complete retained record before the budget decision.
   Factor the minimal construction stages needed; do not duplicate the cell/vector math or introduce a generic admission framework.
4. Implement `get_geometry` so a query miss computes usable direct geometry without mandatory cache retention.
   A hit returns the matching record and updates the existing hit counter at the lookup boundary, not inside numerical leaf functions.
   Keep current query validation/error timing, including coordinate/basin lookup order where externally observable.
5. Preserve `prepare_grid` paired iteration with `zip(..., strict=True)` after equal-length validation.
   Repeated already-retained keys skip rebuilding without counting as query hits; do not form a latitude×longitude Cartesian product.
   Radius validation remains finite/nonnegative; avoid silently adding coordinate policies under this refactor.
6. Preserve geometry budget semantics: over-budget entries are skipped, not evicted/replaced through the file-LRU policy.
   Budget cost remains `cells.nbytes + vector.nbytes`; document that Python objects/dictionaries and transient allocations are excluded.
   `geometry_builds` counts successful retained preparations, `geometry_hits` matching query reuse, and `geometry_bytes` retained array bytes.
   Direct misses and skipped/repeated preparation entries must not silently acquire new counter meanings.
7. Keep geometry independent of `WindowKey`, mission selection, source paths, decoded row arrays, and `PreparedWindow` lifetime.
   Multiple windows may reuse the same entry; `refresh()` clears file/active RAM but retains geometry and cumulative statistics.
   Cell size and packaged assets are fixed for a constructed owner, so constructing a new owner naturally scopes its cache correctly.
   Do not invent mutable grid/assets setters or persistent geometry invalidation mechanisms.
8. Route the existing radius path through `get_geometry`, updating private callers directly rather than keeping two selector branches.
   Where feasible, arrange nearest expansion to reuse center basin/vector while recomputing radius-dependent cell covers.
   Do not fake a radius-cache hit or alter expansion/stopping/ranking policy; larger nearest changes remain task 07.
9. Keep batches lazy: no eager `prepare_grid` call over every future batch center and no date regrouping.
   Any paired/bulk geometry helper used by `_batch` must be consumed lazily and must not touch a later center before its turn.
   Explicit caller-invoked `prepare_grid` may still prepare its supplied list eagerly; that is distinct from query batching.
10. Keep statistics in component-owned counters or the agreed small neutral record and expose the existing unified backend `stats` view.
    Lower layers must not import `IndexedAlongTrackBackend`; avoid cycles with `indexed_files.py` and `indexed_geometry.py`.
    Add precise annotations and Sphinx/reStructuredText docstrings for identity, array ownership, admission, and shape contracts.

## Invariants to carry through the change

- Four public query APIs keep defaults/signatures and `Dataset | None` scalar/yielded results.
- Defaults remain radius 500,000 m, half-window 10 days, nearest cap `None`, and `n_jobs=1`/`chunk_size=16`; no parallel/thread-safety claim is added.
- Inclusive microsecond windows and WGS84 radius membership, source precision, and raw stored longitude are unchanged.
- Basin filtering remains direct connections plus self; land is self-only; no transitive connectivity is inferred.
- Packed science/masks stay aligned and scale only through `Dataset`; `delta_t` stays signed.
- Radius `distance` remains optional, nearest `distance` mandatory, and nearest ordering remains distance/time/latitude/stored longitude with at most three rows.
- A matched empty-field radius projection remains a zero-column `Dataset`, not `None`.
- Full pinned inventory is validated once per nonempty batch; empty batches do no source/window/geometry work.
- Mixed dates, input order, generator laziness, eager/deferred validation timing, and unsupported midbatch source edits remain unchanged.
- Only the most recent prepared window is retained; adjacent dates rebuild; file-cache version-2 format/keys are unchanged.

## Proposed atomic commits

1. **Introduce the geometry record and shared construction stages.**
   Purpose: replace anonymous geometry tuples with one precise mathematical/ownership contract.
   Tests: extend proposed `tests/along_track/test_indexed_spatial.py` with direct construction, read-only arrays, and paired-center cases; mark all new cases unit-only.
   Acceptance: no file/window dependence, exact key semantics documented, and cached/direct math is not duplicated.
2. **Move lookup, preparation admission, and counters to the spatial owner.**
   Purpose: give `get_geometry` and `prepare_grid` one cache policy while retaining existing counter meanings.
   Tests: cache hits/misses, repeated preparations, zero/small budgets, radius mismatch, skipped entries, and retained-array accounting.
   Acceptance: misses work without retention, budget skips do not behave like LRU eviction, and basin lookup timing stays compatible.
3. **Integrate traversal and prove cross-window/lazy reuse.**
   Purpose: remove adapter cache branching without changing query results or pulling future batch work forward.
   Tests: existing dateline/high-latitude regression, refresh/multiple-window reuse, mixed-date partial iteration, and nearest-radius mismatch.
   Acceptance: identical results and source longitudes; geometry survives refresh; no later-center work or false geometry hits.

## Validation cases and commands

Use existing `test_dateline_high_latitude_and_grid_geometry_reuse` as a public regression, not as the only mathematical oracle.
Compare cached/direct basin labels, cell IDs, vectors, selected rows, and distances for tiny dateline-crossing and high-latitude caps.
Test exact repeated keys, mismatched radii, unrounded longitudes, multiple windows/missions, and reusing geometry after `refresh()`.
Check zero budget, budget smaller than one entry, exact-fit admission, later smaller entries, and no eviction of already admitted entries.
Verify preparation counters, query-only hit increments, and byte accounting independently; describe the metric as array bytes, not full RAM.
Test unequal paired lengths, paired-order preservation, empty inputs, invalid radii, and budget-dependent skipped basin lookup behavior.
Spy on geometry construction during partial batch consumption: later invalid centers must not fail or be prepared early.
Run from `/home/rowedaniel/work/nwra/OceanDB`; these are proposed commands, not completed checks:

```bash
PYTHONPATH=src:. uv run --no-sync pytest -m unit tests/along_track/test_indexed_backend.py tests/along_track/test_backend_contract.py
PYTHONPATH=src:. uv run --no-sync pytest -m unit tests/along_track/test_indexed_spatial.py
```

`test_indexed_spatial.py` is **proposed new in task 04**; create/retain it before using the second command.
Follow [README.md](README.md)'s targeted type-checker protocol for `indexed_spatial.py`, `indexed_along_track.py`, and affected tests/helpers.
No checker configuration exists currently: use the README-agreed checker/version through `uv run`, record baseline/head diagnostics, and resolve new errors.
Never use `typing.cast` or new ignores/suppressions; narrow values or improve boundary types instead.
Compare task-01 cached/direct geometry timing and array-byte observations without claiming total-memory bounds or unmeasured speedups.
Do not run routine whole-suite `pytest` or `make check`: database fixtures can drop databases, and the latter formats unrelated files.

## Independent-agent audit checklist

- [ ] A **different agent in a fresh review session** performs a no-edit review using the README common audit protocol.
- [ ] Inspect `BASE_REF..HEAD_REF`, construction order, exact keys, and all preparation/direct-query call sites independently.
- [ ] Confirm paired centers, shared math, immutable arrays, direct misses, admission skips, and counter semantics.
- [ ] Challenge radius mismatches and nearest expansion: basin/vector reuse must not substitute an old cell cover.
- [ ] Check refresh/cross-window lifetime, no lower-layer backend imports, and no hidden dependency on source inventory.
- [ ] Independently check cached/direct scientific equality and lazy error timing, not just implementation-authored assertions.
- [ ] Return evidence, severity-ranked findings, verdict, and explicit untested risks; do not repair findings during the audit.

### Copyable audit prompt

```text
You are a DIFFERENT agent from the implementer. Review only: no edits and no commits.
BASE_REF=<revision immediately before task 05>
HEAD_REF=<revision containing task 05>
Read tasks/README.md's common audit protocol and tasks/05-extract-query-geometry.md.
Independently inspect BASE_REF..HEAD_REF and relevant callers; do not trust reported test results.
Check exact longitude/latitude/radius identity, paired centers, shared cached/direct construction,
admission/validation order, array-byte budgets, immutable arrays, hit/build counters, and direct misses.
Verify multiple windows and refresh retain source-independent geometry; reject false radius hits,
file-LRU policy reuse, north-pole policy changes, or eager work on later batch centers.
Run targeted database-free uv checks and the README type-checker/baseline protocol where available.
Return evidence (commands/results and symbols), severity-ranked findings, and the README verdict
(accept / changes required / blocked by missing evidence),
and residual risks/unavailable checks. Make no edits and follow README safety restrictions.
```

## Definition of done

- `QueryGeometry`/`get_geometry` make cached/direct geometry explicit without changing preparation admission or counter behavior.
- Geometry remains independent of source/window lifecycle, and batches remain lazy with unchanged public/scientific results.
- Targeted tests/type checks and measured cached/direct costs are recorded with limitations, not asserted as completed by this document.
- A different agent's evidence-backed audit has no unresolved blocking findings before task 06 begins.
