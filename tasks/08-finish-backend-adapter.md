# 08 — Finish the indexed backend adapter

## Status, prerequisites, and navigation

- **Status: proposal; no implementation is claimed.** This closes the behavior-preserving cleanup series, not the separately gated persistence feature.
- Prerequisites: accepted steps [01](01-characterize-behavior-and-performance.md)–[07](07-refactor-nearest-selection.md), including explicit prepared windows and audited radius/nearest selection.
- Navigation: [series README and shared audit protocol](README.md) · [previous: nearest selection](07-refactor-nearest-selection.md) · [next: optional snapshots](09-persist-index-snapshots.md).
- Read `AGENTS.md`, `markdown/queries.md`, and `markdown/along-track-contract.md`; preserve the existing unrelated modification to `markdown/queries.md`.

## Goal and current relevant symbols

Leave `src/OceanDB/data_access/indexed_along_track.py` as the public protocol adapter: four query methods, lazy orchestration, and schema-backed `Dataset` projection.
Its present `_batch` combines deferred validation, inventory checks, window preparation, a boolean radius/nearest branch, and materialization.
Its `_materialize` reads mutable `_active` and `_active_sources`, deduplicates requested fields, adds NN distance, reconstructs timestamps/source metadata, and supplies schema dtypes.
`prepare_grid`, `prepare_window`, `refresh`, `IndexedCacheInfo`, and the constructor are additional supported adapter surfaces requiring clear delegation.
`AlongTrackBackend` in `along_track.py` is already a structural `Protocol`; `AlongTrack` owns facade validation and unsupported-capability checks.
`tests/along_track/test_backend_contract.py` protects facade defaults, delegation, and unsupported capabilities; `test_indexed_backend.py` protects synthetic science and cache behavior.
Existing source catalogs and decoded `.npy` files are reusable across restarts, but current active spatial windows and query geometry are memory-only.

## Simplification, coupling, and readability rationale

| Concern | Before | Proposed after |
| --- | --- | --- |
| Simplicity | A single backend owns discovery, decoding, indexing, selection, projection, and counters. | The adapter states the query workflow; file and spatial owners implement their own responsibilities. |
| Coupling | Materialization relies on whichever mutable active arrays/source tuple exist at call time. | Projection takes the exact `PreparedWindow` and aligned `Selection` that produced the result. |
| Readability | Boolean/nullable mode combinations hide radius versus nearest behavior. | Named radius/nearest orchestration makes differences explicit while retaining one ordered lazy batch loop. |
| Observability | All components mutate one backend-owned statistics object. | Component-owned counters feed a unified public view without lower-layer imports of the adapter. |

Keep discovery/catalog/source validation/decoding/prepared-file disk cache and LRU in proposed `indexed_files.py`.
Keep prepared-window building, `WindowKey`/`PreparedWindow`, query geometry/cache, `Selection`, and radius/nearest traversal in proposed `indexed_spatial.py`.
Keep numerical math in `indexed_geometry.py`; value records live near their owners, not in a speculative universal types module.
Do not keep old private methods as compatibility shims or create import cycles to preserve their names.

## Scope and non-goals

- Complete delegation and remove redundant adapter state only after every remaining reference has been traced.
- Retain the public import path `OceanDB.data_access.indexed_along_track.IndexedAlongTrackBackend`, constructor defaults, four protocol signatures, and structural conformance.
- Preserve source discovery/validation timing, `prepare_grid`/`prepare_window`/`refresh` behavior, and the unified statistics surface.
- Update affected user-facing behavior documentation and Sphinx runtime docstrings in the same implementation commits.
- Do not redesign `AlongTrack`, clients, SQL, `Dataset`, mission aliases, basin policy, source discovery layout, or ETL.
- Do not add a new ABC/generic interface, SQL fallback, `n_jobs > 1`, indexed streams, or a thread-safety claim.
- Do not change `_CACHE_VERSION = 2`, existing disk artifacts, default resource limits, or add snapshot writes; step 09 remains opt-in and separately authorized.
- Preserve unrelated edits to `markdown/queries.md`, `graph.py`, and `graph_along_track.py`; do not include their unrelated hunks in commits.

## Numbered implementation checklist

1. Trace all references to `_active`, `_vectors`, `_offsets`, `_active_sources`, `_active_key`, `_files`, and `_grid_geometry` after steps 02–07.
   Remove duplicate state only once its owning file/spatial component and callers are explicit.
2. Verify and simplify the projection boundary already made explicit in steps 04/06: `PreparedWindow`, `Selection`, requested fields, center date, and a clear NN-distance requirement.
   Read row indices, distances, ordered source mapping, and window identity from those arguments, never a subsequently replaced active cache entry.
3. Keep projection explicit: validate fields at the established boundary, preserve first-occurrence field order with deduplication, and append missing NN `distance` only once.
   Use schema Python dtypes and packed science inputs; reconstruct UTC-naive timestamps at exact microsecond resolution and source text through window-local `file_id` mapping.
4. Preserve `delta_t = center - observation` in seconds; radius distance is optional, NN distance mandatory.
   Return `None` only for no matching rows; preserve a matching radius `fields=[]` as the legacy empty-column `Dataset`, not `None`.
5. Keep one generator-driven ordered center loop; use named radius/nearest selection paths if that removes boolean mode soup without duplicating entire loops.
   Acquire each center's required window immediately before selection; do not regroup mixed dates or prepare future windows eagerly.
   Ensure previous loop-local snapshots are released before replacement acquisition; a small per-center helper may keep those references out of the suspended generator frame.
6. Pin and validate the **whole** inventory once before a nonempty batch relies on cached coverage; no source/window work for an empty batch.
   Preserve fail-closed changed/disappeared files, including previously unselected sources; additions require `refresh`, and mid-batch mutation remains unsupported.
7. Keep validation locations and exception timing precise, including direct-backend calls versus facade calls; use the table below as a review obligation.
   A generator refactor must not move deferred work into its ordinary wrapper, or defer facade capability/shape validation until first iteration.
8. Delegate `prepare_grid`, `prepare_window`, and `refresh` while retaining their public call/return behavior and source-check scope.
   Refresh clears prepared-file memory and active windows, resets their current byte counts, and retains grid geometry and cumulative counters/high-water history.
9. Aggregate component-owned statistics into the existing public names; preserve update/observation semantics characterized in step 01 without requiring lower layers to import `IndexedCacheInfo`.
   Keep the ownership and live-observer semantics established in step 02; do not switch retained stats references to disconnected snapshots at this late stage.
   Prefer simple typed counter records/views over a callback bus.
10. Check structural `AlongTrackBackend` conformance through precise annotations and a targeted type-check fixture; add no nominal inheritance requirement.
    Use concrete/narrowed types without casts, suppressions, or unnecessary `Any`; use Sphinx/reStructuredText docstrings for changed Python APIs.
11. Update only relevant user documentation alongside code, documenting unchanged capabilities, laziness, refresh, statistics, and resource semantics accurately.
    Review complete cold/warm/adjacent-window/restart results and measured step-01 baselines before declaring cleanup complete.

### Validation timing to preserve

| Surface | At ordinary call time | At first iteration / per consumed center |
| --- | --- | --- |
| `AlongTrack` facade batch | Equal lengths, worker/chunk limits, unsupported parallel capability; NN cap validation remains eager. | Delegated backend generator work only. |
| Direct indexed batch wrapper | `n_jobs=1` and positive `chunk_size` checks. | `_batch` length/field/radius validation, then whole inventory once for nonempty input. |
| Lazy center loop | No window/source work before consumption. | Window/time validation and preparation, spatial selection, then projection for the current center. |
| Scalar query | Executes its singleton batch immediately; facade NN cap validation is eager. | No independently exposed deferred scalar result. |

Preserve exception precedence and empty-batch edge cases, including lazy field/radius checks but no per-center date/time-window work when there are no centers.
The current direct `prepare_window` has its own source-check scope; do not silently broaden or weaken it while moving public-query inventory checks.

## Invariants to preserve

- Original float64 coordinates, unmodified stored longitude, integer microseconds, source row association, and complete metadata survive every cache/window transition.
- Direct basin plus self eligibility, land self-only, inclusive time/radius/NN cap, and at-most-three deterministic NN order remain unchanged.
- NN ties use equality of computed distances only, then time/latitude/stored longitude; no tolerance creates ties or changes membership.
- Packed science/masks stay aligned; schema scaling is applied once, scalar correction masks broadcast, and missing science does not change selection.
- One result per input center, including `None`, with mixed-date input order and incremental generator consumption preserved.
- No warmed-memory/disk path bypasses whole-inventory fail-closed checks before a nonempty public query/batch.
- Existing stats names remain: `source_reads`, `catalog_reads`, `disk_hits`, `memory_hits`, `window_builds`, `preparation_seconds`, `cached_bytes`, `active_bytes`, `peak_retained_bytes`, `geometry_bytes`, `geometry_builds`, and `geometry_hits`.
- Source/catalog reads, hits, window builds, cumulative preparation time, current bytes, geometry costs, and high-water semantics remain distinguishable.
- Retained array `nbytes` is not process RSS, temporary allocation peak, or an on-disk retention limit.
- Version-2 decoded-file/catalog artifacts remain reusable; no public SQL/default-backend/capability behavior changes.

## Proposed atomic commits

### Commit 1 — Simplify and verify explicit schema projection

- **Purpose:** audit the existing explicit boundary from steps 04/06, remove residual branching or hidden lookups, and consolidate schema-projection regression coverage; do not defer earlier provenance guarantees to this commit.
- **Tests:** extend `test_indexed_backend.py`; propose DB-free `tests/along_track/test_indexed_adapter.py` for projection after active-window replacement/refresh.
- **Acceptance:** exact identity/metadata/science alignment, projection order/dedup/dtypes, signed time differences, and mandatory/optional distances are unchanged.

### Commit 2 — Finish lazy orchestration and owner delegation

- **Purpose:** leave one small ordered batch loop and delegate file, window, grid, refresh, and statistics ownership without cycles or shims.
- **Tests:** instrument generator creation/first `next`/partial consumption/empty input, full-inventory counts, mixed dates, unheld-window release during replacement, and refresh/statistics behavior.
- **Acceptance:** timing table and release-before-rebuild hold; no future-window work, date regrouping, hidden source reads, retained loop-local old windows, or lower-to-adapter imports.

### Commit 3 — Document and validate the finished adapter

- **Purpose:** update affected behavior docs and Sphinx docstrings, remove dead adapter helpers, and record final cleanup comparison evidence.
- **Tests:** both existing DB-free suites, proposed adapter tests, affected-file format/type checks, and baseline cold/warm/adjacent/restart comparisons.
- **Acceptance:** complete outputs agree and costs are measured; preserved unrelated documentation/script work is excluded; synthetic evidence is not presented as real-data parity.

Each commit must leave the backend functional; couple behavioral regression tests and relevant documentation to their implementation changes.

## Validation commands and cases

Run from the OceanDB repository root; new paths below are proposed and must be created before their commands are used.

```sh
uv run --no-sync pytest -m unit tests/along_track/test_indexed_backend.py tests/along_track/test_backend_contract.py -q
uv run --no-sync pytest -m unit tests/along_track/test_indexed_adapter.py -q
uv run --no-sync black --check src/OceanDB/data_access/indexed_along_track.py tests/along_track/test_indexed_adapter.py
uv run --no-sync isort --check-only src/OceanDB/data_access/indexed_along_track.py tests/along_track/test_indexed_adapter.py
```

Also run the accepted earlier-step DB-free suites and format-check every changed Python file using `uv run`.
No configured type checker exists in `pyproject.toml`; use [README targeted checker guidance](README.md), record tool/version/arguments, and resolve new errors without casts or suppressions.
Do not run pytest-all, `make test`, or `make check`. Integration fixtures **drop the configured database**; only the README's separately authorized disposable integration procedure is acceptable.

- Check all four public methods with full/narrow/repeated/empty field requests, unknown fields, absent matches, and exact NN order.
- Assert whole rows: source keys, original coordinates/timestamps, metadata, packed/scaled science, masks, distances, and signed `delta_t`; counts alone are insufficient.
- Hold a selection/window reference, replace the backend's active window, and materialize the old reference; metadata must still refer to its original files.
- Test direct/facade validation before generator creation, at first `next`, after partial consumption, and on empty inputs, including exception precedence.
- Count full-inventory validations once per nonempty batch; mutate/delete an unselected pinned source before consumption and require failure.
- Add files and verify invisibility until refresh; verify refresh retains geometry/cumulative history but clears file/active retained bytes.
- Measure cold sources, warmed files, repeated/adjacent windows, fixed-grid moving-date batches, and restart reuse against step-01 baselines.
- Record sources, query workload, revision, environment, output comparison rules, and untested real-data/integration risks; claim no speedup without measurements.

## Independent-agent audit checklist

- [ ] Independent reviewer follows the [shared audit protocol](README.md), reading actual diff/callers rather than only this plan or the implementation summary.
- [ ] The adapter owns only protocol methods, lazy workflow, projection, and simple delegation; lower layers never import it.
- [ ] Projection's explicit window/source identity survives active-state changes and retains schema scaling/field-order semantics.
- [ ] Validation timing/precedence and generator laziness match the table, including empty inputs and direct backend calls.
- [ ] Full inventory checks, refresh, stats ownership, byte semantics, unsupported capabilities, and version-2 artifact reuse are preserved.
- [ ] Independent tests inspect complete scientific outputs, with trustworthy baseline measurements and clearly bounded parity claims.
- [ ] Unrelated `markdown/queries.md`, `graph.py`, and `graph_along_track.py` work is preserved; no generated caches, source datasets, or secrets are included.

### Copyable audit prompt

```text
Independently audit step 08 in tasks/08-finish-backend-adapter.md.
BASE_REF=<pre-step revision>
HEAD_REF=<proposed step revision>
Follow tasks/README.md's shared audit protocol. Do not edit or repair files.
Read git diff "$BASE_REF" "$HEAD_REF", protocol/facade callers, and component owners.
Independently inspect projection provenance/scaling/order, generator validation
timing and exception precedence, empty/mixed-date batches, whole-inventory checks,
refresh/statistics/byte semantics, imports, and unsupported capability boundaries.
Run safe targeted DB-free tests/checks through uv run; scrutinize assertions and
baseline measurements rather than accepting implementer claims. No broad suite
or integration run without explicit disposable-database authorization.
Report prioritized findings with symbols/file lines and concrete evidence,
commands/results, a clear verdict, and untested risks. Preserve unrelated work.
```

## Definition of done

- [ ] A thin, structurally conforming public adapter delegates to clear file/spatial owners and projects explicit immutable-window selections.
- [ ] Targeted science, laziness, invalidation, cache, statistics, formatting, and type checks pass or have explicitly documented unrelated blockers.
- [ ] Affected user docs/runtime docstrings and measured complete-output baseline comparisons are current; independent review findings are resolved or explicitly accepted.
- [ ] Steps 01–08 can close without step 09; persistence remains a separate optional feature and requires its own design/authorization decision.
