# 04 — Introduce an explicit prepared window

## Status, prerequisites, and navigation

- **Status: proposed; no implementation is supplied by this task document.**
- Read [README.md](README.md), including its common invariants and common audit protocol.
- Prerequisites: accepted [01 characterization](01-characterize-behavior-and-performance.md), [02 file management](02-extract-file-management.md), and [03 decoding](03-clarify-netcdf-decoding.md).
- Previous: [03 — Clarify NetCDF decoding](03-clarify-netcdf-decoding.md).
- Next: [05 — Extract query geometry](05-extract-query-geometry.md).
- Steps 01–08 preserve scientific/public behavior; [09 snapshots](09-persist-index-snapshots.md) is a separate, opt-in feature decision.

## Goal and concrete starting points

Replace the backend's parallel active-window attributes with one complete, read-only `PreparedWindow`.
The starting implementation is `IndexedAlongTrackBackend.prepare_window` in `src/OceanDB/data_access/indexed_along_track.py`.
It currently coordinates `_active`, `_vectors`, `_offsets`, `_active_key`, and `_active_sources`.
Its inputs come from `_sources` and `_load_file`; `_select`, `_nearest`, and `_materialize` later read those mutable attributes independently.
The relevant layout is `_WINDOW_DTYPE`, extending `_FILE_DTYPE` with the provenance index `file_id`.
Move window construction into proposed `src/OceanDB/data_access/indexed_spatial.py`; retain public/lazy orchestration in the adapter.

## Before/after: simplicity, coupling, and readability

| Concern | Before | Proposed after |
| --- | --- | --- |
| Valid state | Five attributes must agree | One installed bundle identifies its rows, index, and provenance |
| Assembly | Loading, temporal filtering, indexing, and publication interleave | File manager supplies decoded rows; builder assembles locals |
| Readers | Implicitly consult whichever window is active | Receive the same explicit snapshot used for selection |
| Failure | Preparation clears old attributes before completion | A failed build never publishes partial state |
| Tests | Need a backend and source files to exercise assembly | Small decoded arrays directly exercise the builder |

This is a small value-record boundary, not a generic snapshot/cache framework.
Remove superseded private fields and update their callers directly; do not introduce aliases or wrapper shims.

## Scope and non-goals

- Introduce small typed `WindowKey` and `PreparedWindow` records near their spatial owner.
- Keep discovery, validation, decoding, disk caching, and file LRU in proposed `indexed_files.py` from tasks 02–03.
- Keep pure coordinate/index mathematics in `indexed_geometry.py`; keep `Dataset` projection in the adapter.
- Do not implement incremental adjacent-window updates, multiple retained windows, persisted windows, concurrency, or new public query options.
- Do not change file-cache identities, `_CACHE_VERSION == 2`, the `.npy` layout, or catalog format.
- Do not refactor radius/nearest algorithms here; their later owners are [06](06-refactor-radius-selection.md) and [07](07-refactor-nearest-selection.md).

## Numbered implementation checklist

1. Define `WindowKey` with exact center microseconds, exact nonnegative temporal half-width microseconds, and resolved missions.
   Use integer timedelta arithmetic as `_microseconds`/`prepare_window` do today, never floating-day conversions.
   Normalize missions with sorted deduplication; `None` resolves to all pinned missions, whereas `[]` resolves to none.
   Equivalent explicit/all mission selections may reuse a key; changed date, half-width, or resolved missions may not.
   Radius, requested fields, spatial center, and radius-versus-nearest query type are not window-key fields.
   This is an in-memory reuse key, **not** a disk identity: persistence would additionally need source-snapshot, asset, grid, and algorithm/version identity under task 09.
2. Define `PreparedWindow` to hold the key, ordered source provenance, structured rows, aligned unit vectors, and dense cell offsets.
   Include the immutable grid configuration needed to interpret those offsets and check geometry compatibility, without adding grid parameters to the temporal reuse key.
   Preserve the selected-source order and `file_id` indexing, including sources contributing zero rows.
   Keep the file manager independent of the adapter and spatial query engine; a shared source-metadata dependency may be a small value type.
   Neither file management nor pure geometry may import the backend to obtain types or statistics.
3. Keep source selection/loading and source validity checks outside spatial construction; do not pass a loader callback that hides disk I/O.
   After key validation and an exact-key reuse check, clear the previous owner-held snapshot and active-byte count at the existing release-before-rebuild boundary, before loading replacement files.
   Load one source, run a pure temporal filter/copy operation into a compact window chunk, then release unnecessary decoded references before loading the next source.
   Give the final pure assembler those compact chunks, provenance, and grid parameters; it must never open/stat files or construct a `Dataset`.
   Do not retain all full-file mappings in a list simply to obtain a pure final assembler; that would defeat zero/small LRU budgets during construction.
4. Separate inclusive temporal assembly, active duplicate-key rejection, stable cell sorting, and index/vector construction into readable stages.
   Reject duplicate `(time_us, latitude, longitude)` keys in assembled active rows; do not silently deduplicate or choose a source winner.
   Retain the decoder's separate all-row, within-file duplicate validation, even outside the requested window.
5. Compute `cell_ids`, `np.argsort(..., kind="stable")`, `np.bincount`, cumulative offsets, and `unit_vectors` in locals.
   Validate shape/alignment at construction, not by rescanning whole arrays for each spatial center.
   Empty windows still contain correctly typed rows/vectors and `cell_count + 1` zero offsets, not missing attributes.
6. Freeze the record and mark every owned published array read-only; avoid writable aliases that can mutate a published bundle.
   Install only after successful construction, then update successful-build counters and active-byte accounting together.
   On replacement-build failure, propagate the error and leave explicit unprepared state, matching the current rebuild lifecycle; do not restore a hidden retained old-window cache.
   Release consumed snapshot references held by generator locals before acquiring the next window: clearing only an owner attribute is not enough.
7. Preserve `prepare_window(...)->None`; give lazy orchestration one focused acquisition path returning the completed bundle.
   Adapt `_select`, `_nearest`, and `_materialize` to consume that bundle directly, without retaining old private-state shims.
   Pass the selected snapshot into materialization now, including `file_name`/`mission` provenance; do not defer this safety boundary to [08](08-finish-backend-adapter.md).
8. Retain only the most recent prepared window in the owning component; A→B→A rebuilds A, and adjacent dates rebuild rather than incrementally patch.
   `refresh()` clears active-window and file RAM, while preserving source-independent geometry and cumulative statistics.
   Keep the unified `stats` view via component-owned counters or a small neutral record, never a lower-layer import of the backend.
9. Preserve public validation boundaries: full pinned inventory once per nonempty `_batch`, no inventory/window work for an empty batch.
   Preserve direct `prepare_window` source checks, selected-source reuse validation, and current eager/deferred error timing.
   Do not prepare later centers/windows, regroup dates, or claim support for source edits during a suspended batch.

## Invariants to carry through the change

- All four query APIs keep signatures/defaults; scalar results and yielded items remain `Dataset | None`.
- Defaults remain radius 500,000 m, half-window 10 days, unbounded nearest cap `None`, and serial batches with `n_jobs=1`, `chunk_size=16`.
- Time comparisons remain inclusive at exact microsecond precision; WGS84 radius membership remains inclusive.
- Stored source coordinate precision and raw longitude survive sorting, provenance, and projection unchanged.
- Basin eligibility remains direct connections plus self; land remains self-only, with no transitive inference.
- Packed science values and masks remain aligned; scaling happens only through `Dataset`, once.
- `delta_t` remains signed center-minus-observation; radius `distance` is optional and nearest `distance` mandatory.
- Nearest returns at most three, ordered by distance, time, latitude, then stored longitude.
- A matched zero-column radius projection remains the legacy zero-column `Dataset`, not `None`.
- Mixed-date batches remain lazy and in input order; no later window is built before its center is consumed.
- Accounting describes component-retained arrays; externally held older bundles/temporaries are not an additional retained-window cache. Internal generator locals are not an external-reference exemption.

## Proposed atomic commits

1. **Introduce typed records and the pure builder.**
   Purpose: encode window completeness and separate assembly from file I/O without changing the live backend yet.
   Tests: proposed `tests/along_track/test_indexed_spatial.py` covers multi-file assembly, empty arrays, sorting, duplicates, offsets, and vectors; mark it `pytest.mark.unit`.
   Acceptance: builder tests pass; no file access or `Dataset` imports; precise annotations and Sphinx/reStructuredText docstrings describe contracts.
2. **Use one explicit bundle throughout the backend.**
   Purpose: replace parallel attributes and bind selection/materialization to the same source provenance.
   Tests: existing indexed/backend-contract modules plus key-reuse, failed-build, and source-name/mission projection regressions.
   Acceptance: old private fields disappear, partial state cannot escape, public defaults/results and lazy validation timing are unchanged.
3. **Lock down lifetime and accounting.**
   Purpose: make replacement, refresh, and one-window retention observable without redesigning statistics.
   Tests: A→B→A counts, read-only arrays, intentionally held old-bundle stability, unheld old-window release, bounded decoded-file handoff, refresh, and empty-batch work spies.
   Acceptance: release-before-rebuild holds without external references; cumulative counters remain cumulative; active bytes match the installed bundle; changed sources still fail closed.

## Validation cases and commands

Extend existing `test_disk_cache_survives_restart_and_adjacent_time_windows`, mission-filter, microsecond-boundary, duplicate, and zero-column regressions.
Add a successful multi-file fixture with interleaved cell IDs and unique source keys; check every column and provenance after stable sorting.
Check repeated keys, reordered/duplicate missions, `None` versus explicit all, `[]`, changed half-width/date, and radius/query-type independence.
Inject a builder failure after valid A, request B, and assert no partial B or misleading successful build/active-byte update is visible.
Assert all published arrays reject mutation; retain an old bundle during B construction and verify it stays coherent without backend retention.
Separately hold **no** external A reference: use weak references or an ownership probe at B's construction boundary to show neither the owner nor suspended generator locals retain A.
Exercise multiple oversized sources under zero/small file budgets; prove evicted full-file mappings are released after temporal copying rather than pinned by an input list.
Compare actual construction RSS as well as retained counters; metadata-only accounting cannot detect these lifetime regressions.
Test mixed dates and partial generator consumption with spies: one full inventory validation, no later-window acquisition, no empty-batch work.
Run from `/home/rowedaniel/work/nwra/OceanDB`; these commands are proposed validation, not evidence of an execution:

```bash
PYTHONPATH=src:. uv run --no-sync pytest -m unit tests/along_track/test_indexed_backend.py tests/along_track/test_backend_contract.py
PYTHONPATH=src:. uv run --no-sync pytest -m unit tests/along_track/test_indexed_spatial.py
```

The second path is **proposed new**, not an existing test module; run it only after creation.
Follow [README.md](README.md)'s targeted type-checker protocol for `indexed_spatial.py`, `indexed_along_track.py`, affected file-manager types, and changed tests.
There is currently no configured checker; use the agreed `uv run` command/version and compare baseline/head diagnostics rather than hiding them.
Do not use `typing.cast`, unchecked shape assumptions, ignores, or suppressions; add runtime narrowing where required.
Record task-01 preparation time/retained-array measurements; avoid new per-center whole-window validation and unmeasured speedup claims.
Do not run routine whole-suite `pytest` or `make check`: database fixtures can drop databases, and `make check` formats broadly.

## Independent-agent audit checklist

- [ ] Reviewer is a **different agent in a fresh review session** and follows the README common audit protocol without edits.
- [ ] Inspect `BASE_REF..HEAD_REF`, including tests, imports, deleted fields, and all selection/materialization callers.
- [ ] Independently verify complete construction, immutable aligned arrays, source provenance, duplicate scopes, and empty-window offsets.
- [ ] Challenge key equality and failed-build reuse; check one-window lifetime and refresh/statistics behavior.
- [ ] Verify release-before-rebuild, including batch-frame locals and no externally held old bundle; check bounded per-source copying under zero/small file budgets.
- [ ] Check lazy mixed-date iteration and eager/deferred validation separately at facade and direct-backend boundaries.
- [ ] Independently run targeted checks and compare scientific results to literals/baseline, not merely new helpers to themselves.
- [ ] Report evidence, severity-ranked findings, verdict, and residual risks; distinguish untested cases from passes.

### Copyable audit prompt

```text
You are the independent reviewer, not the implementation agent. Do not edit files or commit.
BASE_REF=<revision immediately before task 04>
HEAD_REF=<revision containing task 04>
Read tasks/README.md's common audit protocol and tasks/04-introduce-prepared-window.md.
Independently inspect BASE_REF..HEAD_REF and surrounding callers; do not trust the implementation summary.
Check WindowKey identity, atomic PreparedWindow publication, read-only aligned arrays, source provenance,
duplicate-validation scopes, empty windows, one-window lifetime, refresh/statistics, and no import cycles.
Check release-before-rebuild with no external old-window reference, generator-frame lifetimes,
and load/filter/copy-per-source handoff; reject lists pinning every decoded mapping.
Verify public scientific behavior and lazy validation timing using targeted database-free uv checks;
check partial mixed-date iteration, source invalidation, and failed-build behavior explicitly.
Return evidence (commands/results and symbols), severity-ranked findings, and the README verdict
(accept / changes required / blocked by missing evidence),
and remaining risks or unavailable validation. Make no edits; follow README safety restrictions.
```

## Definition of done

- One explicit immutable window replaces all five parallel fields, and materialization uses its actual snapshot.
- Construction has no disk I/O, cache-format changes, incremental updates, or new public/scientific behavior.
- Targeted tests/type checks and measured preparation costs are recorded, with baseline issues and omissions explicit.
- A different agent has supplied an evidence-backed audit; blocking findings are resolved before task 05 proceeds.
