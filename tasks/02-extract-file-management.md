# 02 — Extract file management without changing cache behavior

## Status, prerequisites, and navigation

- **Status: proposed; no runtime extraction has been implemented by this document.**
- Prerequisite: [01 — Characterize behavior and performance](01-characterize-behavior-and-performance.md), including independent audit and recorded baseline.
- Read [the series README](README.md) for common invariants, audit protocol, and targeted checker protocol.
- Next: [03 — Clarify NetCDF decoding](03-clarify-netcdf-decoding.md); do not combine its decoder rewrite with this move.
- Later, [04 — Introduce prepared window](04-introduce-prepared-window.md) owns window records and spatial construction.
- Steps 01–08 preserve behavior; [09 — Persist index snapshots](09-persist-index-snapshots.md) is a separate opt-in feature gate.

## Goal and current problem

Move the file lifecycle out of `src/OceanDB/data_access/indexed_along_track.py::IndexedAlongTrackBackend`.
Create the **proposed** `src/OceanDB/data_access/indexed_files.py` as the owner of discovery, catalog metadata, source checks, file decoding entry, disk artifacts, and per-file LRU.
Today `_Source`, `_coverage`, `refresh`, `_check_source`, `_validate_inventory`, `_prepare_file`, and `_load_file` are intertwined with active spatial arrays.
`_atomic_json`, file artifact writes, `_asset_signature`, `_mission`, `_CACHE_VERSION`, and `_FILE_DTYPE` belong to the same lower-level lifecycle.
`prepare_window` needs rows and source provenance, not filesystem/stat/manifest implementation details.

The eventual adapter keeps protocol methods, lazy orchestration, and schema projection; `indexed_spatial.py` will own prepared windows and selection later.
`src/OceanDB/data_access/indexed_geometry.py` remains pure mathematical support, with no filesystem dependency.
Do not use this task to implement all layers at once or to change decoding policies while moving their owner.

## Before / after: simplicity, coupling, and readability

| Concern | Before | Proposed after |
| --- | --- | --- |
| Simplicity | The backend mixes cache recovery, file decoding, and query preparation. | One file owner answers inventory/load/refresh questions; the adapter delegates explicitly. |
| Coupling | File helpers read or mutate a large backend instance. | The owner receives paths, budget, basin dependency, and counters, not the backend itself. |
| Readability | `_load_file` cache branches sit beside nearest/radius algorithms. | Cache identity, source freshness, and LRU policy can be reviewed together. |
| State ownership | Refresh invalidation spans unrelated arrays and catalogs. | File reset and adapter active reset are distinct, plainly sequenced operations. |

Use a small concrete owner, provisionally `IndexedFiles`, not a cache framework, strategy registry, inheritance tree, or whole-backend callback bundle.
Move bodies with minimal edits first; decoder intent-naming is task 03 and spatial value records are task 04 onward.

## Scope and non-goals

- Own source discovery/catalog/source validation, file decoding entry, atomic writes, mmap loading, and file LRU in `indexed_files.py`.
- Keep the source record frozen, with path, size, nanosecond mtime, observation bounds, and mission; preserve deterministic source ordering.
- Keep file dtype definitions at the file owner; window dtype/provenance assembly stays with current window construction until task 04.
- Retain the current NetCDF decoding body rather than replacing it with ETL/schema-reader calls.
- Keep public constructor parameters, import surface, four query methods, `prepare_window`, `prepare_grid`, `refresh`, and observable `stats` behavior.
- No private compatibility shims, copied old implementations, new cache version, snapshots, disk eviction, or cache-format migrations.
- Do not change malformed-input policy, supported calendars/coordinates, geodesics, or nearest/radius selection.

## Numbered implementation checklist

1. Record existing file/cache dependencies and split refresh responsibilities before editing.
   Identify file-owned state (`_sources`, `_files`, catalog paths/signature, file budget) versus adapter-owned active and geometry state.
2. Add the proposed `IndexedFiles` owner and move `_Source`, discovery, `_coverage`, `_mission`, `_asset_signature`, and catalog handling together.
   Keep constructor root resolution/product-directory handling, discovery glob `*/*/*/*.nc`, sorted paths, and mission validation behavior.
   Give consumers a stable source sequence instead of exposing mutable catalog containers for arbitrary modification.
3. Move `_times`, `_prepare_file`, and required conversion/schema/dtype helpers with minimal semantic movement.
   Keep the complete-file decode and post-decode `_check_source` call intact; retain `source_reads` increment placement after successful validation.
   Shared epoch/microsecond conversion must live below the adapter or be explicitly imported from its lower-level owner, never imported back from the adapter.
4. Move `_check_source` and `_validate_inventory` into the file owner with narrow method names and precise source types.
   The adapter must still invoke the full inventory check once before the first nonempty batch center, not at generator construction.
   Keep selected-source checks on active-window hits and before file memory/disk hits; do not replace full inventory checking with selected-only checking.
5. Move `_load_file`, `_atomic_json`, and prepared-file temporary-write/replace cleanup without changing serialization.
   Preserve `allow_pickle=False`, `mmap_mode="r"`, dtype/dimension checks, cache-hit recovery branches, and same-directory atomic replacement.
   Keep the existing cache version 2 and exact fingerprint input ordering/JSON separators so previous artifacts remain hits.
6. Preserve relative catalog source paths and root-based catalog filename hashing separately from absolute prepared-file fingerprint paths.
   Catalog reuse requires the existing version/root/stat/bounds conditions; observation bounds still come from decoded `time` when reuse fails.
   Retain unchanged-source artifacts; do not infer time coverage from directory or basename dates.
7. Move LRU ownership and byte accounting with the arrays; preserve hit promotion, insertion, peak recording, then eviction order.
   Zero budget still allows the current load to return rows but retains none; an oversized file is not retained merely because it was just requested.
   A cache eviction drops the owner's reference, not mappings/arrays still needed by active construction or previously returned results.
8. Resolve statistics wiring explicitly before integration: component-owned file counters or one small neutral counter record are acceptable.
   Preserve the unified `backend.stats` fields and cumulative values, including cross-component peak-retained accounting; test retained observer references.
   `indexed_files.py` must not import `indexed_along_track.py` for `IndexedCacheInfo`, `_record_retained`, types, or callbacks.
   If a neutral record is necessary, keep it a small data/accounting record, not a shared services/context framework; document the chosen owner.
9. Make adapter `refresh` explicitly clear active state and ask the file owner to reinventory/clear file retention.
   Preserve reset ordering and failure behavior as characterized; retain geometry entries/bytes and cumulative counters rather than replacing the stats object.
   `prepare_window` continues selecting sources, assembling `file_id`, checking active cross-file duplicates, and building spatial arrays for now.
10. Replace file implementation calls at their existing orchestration sites and remove duplicate moved logic.
    Do not move `_batch` checks, precompute future windows, or change `_materialize`'s filename/mission mapping through selected-source order.
11. Add proposed compatibility/recovery/eviction cases listed below; run scientific and lifecycle regressions plus the phase-separated baseline.
    Use precise annotations, runtime narrowing for external JSON, and Sphinx `:param name:`, `:returns:`, and `:raises ExceptionType:` docstring fields.
12. Apply the README targeted checker protocol; compare existing diagnostics with new ones and fix new type errors without `typing.cast()` or suppressions.

## Invariants and cache compatibility

- A prepared-file fingerprint remains `[version, absolute path, size, mtime_ns, basin asset signature, dtype descriptor]` serialized exactly as before.
- Version remains **2**; catalog keys `version`, `root`, `sources` and source keys `path`, `size`, `mtime_ns`, `start_us`, `end_us` retain meanings and representation.
- The basin signature still hashes the packaged raster and direct-connection table; moving code must not change artifact keys.
- Every pinned source is validated before a nonempty public query/batch relies on catalog coverage, including previously unselected files.
- Changed or deleted sources fail before serving memory, active-window, or disk hits; new sources become visible only after explicit refresh.
- Source mutation during a batch is unsupported; do not claim a transactional filesystem snapshot or remove existing freshness checks.
- File decoding checks every row, including out-of-window duplicate keys; cross-file duplicates remain an assembled-window concern.
- File caches are read-only memory mappings with an `nbytes`-accounted LRU; the file budget is neither process RSS nor a disk quota.
- Geometry has its own admission policy/budget and survives refresh. File extraction must not combine it with the file LRU.
- Four query APIs, exact source coordinates, single science scaling, aligned masks, inclusive bounds, signed `delta_t`, and deterministic nearest ties remain unchanged.
- Basin eligibility remains direct neighbors plus self, including self-only land; no transitive expansion or ocean-only filtering is introduced.

## Proposed atomic commits

1. **Pin artifact and recovery contracts.** Scope: proposed tests for old artifacts, malformed caches/catalogs, and budget edges using the existing backend.
   Tests: focused new cases plus both existing unit modules. Acceptance: a version-2 compatibility oracle independent of the new file owner exists.
2. **Extract the concrete file owner and wire the adapter.** Scope: file/catalog/decode-entry movement, explicit dependencies, stats wiring, refresh delegation.
   Tests: all targeted scientific/lifecycle/cache tests and targeted checker. Acceptance: no changed artifact key, observable counter, validation timing, or selection logic.
3. **Verify boundary lifetimes and extraction cost.** Scope: any missing focused regression cases and concise ownership documentation, not a decoder rewrite.
   Tests: mmap/LRU/refresh/result-lifetime cases and task 01 phase baseline. Acceptance: original artifacts hit without decode, no new coupling cycle, measured regressions explained against the agreed gate.

## Validation commands and proposed test cases

All commands below are for future implementation from the OceanDB root; this planning task does not run them.

```bash
PYTHONPATH=src:. uv run --no-sync pytest -m unit tests/along_track/test_indexed_backend.py tests/along_track/test_backend_contract.py
PYTHONPATH=src:. uv run --no-sync pytest -m unit tests/along_track/test_indexed_backend.py -k 'cache or source or inventory or refresh or budget or artifact'
```

Add these **proposed new cases** to `tests/along_track/test_indexed_backend.py`; no separate test file is assumed to exist:

- `test_version_two_artifacts_survive_file_owner_extraction`: independently encode the old key/catalog contract or use artifacts produced at the recorded base revision; a restart must report disk/catalog reuse without source decode.
- `test_malformed_catalog_rebuilds_supported_recovery_cases`: cover truncated JSON, wrong version/root, missing required structure, and invalid reusable-bound types; compare current fallback behavior rather than hardening arbitrary JSON shapes here.
- `test_prepared_cache_wrong_dtype_or_rank_rebuilds`: supplement existing `test_corrupt_prepared_file_is_rebuilt_from_source` with valid `.npy` files that fail schema checks, always `allow_pickle=False`.
- `test_file_budget_zero_returns_rows_without_retention`: verify correct full results, no retained file bytes, and expected later disk reuse.
- `test_small_file_budget_evicts_least_recently_used_mapping`: use at least two nonduplicate files; assert hit promotion, retained-byte sums, eviction, and subsequent result validity.
- `test_refresh_retains_geometry_counters_and_reuses_unchanged_files`: check stats observer lifetime, file/active reset, cumulative high water, geometry identity/reuse, and visibility of newly added files.
- Extend changed/deleted-source cases across memory/disk/exact-window hits; preserve `test_changed_file_outside_window_invalidates_cached_coverage` and task 01 lazy/empty/partial cases.

A malformed cache that previously crashed rather than recovered is a separate bugfix candidate, not silent behavior expansion in this extraction.
Rerun task 01's approved baseline by phase; compare complete results, masks, metadata, and required order, not counts alone.
Follow the README targeted checker protocol for `indexed_files.py`, the changed adapter, any neutral record, and modified tests.
Do not run unrestricted pytest, `make test`, or `make check`: integration fixtures may **DROP the configured database**, and check reformats broadly.

## Independent-agent audit checklist

- [ ] A different agent checks the moved bodies against the base rather than trusting a rename-heavy diff summary.
- [ ] Actual imports flow downward; the file owner has no back-import of the adapter and receives no whole-backend reference.
- [ ] Exact version-2 identity serialization, catalog representation, atomics, mmap loading, and malformed-cache fallback are evidenced.
- [ ] Old artifacts are independent of the extracted implementation; a new writer reading its own output is insufficient compatibility evidence.
- [ ] Freshness checks cover cached and unselected sources without eager/empty-batch source work.
- [ ] Unified stats, peak accounting, observer references, zero/small budgets, refresh, and result lifetimes preserve behavior.
- [ ] Decoder/scientific/selection behavior did not change; new internal APIs are minimal and no speculative compatibility layer remains.

### Copyable prompt for a different audit agent

```text
Independently audit task 02 in OceanDB between BASE_REF and HEAD_REF.
Read tasks/README.md's common audit protocol and
tasks/02-extract-file-management.md. Make no edits or commits.
Compare the actual moved bodies and callers; do not trust mechanical-move claims.
Trace discovery/catalog bounds, all-pinned-source validation, memory/disk/active
hit checks, atomic artifacts, exact version-2 fingerprints, independently produced
old-artifact reuse evidence, mmap/LRU behavior, refresh, and unified live stats
without adapter back-imports. Check lazy/empty/partial batches and complete
scientific/provenance equality. Only run explicitly DB-free targeted uv checks;
never unrestricted pytest or make test/check. Return severity-ranked file:symbol
findings, concrete evidence, commands actually run, untested risks, and the README
verdict (accept / changes required / blocked by missing evidence). Do not fix findings.
```

## Definition of done

File lifecycle code has one lower-level owner, the adapter delegates through a small concrete interface, and moved logic has not been rewritten opportunistically.
Original version-2 artifacts, scientific results, generator timing, source invalidation, cache accounting, and refresh/result lifetimes are independently verified.
The targeted checker and phase baseline are reported honestly; a different agent's audit has no unresolved blockers before task 03 begins.
