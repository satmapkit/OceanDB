# 01 — Characterize behavior and performance before extraction

## Status, prerequisites, and navigation

- **Status: implemented and independently accepted; task 02 remains gated on user workload/threshold decisions.** See the [behavior matrix](01-behavior-matrix.md), [implementation evidence](01-evidence.md), and [audit](01-audit.md).
- Read [the series README](README.md), especially the common audit and targeted checker protocols.
- This is the prerequisite for [02 — Extract file management](02-extract-file-management.md).
- Steps 01–08 are behavior-preserving cleanup; [09 — Persist index snapshots](09-persist-index-snapshots.md) is a separately gated, opt-in feature.
- Work from the OceanDB repository root. Record the starting revision and existing changes before implementation.
- Obtain the user's actual query workload if available; otherwise explicitly label all performance evidence synthetic.

## Goal and current problem

Establish a trustworthy comparison surface before splitting `src/OceanDB/data_access/indexed_along_track.py`.
`IndexedAlongTrackBackend` currently owns source discovery, decoding, caches, active arrays, spatial selection, projection, and lazy batches.
Its `_batch`, `prepare_window`, `_load_file`, `_materialize`, `refresh`, and `IndexedCacheInfo` expose behavior that a mechanical split could accidentally change.
`src/OceanDB/data_access/along_track.py::AlongTrack` adds eager facade checks around the backend's deferred work.

Existing `tests/along_track/test_indexed_backend.py` uses independent literals from `tests/data/along_track_reference/generate.py`.
Its `FULL_FIELDS`, `NEAREST_FIELDS`, `_assert_source_row`, and `source_layout` are useful foundations, not a real-data performance baseline.
`tests/along_track/test_backend_contract.py` checks injection, defaults, capabilities, and forwarding using `_FakeBackend`.
Some existing checks consume generators with `list(...)`; that does not distinguish call-time from first-iteration failures.
`benchmark_runtime.py` supports a PostgreSQL index experiment and is **not** an indexed-backend baseline runner.

## Before / after: simplicity, coupling, and readability

| Concern | Before | Proposed after |
| --- | --- | --- |
| Simplicity | Each extraction must rediscover subtle lifecycle behavior. | A small, explicit behavior matrix is the shared acceptance oracle. |
| Coupling | Tests can accidentally depend on the backend's private array layout. | Assertions emphasize public results, work counters, and narrowly scoped event probes. |
| Readability | Scientific correctness and reuse assumptions are scattered. | Named lifecycle cases distinguish facade validation, preparation, and iteration. |
| Performance | A single elapsed time can conflate catalog reads, decoding, and selection. | Separate phase measurements explain changes without claiming an unmeasured speedup. |

Do not replace literal science oracles with values computed by the production reader.
This task simplifies later reasoning; it does not introduce the proposed `PreparedWindow` or change runtime ownership.

## Scope and non-goals

- Extend the two existing database-free test modules; proposed test names below are not present yet.
- Add only a narrowly scoped, reproducible benchmark driver if needed; `benchmarks/benchmark_indexed_along_track.py` is a **proposed new file**, not an existing command.
- Keep synthetic sources, cache directories, and generated results in temporary or approved external locations.
- Do not alter production algorithms, validation policy, public signatures, PostgreSQL behavior, or cache formats.
- Do not introduce snapshots, parallel execution, eager prefetch, generic benchmark infrastructure, or new dependencies without agreement.
- Never commit downloaded datasets, `.env` contents, credentials, prepared caches, or generated benchmark outputs.

## Numbered implementation checklist

1. Inventory current behavior using `AlongTrackBackend`'s four methods and the facade's `_validate_batch` and nearest-cap checks.
   Write a call-versus-iteration matrix before changing tests; retain differences rather than making validation uniformly eager.
2. Add proposed `test_indexed_batches_defer_work_until_first_next` for both direct-backend batch methods.
   After construction, create a generator and assert no new inventory validation, file loading, or window construction occurs until `next(...)`.
   Separately assert worker/chunk errors occur at direct-backend call time, while `_batch` shape, fields, and radius checks are deferred.
3. Strengthen facade contract cases to assert shape, batch-limit, nearest-cap, and unsupported-capability errors without consuming a generator.
   Do not claim radius/fields checks are eager in the facade: those still belong to the deferred backend path.
4. Add proposed `test_partial_batch_consumption_prepares_only_consumed_centers` with distinct, deliberately nonmonotonic dates.
   Count window preparation events; stop after one result and close the generator, proving no future window was prepared or centers reordered.
5. Add proposed `test_empty_batch_skips_inventory_and_preparation` for both APIs and both entry paths.
   Consume an empty generator and assert zero source/window work; valid empty input does not bypass the existing field/radius checks.
6. Add proposed `test_nonempty_batch_validates_all_pinned_sources_once` using a narrow `_check_source`/inventory probe.
   Distinguish the once-per-batch inventory pass from selected-file and active-window safety checks, which can also stat sources.
   Change or delete a previously unselected source **before iteration** and require failure before the first result; mutation mid-batch remains unsupported.
7. Add proposed `test_exact_window_reuse_canonicalizes_missions` using reordered/repeated valid mission selections.
   Exercise `None`, equivalent explicit inventories, and `[]`; distinguish an exact key hit from overlapping or adjacent windows.
   Also add proposed `test_exact_window_reuse_ignores_projection_radius_and_query_kind`: vary fields, spatial centers/radii, and radius-versus-nearest operations while keeping the temporal/mission key fixed.
   Require unchanged `window_builds` across those calls and independently correct results for each query.
   Assert counter deltas and complete results, not exact private key representation that step 04 will replace.
8. Add proposed `test_multifile_projection_preserves_provenance_after_cell_sort` with nonduplicate rows and multiple valid missions.
   Compare each returned key, filename, mission, packed/physical science value, mask, coordinate, and signed `delta_t` with independent expectations.
   Arrange filenames and cell order differently; assert nearest's ordered three-row cutoff separately from radius membership.
9. Add proposed `test_refresh_visibility_geometry_and_cumulative_counters` with a newly added nonduplicate source.
   New files stay invisible until refresh; refresh clears file retention and active state but preserves prepared grid geometry and cumulative counters.
   Verify unchanged prepared files remain reusable on disk, and current retained-byte fields reset appropriately.
10. Add proposed `test_returned_dataset_survives_eviction_window_change_and_refresh`.
    Retain an earlier result, change windows, force file eviction, refresh, and then access both previously read and unread scaled columns.
    Ensure no result is a dangling view of a subsequently replaced active array or a closed file mapping.
11. Establish the phase-separated baseline below on the unrefactored implementation and save a sanitized evidence summary.
    Preserve invocation, revision, dependency versions, workload parameters, repetitions, counter deltas, and the equivalence procedure.
12. Apply the README targeted checker protocol to new/modified Python, using precise annotations and Sphinx `:param name:`, `:returns:`, and `:raises ExceptionType:` fields.
    Do not use `typing.cast()`, new suppressions, or unrelated annotation cleanups to hide baseline diagnostics.

## Invariants to freeze

- Preserve all four protocol APIs, defaults, ordered batch results, `None` for no matches, and inclusive time/radius boundaries.
- Nearest returns at most three rows ordered by distance, time, latitude, then **stored** longitude; it always includes `distance`.
- Radius distance is optional; eligible radius `fields=[]` remains a `Dataset` with the legacy zero-column/zero-row-count shape.
- Source coordinates are never rounded or normalized. Basin eligibility is direct connections plus self, including self-only land, not transitive closure.
- Packed science reaches `Dataset` unscaled, is scaled exactly once on access, and retains aligned missing values; `delta_t` is center minus observation.
- Full-file validation includes out-of-window duplicate keys; cross-file duplicate detection concerns the assembled active window.
- Catalog coverage comes from decoded observations, not filename dates; changed/deleted pinned files fail even if previously unselected.
- Empty batches do no source/window work; no eager future preparation, center reordering, or new thread-safety guarantee is allowed.
- File and geometry budgets remain distinct retained-array accounting policies, not RSS guarantees or disk quotas.

## Baseline design and acceptance evidence

| Phase | Setup to record | Evidence to collect |
| --- | --- | --- |
| Cold construction/catalog | New empty cache, unchanged source inventory. | Constructor wall time and `catalog_reads`; keep this separate from first query. |
| Cold file preparation | First consumed query, no prepared file artifact. | Decode/window/query timings, `source_reads`, `window_builds`, retained bytes. |
| Restarted prepared-file hits | New backend/process, same sources and cache. | Catalog reuse, `disk_hits`, zero source decode, window reconstruction cost. |
| Warm identical window | Same exact date/half-window/canonical missions. | Query latency, unchanged `window_builds`, complete result equivalence. |
| Adjacent windows | A fixed ordered sequence with overlap and changing dates. | Window costs, file reuse/eviction, counter deltas, no future preparation. |
| Prepared query geometry | Same paired centers/radius with and without `prepare_grid`. | Separate grid preparation cost, geometry hits, selection latency, geometry bytes. |

Use fixed center/date/mission/field lists, both radius and nearest queries, and controlled file/geometry budgets.
For real sources, record user-approved provenance and workload shape without exposing private paths or contents.
For synthetic runs, record fixture generation/seed and scale; the six-row reference alone cannot establish realistic throughput.
Run repeated samples on the same machine; report medians and dispersion and distinguish process restart from a truly cold OS page cache.
Do not flush system caches or delete user cache directories. Use unique disposable caches for cold trials.
Measure noise first, then agree a regression threshold with the user; do not select a convenient threshold after seeing the refactor.
Compare complete columns, missing-value masks, packed and scaled science, metadata, and required ordering before interpreting timings.
Compare radius outputs by scientific key for set equivalence and characterize deterministic sequence behavior separately; do not invent a new radius sort contract.
Retained `nbytes` statistics exclude temporary arrays and do not measure RSS; report any separately measured RSS under its own label.

## Proposed atomic commits

1. **Characterize lazy API boundaries and reuse.** Scope: timing matrix, facade/direct tests, empty/partial batches, canonical missions.
   Tests: proposed lifecycle cases plus both existing unit modules. Acceptance: no production diff and assertions distinguish call from iteration.
2. **Characterize provenance, refresh, and result lifetime.** Scope: temporary multifile fixtures and focused regressions.
   Tests: proposed provenance/refresh/lifetime cases plus existing scientific oracles. Acceptance: all fields and masks remain independently checked.
3. **Establish reproducible performance evidence.** Scope: minimal proposed benchmark driver and sanitized run instructions, not generated results.
   Tests: small synthetic driver smoke case and full targeted unit run. Acceptance: separate phases, full equivalence, measured noise, agreed threshold or explicit pending gate.

## Validation commands and cases

These are future implementer commands, **not executions or successful results reported by this task**; run from the OceanDB root.

```bash
PYTHONPATH=src:. uv run --no-sync pytest -m unit tests/along_track/test_indexed_backend.py tests/along_track/test_backend_contract.py
PYTHONPATH=src:. uv run --no-sync pytest -m unit tests/along_track/test_indexed_backend.py -k 'batch or refresh or provenance or reuse or lifetime'
```

After the proposed benchmark driver exists, inspect its interface with `PYTHONPATH=src:. uv run --no-sync python benchmarks/benchmark_indexed_along_track.py --help`.
The driver must document the exact approved synthetic/real invocation; do not substitute the unrelated PostgreSQL runner.
Run the README targeted checker protocol on changed test/driver files, recording baseline versus new errors and missing checker availability.
Both named test modules are marked `pytest.mark.unit` and forbid indexed database connections or use fake backends.
Do not run unrestricted pytest, `make test`, or `make check`: database suites may **DROP the configured database**, and check formats broadly.

## Independent-agent audit checklist

- [x] A different agent inspects actual changes and independently maps each claimed invariant to evidence.
- [x] Call-time and iteration-time tests do not accidentally turn every generator into a list before checking errors.
- [x] Once-per-batch inventory validation is not confused with all `_check_source` calls; empty and partial batches are explicit.
- [x] Multifile/provenance cases do not introduce duplicates unless testing rejection; science expectations do not call the decoder.
- [x] Refresh and result-lifetime evidence covers lazily accessed scaled columns and unchanged grid geometry.
- [x] Performance phases, cache states, workload identity, full equivalence, noise, and threshold are documented without fabricated results.
- [x] No production behavior, source assets, secrets, generated outputs, or database lifecycle changed.

### Copyable prompt for a different audit agent

```text
Independently audit task 01 in OceanDB between BASE_REF and HEAD_REF.
Read tasks/README.md's common audit protocol and
tasks/01-characterize-behavior-and-performance.md. Do not edit or commit.
Inspect the actual diff and source/test symbols rather than accepting the summary.
Check facade/direct generator timing, partial/empty batches, inventory validation,
canonical reuse across fields/radii/query kinds, provenance, refresh, result
lifetime, and independent complete science oracles. Scrutinize benchmark phases,
workload provenance, cache state, measured noise, and any agreed threshold;
distinguish synthetic evidence from real-workload claims. Only run explicitly
DB-free targeted uv checks; never unrestricted pytest or make test/check.
Return severity-ranked file:symbol findings, evidence, commands actually run,
the README verdict (accept / changes required / blocked by missing evidence),
and untested risks or missing baseline gates. Do not implement fixes.
```

## Definition of done

The characterization suite protects the lifecycle and scientific invariants without runtime changes, and a reproducible baseline exists at the recorded revision.
Real-workload availability, measured noise, threshold agreement, checker limitations, and any untested risks are explicit, not assumed away.
A different agent has delivered the read-only audit verdict; unresolved blockers must be resolved or explicitly accepted before task 02.
