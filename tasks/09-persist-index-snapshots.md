# 09 — Persist optional index snapshots

## Status, prerequisites, and navigation

- **Status: gated feature proposal, not cleanup and not authorization to implement.** Completing steps 01–08 does not approve this feature.
- Prerequisites: accepted [finished adapter](08-finish-backend-adapter.md), independent cleanup audit, and explicit approval of the design gate below.
- Navigation: [series README and shared audit protocol](README.md) · [previous: backend adapter](08-finish-backend-adapter.md) · end of proposed series.
- Read `AGENTS.md`, the scientific contract, and README safety/type-checker guidance before design or implementation work.
- Persistence must remain optional; the existing behavior-preserving path must work unchanged when it is disabled.

## Goal and current relevant symbols

Avoid rebuilding a deliberately selected reusable spatial artifact after restart, only when measured workload benefits justify storage and operational cost.
Current `IndexedAlongTrackBackend.refresh` saves source coverage in `catalog-<root digest>.json`; `_load_file` stores decoded rows in `files/<digest>.npy`.
Those artifacts are **not spatial indexes**: `prepare_window` still filters/concatenates rows, sorts cells, builds offsets/vectors, and checks cross-file duplicates.
`prepare_grid` currently retains basin IDs, vectors, and radius-dependent cell covers only in memory.
`_active_key = (center_us, half_us, resolved missions)` is adequate only within the existing pinned-memory context, not as a disk identity.
After cleanup, `indexed_files.py` owns storage/source validation, `indexed_spatial.py` owns pure construction/selection and prepared values, and the adapter orchestrates them.
Existing `_CACHE_VERSION = 2` catalog/decoded-file artifacts must remain separate from any new artifact-format versions.

## Simplification, coupling, and readability rationale

| Concern | Without this feature | Proposed minimum, if approved |
| --- | --- | --- |
| Simplicity | Restart requires rebuilding useful geometry/windows from existing file caches. | Persist one chosen artifact class with explicit identity and a small manifest, not a universal cache framework. |
| Coupling | Putting IO into spatial traversal would entwine numerical correctness with publication/recovery. | Storage validates and maps artifacts; pure spatial constructors consume typed validated values. |
| Readability | An opaque snapshot could conceal scientific dependencies and partial generations. | Named arrays, typed JSON metadata, artifact-specific versions, and manifest-last publication expose exactly what is reusable. |
| Operations | Retained array-byte limits say nothing about disk growth. | Explicit opt-in configuration and a separately documented disk/retention policy make costs visible. |

This adds complexity rather than simplifying every workload; reject or defer it if restart savings do not justify that complexity.
No lower layer may import the backend adapter to obtain `PreparedWindow`, statistics, configuration, or source records.
At the design gate choose small artifact value records near their owning storage/spatial components and a one-way conversion boundary that avoids import cycles.
Prefer passing typed arrays/metadata into pure constructors over introducing a new shared framework or adapter-to-file-manager back-import.

## Design and authorization gate

1. Measure the intended workload and choose **one first artifact**, or explicitly defer the feature.
   Prefer `QueryGeometry` for a repeated fixed grid with moving dates; prefer full `PreparedWindow` only for repeated **exact-window** restarts.
2. Compare build time, serialization time, cold/warm restart load time, artifact bytes, peak RSS, and retained array bytes against the step-01 baseline.
   Do not automatically persist every overlapping daily window; quantify duplication and select explicit save/load triggers.
   Before approval, use existing build measurements, clearly labeled byte estimates, and isolated scratch serialization experiments if needed; integrated restart claims require post-approval implementation evidence.
3. Specify the optional API/configuration, defaults, failure policy, and ownership of artifact value records and conversion functions.
   Record an explicit disk-space budget/retention policy, cleanup trigger, incomplete-generation handling, and behavior while old readers hold mappings.
4. Decide checksum verification cost versus restart latency, and whether crash durability requires file/directory `fsync`; atomic visibility alone does not guarantee durability.
5. Obtain explicit approval for the selected artifact, format/identity, retention, and opt-in API before implementation commits proceed.
   Neither this document nor benchmark completion approves a default change or authorizes the second artifact class.

## Scope and non-goals

- Use existing NumPy and standard-library facilities only: separate `.npy` arrays plus a small typed JSON manifest, `allow_pickle=False`, read-only memory mapping.
- Never use pickle, object arrays, or `savez`/compressed bundles that give up the intended independent mmap behavior.
- Do not persist query results/distances, build a new ETL/archive format, or introduce pyproj, zarr, joblib, or another dependency.
- Do not add per-file spatial architecture, offline-source or relocation guarantees, broader concurrency/thread safety, or new source-content hashing assumptions.
- Default source identity remains pinned path/size/mtime metadata; metadata-preserving source replacement stays unsupported.
- For a full window, persist complete packed science fields/missing masks plus metadata needed for normal projection, not a narrow request-specific snapshot.
- A row-reference-only window format is deferred: it would reintroduce decoded-file dependencies and lifecycle complexity into loading.
- Keep `_CACHE_VERSION = 2` artifacts unchanged; use separate query-geometry/window format versions and relevant decoder/index dependency versions.

## Artifact contents and dependency identities

### If full `PreparedWindow` is selected

- Persist cell-sorted structured rows including integer `time_us`, original latitude/stored longitude, basin, source-row offset, packed science, track/cycle, and `file_id`.
- Persist dense cell offsets and row-aligned three-component unit vectors, plus **ordered** source metadata mapping each `file_id` to its source.
- JSON source records carry serializable mission/path/size/mtime/coverage metadata; never serialize Python `Path`, datetime objects, or object-dtype arrays.
- Identity includes the normalized validated pinned-source catalog, source-root identity, integer window center/half-width or bounds, and resolved literal mission selection.
- Include row dtype/schema/decoder versions, window/index format versions, cell size/dimensions, and basin/geometry dependencies actually used to build stored arrays.
- Initially digest the complete normalized source catalog conservatively, including previously unselected files; selected-file-only dependency tracking is optional future work.
- The in-memory `WindowKey` alone is insufficient: explicitly bind source snapshot identity and all construction dependencies before deriving a stable artifact key.
- A stored window is date/mission/source-bound; its sorted rows and offsets/cell cover relationships are not time-agnostic reusable query geometry.

### If `QueryGeometry` is selected

- Persist precise paired center coordinates/radii, center basin IDs, unit vectors, and ragged cap-cell IDs encoded as flat integer IDs plus offsets.
- Preserve pair order, duplicates where the chosen API promises them, and exact floating input representation; no center quantization or Cartesian-axis interpretation.
- Identity includes centers/radii, cell size/dimensions, basin-raster dependency, and cover/vector algorithm/format versions.
- Include a connection-table dependency only if stored values actually depend on it; runtime direct connectivity need not invalidate basin-only geometry.
- Do **not** bind geometry to dates, missions, a source inventory, prepared-file decoder versions, or unrelated science schemas.
- A distance-library version need not invalidate an artifact containing no distances; include dependencies because of stored content, not indiscriminately.
- Moving-date queries still prepare their own eligible source window; a geometry hit cannot replace temporal/mission filtering or inventory validation.

Both manifests must identify artifact kind, version, generation, array names/shapes/dtypes/expected byte lengths, and their dependency fingerprint.
Use deterministic canonical serialization for identities; validate typed fields at the JSON boundary instead of passing untyped manifest dictionaries through the application.

## Numbered implementation checklist

1. Complete and approve the design gate; document the selected artifact and explicitly deferred alternatives before writing persistence code.
2. Define artifact-specific typed manifest/value records and precise array signatures; reject booleans masquerading as integer metadata and unexpected dtype/object payloads.
   Keep storage IO in `indexed_files.py` or its explicitly approved small storage boundary, with no backend import or file/spatial import cycle.
   Restrict array names and paths to the owned artifact generation; reject absolute paths, parent traversal, and unknown payload names before opening or cleaning anything.
3. Build a complete normalized identity from validated owner inputs; scope version invalidations to the chosen artifact's real dependencies.
   For windows, use the full source-catalog digest initially; geometry must be reusable across moving dates without source-bound identity churn.
4. Write arrays to a unique staging generation using `np.save(..., allow_pickle=False)`; validate written metadata before publication.
   Use immutable generation names, then manifest-last publication or an atomic pointer to a completed generation; readers must never see a partially assembled multi-array artifact.
5. Make concurrent duplicate writers safe: equivalent identities publish equivalent completed generations and deterministic valid winners.
   Do not overwrite arrays that existing mappings reference, delete another writer's staging/winning generation, or claim that single-file `os.replace` makes multiple arrays atomic.
6. Load only completed manifests; open arrays with `np.load(..., mmap_mode="r", allow_pickle=False)` and validate before constructing public prepared values.
   Keep mapped arrays read-only and preserve their lifetimes while prepared values/results reference them.
7. Validate identity/versions, shape/dtype/expected file and payload bytes, row counts, and allocation bounds before trusting offsets or indexing mappings.
   Require offsets to start at zero, remain monotonic, and end at `N`: window row count or flat ragged-cell count as appropriate.
8. Validate cell-ID bounds, offset lengths, vector/row or vector/center alignment, ordered `file_id` mappings, source-row validity, and window time/mission constraints.
   Define how stored cell/vector correspondence is checked; record checksum coverage and verification cost rather than promising undetected-corruption immunity.
9. Treat corrupt/incompatible/interrupted artifacts as misses and rebuild from valid inputs; distinguish those from changed/missing sources, which still raise.
   A disk-window hit must not bypass the full pinned-inventory check before a nonempty batch; cache miss handling must not swallow source-validation failures.
10. Add opt-in acquisition/publication orchestration and retention only at the approved owner boundary; retain existing generator laziness and no-work empty batches.
    Keep direct spatial math/selection free of filesystem IO and lower-layer statistics free of adapter dependencies.
11. Add counters separating new artifact kinds, load/build/miss costs, and existing decoded-file hits; preserve existing statistics names/meanings.
    Distinguish disk footprint, mapped-array `nbytes`, retained-array bounds, temporary allocations, and process RSS.
12. Document the opt-in API/configuration, portability/invalidation limits, retention/recovery, and atomicity versus durability; use Sphinx/reStructuredText runtime docstrings.
    Implement with precise types, no casts/type suppressions, no new dependencies, and no committed cache artifacts/source datasets/secrets.

## Invariants

- Opt-out behavior and existing version-2 artifacts are unchanged; steps 01–08 remain independently useful without persistence.
- Loaded and freshly built prepared values have equivalent identity, immutable arrays, ordered source provenance, and selection results.
- Original coordinates/microseconds/stored longitude and packed science/masks stay aligned; schema scales once and signed `delta_t` is unchanged.
- Direct basin+self, land self-only, inclusive time/radius/cap, and computed-distance/time/latitude/stored-longitude NN order remain unchanged.
- NN distance is mandatory, radius distance optional, and matching radius `fields=[]` retains its legacy empty-column `Dataset`.
- Ordered mixed-date lazy batches do no eager future-window work; inventory validates once before nonempty batch work and no source/window work occurs for empty input.
- Changed/disappeared pinned sources, even previously unselected files, fail closed; additions need refresh and mid-batch mutations remain unsupported.
- Refresh clears current file/window state while retaining geometry/cumulative statistics according to the existing contract; persistence does not implicitly change that policy.
- Only completed, validated generations are served; corruption is a cache miss, not scientific authority or permission to ignore invalid source state.

## Proposed atomic commits

### Commit 1 — Record the gated artifact design (documentation only)

- **Purpose:** record workload measurements, selected artifact, identity/format, API, disk retention, checksums, and publication/durability decisions.
- **Tests/evidence:** reproducible baseline/build/load/serialization/RSS/disk measurements; no runtime behavior change.
- **Acceptance:** explicit approval is recorded; stop here if approval is absent or benefits do not justify the feature.

### Commit 2 — Implement typed serialization and atomic publication

- **Purpose:** add only the approved storage artifact and generation publication/validation boundary, not automatic adapter use.
- **Tests:** proposed DB-free `tests/along_track/test_indexed_snapshots.py` covers round-trip, invalid metadata, partial generations, and duplicate writers.
- **Acceptance:** read-only mmap values validate; no pickle/object arrays, dependencies, adapter imports, old-array overwrite, or version-2 cache changes.

### Commit 3 — Bound retention before activating acquisition

- **Purpose:** implement the approved disk policy and safe recovery/cleanup before adapter integration can publish automatically; include the relevant storage-operation docs/Sphinx docstrings.
- **Tests:** budget/retention decisions, interrupted staging cleanup, old mapped-reader lifetimes, unwritable cache/miss policy, and restart behavior.
- **Acceptance:** disk limits are explicit, no active mappings are overwritten, and cleanup never traverses/deletes source datasets or unrelated cache kinds.

### Commit 4 — Add opt-in restart acquisition and invalidation

- **Purpose:** wire approved load/save triggers, correct dependency identity, inventory checks, artifact-specific statistics, and user-facing API docs, using the already-implemented retention policy.
- **Tests:** no-rebuild restart load, complete output equality, artifact-appropriate source/catalog and asset/version invalidation, disabled-path equality, empty/lazy batches.
- **Acceptance:** a hit does not rebuild the chosen artifact; a window hit does not decode sources, while a geometry hit may still prepare windows. Source metadata validation still runs and invalid sources still raise; no intermediate public feature promises an unenforced disk limit.

### Commit 5 — Record comparative evidence and independent review

- **Purpose:** finish approved-workload benchmarks and audited feature documentation; do not enable a new default.
- **Tests/evidence:** selected artifact restart/build/serialization costs, footprint and RSS, full scientific comparisons, targeted type/format checks, and audit results.
- **Acceptance:** optional value is measured and limitations are explicit; a default change or second artifact needs a fresh decision.

## Validation commands and cases

From the OceanDB repository root, after the proposed snapshot test path exists:

```sh
uv run --no-sync pytest -m unit tests/along_track/test_indexed_backend.py tests/along_track/test_backend_contract.py -q
uv run --no-sync pytest -m unit tests/along_track/test_indexed_snapshots.py -q
uv run --no-sync black --check src/OceanDB/data_access/indexed_files.py tests/along_track/test_indexed_snapshots.py
uv run --no-sync isort --check-only src/OceanDB/data_access/indexed_files.py tests/along_track/test_indexed_snapshots.py
```

Check all other changed Python files and accepted component suites through `uv run`; follow [README targeted checker guidance](README.md), as no checker is configured.
Do not use pytest-all, `make test`, or `make check`: integration fixtures **drop the database**. Only explicitly approved disposable integration checks from the README are allowed.

- If windows are selected: round-trip all packed fields/masks, microsecond boundaries, ordered metadata, duplicate rejection, and exact selected identities/order.
- If geometry is selected: round-trip paired order, dateline/high-latitude/zero/global caps, ragged offsets, and moving-date reuse without source-specific invalidation. Do not implement the other artifact merely to satisfy this list.
- For the chosen artifact, monkeypatch its builder to fail on restart and verify successful load; for windows also forbid source decode and check distinct artifact-hit counters.
- Mutate shape/dtype/bytes/offsets/cell IDs/vectors/file IDs/versions/manifests; required structural checks and checksum-covered corruption must cause safe miss/rebuild, never unchecked allocation or indexing.
- State which same-shape payload changes the approved integrity policy detects; if checksums are omitted, do not promise detection of every arbitrary finite-value mutation or avoid measuring the cost of numerical revalidation.
- Cover truncated arrays, manifest absent, interrupted publication before/after pointer switch, two deterministic writers, and a reader holding an old generation.
- Exercise changed/deleted unselected sources despite warm hits, additions before/after refresh, and corrupt-cache plus invalid-source error precedence. Invalidate only actual decoder/index/basin/grid dependencies; verify geometry survives unrelated source/decoder/date/mission changes.
- Benchmark chosen workload serialization, restart latency, disk footprint, retained bytes, and peak RSS; compare complete outputs before interpreting timing and keep generated artifacts out of commits.

## Independent-agent audit and copyable prompt

- [ ] Approval, selected artifact, opt-in defaults, budget/retention, checksum policy, and durability decisions exist; scope has not drifted into both artifact classes.
- [ ] Independent reviewer follows the [shared audit protocol](README.md), derives dependency identities, and scrutinizes publication/reader/writer races from actual code.
- [ ] Typed validation covers corruption and scientific alignment; no cache path weakens full-inventory validation or imports the adapter into lower layers.
- [ ] Reviewer independently runs safe targeted checks, inspects complete outputs and measured restart/storage costs, and reports untested crash/filesystem risks.

```text
Independently audit step 09 in tasks/09-persist-index-snapshots.md.
BASE_REF=<pre-feature revision>
HEAD_REF=<proposed feature revision>
Follow tasks/README.md's shared audit protocol. Do not edit files or fix findings.
Read git diff "$BASE_REF" "$HEAD_REF" and confirm separate design authorization,
chosen artifact, opt-in defaults, and disk/retention/checksum/durability policy.
Independently inspect dependency identities, mmap/typed-array validation, source
fail-closed checks, atomic multi-array generations, duplicate-writer races,
old-reader lifetime, statistics, and complete scientific round-trip assertions.
Run safe targeted DB-free checks through uv run and scrutinize benchmark inputs,
outputs, and claimed benefits; no broad suite or unauthorized integration run.
Report prioritized findings with symbols/file lines and concrete evidence,
commands/results, verdict, and untested corruption/crash/filesystem risks.
```

## Definition of done

- [ ] Either the feature is explicitly deferred, or its approved single-artifact design is implemented as a separately opt-in capability.
- [ ] Typed read-only round-trips, safe publication/recovery/retention, full-inventory fail-closed behavior, and complete scientific outputs have independent evidence.
- [ ] User/API documentation, targeted checks, workload cost measurements, and independent audit findings/verdict are recorded without overstating durability or real-data parity.
- [ ] Existing version-2 caches and disabled-path behavior remain intact; no unapproved default, second artifact, generated cache, source dataset, or secret enters the change.
