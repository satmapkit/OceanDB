# 07 — Refactor nearest-neighbor selection

## Status, prerequisites, and navigation

- **Status: proposal; no implementation or validation is claimed.** This is behavior-preserving cleanup, not a new nearest-neighbor policy.
- Prerequisites: accepted [characterization](01-characterize-behavior-and-performance.md), [prepared windows](04-introduce-prepared-window.md), [query geometry](05-extract-query-geometry.md), and [radius selection](06-refactor-radius-selection.md).
- Navigation: [series README and shared audit protocol](README.md) · [previous: radius selection](06-refactor-radius-selection.md) · [next: backend adapter](08-finish-backend-adapter.md).
- Read the repository `AGENTS.md`, `markdown/along-track-contract.md`, and the shared validation restrictions before implementation.

## Goal and current relevant symbols

Make the completeness argument, expansion termination, WGS84 refinement, and deterministic ranking readable without understanding backend cache state.
The current implementation is `IndexedAlongTrackBackend._nearest` in `src/OceanDB/data_access/indexed_along_track.py`.
It invokes `_select` repeatedly, then reads `self._active` for refinement and tie keys; `_batch` invokes it and `_materialize` projects its result.
Relevant numerical helpers are `MAX_WGS84_DISTANCE_M`, `wgs84_distance_m`, and `exact_wgs84_distance_m` in `indexed_geometry.py`.
The current `_DECISION_MARGIN_M` is `0.01`; it chooses rows to recompute, not rows to admit or distances to treat as equal.
Existing regressions in `tests/along_track/test_indexed_backend.py` cover unbounded distance, zero/inclusive caps, time/coordinate ties, and Vincenty rounding.
The six-row synthetic reference does not establish exhaustive algorithm coverage or real-data parity.

## Simplification, coupling, and readability rationale

| Concern | Before | Proposed after |
| --- | --- | --- |
| Simplicity | One backend method mixes search growth, stopping, refinement, and sorting. | A short nearest traversal with clearly named phases and only justified helpers. |
| Coupling | Selection depends on whichever mutable `_active` the backend currently owns. | Spatial selection consumes an explicit `PreparedWindow` and returns `Selection`. |
| Readability | Constants and the strict-inner stopping predicate are embedded in control flow. | The code explains why each expansion is complete and why boundary rows do not justify early termination. |
| Testability | Interesting cases require constructing file/backend state. | Array-level spatial tests exercise search/rank behavior; public tests retain end-to-end projection coverage. |

Keep traversal and its `Selection` record in the proposed `src/OceanDB/data_access/indexed_spatial.py`.
Use the owner-local `PreparedWindow`, `WindowKey`, and `QueryGeometry` established by preceding steps, not another hierarchy.
`indexed_geometry.py` remains pure numerical math; neither spatial code nor file management imports the adapter.
Avoid a generic search engine, configurable ranking framework, new ABC, and private compatibility wrappers for removed methods.

## Scope and non-goals

- Extract and clarify the existing algorithm and numeric policy; do not change its initial radius, doubling schedule, refinement margin, or tie definition.
- Reuse the radius selector's conservative screens and explicit `PreparedWindow`/`Selection` boundary.
- Reuse center vector and basin eligibility within an expansion sequence where straightforward; each cap still needs radius-dependent coverage.
- Preserve observable geometry-cache counter meanings; do not invent cache hits for local reuse or hide changed costs.
- Do not introduce a fixed spherical top-k, approximate NN, new dependency, SQL fallback, or parallel execution.
- Do not change decoding, source identity, schema projection, file-cache format, or `_CACHE_VERSION = 2` artifacts.
- Do not silently clamp huge finite caps to the global bound: repeated full scans with fewer than three rows are a separately benchmarked optimization decision.
- Do not add snapshot persistence here; [step 09](09-persist-index-snapshots.md) has a separate authorization gate.

## Numbered implementation checklist

1. Record the current search/ranking behavior using independent expected row identities before moving code.
   Add the proposed DB-free `tests/along_track/test_indexed_nearest.py`, marked `unit`, without importing database fixtures.
2. Define a precisely typed nearest entry point taking `PreparedWindow`, center geometry inputs, and `max_radius: float | None`, returning `Selection`.
   Keep aligned integer row indices and float64 distances; avoid `Any`, casts, unchecked JSON-like containers, and type-error suppressions.
3. Express expansion as an explicit phase: `limit = max_radius` when supplied, otherwise `MAX_WGS84_DISTANCE_M` (`20_003_932.0`).
   Set `search = min(limit, 150_000.0)` and call the conservative inclusive radius selector at that cap.
4. Preserve the stopping predicate exactly: stop at `search >= limit`, **or** when at least three rows exist and at least three distances are strictly `< search - 1e-5`.
   Otherwise set `search = min(limit, search * 2)`; a zero limit must terminate after its first selection.
5. Explain completeness: temporal/mission eligibility comes from the prepared window; direct basin eligibility and conservative cell/chord screens may only prune impossible candidates.
   The search must include every potentially nearer WGS84 row, not a fixed number of spherical neighbors.
6. Obtain provisional top-three positions with the existing `argpartition` rule using `min(3, len(rows)) - 1`.
   Handle zero candidates before partitioning; one or two candidates still participate in refinement and final ordering.
7. Recompute GeographicLib distances for **all** candidate rows within `0.01 m` of **any** provisional top-three distance.
   This always includes the provisional top three themselves, even when no other near ties exist; do not describe refinement as rare-tie-only work.
8. Apply the existing final `lexsort`: ascending computed distance, integer `time_us`, latitude, then **stored** longitude; return `[:3]`.
   Refinement is numerical recomputation, not symbolic exactness; only equal computed distances invoke secondary keys.
9. Wire the adapter to the new selector and remove its obsolete private nearest implementation, updating affected callers/tests directly.
   Preserve the explicit-window materialization handoff established in steps 04 and 06; step 08 only verifies and simplifies that existing boundary.
10. Write Sphinx/reStructuredText docstrings explaining parameters, returned alignment, the completeness argument, and termination assumptions.
    Run targeted checks and the README's targeted type-checker procedure; record benchmark deltas without claiming an unmeasured speedup.

## Invariants to preserve

- At most three results; inclusive temporal endpoints and NN cap, including exact zero and the exact GeographicLib boundary.
- `max_radius=None` is unbounded, not the radius API's 500 km default; a finite cap may exceed the global bound.
- Direct basin connections plus self only, never transitive closure; land/unlisted basins remain self-only.
- WGS84 determines membership and rank. Cell/chord padding cannot become a selection tolerance or synthetic tie.
- Equal-distance cutoff order is `(distance, time_us, latitude, stored longitude)` independent of file, insertion, and spatial-cell order.
- Preserve decoded coordinates, original longitude representation, microseconds, source-row association, packed values, and missing masks.
- Missing science does not remove candidates; schema scaling occurs once during `Dataset` access, not during selection.
- NN distance remains mandatory; radius distance remains optional; signed `delta_t` and the radius `fields=[]` legacy result are unchanged.
- Batch input order, mixed-date laziness, source fail-closed checks, refresh behavior, and statistics meanings remain unchanged.
- All selected rows/distances belong to the same immutable prepared window; cache replacement cannot change their interpretation.

## Proposed atomic commits

### Commit 1 — Characterize nearest expansion and complete-pool ranking

- **Purpose:** add focused array-level/oracle cases before moving selection; keep existing public regressions intact.
- **Tests:** proposed `test_indexed_nearest.py` plus relevant existing nearest and boundary cases in `test_indexed_backend.py`.
- **Acceptance:** assert exact selected source/scientific IDs and their order, not only count or approximate distances; the oracle does not call production selection helpers.

### Commit 2 — Extract explicit nearest traversal and refinement

- **Purpose:** move nearest selection into `indexed_spatial.py`, separating expansion, refinement, and deterministic ranking only where it improves clarity.
- **Tests:** exhaustive GeographicLib oracle comparisons, expansion-stop instrumentation, and existing four-method scientific regressions.
- **Acceptance:** no changed numeric formula, limit policy, import cycle, compatibility shim, schema behavior, or version-2 file artifact.

### Commit 3 — Integrate and explain the nearest boundary

- **Purpose:** remove dead adapter nearest state access and document the spatial entry point and its cost/termination policy.
- **Tests:** both existing DB-free suites, proposed nearest suite, affected-file formatting, targeted type checking, and step-01 baseline comparisons.
- **Acceptance:** complete outputs match; cold/warm/expansion costs are reported; huge-cap optimization remains deferred unless separately approved and tested.

Each commit should remain functional. Keep any standalone, unrelated documentation cleanup outside these commits.

## Validation commands and cases

Run from the OceanDB repository root, with dependencies available to `uv`; these are future implementation commands, not evidence of a completed run.

```sh
uv run --no-sync pytest -m unit tests/along_track/test_indexed_backend.py tests/along_track/test_backend_contract.py -q
uv run --no-sync pytest -m unit tests/along_track/test_indexed_nearest.py -q
uv run --no-sync black --check src/OceanDB/data_access/indexed_spatial.py tests/along_track/test_indexed_nearest.py
uv run --no-sync isort --check-only src/OceanDB/data_access/indexed_spatial.py tests/along_track/test_indexed_nearest.py
```

The proposed new test path must exist before invoking its command; also check any other changed Python files.
No type checker is configured in `pyproject.toml`; follow [README targeted checker guidance](README.md), record tool/version/arguments, and separate pre-existing errors from new ones without casts or suppressions.
Do not run pytest-all, `make test`, or `make check`; database fixtures **drop the configured database**.
Any optional integration run requires the README's explicit disposable-database procedure and separate permission.

- Test zero, one, and two eligible rows; no eligible rows; three boundary rows; three strictly inner rows; and expansion through multiple doubled caps.
- Assert the exact expansion sequence, zero-cap termination, global/unbounded completion, and unbounded nearest beyond 500 km.
- Include a finite cap larger than `MAX_WGS84_DISTANCE_M` with fewer than three rows; expose repeated scans without silently optimizing them away.
- Cover more than three equal-distance rows, permuted source/cell order, timestamp ties, latitude ties, and stored-longitude tie keys.
- Cover near-but-not-equal distances on both sides of provisional cutoffs; verify all rows near **each** provisional top-three distance are refined.
- Include an isolated provisional winner requiring refinement even without ties, spherical-versus-WGS84 ranking inversion, and near-antipodal fallback.
- Compare against exhaustive GeographicLib ranking of the full independently filtered pool, including direct-only basin and mission/time exclusions.
- Compare complete public results: identities, coordinates, microseconds, masks, packed/scaled science, metadata, distances, and signed `delta_t`.
- Reuse step-01 benchmark inputs and provenance; distinguish expansion cost from preparation and avoid claiming synthetic tests prove real-data parity.

## Independent-agent audit checklist

- [ ] A reviewer independent of implementation follows the [shared audit protocol](README.md) and reads the actual base-to-head diff.
- [ ] The reviewer derives termination/completeness from code, checking the strict-inner predicate, zero, few-row, global, and huge-cap paths.
- [ ] Refinement includes every provisional top-three row and all rows near any of their distances; tie ordering introduces no tolerance.
- [ ] Oracle expectations are independent of production pruning, basin closure, and sorting; exact selected IDs/order are asserted.
- [ ] Explicit-window ownership prevents stale-array interpretation; lower layers do not import the adapter.
- [ ] Relevant tests/checks are independently run safely, with output, limitations, and benchmark evidence reviewed rather than trusted from the implementer.
- [ ] No version-2 cache churn, unrelated user edits, generated artifacts, source datasets, secrets, or unsupported performance claims enter the diff.

### Copyable audit prompt

```text
Independently audit step 07 in tasks/07-refactor-nearest-selection.md.
BASE_REF=<pre-step revision>
HEAD_REF=<proposed step revision>
Follow tasks/README.md's shared audit protocol. Do not edit files or fix findings.
Inspect git diff "$BASE_REF" "$HEAD_REF" and the surrounding selection/geometry code;
do not treat the implementation summary or green tests as proof of correctness.
Verify expansion limits/stopping, conservative full-pool eligibility, mandatory
top-three refinement, exact-computed tie rules, explicit-window ownership, and
behavior preservation. Scrutinize the independent oracle and run safe targeted
DB-free checks yourself through uv run. No broad suite or database run without
explicit disposable-database authorization. Check performance evidence separately.
Report prioritized findings with file/line or symbol references and concrete
counterexamples/evidence, commands/results, a verdict, and untested risks.
```

## Definition of done

- [ ] Nearest traversal resides with spatial selection and has explicit typed inputs/results, short readable phases, and Sphinx docstrings.
- [ ] Scientific selection, projection, timing/laziness, cache-version, and source-invalidation behavior are preserved with independent regression evidence.
- [ ] Targeted test/format/type results and measured baseline comparisons are recorded; unrun integration/real-data checks are clearly identified.
- [ ] Independent review has a verdict and disposition for every finding; step 08 may proceed, but persistence and huge-cap optimization are not implicitly authorized.
