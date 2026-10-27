# 06 — Refactor radius selection into explicit stages

## Status, prerequisites, and navigation

- **Status: proposed; no runtime or test implementation is included here.**
- Read [README.md](README.md), particularly common invariants, safe validation, and the common audit protocol.
- Prerequisites: accepted [01 characterization](01-characterize-behavior-and-performance.md) through [05 query geometry](05-extract-query-geometry.md).
- Previous: [05 — Extract query geometry](05-extract-query-geometry.md).
- Next: [07 — Refactor nearest selection](07-refactor-nearest-selection.md); adapter completion follows in [08](08-finish-backend-adapter.md).
- Steps 01–08 preserve behavior; [09 persisted snapshots](09-persist-index-snapshots.md) remains a separate opt-in feature decision.

## Goal and concrete starting points

Turn `IndexedAlongTrackBackend._select` into an auditable radius pipeline over one explicit task-04 `PreparedWindow`.
The existing method combines geometry lookup, dense-offset candidate gathering, basin filtering, chord screening, WGS84 distances, refinement, and membership.
Its mathematical dependencies are `cap_cells`, `unit_vectors`, `wgs84_distance_m`, and `exact_wgs84_distance_m` in `src/OceanDB/data_access/indexed_geometry.py`.
It also owns `_DECISION_MARGIN_M`; the chord calculation duplicates `CAP_EARTH_RADIUS_M` as literal `6_335_000.0`.
The proposed traversal owner is `src/OceanDB/data_access/indexed_spatial.py`; pure geodesic/index mathematics stays in `indexed_geometry.py`.
Keep `Dataset` schema projection and lazy public orchestration in `indexed_along_track.py`, and file I/O in task-02/03 `indexed_files.py`.

## Before/after: simplicity, coupling, and readability

| Concern | Before | Proposed after |
| --- | --- | --- |
| Inputs | Selector reads mutable backend internals | Explicit prepared snapshot, geometry, and connection policy |
| Results | Anonymous pair, several empty-return branches | Typed `Selection(row_indices, distances)` with one empty convention |
| Reasoning | Pruning and scientific acceptance are interleaved | Named stages distinguish conservative screening from membership |
| Distance names | “Exact” can imply symbolic exactness | Algorithm-specific names describe Vincenty and GeographicLib |
| Testing | Mostly backend-driven examples | Leaf contracts plus independent brute-force geodesic comparisons |

Use a few concrete array functions, not a filter-object hierarchy, generic pipeline framework, or compatibility wrapper chain.
Named numerical roles and explicit data flow should make false-negative risks and alignment visible in ordinary code review.

## Scope and non-goals

- Extract ordered radius traversal and one-center selection; keep current traversal order during the move without promoting unordered radius results to a new public sort contract.
- Retain public defaults, masks, and projection behavior.
- Centralize distance/refinement policy without changing algorithms, dependencies, convergence settings, or numerical values.
- Do not replace Vincenty with GeographicLib wholesale without a separate measured decision; benchmark first if proposing that simplification.
- Do not redesign nearest expansion/ranking here; task 07 will reuse the policy, and existing nearest regressions must still pass now.
- Do not fix `unit_vectors`' broad “broadcastable” docstring promise by silently changing supported behavior in this cleanup.
  Either narrowly document/test the supported scalar-center and paired-row shapes used here, or propose a separate broadcasting bugfix.

## Numbered implementation checklist

1. Introduce precise `Selection(row_indices, distances)` near traversal, with aligned one-dimensional `np.intp` indices and `np.float64` distances.
   Use a common typed empty result; prevent shared writable empty arrays or caller mutations from creating hidden coupling.
2. Define `select_all` for ordered radius traversal against a single explicit `PreparedWindow`, geometry lookup, and direct connection policy.
   If accepting paired centers, yield selections lazily; do not accept mixed dates or silently acquire another snapshot inside traversal.
   Keep inventory validation and prepared-window acquisition outside its per-center loop; do no file I/O in traversal or leaf stages.
   The adapter must consume only centers belonging to its currently acquired snapshot, without eager regrouping or future-window preparation.
3. Make `select_one` readable as candidate cell ranges → direct basin filter → conservative chord screen → geodesic/refinement → inclusive selection.
   Each filter returns explicit arrays rather than mutating hidden candidate state; preserve alignment through every mask.
   Preserve ascending cell traversal and within-cell stable row order; do not add a new radius-result distance sort.
4. Gather candidate indices from `PreparedWindow.offsets[cell:cell+2]` using task-05 `QueryGeometry.cells`.
   Handle no occupied cells, no connected candidates, and no final matches with the same empty convention.
   Trust construction-time bundle invariants; do not validate whole window arrays for every query center.
5. Use checked connection lookup with `connections[basin]`, not unchecked `.get()` returning `None` to `np.isin`.
   Preserve `BasinConnections.connection_map`: self plus directly listed basins, land self-only, no graph closure or ocean-only filter.
6. Centralize the conservative Earth lower bound `CAP_EARTH_RADIUS_M == 6_335_000.0` in pure geometry and reuse it for cover/chord screens.
   Document its role relative to the WGS84 minimum meridional curvature radius; finite random testing is not a proof of a global bound.
   Name cell-cover degree padding `1e-10`, chord radius padding `1e-5` meters, and squared-chord slack `1e-14` separately.
   Explain their units, roundoff roles, and existing derivations/comments; do not conflate them or invent new numerical guarantees.
7. Keep chord screening conservative: angle `min((radius + 1e-5) / CAP_EARTH_RADIUS_M, pi)` and squared-threshold slack `1e-14`.
   Preserve the `MAX_WGS84_DISTANCE_M` bypass; none of these candidate-filter margins is a scientific membership tolerance.
8. Rename distance helpers to algorithm-specific names, such as `vincenty_wgs84_distance_m` and `geographiclib_wgs84_distance_m`, updating all affected callers directly.
   Current `wgs84_distance_m` is vectorized Vincenty with GeographicLib fallback on nonconvergence after 100 iterations at threshold `1e-12`.
   Current `exact_wgs84_distance_m` calls scalar GeographicLib inverse geodesics; it is numerical, not symbolic/exact arithmetic.
   Retain QuickMap attribution/license and clarify Sphinx/reStructuredText docstrings without altering the geodesic mathematics.
9. Name and centralize the existing 0.01-meter decisive-distance refinement policy, distinct from pruning margins.
   Radius candidates with `abs(distance - radius) <= 0.01` are recomputed with GeographicLib, then admitted only by literal `distance <= radius`.
   No radius epsilon, fuzzy tie, rounding, or new acceptance tolerance is allowed; keep aligned `float64` outputs.
   Nearest shares this numerical policy, but its cutoff/tie traversal remains for task 07; only required helper-call updates belong here.
10. Keep geometry-cache statistics at geometry lookup and preparation statistics at their owner, not inside numerical leaves where avoidable.
    Preserve the unified backend `stats` view via component-owned counters or a small neutral record; no lower-layer backend imports.
    Update adapter callers to pass the actual snapshot and `Selection` into materialization; delete superseded private selection code, not shim it.

## Invariants to carry through the change

- All four public query APIs keep defaults/signatures and `Dataset | None` scalar/yielded results.
- Defaults remain radius 500,000 m, half-window 10 days, nearest cap `None`, and serial `n_jobs=1`/`chunk_size=16`; no new thread-safety guarantee.
- Inclusive microsecond time bounds and WGS84 radius membership remain unchanged; no screening margin changes membership.
- Source precision, raw longitude, packed science values, masks, and provenance stay aligned; `Dataset` alone applies schema scaling once.
- `delta_t` stays signed; radius distance is optional, nearest distance mandatory, and zero-column radius matches remain a `Dataset`.
- Nearest still returns at most three by distance, time, latitude, then stored longitude; radius refactoring must not perturb tie results.
- Batches remain lazy, mixed-date, and in input order, with eager/deferred validation timing preserved at each API boundary.
- Validate the full pinned inventory once per nonempty batch; empty batches do no source/window/geometry work; midbatch source edits remain unsupported.
- `refresh()` clears file/active RAM, retains source-independent geometry/cumulative stats, and file disk-cache format/keys remain version 2.
- Only the most recent prepared window is retained, and adjacent dates rebuild rather than update incrementally.

## Proposed atomic commits

1. **Clarify numerical names and centralize unchanged policy.**
   Purpose: expose Vincenty/GeographicLib roles and distinguish conservative pruning constants from decisive refinement.
   Tests: proposed `tests/along_track/test_indexed_geometry.py` tests geodesics, fallback, and supported paired/broadcast contracts; mark it `pytest.mark.unit`.
   Acceptance: algorithms/settings/values unchanged; boundary and nearest-tie regressions pass; no compatibility aliases or new dependency.
2. **Extract typed radius traversal and pure filtering stages.**
   Purpose: introduce `Selection`, `select_all`, and `select_one` over one bundle with explicit array data flow.
   Tests: extend proposed task-04 `tests/along_track/test_indexed_spatial.py` for empty stages, basin policy, ordering, alignment, and membership.
   Acceptance: no file I/O/`Dataset` construction, no hidden mutation, no whole-window per-center validation, and no backend import cycles.
3. **Integrate and harden independent geodesic regressions.**
   Purpose: replace backend `_select` and independently verify conservative pruning and literal acceptance.
   Tests: existing two unit modules, deterministic random/adversarial GeographicLib oracle checks, lazy mixed-date tests, and nearest regressions.
   Acceptance: same public values/ordering/timing and stats meanings; measured costs reported; screening limitations and any untested cases explicit.

## Validation cases and commands

Retain existing `test_time_edges_radius_edges_and_nearest_cap_are_inclusive`, `test_geographiclib_boundary_is_inclusive_not_vincenty_rounded`, and nearest-tie regressions.
Add direct geodesic tests for coincident points, dateline crossings, high latitudes, near-antipodal fallback, and paired arrays/scalar centers.
Exercise documented supported broadcasting in the distance helper and characterize `unit_vectors` shape limitations without hiding a bugfix here.
Use deterministic random/adversarial points and independent GeographicLib inverse distances to find false negatives in cell and chord screens.
Cover zero/tiny/global radii, exact radius and ±1 mm edges, cell boundaries, several supported cell sizes, empty windows, and disconnected/land basins.
Verify every oracle-eligible point survives pruning; separately check final inclusion, selected-row/distance alignment, source precision, and stable radius order.
Finite sampling supplies regression evidence, not proof of the lower-bound constant or complete global no-false-negative correctness.
Benchmark task-01 candidate/pruning/geodesic costs; any future Vincenty-versus-GeographicLib simplification needs representative timing and fallback frequency first.
Run from `/home/rowedaniel/work/nwra/OceanDB`; these are proposed validation commands, not completed executions:

```bash
PYTHONPATH=src:. uv run --no-sync pytest -m unit tests/along_track/test_indexed_backend.py tests/along_track/test_backend_contract.py
PYTHONPATH=src:. uv run --no-sync pytest -m unit tests/along_track/test_indexed_spatial.py tests/along_track/test_indexed_geometry.py
```

`test_indexed_spatial.py` is **proposed in task 04** and `test_indexed_geometry.py` is **proposed new here**; use the second command after those modules exist.
Follow [README.md](README.md)'s targeted type-checker protocol for `indexed_spatial.py`, `indexed_geometry.py`, `indexed_along_track.py`, and affected tests.
No checker is configured currently: use the README-agreed checker/version via `uv run`, compare baseline/head, and fix new errors without casts/ignores/suppressions.
Do not run routine whole-suite `pytest` or `make check`: database fixtures can drop databases, and `make check` formats broad paths.

## Independent-agent audit checklist

- [ ] A **different agent in a fresh review session** follows README common audit protocol and makes no edits or commits.
- [ ] Independently inspect `BASE_REF..HEAD_REF`, all helper renames/callers, attribution, and adapter integration.
- [ ] Trace each pruning/acceptance stage and array dtype/order; verify direct basin policy and common empty selections.
- [ ] Check numerical values, units, bypasses, 100-iteration/`1e-12` fallback behavior, and literal refined acceptance.
- [ ] Compare independently to GeographicLib, including adversarial boundary/fallback cases; do not treat shared helpers as independent oracles.
- [ ] Recheck nearest ties, lazy mixed-date timing, no per-center inventory scans, source precision, and untouched file-cache identities.
- [ ] Return evidence, severity-ranked findings, verdict, and residual risks; state finite-sampling limitations and unavailable benchmarks honestly.

### Copyable audit prompt

```text
You are the independent reviewer, a DIFFERENT agent from the implementer. No edits or commits.
BASE_REF=<revision immediately before task 06>
HEAD_REF=<revision containing task 06>
Read tasks/README.md's common audit protocol and tasks/06-refactor-radius-selection.md.
Independently inspect BASE_REF..HEAD_REF and callers; verify Selection alignment, explicit snapshots,
lazy select_all/select_one, direct basin policy, stage ordering, and absence of file I/O/hidden mutation.
Audit unchanged conservative constants separately from 0.01 m refinement and literal <= radius;
confirm vectorized Vincenty, 100 iterations/1e-12 convergence, scalar GeographicLib fallback/refinement,
algorithm-specific names, no new libraries, and unchanged nearest behavior/source precision/cache keys.
Run targeted database-free uv checks and independently challenge screens with GeographicLib oracles.
Return evidence (commands/results and symbols), severity-ranked findings, and the README verdict
(accept / changes required / blocked by missing evidence),
and risks/unavailable checks; finite samples are not a global-bound proof. Follow README safety rules.
```

## Definition of done

- Radius selection is a small explicit, typed pipeline over one immutable window, with conservative screens separate from final membership.
- Public/scientific behavior, nearest regressions, validation timing, cache identity, and numerical algorithms/settings/values are preserved.
- Targeted unit/type checks, oracle evidence, and measured costs are recorded with limitations and no casts or suppressions.
- A different agent's evidence-backed audit has no unresolved blockers before task 07 begins.
