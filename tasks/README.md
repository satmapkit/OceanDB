# Indexed along-track backend cleanup tasks

## Status and scope

This is an implementation and review backlog, not a record of completed work.
The task files expand the cleanup plan for
`src/OceanDB/data_access/indexed_along_track.py`. Creating these documents does
not change the backend, run its tests, or authorize implementing every task.
Commit subjects below and in individual tasks are proposals, not instructions
to create commits without authorization.

Steps 01–08 are a behavior-preserving cleanup. Step 09 is a separately gated,
opt-in persistence feature. Complete and audit the cleanup before deciding
which additional index artifacts are worth persisting.

All source paths and commands in this series are relative to the **OceanDB
repository root**, not its parent workspace. File names, helper names, and new
test paths described as proposed are design targets, not existing APIs.
The inspected source baseline is commit `766db42`; recheck the current code
before executing a task if the branch has moved. Later audit ranges must use
the actual implementation base and head, not blindly reuse this planning base.

## Task sequence

| Step | Task | Prerequisite | Main acceptance boundary |
| --- | --- | --- | --- |
| 01 | [Characterize behavior and performance](01-characterize-behavior-and-performance.md) | None | Regression tests and a reproducible, honestly scoped baseline exist before refactoring. |
| 02 | [Extract file management](02-extract-file-management.md) | 01 | Source/catalog/cache ownership moves without changing cache identities or source-freshness rules. |
| 03 | [Clarify NetCDF decoding](03-clarify-netcdf-decoding.md) | 02 | Named decoding operations preserve packed values, masks, timestamps, and rejection behavior. |
| 04 | [Introduce a prepared window](04-introduce-prepared-window.md) | 03 | One complete read-only snapshot owns aligned index arrays and provenance. |
| 05 | [Extract query geometry](05-extract-query-geometry.md) | 04 | Cached and direct geometry share one path and remain independent of observation windows. |
| 06 | [Refactor radius selection](06-refactor-radius-selection.md) | 05 | The candidate pipeline and distance-refinement policy are explicit and independently testable. |
| 07 | [Refactor nearest selection](07-refactor-nearest-selection.md) | 06 | Cap expansion, stopping, refinement, and deterministic ranking have clear responsibilities. |
| 08 | [Finish the backend adapter](08-finish-backend-adapter.md) | 07 | The adapter only coordinates the protocol, lazy batches, preparation, statistics, and schema projection. |
| 09 | [Persist index snapshots](09-persist-index-snapshots.md) | 08 and a design decision | A selected artifact has safe, versioned, opt-in persistence with measured benefit. |

The serial sequence avoids several agents changing the same backend methods at
once. Independent test design and review may be delegated, but implementation
ownership and integration responsibility must remain explicit.

## Target architecture and ownership

| Module | Owns | Must not own |
| --- | --- | --- |
| `indexed_along_track.py` | The existing public backend import, protocol methods, lazy orchestration, preparation delegation, schema-backed projection, and a unified statistics view. | NetCDF decoding, catalog persistence, cell traversal, or geodesic formulas. |
| `indexed_files.py` (proposed) | Source records and inventory, coverage, source checks, decoding, prepared-file disk artifacts, and the retained-file LRU. | Radius/nearest selection, `Dataset` construction, or imports back into the adapter. |
| `indexed_spatial.py` (proposed) | Window assembly/index building from supplied decoded arrays, query geometry, candidate traversal, and nearest search. | Opening source files, publishing disk artifacts, constructing `Dataset`, or SQL access. |
| `indexed_geometry.py` | Pure cell, vector, conservative-cover, and geodesic numerical operations. | Source inventories, mutable backend state, schema projection, or cache policy. |

Use small value records where they remove implicit assumptions:

- `WindowKey`: exact temporal parameters and canonical resolved missions.
- `PreparedWindow`: aligned rows, offsets, vectors, ordered source provenance,
  and identity, installed as a complete state rather than separate attributes.
- `QueryGeometry`: paired center information, basin, cover, and vector.
- `Selection`: row indices and their aligned distances.

Place records near their owners. A narrowly scoped shared value-type module is
acceptable only if an actual dependency cycle requires it; a generic repository,
cache framework, plugin system, or compatibility layer is not the goal. In
particular, lower layers must not import the adapter to obtain statistics or
types. Source metadata can be passed as immutable values rather than exposing
file-manager internals to traversal.

Complete-state publication does not require retaining the previous large window
throughout a rebuild. Preserve the current release-before-rebuild lifecycle for
a different key: clear the active snapshot, build the replacement in locals, and
install only a validated result. Keeping the last good window during construction
is a separate memory-lifetime decision, not an automatic consequence of using an
immutable record. Measure any such change and account for both retained snapshots.
Check generator frames as well as owner attributes: a previous loop-local window
can stay alive while the right-hand side acquires the next one. A small
per-center execution helper, or explicit release of consumed local references,
can make the existing lifetime obvious without adding a new abstraction.

Do not collect every full decoded file just to feed a pure assembler. Keep the
existing load/filter/copy-per-source lifetime: a pure per-source temporal chunk
operation may run immediately after loading, and the final pure assembler can
consume those compact chunks. Loading stays outside spatial code, while evicted
full-file mappings do not remain accidentally pinned by a new input list.

## Shared behavior contract

Every task must preserve the applicable parts of this list, not just the tests
that happen to exercise its local helper.

### Scientific results

- Preserve all four `AlongTrackBackend` query signatures/defaults and ordinary
  `Dataset | None` results. Nearest returns at most three observations.
- Time bounds are inclusive and use exact UTC-naive microseconds. Spatial
  membership and nearest caps are inclusive WGS84 comparisons.
- Nearest order is `(distance, time_us, latitude, stored longitude)`. Only equal
  computed distances use secondary keys; do not invent tolerance-based ties.
- Keep decoded coordinate precision and stored longitude unchanged. Wrapping
  for basin/cell calculations does not authorize rewriting source coordinates.
- Basin eligibility uses packaged direct connections plus self, not transitive
  closure or an ocean-only rule. Unlisted raster IDs, including land, are
  self-only. The basin-raster profile is `[-90, 90)`; expanding pole support is
  not part of the cleanup.
- Preserve packed science values, missing masks, and row/metadata alignment.
  Schema-backed `Dataset` access applies scaling exactly once. `delta_t` remains
  signed center time minus observation time.
- Radius `distance` is optional; nearest `distance` is mandatory. A matched
  radius query with `fields=[]` retains the legacy zero-column `Dataset`, not
  `None`.
- Within-file duplicate checks cover all decoded rows. Cross-file duplicate
  checks cover the assembled window. Neither is a whole-archive integrity scan.

### Execution and lifecycle

- Batches are ordered and lazy, may contain different dates, and do not eagerly
  prepare later windows. Preserve the existing distinction between facade,
  direct-backend, and generator-time validation.
- Validate the complete pinned inventory once before the first center of a
  nonempty batch. Empty batches do not touch sources or build a window. Existing
  selected-source rechecks must not silently disappear during a move-only task.
- Changed or deleted pinned files fail closed, including previously unselected
  files whose coverage could have changed. New files require explicit refresh.
  Source edits during a running batch are unsupported.
- Refresh clears retained file mappings and the active window, but retains
  source-independent query geometry and cumulative counters.
- Retain only the current active window unless a later feature is approved.
  Adjacent windows rebuild the spatial index while reusing decoded files;
  incremental/sliding-window indexing is not implied.
- Preserve existing prepared-file identities and version-2 artifacts during the
  cleanup. Metadata fingerprints are size/mtime based, not full content hashes;
  relocation and same-size/same-mtime replacements remain outside that contract.
- File LRU retention and geometry admission are distinct policies. Array-byte
  counters are not RSS, temporary-allocation measurements, or disk quotas.
- No PostgreSQL fallback, parallel batch support, completion-order streaming, or
  new thread-safety claim is introduced.

### Numerical policy

Cells and spherical chord distances only conservatively prune candidates.
Vectorized Vincenty evaluates the broad set and uses GeographicLib for
nonconvergence. Radius decisions refine distances within 1 cm of the boundary;
nearest refines at least the provisional top three and nearby competitors.
The refinement margin is not a membership epsilon. Keep pruning padding,
convergence criteria, stopping margins, and final comparisons distinct.

Do not claim a certified global error bound or measured speed advantage from
the existing synthetic cases. Shape-contract discrepancies in geometry helpers,
very large nearest-cap optimizations, new pole behavior, or algorithm/library
replacement require separately scoped fixes with tests, not hidden refactoring.

## Common validation protocol

Read both applicable `AGENTS.md` files before implementation. Use `uv` for all
Python work, precise annotations, no `typing.cast()` or equivalents, and
Sphinx/reStructuredText fields in new or modified docstrings.

With an already prepared environment, run the existing database-free baseline:

```sh
uv run --no-sync pytest -m unit tests/along_track/test_indexed_backend.py tests/along_track/test_backend_contract.py
```

Add each task's proposed focused tests to that invocation as they become real
files. Record commands, versions, exit status, and test counts. Do not treat a
missing dependency or unexecuted command as a passing check. If environment
setup is needed, arrange it through `uv` without unrelated dependency upgrades.

Run formatting and lint checks only on changed Python files. Examples, with
`PATHS` replaced by the actual changed paths, are
`uv run --no-sync black --check PATHS`,
`uv run --no-sync isort --check-only PATHS`, and the available linter.
Do not use `make check` as a harmless verifier: it formats the wider tree.

There is no configured project type checker at this planning baseline. Select
and record a targeted checker before the first extraction; for example, after
approval, `uv run --no-sync --with pyright pyright PATHS`. Record its version,
paths, configuration, and pre-existing errors. Fix errors in touched code rather
than introducing casts, blanket exclusions, or suppressions to conceal them.

Database-free tests are the default gate. Integration tests such as
`tests/along_track/test_source_reference.py` and
`tests/along_track/test_ellipsoidal_queries.py` require an explicitly disposable
test database. Inspect `tests/database/fixtures.py` and confirm the target
configuration before running them: teardown can **drop the configured database**.
Do not run the entire suite, `make test`, or a database marker against an
unverified local configuration.

Performance evidence must distinguish cold catalog/source work, restarted
prepared-file hits, warm exact-window reuse, adjacent-window rebuilding, and
prepared/direct query geometry. Record source/query provenance, variability,
complete scientific equivalence, RSS, and disk footprint where relevant.
Retained-array counters alone do not establish a memory improvement. The
existing `benchmark_runtime.py` is not an indexed-backend benchmark.
Use separate temporary artifact directories for cold-cache scenarios; do not
delete real downloads or flush the host's operating-system caches. Describe
whether a run is artifact-cold, process-cold, or actually storage-cold.

## Implementation handoff

Treat the proposed commits as small review units: each must leave the code
functional, include tests relevant to its change, and pass its applicable gates.
Do not defer all tests to a final catch-up commit or mix standalone cleanup of
unrelated files into an extraction. Obtain a scope decision before changing a
scientific rule, capability, dependency, or persistence default.

For each task, the implementation agent should provide:

1. The task file and an explicit change list, base revision, and review head.
2. The chosen commit/review-unit sequence and any justified departures.
3. A before/after ownership explanation: which implicit assumption or mixed
   responsibility was actually removed, not merely which file became shorter.
4. New regression cases, exact validation results, baseline comparisons, and
   limitations. Keep generated runs under an ignored directory or
   `/tmp/opencode`, not committed alongside the task documents.
5. An implementation-to-audit handoff. Do not mark a task complete solely because
   its author says the tests passed.

The initial worktree contains unrelated edits to `markdown/queries.md` and
untracked `graph.py` / `graph_along_track.py`. Preserve them; never reset, clean,
or stage them as a side effect of these tasks. Recheck status at execution time
rather than assuming this inventory remains exhaustive.

## Independent-agent audit protocol

Use a **different agent in a fresh review session**, not the implementation
agent reviewing its own work. A different model is optional; independent context
and an independent examination of the evidence are required. Each task provides
a copyable task-specific prompt to combine with this protocol.

Give the reviewer the task, repository root, `BASE_REF` and `HEAD_REF`, expected
scope, test commands/results, and any benchmark inputs. The implementer's summary
is a claim to verify, not the review's source of truth. Do not ask the reviewer
to patch or refactor while auditing.

For committed changes, set `BASE_REF` and `HEAD_REF` to the supplied revisions:

```sh
git status --short
git log --oneline "${BASE_REF}..${HEAD_REF}"
git diff --name-status "$BASE_REF" "$HEAD_REF"
git diff --check "$BASE_REF" "$HEAD_REF"
git diff "$BASE_REF" "$HEAD_REF"
```

For uncommitted work, supply explicit paths and the starting diff. Review the
tracked diff and read new files separately: `git diff` omits untracked files.
Do not manufacture a clean worktree by discarding changes. Protect credentials
and avoid copying `.env` contents or private dataset paths into reports.

The reviewer must:

1. Read the original behavior, task requirements, full scoped diff, affected
   callers, and independent fixture expectations. Check for changes outside the
   declared scope and for dependency cycles or compatibility scaffolding.
2. Trace a nonempty result, an empty result, a failure, and reuse/refresh through
   the changed boundary. For batch work, also trace construction, first `next()`,
   partial consumption, mixed dates, and generator close.
3. Challenge the tests: could a wrong row association, stale source, missed
   boundary, changed tie, or extra future-window build still pass? Independently
   run applicable safe checks when feasible; distinguish reproduction from
   merely reading the author's logs.
4. Review types, array alignment/read-only guarantees, source provenance,
   numerical policy, cache identity, memory accounting, and documentation as
   applicable. Assess whether the code is actually easier to follow.
5. Return findings by severity with `path:symbol`, a concrete failure scenario,
   evidence, and a suggested focused remedy. State exactly which commands were
   run, what was not run, and whether performance claims are substantiated.
6. Give one verdict: **accept**, **changes required**, or **blocked by missing
   evidence**. List remaining risks even when accepting. Send fixes back to the
   implementation agent, then review the resulting delta before closing.

## Completion boundaries

Cleanup is complete only after steps 01–08 have implementation evidence,
independent audits, no unexplained scientific or performance regressions, and
consistent user-facing documentation. Synthetic success is not real-data
PostgreSQL parity, whole-archive validation, or a certified numerical proof.

Persistence requires its own decision and acceptance evidence. Making arrays
serializable does not by itself justify automatic daily-window caching, an
unbounded disk cache, or changing the default backend behavior.
