# Task 01 — characterization evidence

## Scope and starting state

Implementation base: `766db427da69f0f682604bd2dfa7e33ae6a9fb95`.
Audit review head: the uncommitted working tree on that revision. The audit
completed before the user authorized commits.
Production code, dependencies, database lifecycle, and packaged data are outside
this change.

Starting `git status --short`:

```text
 M markdown/queries.md
?? graph.py
?? graph_along_track.py
?? tasks/
```

These pre-existing changes are preserved. The starting tracked user diff is
stored outside the repository at
`/tmp/opencode/oceandb-task01-starting-user.diff` for the local audit.

Implementation review units followed the proposed task boundaries before commit
authorization:

1. Record the call/iteration matrix and characterize lazy validation, empty and
   partial batches, pinned inventories, and exact-window reuse.
2. Add independent multifile provenance, refresh, and result-lifetime oracles.
3. Add a narrow synthetic phase benchmark and record reproducible evidence.
4. Have a fresh, read-only agent audit the actual scoped changes and evidence.

Runtime ownership is deliberately unchanged. Named lifecycle cases and a shared
matrix replace implicit assumptions with acceptance evidence for later extraction.
The indexed backend's original Git blob is
`701f5839765d0b6cda5f80dc676ee3fd48c32123`; the facade's is
`910c4b56104c777de2cf69e5d15824fb6068e3eb`.

Final change list:

- Extended `tests/along_track/test_indexed_backend.py` and
  `tests/along_track/test_backend_contract.py`.
- Added `benchmarks/benchmark_indexed_along_track.py` and `benchmarks/README.md`.
- Added task 01's behavior matrix, this evidence summary, performance baseline,
  and independent audit report under `tasks/`.
- Locally updated only task 01's status and audit checkboxes in the original
  task file. The pre-existing planning backlog, including that file, remains
  untracked and is excluded from the implementation commits.

## Commit handoff

After independent acceptance, the user authorized focused commits:

1. `03b6946` — characterize lifecycle/provenance and record the behavior matrix.
2. `ca9cd68` — add the synthetic benchmark, instructions, and measured baseline.
3. The following documentation-only commit records this evidence and the audit.

The first two commits combine the test review units into one coherent regression
suite, then separate the benchmark and its measurements. Audit review was of the
equivalent pre-commit working tree; no audited Python content changed during
committing. The full 112-test gate was rerun before committing (exit 0, 3.62 s),
and all benchmark/production hashes still matched the audited raw baseline.
Use `766db427..HEAD` for the resulting committed review range. Existing query
documentation, graph scripts, and the pre-existing planning backlog were not
staged.

## Before-change checks

Executed from the OceanDB root:

```sh
PYTHONPATH=src:. uv run --no-sync pytest -m unit tests/along_track/test_indexed_backend.py tests/along_track/test_backend_contract.py
MYPYPATH=src PYTHONPATH=src:. uv run --no-sync mypy --follow-imports=silent tests/along_track/test_indexed_backend.py tests/along_track/test_backend_contract.py
```

- pytest: exit 0; **49 passed**, 2.20 s.
- mypy: exit 0; no issues in 2 source files. Version 2.3.1; no project checker
  configuration, casts, new exclusions, or suppressions. `--follow-imports=silent`
  checks the named files using local imported types without reporting unrelated
  imported-module diagnostics.
- An initial invocation without `MYPYPATH=src` exited 1 with nine
  `import-untyped` diagnostics because the installed package lacks `py.typed`.
  Correcting source discovery, rather than suppressing diagnostics, resolves
  these on the unchanged baseline.
- Environment: Python 3.14.5, NumPy 2.0.2, netCDF4 1.7.4, GeographicLib 2.1,
  pytest 9.0.3, black 26.5.1, isort 8.0.1, uv 0.13.5. The measurement machine
  is Linux x86_64, AMD Ryzen 5 7640U (6 cores / 12 logical CPUs); frequency boost
  is enabled. Trials are not CPU-isolated, so dispersion is part of the evidence.
- No standalone linter is installed (`ruff` and `flake8` unavailable). No
  dependency installation or upgrade is part of this task.

The validation scope is the two named unit modules, not every test bearing the
`unit` marker. Commands always name the safe files explicitly.

Formatting checks use `isort --profile black` explicitly: default isort wrapping
conflicts with Black-style imports in these modules. This is a targeted command
choice, not a repository-wide formatting configuration change.

## Regression changes and verification

The source-derived [behavior matrix](01-behavior-matrix.md) was written before
test edits. Only the two existing database-free modules were extended; original
literal scientific oracles remain intact.

| Concern | Added or strengthened evidence |
| --- | --- |
| Lazy direct batches | `test_indexed_batches_defer_work_until_first_next`; worker/chunk errors are asserted on the call, shape/field/radius errors on `next()`. |
| Eager facade checks | Contract cases invoke malformed shape, batch limits, nearest caps, parallel requests, and streams without consuming a generator. |
| Empty and partial work | Both query kinds and entry paths; empty field/radius validation remains active; nonmonotonic partial batches close without future preparation. |
| Pinned inventories | An inventory-scoped event log distinguishes one full pass from load/decode/active-key checks. Previously unselected sources are changed/deleted after generator creation, before iteration. |
| Exact-window reuse | Multiple mission selections, repetitions/reordering, `None`, full inventory, and `[]`; different temporal keys rebuild. Projection, center, radius/cap, and query kind vary with independently checked results. |
| Multifile provenance | Twelve nonduplicate rows across two missions interleave in cell order; every metadata/science column, mask, packed/physical value, and signed offset is checked independently. Nearest's ordered three-row cutoff is separate from radius membership. |
| Refresh | New file hidden until refresh; retained bytes reset, geometry and cumulative counters survive, old prepared artifacts remain byte/mtime-identical and yield disk hits. |
| Result lifetime | Both query kinds retain results through window changes, proven file eviction and refresh; read and previously unread scaled columns remain correct. |

Executed after integration (all exit 0):

```sh
PYTHONPATH=src:. uv run --no-sync pytest -m unit tests/along_track/test_indexed_backend.py tests/along_track/test_backend_contract.py
PYTHONPATH=src:. uv run --no-sync pytest -m unit tests/along_track/test_indexed_backend.py -k 'batch or refresh or provenance or reuse or lifetime'
MYPYPATH=src PYTHONPATH=src:. uv run --no-sync mypy --follow-imports=silent tests/along_track/test_indexed_backend.py tests/along_track/test_backend_contract.py benchmarks/benchmark_indexed_along_track.py
uv run --no-sync black --check tests/along_track/test_indexed_backend.py tests/along_track/test_backend_contract.py benchmarks/benchmark_indexed_along_track.py
uv run --no-sync isort --profile black --check-only tests/along_track/test_indexed_backend.py tests/along_track/test_backend_contract.py benchmarks/benchmark_indexed_along_track.py
git diff --check
git diff --exit-code -- src/OceanDB pyproject.toml uv.lock requirements.txt tests/data AGENTS.md
```

- Full targeted suite: **112 passed**, 3.49 s (baseline 49; 63 additional cases).
- Focused indexed suite: **57 passed, 35 deselected**, 2.06 s.
- mypy: no issues in 3 source files; Black/isort: clean.
- No production, dependency, source-asset, or database-lifecycle diff. The
  pre-existing `markdown/queries.md` diff was compared byte-for-byte with the
  captured starting diff and is unchanged.

## Completion gates

Regression implementation passes and a reproducible
[synthetic baseline](01-performance-baseline.md) exists (180 final instrumented
phase samples, plus a 36-sample CLI smoke). Independent review has accepted it.
Its initial test audit found one medium coverage defect: rebinding the backend's
schema did not expose a wrongly eager facade validator. The fix mutates the
shared schema with scoped restoration and explicitly tests empty/nonempty
facade field timing for both query kinds; additional facade radius timing cases
also distinguish deferred radius checks from eager nearest caps. The final
[independent audit accepted](01-audit.md) the implementation and evidence with
no unresolved findings. It verified the fix by rerunning the same eager-field
mutation: the intended regression now causes six failures, while a fresh normal
process passes all 112 tests. It also independently checked all published timing
and component cells against the raw evidence and ran a new 36-sample smoke.

A real user query workload has not been supplied: benchmark evidence is
explicitly **synthetic**. No regression threshold has been agreed with the user;
measured variability is reported before that decision. No refactor speedup is
claimed.

Task 02 remains gated on an actual workload or explicit synthetic-only
acceptance, and resolution or explicit acceptance of the pending regression
threshold. Acceptance of this implementation is not authorization to start
extraction.

## Evidence limits

- Existing literal-oracle tests remain the independent acceptance surface for
  inclusive microsecond/radius boundaries, optional radius distance, mandatory
  nearest distance, nearest tie ranking, stored coordinate precision,
  packed scaling/missing masks, signed time differences, self-only land, and
  duplicate rejection. New lifecycle tests supplement rather than replace them.
- Negative-half-window and timezone-aware datetime timing are source-verified
  matrix notes, not dedicated new timing regressions. The independent reviewer
  accepted this residual coverage limitation for task 01.
- The literal six-row scientific reference is a correctness oracle, not a
  realistic throughput workload. The scalable benchmark is also synthetic and
  cannot establish real-archive or PostgreSQL parity.
- No unrestricted pytest invocation, database fixture, `make test`, or
  `make check` is authorized or used. Integration and real-source results remain
  untested.
- Retained `nbytes` counters exclude temporary allocations, mappings held by
  returned results, Python objects, and operating-system caches. They are not
  RSS measurements or disk quotas. No memory reduction is claimed.
- A fresh backend reusing disk artifacts is not a fresh Python process. A new
  artifact directory is not a cold operating-system page cache. No system cache
  flushing or removal of existing user caches is permitted.
