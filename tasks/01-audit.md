# Task 01 — independent audit

## Verdict: accept

A fresh read-only reviewer session, distinct from every implementation session,
accepted the actual changes and honestly scoped synthetic evidence. There are
**no unresolved findings**. This accepts task 01's implementation, **not task
02's commencement**: workload/synthetic-only acceptance and the regression
threshold remain explicit user-decision gates.

- Base: `766db427da69f0f682604bd2dfa7e33ae6a9fb95`.
- At audit time, the review head was uncommitted work on that same revision;
  commit authorization came afterward. The commit handoff is recorded in
  [01-evidence.md](01-evidence.md).
- Reviewer session: `ses_e8554be7effeTtZ92PC1xMMkN6`.
- Read the full scoped test diff, original behavior and independent source
  literals, new behavior matrix, complete 1,173-line benchmark driver, benchmark
  README, implementation evidence, and performance baseline.
- Inspected the two full 30,000-row raw reports and supplied smoke; independently
  ran a new small smoke. Full timing runs were inspected, **not rerun** by the
  reviewer.
- No source files were edited or committed by the reviewer. No database suites,
  unrestricted pytest, or make commands ran.

## Finding and verified fix

The first review found one **medium** coverage defect: field-error tests rebound
only the backend's schema alias. An incorrectly eager facade validator against
the shared schema could still pass all 102 tests. The reviewer demonstrated this
with an in-memory mutation, without changing production files.

The fix uses scoped mutation/restoration of the shared schema mapping and adds
explicit empty/nonempty field-timing cases for both facade query kinds. Additional
cases protect deferred facade radius validation separately from eager nearest
caps. The reviewer reran the **identical** eager-field mutation:

- Expected mutation result: exit 1, **6 failed, 106 passed**, including all four
  new empty/nonempty × radius/nearest field cases.
- Fresh normal process afterward: exit 0, **112 passed**.

The finding is resolved, with no casts, suppressions, or runtime changes.

## Independent checks

Executed from the OceanDB root:

```sh
PYTHONPATH=src:. uv run --no-sync pytest -m unit tests/along_track/test_indexed_backend.py tests/along_track/test_backend_contract.py
PYTHONPATH=src:. uv run --no-sync pytest -m unit tests/along_track/test_indexed_backend.py -k 'batch or refresh or provenance or reuse or lifetime'
MYPYPATH=src PYTHONPATH=src:. uv run --no-sync mypy --follow-imports=silent tests/along_track/test_indexed_backend.py tests/along_track/test_backend_contract.py benchmarks/benchmark_indexed_along_track.py
uv run --no-sync black --check tests/along_track/test_indexed_backend.py tests/along_track/test_backend_contract.py benchmarks/benchmark_indexed_along_track.py
uv run --no-sync isort --profile black --check-only tests/along_track/test_indexed_backend.py tests/along_track/test_backend_contract.py benchmarks/benchmark_indexed_along_track.py
PYTHONPATH=src:. uv run --no-sync python benchmarks/benchmark_indexed_along_track.py --help
PYTHONPATH=src:. uv run --no-sync python benchmarks/benchmark_indexed_along_track.py --rows-per-file 30 --files 3 --repetitions 2
```

All normal commands exited 0:

- Full suite: **112 passed**, 3.36 s.
- Focused suite: **57 passed, 35 deselected**, 2.03 s.
- mypy: no issues in three files; Black/isort clean.
- Independent smoke: **36 samples**, complete equivalence passed; external
  evidence `/tmp/opencode/indexed-along-track-f8ybo9dz/results.json`.

Read-only checks also verified:

- Production, fixtures, dependencies, and `AGENTS.md` remain unchanged; the
  pre-existing user query-documentation diff is byte-identical to its capture.
- Driver/raw report hashes and source/schema/workload/basin/code provenance.
- All 36 published wall-time cells and 32 component cells against recomputed
  raw statistics, plus counter/gauge/byte/row-count claims.
- Nested timing containment, residual arithmetic and added clock-read counts.
- Identical-query scientific membership and observed sequence fingerprints
  across phases, five repetitions, and both cache budgets.
- Complete science, masks, metadata/provenance and ordered nearest cutoff are
  independent of the production decoder. Radius order is not newly prescribed.
- Synthetic provenance, same-process restarts, uncontrolled OS caches, observed
  outliers, uncalibrated probe overhead and absent RSS measurements are explicit.

## Remaining limitations and handoff

Negative-half-window and timezone-aware datetime timing are source-verified
matrix notes, not dedicated new timing regressions; the reviewer judged this a
residual limitation, **not a task 01 blocker**. Real-data/PostgreSQL parity and
RSS remain untested; no standalone linter is installed. No speedup or accepted
performance threshold is claimed.

Before task 02, obtain an actual workload or explicit synthetic-only acceptance,
and resolve or explicitly accept the pending regression-threshold gate. See
[01-evidence.md](01-evidence.md) and
[01-performance-baseline.md](01-performance-baseline.md) for reproducible inputs,
measurements, versions, and the final change list.
