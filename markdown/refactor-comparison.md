# Pre-refactor versus current query timing

`compare_refactor.py` runs a focused comparison of the legacy and current batched
radius and nearest-neighbor APIs. It does not change either implementation or use
the general index-sweep report merger: this comparison deliberately changes the
code version while holding the workload and logical index keys constant.

## Workload

- 209 October 2022 source files, 9,519,966 observations, ingested once into a new
  source database using COPY. Two databases are cloned from that source.
- The same 31,966 ocean locations on the 1° grid, in the same order.
- October 15, 2022 ±10 days; all missions.
- Radius: 50 km. NN: nearest three, no radius cap (`max_radius=None` in the new API).
- Four consumed output fields: longitude, latitude, SLA, and time difference.
  Both NN SQL queries also calculate/return the distance needed for ordering.
- Four singleton GiST indexes and BMA in each database, plus identical constraints.
  Legacy M indexes `split_part(file_name, '_'::text, 3)`; current M indexes `mission`.
  Every stored mission must match the filename-derived value.

The old API receives a 20-day time-window argument because it halves that argument;
the new API receives 10 days. The old constructor's configuration file is supplied
in memory, without writing credentials or modifying the legacy checkout. Each worker
runs in a separate interpreter and checks the actual imported module path. Both use
the current virtual environment, isolating code changes from dependency changes.

The basin-mask files must be identical. The current basin connectivity map, including
self-basin fallback for isolated basins, is supplied to both objects before timing.
This is an explicit compatibility adjustment: historical connectivity data and missing
basin behavior are not being benchmarked. Both masks and maps are primed outside timing.
This also avoids the legacy singular `basin_connection` table-name dependency.

## Validation and measurement

The first full pass of each query/version is both validation and warm-up. Returned
rows are normalized only after timing, including legacy NaN sentinels versus current
`None` for empty results. Each location's rows are sorted and compared by count and
SHA-256. Full canonical validation rows, offsets, counts and hashes are saved in NPZ
files. Radius results must match exactly. For every NN discrepancy, an independent,
untimed PostgreSQL query uses `FETCH FIRST 3 ROWS WITH TIES`. Each implementation must
return three eligible observations (or all observations when fewer exist), include
every strictly closer observation, and select any remaining observations only from
the exact distance ties at the third-neighbor cutoff. Full field values and duplicate
counts are verified against the reference rows; matching coordinates alone is insufficient.
The reference SQL, parameters and candidate observations are saved in
`nearest-tie-proofs.json`. Unverified discrepancies stop the run before measured passes.
Neither implementation's timed SQL is changed or given a secondary sort key.

Five measured passes follow, alternating old/new order and query order. The main
metric is wall time from native API invocation through fetching, construction and
access to the four output arrays. Access triggers the current API's lazy SLA scaling.
The same result-retention loop is used for both implementations. Imports, object
initialization, maintenance, correctness sorting/hashing and EXPLAIN are excluded.
A representative EXPLAIN ANALYZE with buffers is saved during validation, outside the
reported batch timer. Debug observers are not enabled during measured API calls.

All index construction and `VACUUM ANALYZE` finish before validation/warm-up. Actual
index definitions, inherited parent statistics, planner settings, dataset fingerprints,
and index sizes are checked and saved. Later passes leave those statistics unchanged.

Each worker gets a 900-second wall deadline after initialization, covering its query
phase and postprocessing; validation workers also include their EXPLAIN replay. The
supervisor stops the process and cancels only sessions matching its unique application
name and database. A cleanup failure aborts the run. Failed/timed-out passes have no
successful timing; remaining passes for that query/version are skipped rather than
retried. A failed validation pass stops the entire comparison.

Report speedup is old median / new median. A speedup requires matching validation and
all five measured passes of each version. The table shows medians, min–max ranges and
percentage time saved separately for radius and NN. These are warm-cache, same-server
measurements, not historical machine performance or cold-cache results. Alternating
order limits drift but does not control thermal state or unrelated workloads.

## Running and outputs

Prepare a new output directory (disk and raw-file preflight only):

```bash
python compare_refactor.py --prepare notes/refactor-comparison-RUN \
  --old-root ../OceanDB-pre-refactor
```

Run its complete ingestion, clone, validation, timing and reporting pipeline:

```bash
python compare_refactor.py --run notes/refactor-comparison-RUN
```

On macOS a detached launch can use:

```bash
nohup caffeinate -i .venv/bin/python -B -u compare_refactor.py \
  --run notes/refactor-comparison-RUN \
  > notes/refactor-comparison-RUN/console.log 2>&1 < /dev/null &
```

`progress.log` has timestamped events. `results.json` is atomically checkpointed.
`summary.html`, `summary.md`, and `summary.json` contain the final comparison, including
unavailable/failure labels. A checksummed ZIP is generated alongside the run directory,
including after a handled failure. The archive includes a source snapshot, pinned
commit IDs, dependency versions, exact index definitions, inputs, validation data,
every pass and diagnostics. Credentials and raw NetCDF files are excluded.

The run uses new uniquely named databases and never ingests into or replaces an
existing database. It checks at least 40 GiB free initially and a 26 GiB reserve before
each clone. Databases are retained for inspection. The source input files, grid, code,
configuration and Python environment must match the prepared manifest. A filesystem
lock prevents concurrent coordinators. This single-run harness does not automatically
resume a partially ingested source or rerun failed passes: inspect the recorded error
and start a new prepared run after resolving it. A hard process kill may require
manual worker cleanup; no new run should overlap an old worker.

After an explicit restart, a new run can reuse the already built databases:

```bash
python compare_refactor.py --prepare notes/refactor-comparison-RESTART \
  --old-root ../OceanDB-pre-refactor \
  --reuse-databases-from notes/refactor-comparison-PREVIOUS
python compare_refactor.py --run notes/refactor-comparison-RESTART
```

This preserves the previous run and ZIP, pins copies of its preparation records,
and creates a fresh schedule of four warm-ups and twenty measured passes. Only the
comparison coordinator and untimed tie-reference SQL may differ from the previous
code snapshot. Database ownership, absence of existing client sessions, dataset
fingerprints, actual index inventories, server settings, basin map and workload are
verified again. Both clones receive `VACUUM ANALYZE` outside timing. No observations
are reingested, and no indexes are rebuilt. The new report records its current code
snapshot and provenance from the previous preparation.

Focused tests use disposable UUID-named databases and do not use the configured source:

```bash
OCEANDB_BENCHMARK_DB_TESTS=1 PYTHONDONTWRITEBYTECODE=1 \
  python -m pytest tests/benchmark/test_refactor_comparison.py -q
```
