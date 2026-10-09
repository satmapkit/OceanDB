# Index benchmark runs

`optimize_geo_points_r_dt.py` runs 32 configurations: seven ordered pairs, twelve
triplets, twelve quadruplets, then a baseline. Every configuration keeps the four
single-column GiST indexes on `mission`, `basin_id`, `along_track_point`, and
`date_time`, plus the source's shared constraints. Non-baseline configurations add
one composite GiST index.

The candidate generator excludes date-leading indexes, indexes with `date_time`
but no `along_track_point`, and indexes placing `date_time` before
`along_track_point`. Mission-leading candidates remain included. The exact ordered
columns, CREATE statements, and reasons for exclusions are recorded in the manifest.

## Workload and timing

Each configuration runs one pass through **six scenarios**, each with 31,966 ocean
locations on the 1° grid (`[-60,60)` latitude, `[-180,180)` longitude, filtered by
the packaged basin mask). All use October 15, 2022 ±10 days and all projected
along-track fields:

| Scenario | Query | Missions |
| --- | --- | --- |
| 1 | Radius 50 km | All |
| 2 | Nearest 3, unlimited radius | All |
| 3 | Nearest 3, maximum radius 500 km | All |
| 4 | Radius 50 km | s6a and j3n |
| 5 | Nearest 3, unlimited radius | s6a and j3n |
| 6 | Nearest 3, maximum radius 500 km | s6a and j3n |

All cloning, index construction, and dataset validation finish before timed query
execution begins. Configurations then run sequentially in deterministic permutation
order. `VACUUM ANALYZE along_track` runs before each configuration; inherited parent
statistics must be present. Maintenance and worker initialization are outside the
**900-second combined wall-clock deadline**. The deadline includes all six batches,
their representative `EXPLAIN ANALYZE` replays, and query-phase bookkeeping.

Individual `total_time` values measure the batch, including Python/result processing;
they exclude the subsequent representative EXPLAIN replay. EXPLAIN describes the first
location of each batch, not every location. A successful total sums all six batch
times. A timeout or failure has no comparable total; its completed scenarios and
exception remain available. `query_phase_wall_seconds` records elapsed query-phase
wall time separately.

The isolated worker uses a unique PostgreSQL application name. On exit or timeout,
the supervisor stops the process, cancels/terminates only client sessions matching
that name **and** its trial database, and confirms those sessions are gone. A cleanup
failure interrupts the entire run before another configuration starts. There are no
automatic retries of failed or timed-out configurations.

## Source and preparation

Use the usual `.env` for a **local** PostgreSQL/PostGIS server and the raw along-track
data directory. The runner verifies disk space on the server's reported data directory;
remote servers are currently rejected. The server must support database cloning and
the user must have the required create-database and session-cleanup privileges.

The source must contain exactly the files discovered for October 2022. Discovery is
month-based, even though the query window is October 5–25. Empty sources, missing files,
extra files, metadata/observation-count mismatches, invalid indexes, and unplanned
along-track indexes are rejected. Column types, date partitioning, and the expected
primary/unique constraints must match the packaged schema. Validation scans each file's row count, date range,
and a PostgreSQL row checksum; schema columns are included in the fingerprint.
Checksums detect accidental changes, not adversarial collisions. A resumed trial's
fingerprint must match the saved source. These scans can take time and are outside
query timings. Close other source-database client sessions before cloning.

A missing source is **not ingested implicitly**. To explicitly create and ingest a
missing source, add `--ingest-source`; existing sources are only validated. Failed or
partial ingestion requires inspection/repair before another run and is never accepted
as a prepared dataset. This command can perform ingestion but stops before cloning:

```bash
source .venv/bin/activate
python optimize_geo_points_r_dt.py --prepare notes/my-benchmark --ingest-source
```

For an already prepared source:

```bash
python optimize_geo_points_r_dt.py --prepare notes/my-benchmark
python optimize_geo_points_r_dt.py --resume notes/my-benchmark
```

Or run `python optimize_geo_points_r_dt.py` to prepare and execute a new timestamped
run automatically. `--timeout SECONDS` overrides the default only when preparing a
new run; resume always uses its manifest. Paths supplied to `--prepare` must not exist.

Disk preflight estimates at least 5.2 GiB per trial, or 2.5 times the source size if
larger, plus a 20 GiB reserve. It deducts storage already allocated to pending trials.
A reserve check also runs before each build. This is an estimate, not a guarantee:
index size and WAL depend on the dataset and server settings.

Each trial database has a unique name, an ownership comment containing the run ID,
and a recorded database OID. Resume refuses to modify a database that fails these
ownership checks. It may finish missing indexes in an owned, interrupted build, then
checks actual ordered keys, method, options, predicates, validity, partition indexes,
and the complete inventory. It never drops an unrelated or mismatched database.
Trial databases are retained for inspection and must be removed separately when done.

## Checkpoints and resume

The run directory contains:

- `progress.log`: timestamped phase, configuration, scenario, timeout/failure, and completion events.
- `manifest.json`: source fingerprint, code hashes, environment, exact workload/grid hashes, candidate definitions, and exclusions.
- `results.json`: a version-2 envelope with comparison metadata and every configuration's status/results.
- `trials/<id>/prepared.json` and `analyzed.json`: actual indexes, index/statistics snapshots, and selected planner settings.
- `source-code.zip`: exact benchmark scripts and package source/resources, without `.env` or raw observations.
- `analysis/`: tables, figure, provenance rows, and analysis status.

Checkpoints use fsync followed by an atomic replacement. A filesystem lock prevents
concurrent coordinators for the same run. `--resume` requires matching manifest,
configuration identities, source, workload, code, Python environment, PostgreSQL
version/extensions, and planner settings. Only pending, prepared, running, or interrupted
configurations can execute. Completed/failed/timed-out configurations are retained.
An interrupted attempt's completed scenarios are archived in the checkpoint before its
query phase restarts from scenario 1. A recorded live worker PID prevents concurrent
resume; resolve that process before retrying. A PID reused by another process can also
cause this conservative refusal.

All 32 configurations must be completed, timed out, or failed for a run to be complete;
a successful configuration must contain six scenario results. A fresh run is required
to retry terminal failures or use changed code/settings. The runner automatically
builds reports and a checksummed ZIP next to the run directory, including on a handled
interruption. An abrupt process kill or machine failure requires resume to finish
reporting/packaging. Reporting errors are logged; the raw checkpoint is still packaged.

For a detached macOS run with sleep prevention (after preparation):

```bash
nohup caffeinate -i .venv/bin/python optimize_geo_points_r_dt.py \
  --resume notes/my-benchmark > notes/my-benchmark/console.log 2>&1 < /dev/null &
```

Use the run directory's `progress.log` for progress. The runner enforces deadlines and
produces the final reports without an external monitor.

## Tables, plots, and compatible comparisons

Regenerate an analysis without connecting to PostgreSQL:

```bash
MPLBACKEND=Agg python graph_index_performance.py notes/my-benchmark/results.json \
  --output-dir notes/my-benchmark/analysis
```

`all_configurations.md` includes every configuration, status, partial scenario count,
failure reason, and available completed total. `relative_scenario_improvement.md` and
`mean_performance_size_tradeoff.png` rank successful configurations by the arithmetic
mean of the six per-scenario percentages of time saved against the run's baseline.
They also show geometric-mean speedup and the worst per-scenario improvement. Each
scenario has equal weight; four of the six are nearest-neighbor scenarios. The figure
uses **physical index storage**, not resident memory. Index sizes include attached
partition indexes.

If the baseline fails or times out, the absolute/status table is still generated and
the relative analysis is explicitly marked unavailable. Partial timings never enter
relative scores or successful totals.

Pass multiple result files to combine compatible runs. The reporter checks workload,
scenario identity/order, exact grid/parameters, dataset, shared indexes, code, Python
environment, PostgreSQL/extensions, deadline, and planner settings **before** merging. Each run
uses its own baseline; later successful measurements replace duplicate index definitions.
`relative_rows.json` records the source file for each selected measurement. Older raw
JSON lists can be plotted individually, but cannot be merged without verified metadata.
In particular, the older four-scenario runs cannot be combined with this six-scenario
workload. These checks are deliberately strict: a changed code hash requires a separate
analysis even when a human believes the change is harmless.

This remains a single-pass benchmark. It does not estimate run-to-run variance or
control the operating-system/PostgreSQL caches, concurrent unrelated workloads, or
thermal state. Deterministic ordering and a final baseline can therefore produce cache
or time-order effects. Those limits should be considered when interpreting small
performance differences.

## Focused validation

Unit tests run without a database:

```bash
MPLBACKEND=Agg python -m unittest discover -s tests/benchmark -p test_reliability.py -v
```

Opt-in PostgreSQL tests create and remove a unique `idx_validation_*` scratch database.
They use the configured server/account, never the configured source database. The
account needs CREATE DATABASE and btree_gist extension privileges:

```bash
OCEANDB_BENCHMARK_DB_TESTS=1 MPLBACKEND=Agg \
  python -m unittest discover -s tests/benchmark -p test_reliability.py -v
```

These check actual partition index definitions, a short cumulative deadline including
EXPLAIN, preservation of partial results, scoped cancellation with an unrelated active
session, and successful execution after a timeout. They do not run the full benchmark.

The reusable `run_index_performance_test_with_timeout` keeps its successful list return
value and accepts optional event callbacks, an application name, and a maintenance flag.
`BenchmarkTimeout` remains a `TimeoutError`; it and `BenchmarkFailure` carry completed
rows. Callers must allow `BenchmarkCleanupError` to abort subsequent trials. The separate
`setup_index_performance_test` helper now refuses mismatched existing databases instead
of dropping them automatically.
