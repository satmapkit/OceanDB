# Benchmark strategy and execution

The benchmark measures native OceanDB retrieval and materialization under a
specified starting cache state. It is a methodology proof of concept, not an
automatic index winner selector. AD is the practical reference; a physical AD
clone supplies A/A controls. A/A measures empirical resolution rather than
passing a predetermined tolerance.

## Install and prepare

Install OceanDB (`pip install .`), a C compiler, local PostgreSQL 18+ and PostGIS,
including `btree_gist` and the `pg_buffercache` extension files. Run beside the
server as the OS owner of its data directory, using a PostgreSQL superuser for
scoped buffer eviction. Only local macOS/Linux and the default tablespace are
supported. Remote servers and container/host filesystem mismatches are rejected.
The rest of OceanDB does not acquire this benchmark-specific PostgreSQL requirement.

The source must already contain populated OceanDB along-track data and metadata.
The benchmark does not download or ingest production data. Set `POSTGRES_HOST`,
`POSTGRES_PORT`, `POSTGRES_USERNAME`, and `POSTGRES_PASSWORD` through the existing
environment/settings mechanism. Keep credentials out of profiles and reports.

Copy the packaged `profiles/core.yaml` or `profiles/exhaustive.yaml`, for example:

```bash
python -c "from importlib.resources import files; print(files('OceanDB.benchmark').joinpath('profiles/core.yaml').read_text())" > /tmp/core.yaml
# Edit source_database, dates and any explicit workload parameters.
oceandb benchmark check /tmp/core.yaml
oceandb benchmark prepare /tmp/core.yaml --output /absolute/external/runs/run-001
oceandb benchmark run /absolute/external/runs/run-001
```

`check` performs read-only prerequisite and inventory checks; it does not measure
workloads. It prints the source-inventory hash and a conservative space estimate.
Preparation clones an owned template, normalizes only the clone's along-track
indexes, and performs VACUUM ANALYZE. Candidate copies share template data,
common indexes and statistics; AD-B is a physical copy of prepared AD-A.
All builds and verification finish before timing. Source databases are never
altered, sessions are never killed to obtain a quiet server, and existing run
names/directories are refused. Database storage and build space are disk costs,
not estimates of memory use. The default free-space reserve is 60 GiB.

The commands run in the foreground and append progress to `progress.log`.
For long local runs, use your machine's sleep inhibitor (for example macOS
`caffeinate -i oceandb benchmark run ...`). Follow progress and status files;
do not launch a second runner or diagnostic workload during timing.

## Key tests

| Workflow | Purpose | Default requests |
| --- | --- | --- |
| Daily traversal | Advancing dates with reuse inherited from earlier days | October 26–November 6; one 512-point batch per date |
| Isolated interior | Independent start away from a month boundary | October 15; one 512-point batch |
| Isolated boundary | Independent start spanning monthly partitions | November 1; one 512-point batch |
| Entire-record local sweep | Repeated overlapping circles suitable for downstream mean-field construction | 512 centers; sixteen sequential 32-point batches |

The first three use a ±5-day window (10 total days), radius 50 km or unbounded
nearest 3, and either all missions or s6a+j3n. They materialize longitude,
latitude, sla_filtered and delta_t. Points are a seeded sample of the packaged
ocean mask on a one-degree grid, sorted in original grid traversal order, with
no forced historical prefix. The profile fixes dates explicitly; changing the
dataset never silently moves them.

The entire-record test uses `AlongTrack.geographic_point_in_r_batch()` with
50 km radius, nominal 25 km spacing, and a 16×32 serpentine patch centered at
30°N, 40°W. Spherical projections establish nominal spacing; PostGIS geography
retains its radius semantics. Land-containing patches are rejected, not thinned.
All/filtered mission cases materialize longitude, latitude, sla_filtered and
integer-microsecond date_time fingerprints. The SQL has the existing connected-
basin and radius predicates, optionally mission filtering, **no date predicate**,
no LIMIT and no SQL AVG. Entire record means all stored observations, including
spillover and missing-source coverage. Scientific mean estimation is downstream.

The count is fixed before candidate measurements. Changing sample, radius,
spacing, chunk size or coverage creates a different workload. On a larger dataset,
perform any sizing separately, then freeze one common count for all candidates;
never give slow configurations fewer queries. Canonical payload bytes exclude
client overhead and are not peak memory measurements.

## Core and exhaustive index profiles

A=along_track_point, B=basin_id, D=date_time, M=mission; letter order is index key
order. Common singleton GiST indexes and table constraints remain in every copy.
Singleton-only timing is excluded.

The **core** profile uses AD, AMD, BAD, ABD, BMA, BMAD and MBA, plus AD-B. It has
14 workflow/scenario groups, 196 independently reset episodes and 1,232 batches.

The **exhaustive** profile enumerates ordered pairs, triplets and quadruplets of
A/B/D/M, omitting date-leading indexes and indexes with D before A when both occur.
It retains eligible mission-leading and nonspatial indexes: 37 composites plus
the duplicate AD. The same tests and measurement strategy apply; this does not
restore warmed-repeat screening, rebuild experiments or finalist confirmation.

### Explicit exclusion policy

Reference policy is packaged in `profiles/exclusions.yaml`, with reasons and
historical experiment identifiers but no measured timings:

| Index | Excluded reference case |
| --- | --- |
| AM, MA | Isolated November 1, radius/all missions |
| BM, MB, MD, MDB | Isolated November 1, both radius scenarios |
| MDB | Isolated October 15, both radius scenarios |
| BMA, MBA | Whole October 26–November 6 traversal, both NN scenarios |

Other cases, including entire-record radius, remain eligible. An interrupted
historical daily episode motivates omission of the whole episode, not a made-up
partial total. These are practical selection rules from historical evidence,
not claims of universally slow indexes on other workloads or machines.

First run `check` with `reference_exclusions: false`. Review source coverage,
then set `exclusion_dataset_sha256` to its returned inventory hash and enable
`reference_exclusions: true`. This explicit binding is required: the code cannot
infer that a different dataset deserves the historical policy. Custom `exclusions`
use configurations, workflow, exact dates, scenarios, reason and evidence fields.
Unmatched rules and exclusions of AD controls are errors.

The reference exhaustive schedule selects **600 episodes / 3,812 batches**, with
16 excluded episodes explicitly listed. Without policy it schedules 616 / 3,872.
Exclusions are neither timed-out requests nor measured values. Policy is frozen
before database preparation and shown in the report. No runtime ranking changes
which cases are scheduled.

## Timing, cache state and controls

Each configuration gets one exploratory traversal per case. Four adjacent A/A
pairs, including the exploratory pair, use two AB and two BA orders. Seeded block
order spreads configurations across the schedule. A/A reports actual count,
median/range/CV, order and acquisition segments. Four pairs give coarse empirical
resolution, not precise confidence intervals. Candidate n=1 cannot estimate
candidate variability or establish reproducible winners.

Client initialization occurs before the episode cache reset. Native connection,
query execution and four-field materialization are timed; setup, cache preparation
and fingerprints are excluded. Every daily/chunk batch opens a fresh connection
and uses automatic plans. There are no warmups or conditioning requests.

Before an episode, the runner requires a quiet server, verifies the owned target,
evicts only inventoried public/TOAST PostgreSQL buffers and relation-file pages,
and independently checks zero target buffers and zero resident file pages.
macOS uses read-only mmap/msync; Linux uses fsync/posix_fadvise followed by mincore.
An advisory eviction success alone does not count. A setup canary proves populated
target caches clear twice while unrelated database buffers survive. Every attempt,
wait, occupancy snapshot, load and memory observation is saved.

Transient clients/autovacuum, pinned buffers or remaining pages cause untimed
waiting and another preparation attempt. Ownership/file identity, changed data
inventory/statistics or I/O failures are fatal. No global purge or server restart
is used. Catalogs, process/library state, other databases, device caches, memory
pressure and index-build histories remain. This is a controlled target-cache
start, not a whole-machine cold boot.

No cache probes, controls or resets occur between chronological dates or spatial
chunks. Later requests inherit earlier activity. The first 32-point chunk already
includes reuse among neighboring queries; it is not 32 independently cold queries.

## Failures, correctness and reporting

Every batch has a 30-second worker/SQL cap. Timeout censors the batch and skips
remaining dates/chunks in that episode; other episodes continue. Worker errors
or output mismatches abort. Start markers are written durably before execution;
a started or ambiguously started run cannot be resumed or silently retried.
Inspect saved failure/status files. Fix infrastructure outside timing and use a
new independently identified run; never overwrite or splice replacement timings.
Cancellation is restricted to the owned worker's database/application identity.

Save canonical arrays and per-center fingerprints outside timing. Radius outputs
must match exactly. NN different values are qualified only if selected coordinates
and row counts agree; other differences fail. Validate window bounds and saved
arrays again before reporting. Only complete sequences have totals.

After all timing, warm automatic-plan prefix replays have 350 ms statements and
a 180-second total budget. Save actual prefix completeness. These are diagnostic
samples, not plans captured during timed cache starts; diagnostic timeouts never
change benchmark outcomes or trigger replay of timed work.

`run` produces an external sibling report directory and ZIP. Rebuild without any
database connection:

```bash
oceandb benchmark report /absolute/external/runs/run-001 --output /absolute/external/reports/rebuilt-001
```

The self-contained HTML has colored scenario cells, date/chunk and total views,
A/A first, unpaired AD ratios and explicit exclusions. JSON, CSV, validation,
cache evidence, plans, package/input hashes and the source run accompany it.
Archives are checked by CRC and SHA256. A compatibility reader accepts the current
combined historical report as data, never executing its archived Python.

All future outputs must be outside Git checkouts, even ignored subdirectories.
Existing ignored historical artifacts stay unchanged. Git contains code, profiles,
documentation and synthetic tests only—no measured timings, plots, reports or dumps.
The HTML can be shared directly; publishing results is separate from this code commit.

Timeout and exclusion biases, limited regional/temporal coverage, small samples,
and acquisition history limit interpretation. This protocol does not establish
full-grid, multiyear, concurrent production, independent rebuild effects, or the
impact of 24 hours of intervening activity.

## Developer verification

Run `pytest tests/benchmark -m unit`. The `benchmark_database` tests are opt-in:
set `OCEANDB_BENCHMARK_SMOKE=1` against a **disposable local PostgreSQL 18 cluster**
using the normal POSTGRES_* environment. They create synthetic sources and owned
clones, verify cache canaries and cancellation isolation, execute small native
cases, then drop only their verified disposable databases. Do not run the generic
production-database fixtures against a benchmark source. Linux CI provisions its
own native cluster; unavailable platform checks must be reported, not assumed.
