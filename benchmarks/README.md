# Indexed along-track synthetic baseline

`benchmark_indexed_along_track.py` is a narrow, database-free task 01 driver.
**No actual user workload was supplied: all evidence is synthetic.** It is not
the PostgreSQL runner, a real-data parity check, a numerical proof, or a claim
about realistic throughput. It changes no production code or dependencies.

From the OceanDB repository root, using the existing environment:

```sh
PYTHONPATH=src:. uv run --no-sync python benchmarks/benchmark_indexed_along_track.py --help
PYTHONPATH=src:. uv run --no-sync python benchmarks/benchmark_indexed_along_track.py --rows-per-file 30 --files 3 --repetitions 2
PYTHONPATH=src:. uv run --no-sync python benchmarks/benchmark_indexed_along_track.py
PYTHONPATH=src:. uv run --no-sync python benchmarks/benchmark_indexed_along_track.py --max-cache-bytes 1000000
```

The default generates six 5,000-row sources and takes five samples per query
kind. The small invocation is also the CLI smoke check: it exercises every
phase and asserts independent equivalence and expected cache transitions.
The two baseline commands (default and `--max-cache-bytes 1000000`) each take
five repetitions; the second keeps geometry's default budget but forces decoded
file-array eviction at the default fixture scale. Run both again after changing
the probes; old uninstrumented results remain evidence, not a directly comparable
instrumented timing baseline.
Use normal Python execution, **not `python -O`**, because checks use assertions.
No existing tests or database lifecycle commands are invoked.

Every invocation creates a unique `/tmp/opencode/indexed-along-track-*` directory
and prints its location. Sources, each repetition/kind's private cache, and
`results.json` remain there for review. Nothing is generated in this repository;
the driver never reads real downloads, removes caches, or flushes OS caches.
Keep the printed JSON outside version control; sanitize evidence before sharing
(it contains local invocation/repository paths and Git status filenames, not
environment variables or `.env` contents).

## Fixed workload and scientific checks

The recorded seed drives NumPy PCG64 generation of multi-mission NetCDF3 files
(`j2`, `j3`, `s3a`, cycling as needed). Each file covers eight days, has distinct
microsecond timestamps, packed microdegree coordinates near `(0, 180)`, int16
science with both missing sentinels, scalar track/cycle, and alternating valid
or missing scalar correction. Files/rows scale independently. The exact driver,
production code, source bytes, basin assets, schema, and workload are hashed.

Both radius and nearest workloads use all fields and four paired centers,
including one no-match center. Nearest deliberately omits `distance` from the
request and checks its mandatory inclusion. Radius and nearest cap are 300 km;
the temporal half-window is one day. Dates, ordered adjacent centers, fields,
missions, seed, cell size, batch parameters, repetitions, and independent file
and geometry budgets are included in the JSON. Use `--help` for budget/scale
options; different options produce different workload hashes.

Expected values come from generator-owned definitions, **not the production
NetCDF decoder or another run of the same backend**. The oracle independently
reads the packaged basin raster and direct-plus-self connection table and
exhaustively computes GeographicLib WGS84 distances outside timing. It checks:

- Exact radius membership by `(date_time, latitude, stored longitude)`, separately
  recording actual sequence hashes without inventing a radius ordering contract.
- Nearest's ordered three-row cutoff by `(distance, time, latitude, longitude)`.
- Every column, filename/mission provenance, result name/count, projected column
  order and dtypes, complete returned schema metadata, signed `delta_t`, exact
  coordinates/timestamps, packed/scaled science, and both missing-value masks.
- Independent literal schema expectations for field names/types, scale and
  offset. Full SQL/source metadata is also snapshotted and hashed for comparison.

Distance values allow 0.1 mm absolute difference against GeographicLib (the
backend's broad radius path uses Vincenty); science allows `1e-12` physical
absolute difference and zero packed-value tolerance. Membership and nearest
ordered keys are exact, not tolerance-based ties. This generated workload is not
a substitute for the separate boundary/tie/lifecycle regression tests.

## Phases and interpretation

Each repetition and query kind starts with a distinct empty artifact cache:

| Phase | What the timer includes |
| --- | --- |
| `artifact_cold_constructor_catalog` | New backend and source coverage/catalog work. |
| `artifact_cold_first_decode_window_query` | First fully consumed batch: source decoding, artifact publication, window/index preparation and queries. |
| `warm_identical_window` | Same batch, exact window reused; no new decoding or window builds. |
| `ordered_adjacent_windows` | Three overlapping, chronological windows in one consumed batch; window rebuilds and file reuse/eviction. |
| `restarted_backend_same_process_constructor` | Old backend released; new backend reuses the same catalog, **in the same interpreter process**. |
| `restarted_backend_same_process_prepared_file_query` | New window reconstructed from prepared-file disk hits, with zero source decodes. |
| `direct_geometry_same_window` | Repeated fixed batch before preparing geometry; exact window reused. |
| `grid_geometry_preparation_only` | `prepare_grid` alone, separate from selection latency. Nearest prepares both possible 150/300 km search covers. |
| `prepared_geometry_same_window` | Same batch after grid preparation; geometry hits/bytes reveal budget admission. |

Query wall times include serial batch consumption, validation and result
materialization, **not equivalence checking or lazy scaled-column access**.
The original public `preparation_seconds` delta remains recorded separately:
it combines decode/cache I/O, filter/copy, and window/index assembly on builds,
but excludes exact-key hits. It is not a substitute for the boundary probes below.

### Benchmark-only serial timing probes

A local subclass delegates once to each original method, preserving its arguments,
return object and exception behavior. It wraps only `_prepare_file`, `_load_file`,
and `prepare_window`; production files, algorithms, public statistics and cache
policy are unchanged. It deliberately references the private `_Source` record.
After extraction, relocate the probes/annotation to the **equivalent boundaries**
rather than adding compatibility machinery to production.

Each phase records cumulative `probes_before`, `probes_after`, and `probes_delta`
separately from the original `stats_*`. The probe snapshots include method-call
counts, clock-read counts, and these inclusive times:

| Metric in `probe_timing_seconds` | Meaning |
| --- | --- |
| `decode_seconds` | Entire `_prepare_file`: source open/read, CF/packed decoding, basin labeling and validation, including its final source check; not artifact publication or catalog coverage decoding. |
| `file_load_inclusive_seconds` | Entire `_load_file`, including any nested decode, source/cache checks, prepared-file publication/mapping and retention/LRU work. |
| `prepare_window_inclusive_seconds` | Entire `prepare_window`, including nested loads, key normalization, assembly and **every exact-key hit with its source-stat safety checks**. |
| `file_load_excluding_decode_seconds` | Derived `file_load_inclusive_seconds - decode_seconds`: cache/artifact/retention work plus inner probe overhead, not pure disk I/O. |
| `window_excluding_file_load_seconds` | Derived `prepare_window_inclusive_seconds - file_load_inclusive_seconds`: orchestration, filtering/copying, duplicate checks/index assembly, and exact-hit checks; **not isolated pure spatial assembly**. |
| `query_wall_excluding_window_seconds` | Query phases only: consumed-batch `wall_seconds - prepare_window_inclusive_seconds`; overhead including list/batch handling, inventory validation, geometry/selection/geodesics/projection and outer probe bookkeeping, **not pure selection**. |

Timers nest: decode is inside file load, inside window preparation, inside query
wall time. **Do not add the inclusive metrics.** Derived differences use the raw
per-phase deltas, with no clamping or timing corrections. `timing_summary` keeps
phase wall-time dispersion; `probe_timing_summary` reports the same median, MAD,
range and relative dispersion for each boundary/derived metric. Constructors and
grid preparation have zero boundary deltas and no query residual metric.

Instrumentation adds **two `perf_counter` reads per wrapped call**, delegation,
`finally`, and cumulative time/count updates. Exact wrapped-call/read counts and
clock resolution/implementation are recorded. Inner bookkeeping remains inside
the enclosing timer/residual, while a wrapper's own final bookkeeping is outside
its timed interval. Probe cost is **not calibrated or subtracted**, and may be
significant for very short exact-window hits. Public `preparation_seconds` is
also perturbed by nested probe overhead. Compare the same instrumented driver
between runs; do not interpret an old uninstrumented-to-instrumented difference
as a backend speed regression. No pure assembly, pure selection, or RSS claim is
made by these timers.

JSON includes every raw wall sample, before/after/delta for all public counters
and gauges, cache file byte totals, result hashes/dtypes, and median, median
absolute deviation (MAD), min/max and relative dispersion per phase/kind.
`cached_bytes`, `active_bytes`, `geometry_bytes`, and `peak_retained_bytes` are
**retained-array accounting, not RSS or temporary-allocation measurements**.
RSS is not measured; disk bytes are logical file sizes, not allocated blocks.
Custom zero/tight budgets may prevent retention/grid admission; inspect counters
rather than assuming a prepared geometry entry was admitted.

All samples run in one process, with fixed phase order and no discarded warmup.
Artifacts may be cold, but the OS page cache is uncontrolled and fixtures were
just written; neither process-cold nor storage-cold performance is claimed.
Record competing machine activity externally when collecting a baseline.
The driver records Python/package/native NetCDF/HDF5 versions, platform, Git
revision/status and hashes so dirty-code runs cannot masquerade as pristine
revision measurements.

**Regression threshold: pending user agreement after reviewing repeated baseline
noise.** The driver reports no speedup or performance acceptance verdict. Compare
identical workload/source/schema hashes and complete science checks before
interpreting timings; assess radius sequence hashes separately from membership.
The main task owner must review the raw evidence and prepare the sanitized
baseline summary; a passing smoke run does not complete task 01 or its audit.

## Targeted checks

```sh
uv run --no-sync black --check benchmarks/benchmark_indexed_along_track.py
uv run --no-sync isort --check-only benchmarks/benchmark_indexed_along_track.py
MYPYPATH=src PYTHONPATH=src:. uv run --no-sync mypy --follow-imports=silent benchmarks/benchmark_indexed_along_track.py
```

Use the already installed checker/formatter versions; no dependency update is
needed. Run only the task's explicitly database-free tests separately, not the
unrestricted repository suite.
