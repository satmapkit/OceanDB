# Task 01 — synthetic indexed-backend baseline

**Synthetic only. No actual user workload was supplied. No speedup, RSS result,
or regression acceptance threshold is claimed.** Production algorithms remain
at `766db427da69f0f682604bd2dfa7e33ae6a9fb95`. At measurement time the driver and
characterization changes were uncommitted; commit authorization followed the
independent audit.

## Reproduction and workload

From the OceanDB root, in the environment recorded in
[01-evidence.md](01-evidence.md):

```sh
PYTHONPATH=src:. uv run --no-sync python benchmarks/benchmark_indexed_along_track.py --help
PYTHONPATH=src:. uv run --no-sync python benchmarks/benchmark_indexed_along_track.py --rows-per-file 30 --files 3 --repetitions 2
PYTHONPATH=src:. uv run --no-sync python benchmarks/benchmark_indexed_along_track.py --rows-per-file 5000 --files 6 --repetitions 5
PYTHONPATH=src:. uv run --no-sync python benchmarks/benchmark_indexed_along_track.py --rows-per-file 5000 --files 6 --repetitions 5 --max-cache-bytes 1000000
```

The normal and tight-budget trials run sequentially, not concurrently. Each
invocation owns a new `/tmp/opencode/indexed-along-track-*` directory; the driver
prints the evidence path. Sources, prepared caches, and raw JSON remain external
and are not committed. Do not remove unrelated caches or flush system caches.

- Fixture: six deterministic NetCDF3 files, 5,000 rows each, covering eight days;
  missions `j2`, `j3`, `s3a` (two files each). NumPy PCG64 seed `20240101`.
- Independent packed science includes both missing sentinels and alternating
  valid/missing scalar correction; coordinates preserve decoded microdegree
  precision, and timestamps are distinct integer microseconds.
- Both query kinds request all scientific/metadata fields and signed `delta_t`;
  radius requests distance, nearest omits it and verifies mandatory inclusion.
- Fixed paired `(latitude, longitude)` centers: `(0, 180)`, `(0.75, 180.75)`,
  `(-1, 179.5)`, `(45, 20)`; the last has no matches.
- Date `2024-01-05T00:00:00`, inclusive one-day half-window, radius/cap 300,000 m,
  cell size 2 degrees, all three missions, `n_jobs=1`, `chunk_size=16`.
- Adjacent sequence: first center at +6 h, +12 h, +24 h, in that order.
- File budgets: 64,000,000 B (normal) versus 1,000,000 B (forces eviction).
  Separate geometry budget: 8,000,000 B in both cases.
- Five samples per phase/query kind, nine phases each: 90 raw phase samples
  per baseline invocation. Normal query batches contain four centers; the
  adjacent batch contains three. Times are per operation/batch, not per row.

## Equivalence before timing interpretation

Every returned column is checked against generator-owned rows, direct parsing of
the packaged basin raster/direct-plus-self connections, and exhaustive
GeographicLib distances. No production reader produces science expectations.
Checks include schema metadata, name/count, projected column order/dtypes,
provenance, timestamps/stored coordinates, packed values, physical values, both
missing masks, signed offsets, and nearest's exact ordered three-row cutoff.
Radius membership uses scientific keys; actual sequence hashes are recorded
separately and do not impose a new radius sort contract.

Distance tolerance is 0.1 mm for returned values (not a membership epsilon),
packed-value tolerance is zero, and physical-value tolerance is `1e-12`.
Equivalence and lazy scaled-column access occur outside query timers.

## Recorded evidence

All four commands above exited 0. The final smoke generated 36 phase samples at
`/tmp/opencode/indexed-along-track-00116o8c/results.json`. The two baseline runs
each generated 90 samples and passed complete equivalence checks:

| Budget | Raw evidence (external, not committed) | JSON SHA256 |
| --- | --- | --- |
| Normal | `/tmp/opencode/indexed-along-track-2022_nrj/results.json` | `8aa89194761ab3b8ae3a8a79e30f40dcc5112396016104dcbd597cabaea02cb1` |
| Tight | `/tmp/opencode/indexed-along-track-7mvhc5jo/results.json` | `ed219e588a5ac6fd2df6a9484acf9a722b2989afdd4033ee6603246febf6af08` |

Shared provenance:

- Driver SHA256: `bf716f3830414682fa16c6b57e910ef8d11834d30b55606afde51ea3e02c82e6`.
- Source inventory SHA256: `7b853c9409d84d84e8cbe72cbd34d80794e9486a2f0e028106ee2644485cc52d`.
- Schema SHA256: `2c2c223dc568b4c82cc0129d7fccbc578c4ec0d6d28830ed493ef77fd88bd5e7`.
- Normal workload SHA256: `9789f5ad83290b3c53a54c054cc77aff524c99c9678d00fe6842918303bdb03e`.
- Tight workload SHA256: `fc61a3838978845563e848c9e0acd016dac95245a5a057bda5fb15ff3a46186e`.
- Native NetCDF 4.9.3, HDF5 1.14.6; Python/package/machine versions are recorded
  in the raw evidence and [01-evidence.md](01-evidence.md).

The JSON also preserves each source's relative filename/bytes/hash, basin asset
hashes, complete schema metadata, all seven driver/production code hashes,
invocation/revision/dirty paths, raw wall/probe samples, counter/gauge
before/after/deltas, result fingerprints and dtype metadata. No `.env` contents
or actual user dataset provenance is included.

### Wall time and observed noise

Each cell is **median ± MAD [minimum–maximum] milliseconds**, five samples.
Labels correspond to the exact phase names in the driver README/JSON.

Normal file budget:

| Phase | Radius | Nearest |
| --- | --- | --- |
| Artifact-cold constructor/catalog | 59.284 ± 0.877 [58.407–60.453] | 59.700 ± 0.987 [58.682–100.325] |
| First decode/window/query | 95.058 ± 1.605 [93.453–137.839] | 69.507 ± 0.655 [68.435–70.839] |
| Warm identical window | 30.460 ± 0.064 [30.396–30.891] | 6.069 ± 0.133 [5.901–6.631] |
| Three adjacent windows | 50.991 ± 0.103 [50.374–52.260] | 23.259 ± 0.362 [22.753–24.272] |
| Restarted backend constructor | 23.067 ± 0.190 [22.877–23.429] | 23.377 ± 0.023 [22.495–23.399] |
| Restarted prepared-file query | 41.304 ± 0.365 [40.645–42.148] | 16.811 ± 0.057 [16.727–16.870] |
| Direct geometry, same window | 30.560 ± 0.138 [30.311–30.939] | 6.056 ± 0.064 [5.954–6.235] |
| Grid preparation only | 0.248 ± 0.004 [0.245–0.259] | 0.303 ± 0.011 [0.292–0.335] |
| Prepared geometry, same window | 30.429 ± 0.137 [30.003–30.650] | 5.757 ± 0.069 [5.644–5.842] |

Tight file budget:

| Phase | Radius | Nearest |
| --- | --- | --- |
| Artifact-cold constructor/catalog | 60.650 ± 0.590 [59.943–62.058] | 60.551 ± 0.368 [60.183–101.423] |
| First decode/window/query | 94.585 ± 0.480 [94.106–135.406] | 69.835 ± 0.741 [68.848–71.280] |
| Warm identical window | 30.657 ± 0.218 [30.131–31.180] | 5.954 ± 0.027 [5.922–7.446] |
| Three adjacent windows | 56.364 ± 0.135 [55.268–57.006] | 28.124 ± 0.157 [27.967–28.474] |
| Restarted backend constructor | 23.752 ± 0.348 [23.403–24.428] | 23.320 ± 0.145 [23.175–23.620] |
| Restarted prepared-file query | 41.848 ± 0.267 [41.045–42.643] | 17.407 ± 0.338 [16.889–17.845] |
| Direct geometry, same window | 31.085 ± 0.042 [30.212–40.549] | 5.948 ± 0.062 [5.887–6.350] |
| Grid preparation only | 0.253 ± 0.005 [0.248–0.264] | 0.304 ± 0.011 [0.293–0.327] |
| Prepared geometry, same window | 30.090 ± 0.172 [29.901–30.981] | 5.912 ± 0.080 [5.786–6.055] |

Relative MAD is 0.10–3.53% (normal) and 0.13–3.52% (tight), but large outliers
remain: e.g. nearest cold construction reaches about 100 ms despite a 60 ms
median, and cold radius reaches 138 ms despite a 95 ms median. Tight-budget
direct radius has a 40.549 ms maximum despite a 31.085 ms median. Five samples
do not establish a reliable tail bound. Do not choose a threshold solely from
small MADs, infer a geometry speedup, or interpret sequential budget runs as a
controlled causal A/B experiment.

### Nested preparation components

Benchmark-only wrappers delegate exactly once to `_prepare_file`, `_load_file`,
and `prepare_window`; they return original values and preserve exceptions.
Decode includes source read/validation but excludes prepared artifact publication.
File residual is inclusive load minus decode (checks, cache I/O/publication,
mapping and retention). Window residual is inclusive window calls minus file
loads (orchestration, filtering/copying, duplicate checks/index assembly and
exact-hit source checks), **not pure index assembly**. Query residual is wall
minus all window calls (inventory/batch overhead, geometry, geodesics, selection
and projection), **not pure selection**.

Inclusive timers nest and must not be added together. The following nonoverlapping
components are **median ± MAD milliseconds**; independently computed medians
need not sum to median wall time.

| Budget / phase / kind | Decode | File residual | Window residual | Query residual |
| --- | --- | --- | --- | --- |
| Normal / cold / radius | 55.003 ± 1.178 | 3.987 ± 0.169 | 5.743 ± 0.056 | 30.479 ± 0.172 |
| Normal / cold / nearest | 54.221 ± 0.448 | 3.822 ± 0.153 | 5.653 ± 0.064 | 5.789 ± 0.044 |
| Normal / adjacent / radius | 0 | 0.131 ± 0.004 | 17.043 ± 0.132 | 33.719 ± 0.391 |
| Normal / adjacent / nearest | 0 | 0.115 ± 0.001 | 17.113 ± 0.397 | 6.121 ± 0.064 |
| Normal / restart query / radius | 0 | 1.627 ± 0.053 | 6.137 ± 0.226 | 33.371 ± 0.169 |
| Normal / restart query / nearest | 0 | 1.648 ± 0.024 | 6.162 ± 0.017 | 9.001 ± 0.199 |
| Tight / adjacent / radius | 0 | 4.756 ± 0.036 | 17.886 ± 0.609 | 33.686 ± 0.045 |
| Tight / adjacent / nearest | 0 | 4.797 ± 0.036 | 17.360 ± 0.113 | 6.004 ± 0.094 |

Each wrapped call adds two clock reads plus delegation/bookkeeping. Overhead is
recorded by call/clock-read counts, **not calibrated or subtracted**; inner
bookkeeping remains in enclosing residuals. Cold batches use 6 decode, 6 load,
4 window calls (32 added clock reads); exact warm batches still time 4 window
calls and their source checks (8 reads). The original public
`preparation_seconds` remains separately recorded; it is a combined build-only
statistic, unlike the inclusive window probe. Probes must move to equivalent
boundaries after extraction, not motivate preserving private compatibility APIs.
Earlier uninstrumented exploratory runs remain external but are not this final
comparison baseline. Future timings must use equivalent probes.

### Work, retention and equivalence checks

Integer work-counter deltas were independently checked to be identical across
all five repetitions, for each budget and query kind:

| Phase | Catalog reads | Decodes | Disk hits | Memory hits | Window builds |
| --- | --- | --- | --- | --- | --- |
| Cold constructor | 6 | 0 | 0 | 0 | 0 |
| Cold first batch | 0 | 6 | 0 | 0 | 1 |
| Warm identical | 0 | 0 | 0 | 0 | 0 |
| Adjacent, normal | 0 | 0 | 0 | 18 | 3 |
| Adjacent, tight | 0 | 0 | 18 | 0 | 3 |
| Restart constructor | 0 | 0 | 0 | 0 | 0 |
| Restart prepared-file batch | 0 | 0 | 6 | 0 | 1 |
| Direct/grid/prepared geometry | 0 | 0 | 0 | 0 | 0 |

Geometry preparation builds 4 entries / 292 B for radius and 8 / 488 B for
nearest. Prepared batches record 4 radius or 5 nearest geometry hits, with no
new window build. Normal retained file arrays are 3,420,000 B versus 570,000 B
under tight retention; active arrays are 1,194,608 B in both. Reported array
high-water values after geometry are 4,614,900 B / 4,615,096 B (normal
radius/nearest), or 1,764,900 B / 1,765,096 B (tight). These are **not RSS** or
temporary-allocation measurements.

Generated sources total 911,208 B. Per-trial artifact logical bytes increase
from 1,001 B after cataloging to 3,423,689 B after preparation, unchanged by the
RAM budget. Budgets do not impose a disk quota.

Baseline radius row counts are `[7470, 6483, 6265, 0]`; nearest counts are
`[3, 3, 3, 0]`. Complete scientific checks passed for every measured result.
An independent post-run comparison also found identical membership and observed
sequence hashes for repeated identical queries across phases, repetitions and
budgets. This records current radius sequence behavior without requiring a new
radius sort order. Nested timing arithmetic, containment, and wrapped clock-read
counts were checked in every raw record.

## Measurement state and remaining gate

The [independent audit accepted](01-audit.md) the implementation and checked all
published numbers against raw evidence. This is an implemented, measured
baseline, not a performance acceptance verdict.

All repetitions and backend restarts are in the **same interpreter process**.
Each cold trial uses an empty artifact cache, but generated sources were just
written and OS caches are uncontrolled. These are not storage-cold or
process-cold measurements. No warmup is discarded; CPU boost, scheduling, and
other machine activity are uncontrolled. Medians, MAD, and ranges describe
observed noise, not confidence intervals.

**Regression threshold remains pending user agreement after noise review.**
Real-workload availability and any acceptance of synthetic-only evidence must be
explicit before task 02. Retained-array bytes are not RSS; disk bytes are logical
file sizes, not allocated blocks or quotas.
