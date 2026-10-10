# Native pre-refactor workflow versus current BMA

This comparison supersedes the matched-index experiment for measuring the original
workflow. The previous experiment gave both versions BMA plus four singleton GiST
indexes and matched basin lists; it does **not** represent the historical setup.

The old side now invokes the unmodified branch's `create_along_track_table`, metadata
and basin setup, `insert_along_track_data_from_netcdf_with_tuples`, and
`create_along_track_indices`. The last method creates eight indexes and performs its
own `VACUUM ANALYZE`. It includes B-tree indexes on filename, time, basin and the mission
expression; GiST indexes on the geographic point and its geometry cast; a point/time
GiST index; and a point/time/basin/mission-expression GiST index. The last index's
filename suggests a different order; its actual SQL order is authoritative. BMA is
never added to this database. The old table has its original primary key and lacks
the new stored mission column and unique spatiotemporal constraint.

The new side uses current ingestion's existing verified source, cloned into a fresh
database, with BMA and the four singleton GiST indexes. Both sides use the same 209
October 2022 files and 9,519,966 observations. Native setup uses temporary symlinks to
limit legacy file discovery to that dataset, leaving original files untouched. Local
credentials are supplied in memory. The legacy checkout itself is not edited.

Each version keeps its native basin map, as requested. The same 31,966 locations,
50-km radius, nearest-three limit without a distance cap, all-missions filter and
October 15 ±10-day interval are used. Old receives a 20-day argument because its API
halves that value; new receives 10 days. Each API consumes longitude, latitude, SLA
and time difference. Mask and native connection caches are initialized before timing.

The old importer wraps longitudes to −180…180 and writes filenames from a one-element
list. Those behaviors are preserved in its database. An untimed observation audit
normalizes filename delimiters and longitude representation, and compares per-file
counts and checksums of the shared observed fields. It excludes row IDs, the mission
column, generated geography bytes and the TPA correction the old tuple importer does
not populate. Validation wraps longitude and rounds it to 1e-10 degrees (well below
the source coordinate resolution); timed queries and returned arrays are untouched.

Four warm-ups precede five alternating measured passes of each query/version. At
locations with identical basin filters, results must agree after longitude
normalization, allowing independently verified exact third-neighbor distance ties.
Output differences at locations with different native basin filters are counted and
reported separately. They are **not** asserted to be identical results. Unexplained
differences at equal-filter locations stop measurement. Workflow ratios therefore
describe each native workflow's time, with potentially different outputs at those
documented locations. No claim of identical work at every location is made.

Each query worker retains the 900-second deadline and scoped PostgreSQL cleanup.
Failed or timed-out cases are not retried. Each run has a new output directory and
new old/current trial databases. Earlier databases and result archives are preserved.

```bash
python compare_native_workflow.py --prepare notes/native-comparison-RUN \
  --previous notes/pre-refactor-bma-comparison-PREVIOUS
python compare_native_workflow.py --run notes/native-comparison-RUN
```

`progress.log` records phases and pass completions. `legacy-setup.log` records native
ingestion and index construction. Reports, both actual index inventories, observation
audits, basin-map differences, validation examples, query diagnostics and exact code
snapshots are packaged into the final ZIP. Raw source NetCDF files and credentials
are excluded.
