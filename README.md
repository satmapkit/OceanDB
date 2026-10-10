# OceanDB

OceanDB is a python package for managing oceanic satellite data intelligently. The python package interfaces with a postgres database enabling efficient geospatial/temporal queries. OceanDB comes with a simple CLI that allows users to initialize the database and ingest data.

## Quickstart

For a minimal local setup:

1. Configure your `.env` file with your PostgreSQL settings and data directories.
2. Initialize a postgres database.
```sh
oceandb init
```
3. Download the along-track and, if needed, eddy datasets.
```sh
oceandb download j2 --start-date 2013-01-01 --end-date 2013-01-31 --yes
oceandb download-eddy --yes
```
5. Ingest the downloaded data into OceanDB.
```sh
oceandb ingest-along-track j2 --start-date 2013-01-01 --end-date 2013-01-31
oceandb ingest-eddy
```
5. Create indices so queries perform reliably.
```sh
oceandb index create --default
```

See the detailed instructions linked below for the full setup and workflow.


## Getting Started

To get started with OceanDB, perform the following actions:

1. Install OceanDB and set up the surrounding tools you need for local development.
   See [Installation](markdown/Installation.md).
2. Configure your `.env` file and choose how Postgres will run.
   See [Postgres Setup](markdown/postgres.md).
3. Download SLA along-track data and, if needed, eddy data.
   See [Download Data](markdown/download.md).
4. Ingest the downloaded data into Postgres.
   See [Ingest Data](markdown/ingest.md).
5. Build indexes so OceanDB queries perform reliably.
   See [Index Management](markdown/index.md).
6. Run OceanDB queries against along-track and eddy datasets.
   See [Queries](markdown/queries.md).

## Index performance benchmarks

The benchmark runner uses isolated run directories and owned trial databases, validates
source data and actual index definitions, and checkpoints completed scenarios. Its
900-second query-phase limit covers six scenarios and their EXPLAIN replays; maintenance
runs beforehand. Reports and a checksummed ZIP are generated automatically, with explicit
timeout/failure labels and compatibility checks for combined analyses.

```bash
python optimize_geo_points_r_dt.py --prepare notes/my-benchmark
python optimize_geo_points_r_dt.py --resume notes/my-benchmark
```

A missing source database requires the explicit `--ingest-source` option during preparation.
See [the benchmark guide](markdown/index-benchmark.md) for the 32 configurations, workload,
resume rules, disk requirements, detached execution, and analysis commands.

For the original pre-refactor workflow versus current radius and nearest-neighbor
queries with BMA, see [the native workflow guide](markdown/native-workflow-comparison.md).
`compare_native_workflow.py` invokes the old schema, ingestion and eight-index builder
unchanged, keeps each version's native basin map, reports output differences, and runs
five alternating timed passes with a final timing table and ZIP.

The earlier [matched-index experiment](markdown/refactor-comparison.md) gave both
versions BMA and common basin filters; it does not reproduce the original workflow.
