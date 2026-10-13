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

## Benchmarks

The maintained benchmark has **core** and **exhaustive** index profiles, using the
same independently reset episodes and AD-A/AD-B controls. It covers daily
traversals, isolated dates, and overlapping entire-record radius sweeps.

```bash
# Copy a packaged profile, set source_database and your POSTGRES_* environment.
python -c "from importlib.resources import files; print(files('OceanDB.benchmark').joinpath('profiles/core.yaml').read_text())" > /tmp/oceandb-core.yaml
oceandb benchmark check /tmp/oceandb-core.yaml
oceandb benchmark prepare /tmp/oceandb-core.yaml --output ../benchmark-runs/core-run
oceandb benchmark run ../benchmark-runs/core-run
```

Run directories and reports must be **outside Git checkouts**. No measured results
are distributed with the package. Preparation requires a populated source and
local PostgreSQL 18+/PostGIS; it preserves the source. See the
[benchmark strategy, key tests and exhaustive exclusions](markdown/core-benchmark.md)
for prerequisites, timing boundaries, failure behavior, and report sharing.
