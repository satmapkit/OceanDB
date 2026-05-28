# Quickstart

OceanDB is a python package for managing oceanic satellite data intelligently. It interfaces with a Postgres database so you can run efficient geospatial and temporal queries against ocean satellite datasets.

## Installation

1. Create a Copernicus Marine account if you need one.
2. Clone the OceanDB repository.
3. Copy `.env.example` to `.env`.
4. Activate your Python environment.
5. Install OceanDB.

```bash
pip install OceanDB
pip install -e .
```

## Basic Workflow

1. Initialize the database.
2. Download the datasets you want to work with.
3. Ingest the downloaded files.
4. Run queries against the ingested data.

```bash
oceandb init
oceandb download j3 --start-date 2024-01-01 --end-date 2024-02-01
oceandb ingest-along-track j3 --start-date 2024-01-01 --end-date 2024-02-01
oceandb summary alongtrack
```

## Next Steps

- See [Configure Postgres](configure_postgres.md) for `.env` and database setup.
- See [Download Data](download.md) for along-track and eddy download commands.
- See [Ingest Data](ingest.md) for ingest workflows.
- See [Queries](queries.md) for example query usage.
