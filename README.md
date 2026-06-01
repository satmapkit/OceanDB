# OceanDB

OceanDB is a python package for managing oceanic satellite data intelligently. The python package interfaces with a postgres database enabling efficient geospatial/temporal queries. OceanDB comes with a simple CLI that allows users to initialize the database and ingest data.

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
