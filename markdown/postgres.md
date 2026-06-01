# Postgres Setup

OceanDB expects configuration from a `.env` file in the repository root.

## Local Docker Postgres

For local development, the default approach is to run Postgres with Docker and store the database files in a local repository directory.

## Example `.env`

```bash
POSTGRES_HOST=postgres
POSTGRES_USERNAME=postgres
POSTGRES_PASSWORD=postgres
POSTGRES_PORT=5432
POSTGRES_DATABASE=ocean
POSTGRES_DATA_DIRECTORY=data/postgres_data

```

With this setup, OceanDB's Docker Postgres container stores its data under `data/postgres_data` in the repository.

Start Postgres with:

```bash
make run_postgres
```

## Using an External SSD for Docker Postgres Data

If you want the Docker Postgres data to live on an external SSD instead of inside the repo, point `POSTGRES_DATA_DIRECTORY` at that mounted path:

```bash
POSTGRES_DATA_DIRECTORY=/Volumes/ExternalSSD/oceandb/postgres_data
```

## Using an External Postgres Database

If you already have a Postgres instance running outside Docker, update the connection values in `.env` to point at that database:

```bash
POSTGRES_HOST=my-postgres-host
POSTGRES_USERNAME=my_user
POSTGRES_PASSWORD=my_password
POSTGRES_PORT=5432
POSTGRES_DATABASE=ocean
POSTGRES_DATA_DIRECTORY=data/postgres_data
```

In this setup, `POSTGRES_DATA_DIRECTORY` is just a sensible default for the local Docker workflow and is not used by the external database itself.

## Notes

- `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_USERNAME`, `POSTGRES_PASSWORD`, and `POSTGRES_DATABASE` control how OceanDB connects to Postgres.
- `POSTGRES_DATA_DIRECTORY` controls where the local Docker Postgres container stores its files.
- `ALONG_TRACK_DATA_DIRECTORY` is where Copernicus along-track downloads will be stored.
- `EDDY_DATA_DIRECTORY` is where AVISO eddy files will be stored.
- `OCEANDB_INGEST_MODE` defaults to `insert`; use `copy` for bulk staged COPY ingest.
