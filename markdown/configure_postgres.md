# Configure Postgres

OceanDB expects configuration from a `.env` file in the repository root.

## Example `.env`

```bash
POSTGRES_HOST=postgres
POSTGRES_USERNAME=postgres
POSTGRES_PASSWORD=postgres
POSTGRES_PORT=5432
POSTGRES_DATABASE=ocean
POSTGRES_DATA_DIRECTORY=/Volumes/ExternalDrive/oceandb/postgres

ALONG_TRACK_DATA_DIRECTORY=/app/data/copernicus
EDDY_DATA_DIRECTORY=/app/data/eddies
OCEANDB_INGEST_MODE=insert

COPERNICUS_PASSWORD=copernicus_marine_service_password_placeholder
COPERNICUS_USERNAME=copernicus_marine_service_username
```

## Notes

- `POSTGRES_DATA_DIRECTORY` should point to a writable folder for local Docker Postgres storage.
- `ALONG_TRACK_DATA_DIRECTORY` is where Copernicus along-track downloads will be stored.
- `EDDY_DATA_DIRECTORY` is where AVISO eddy files will be stored.
- `OCEANDB_INGEST_MODE` defaults to `insert`; use `copy` for bulk staged COPY ingest.

## Running Postgres

Use the local Docker-based Postgres environment during development:

```bash
make run_postgres
```

The Postgres container stores its data in the host directory configured by `POSTGRES_DATA_DIRECTORY`.
