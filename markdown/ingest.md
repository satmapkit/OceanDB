# Ingest Data

After downloading data locally, use the ingest commands to load it into OceanDB.

## Along-Track Ingest

`oceandb ingest-along-track` reads from `ALONG_TRACK_DATA_DIRECTORY`.

Set `OCEANDB_INGEST_MODE=copy` in `.env` to stage batches through PostgreSQL `COPY`. The default remains `insert`.

Examples:

```bash
oceandb ingest-along-track
oceandb ingest-along-track s3a
oceandb ingest-along-track s3a j3 c2
oceandb ingest-along-track j3 --start-date 2019-01-01 --end-date 2020-12-03
oceandb ingest-along-track s6a --end-date 2024-01-01
oceandb ingest-along-track s6a --start-date 2024-01-01
oceandb summary alongtrack
```

## Eddy Ingest

Use `oceandb ingest-eddy` to load eddy detection datasets into OceanDB.

```bash
oceandb ingest-eddy
```

This command performs a full historical ingest of the available eddy files in `EDDY_DATA_DIRECTORY`.
