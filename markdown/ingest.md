# Ingest Data

After downloading data locally, use the ingest commands to load it into OceanDB.

## Along-Track Ingest

Before ingesting along-track data, modify the root `.env` file so OceanDB knows where the downloaded NetCDF files are stored.

Update `.env` with a value like:

```bash
ALONG_TRACK_DATA_DIRECTORY=data/copernicus
```

or:

```bash
ALONG_TRACK_DATA_DIRECTORY=/Volumes/ExternalSSD/oceandb/copernicus
```

After you update `.env`, `oceandb ingest-along-track` reads from `ALONG_TRACK_DATA_DIRECTORY`.

To ingest data from a specific satellite mission, pass the mission name as a positional argument.

For example:

```bash
oceandb ingest-along-track s3a
```

You can also pass more than one mission name:

```bash
oceandb ingest-along-track s3a j3 c2
```

If you do not pass any mission names, OceanDB will ingest all matching along-track files found in `ALONG_TRACK_DATA_DIRECTORY`.

You can further limit ingestion to a date range with `--start-date` and `--end-date`.

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

Use `oceandb summary alongtrack` to see what along-track data has already been ingested into Postgres.

## Eddy Ingest

Before ingesting eddy data, modify the root `.env` file so OceanDB knows where the downloaded eddy files are stored.

Update `.env` with a value like:

```bash
EDDY_DATA_DIRECTORY=data/eddies
```

or:

```bash
EDDY_DATA_DIRECTORY=/Volumes/ExternalSSD/oceandb/eddies
```

After you update `.env`, use `oceandb ingest-eddy` to load eddy detection datasets into OceanDB.

```bash
oceandb ingest-eddy
```

This command performs a full historical ingest of the available eddy files in `EDDY_DATA_DIRECTORY`.
