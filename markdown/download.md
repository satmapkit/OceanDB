# Download Data

OceanDB supports downloading along-track data from Copernicus Marine Service and eddy data from AVISO.

## Along-Track Downloads

Before downloading along-track data, modify the root `.env` file so OceanDB knows:

- where to store the downloaded files
- which Copernicus Marine credentials to use

Update `.env` with values like:

```bash
ALONG_TRACK_DATA_DIRECTORY=/path/to/copernicus
COPERNICUS_USERNAME=copernicus_marine_service_username
COPERNICUS_PASSWORD=copernicus_marine_service_password
```

After you update `.env`, use `oceandb download` to fetch along-track data into `ALONG_TRACK_DATA_DIRECTORY`.

To download data from a specific satellite mission, pass the mission name as a positional argument to `oceandb download`.

For example, to download Jason-3 data, pass `j3`:

```bash
oceandb download j3 --start-date 2024-01-01 --end-date 2024-02-01
```

You can also pass more than one mission name:

```bash
oceandb download s3a s3b --start-date 2024-01-01 --end-date 2024-02-01
```

If you want OceanDB to download every supported mission, use `all`:

```bash
oceandb download all --dataset-version 202411
```

Preview a download:

```bash
oceandb download --dry-run j3 --start-date 2024-01-01 --end-date 2024-02-01
```

Download one mission:

```bash
oceandb download j3 --start-date 2024-01-01 --end-date 2024-02-01
```

Skip confirmation prompts:

```bash
oceandb download j3 --start-date 2024-01-01 --end-date 2024-02-01 --yes
```

## Eddy Downloads

Before downloading eddy data, modify the root `.env` file so OceanDB knows:

- where to store the eddy files
- which AVISO credentials to use

Update `.env` with values like:

```bash
EDDY_DATA_DIRECTORY=/path/to/eddies
AVISO_USERNAME=aviso_username
AVISO_PASSWORD=aviso_password
```

After you update `.env`, use `oceandb download-eddy` to fetch AVISO eddy NetCDF files into `EDDY_DATA_DIRECTORY`.

Preview the download:

```bash
oceandb download-eddy --dry-run
```

Download missing files:

```bash
oceandb download-eddy
```

Force re-download:

```bash
oceandb download-eddy --overwrite --yes
```
