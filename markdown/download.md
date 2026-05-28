# Download Data

OceanDB supports downloading along-track data from Copernicus Marine Service and eddy data from AVISO.

## Along-Track Downloads

Use `oceandb download` to fetch along-track data into `ALONG_TRACK_DATA_DIRECTORY`.

```bash
ALONG_TRACK_DATA_DIRECTORY=/path/to/copernicus
COPERNICUS_USERNAME=copernicus_marine_service_username
COPERNICUS_PASSWORD=copernicus_marine_service_password
```

Preview a download:

```bash
oceandb download --dry-run j3 --start-date 2024-01-01 --end-date 2024-02-01
```

Download one mission:

```bash
oceandb download j3 --start-date 2024-01-01 --end-date 2024-02-01
```

Download multiple missions:

```bash
oceandb download s3a s3b --start-date 2024-01-01 --end-date 2024-02-01
```

Download all supported missions:

```bash
oceandb download all --dataset-version 202411
```

Skip confirmation prompts:

```bash
oceandb download j3 --start-date 2024-01-01 --end-date 2024-02-01 --yes
```

## Eddy Downloads

Use `oceandb download-eddy` to fetch AVISO eddy NetCDF files into `EDDY_DATA_DIRECTORY`.

```bash
EDDY_DATA_DIRECTORY=/path/to/eddies
AVISO_USERNAME=aviso_username
AVISO_PASSWORD=aviso_password
```

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
