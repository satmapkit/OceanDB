# Introduction
OceanDB is a python package for managing oceanic satellite data intelligently.  The python package interfaces with a postgres database enabling efficient geospatial/temporal queries.  OceanDB comes with a simple CLI that allows users to initialize the database and ingest data.  

## Installation Instructions

1. **Create a Copernicus Marine account (if needed)**

   If you don't already have a Copernicus Marine account, create one. 

1. **Clone OceanDB repository**

2. **Configure the .env file**
   
   Open the OceanDB directory, and copy the example .env.example file to .env.
   Open the new .env file, and edit to set the postgres server and directories to download the data. 
   ```
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
   `POSTGRES_DATA_DIRECTORY` should be a writable folder on the external drive you want Docker to use for development PostgreSQL storage. The test container still uses the small named Docker volume `oceandb_postgres_data_test`.

4. **Setup python environment**
   
   The details depend on how you use python, e.g. from the command line or an IDE like PyCharm. These instructions are specific to PyCharm.
   

1. **Install OceanDB**
   
   With your python environment activated
   ```bash
   pip install OceanDB 
   pip install -e OceanDB // editable install for development
   ```

## OceanDB Initialization Instructions

The OceanDB package provides a CLI for initializing the database and ingesting data.
1. **Initializing the Database**
   ```bash
   oceandb init // Creates the database, tables, partitions, and reference data
   oceandb index create --all // Build every OceanDB-managed index with the legacy full-table flow
   oceandb index create // Prompt for one along-track index and a partition date range
   oceandb index list // Show indices currently present in the database
   oceandb index drop --all // Drop all OceanDB-managed indices
   ```

2. **Downloading Along-Track Data**

Use `oceandb download along-track` to fetch Copernicus Marine Service along-track data.
The command downloads files into `ALONG_TRACK_DATA_DIRECTORY`, which must be
set in `.env`.

```bash
ALONG_TRACK_DATA_DIRECTORY=/path/to/copernicus
COPERNICUS_USERNAME=copernicus_marine_service_username
COPERNICUS_PASSWORD=copernicus_marine_service_password
```

The downloader first runs a Copernicus dry-run preview. For normal downloads,
it prints the matching file count and total size, then asks whether to continue
before downloading anything.

Preview a download without fetching files:

```bash
oceandb download along-track --dry-run j3 --start-date 2024-01-01 --end-date 2024-02-01
```

Download one mission. The command previews the matching files, then prompts
for confirmation:

```bash
oceandb download along-track j3 --start-date 2024-01-01 --end-date 2024-02-01
```

Download multiple missions:

```bash
oceandb download along-track s3a s3b --start-date 2024-01-01 --end-date 2024-02-01
```

Download all supported along-track missions:

```bash
oceandb download along-track all --dataset-version 202411
```

Skip the confirmation prompt for scripted runs:

```bash
oceandb download along-track j3 --start-date 2024-01-01 --end-date 2024-02-01 --yes
```

After data has been downloaded, ingest it into OceanDB.

Quickly visualize the packaged basin polygons and their basin IDs:

```bash
oceandb visualize-basins --output artifacts/basin_map.html
```

<!-- TODO: figure out how to include these images -->
<!-- ![Screenshot 2025-12-05 at 11.19.07 AM.png](docs/Screenshot%202025-12-05%20at%2011.19.07%E2%80%AFAM.png) -->

3. **Downloading Eddy Data**

Use `oceandb download eddy` to fetch the packaged AVISO eddy NetCDF files.
The command downloads files into `EDDY_DATA_DIRECTORY` and requires AVISO
credentials in `.env`.

```bash
EDDY_DATA_DIRECTORY=/path/to/eddies
AVISO_USERNAME=aviso_username
AVISO_PASSWORD=aviso_password
```

The downloader previews how many eddy files will be downloaded and how many
already exist locally. By default, it skips existing files unless
`--overwrite` is provided.

Preview the download without fetching files:

```bash
oceandb download eddy --dry-run
```

Download the missing eddy files:

```bash
oceandb download eddy
```

Skip the confirmation prompt for scripted runs:

```bash
oceandb download eddy --yes
```

Force a re-download of files that already exist locally:

```bash
oceandb download eddy --overwrite --yes
```

4. **Ingesting Along-Track Data**

`oceandb ingest along-track` reads from the same
`ALONG_TRACK_DATA_DIRECTORY` used by `oceandb download along-track`.
Set `OCEANDB_INGEST_MODE=copy` in `.env` to stage batches through PostgreSQL
`COPY`; the default remains `insert`.

By default if no arguments are provided this CLI command will iterate over all
of the data.

```bash
oceandb ingest along-track
oceandb ingest along-track s3a
oceandb ingest along-track s3a j3 c2
oceandb ingest along-track j3 --start-date 2019-01-01 --end-date 2020-12-03
oceandb ingest along-track s6a --end-date 2024-01-01
oceandb ingest along-track s6a --start-date 2024-01-01
oceandb summary along-track
```


Ingesting Eddy Data
```bash
oceandb ingest eddy
```



 
5. **Querying SLA Data**
   
   To query the sea level anomaly for a given satellite mission, time range & radius around a given point
   ```python
   latitude = -69
   longitude = 28
   date = datetime(year=2013, month=3, day=14, hour=5)
   
   
   data = along_track.geographic_nearest_neighbors_dt(
       latitudes=np.array([latitude]),
       longitudes=np.array([longitude]),
       dates=[date],
       missions=['al']
   )
   
   for d in data:
       print(d)

   ```


## Running OceanDB scripts in PyCharm
1. **Activate the environment & Install OceanDB**
``` 
source .venv/bin/activate
pip install -e . 
```
2. **Set the Pycharm Run Configuration Parameters**

In the top right of the PyCharm window, click the 'edit' button to configure the PyCharm run parameters

<!-- TODO: figure out how to include these images -->
<!-- ![Screenshot 2025-12-18 at 12.14.04 PM.png](docs/Screenshot%202025-12-18%20at%2012.14.04%E2%80%AFPM.png) -->

- **Script path**  
  Select the script you want to run, for example:  
  `src/OceanDB/tests/test_geographic_nearest_neighbor.py`

- **Python interpreter**  
  Ensure the correct virtual environment is selected (the same one where OceanDB was installed).

- **Working directory**  
  Set this to the **repository root** (the directory containing `pyproject.toml`).

- **Environment file (.env)**  
  Set **Paths to .env files** to point to your `.env` file containing PostgreSQL credentials and any other required environment variables.

<!-- TODO: figure out how to include these images -->
<!-- ![Screenshot 2025-12-18 at 12.18.01 PM.png](docs/Screenshot%202025-12-18%20at%2012.18.01%E2%80%AFPM.png) -->



## Running OceanDB in Docker Instructions

1. **Running Postgres**
   
   If you want to spin up a postgres development container with docker-compose
   ```bash
   make run_postgres // runs postgres postgis in docker compose
   ```
   The Postgres container stores its data in the host directory configured by `POSTGRES_DATA_DIRECTORY` in `.env`.
   
2. **Build OceanDB Python Image**
   If building a development image 
   ```bash
   make build_image
   ```


## Query Notes

Had to Modify the Query Slightly


Every projected column must be aliased to its schema name.
No schema object should ever reference a query-specific table alias.
