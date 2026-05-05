# Introduction
OceanDB is a python package for managing oceanic satellite data intelligently.  The python package interfaces with a postgres database enabling efficient geospatial/temporal queries.  OceanDB comes with a simple CLI that allows users to initialize the database and ingest data.  

## Source data
Presently, OceanDB handles both satellite altimetry data, as well as processed eddy tracks.

- Along track satellite data can be obtained from Copernicus Marine; a Copernicus Marine account is required to fetch the data.
- Eddy data is available through AVISO.


## Installation Instructions

1. **Clone OceanDB repository**

    ```sh
    git clone git@github.com:satmapkit/OceanDB.git
    ```

2. **Configure the database and data source files**

   OceanDB is managed through a database, and contains tooling for both *ingesting* data from source
   files into the database, as well as *extracting* data from the database.

   To configure database connection, as well as to specify where data source files exist on disk,
   a `.env` file may be used.

   If you wish to run the database locally using docker, you can copy `.env.example` to `.env`, i.e.
   ```sh
   cp .env.example .env
   ```

4. **Setup python environment**
   
   The details depend on how you use python, e.g. from the command line or an IDE like PyCharm.

   Command-line steps may look like the following.
   ```sh
   python -m venv .venv
   source .venv/bin/activate
   ```
   

5. **Install OceanDB**
   
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
   oceandb create-indices // Build query indices after bulk ingest
   ```

2. **Downloading Along-Track Data**

   Use `oceandb download` to fetch Copernicus Marine Service along-track data.
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
   oceandb download --dry-run j3 --start-date 2017-01-01 --end-date 2017-02-01
   ```

   Download one mission. The command previews the matching files, then prompts
   for confirmation:

   ```bash
   oceandb download j3 --start-date 2017-01-01 --end-date 2017-02-01
   ```

   Download multiple missions:

   ```bash
   oceandb download s3a s3b --start-date 2017-01-01 --end-date 2017-02-01
   ```

   Download all supported along-track missions:

   ```bash
   oceandb download all --dataset-version 202411
   ```

   Skip the confirmation prompt for scripted runs:

   ```bash
   oceandb download j3 --start-date 2017-01-01 --end-date 2017-02-01 --yes
   ```

   After data has been downloaded, ingest it into OceanDB.

   <!-- TODO: figure out how to include these images -->
   <!-- ![Screenshot 2025-12-05 at 11.19.07 AM.png](docs/Screenshot%202025-12-05%20at%2011.19.07%E2%80%AFAM.png) -->

3. **Downloading Eddy Data**

Use `oceandb download-eddy` to fetch the packaged AVISO eddy NetCDF files.
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
oceandb download-eddy --dry-run
```

Download the missing eddy files:

```bash
oceandb download-eddy
```

Skip the confirmation prompt for scripted runs:

```bash
oceandb download-eddy --yes
```

Force a re-download of files that already exist locally:

```bash
oceandb download-eddy --overwrite --yes
```

4. **Ingesting Along-Track Data**

   `oceandb ingest-along-track` reads from the same
   `ALONG_TRACK_DATA_DIRECTORY` used by `oceandb download`.

   By default if no arguments are provided this CLI command will iterate over all
   of the data.

   ```bash
   oceandb ingest-along-track
   oceandb ingest-along-track s3a
   oceandb ingest-along-track s3a j3 c2
   oceandb ingest-along-track j3 --start-date 2019-01-01 --end-date 2020-12-03
   oceandb ingest-along-track s6a --end-date 2017-01-01
   oceandb ingest-along-track s6a --start-date 2017-01-01
   oceandb summary alongtrack
   ```

   Ingesting Eddy Data

   ```bash
   oceandb ingest-eddy
   ```


4. **Visualizing basin mask**

   To quickly visualize the packaged basin polygons and their basin IDs:

   ```bash
   oceandb visualize-basins --output artifacts/basin_map.html
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
   
2. **Build OceanDB Python Image**
   If building a development image 
   ```bash
   make build_image
   ```


## Query Notes

Had to Modify the Query Slightly


Every projected column must be aliased to its schema name.
No schema object should ever reference a query-specific table alias.
