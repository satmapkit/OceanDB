# OceanDB
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
   
   ALONG_TRACK_DATA_DIRECTORY=/app/data/copernicus
   EDDY_DATA_DIRECTORY=/app/data/eddies
   
   COPERNICUS_PASSWORD=copernicus_marine_service_password_placeholder
   COPERNICUS_USERNAME=copernicus_marine_service_username
   ```

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
   oceandb init // Creates the database tables 
   ```

2. **Ingesting Along Track Data** 

With Copernicus Marine Service Along Track downloaded to your computer. ensure that the ALONG_TRACK_DATA_DIRECTORY is set correctly in
the .env file.  ALONG_TRACK_DATA_DIRECTORY should be the file path to the directory at which the SEALEVEL_GLO existis.  so ALONG_TRACK_DATA_DIRECTORY=/path/../../copernicus   



![Screenshot 2025-12-05 at 11.19.07 AM.png](docs/Screenshot%202025-12-05%20at%2011.19.07%E2%80%AFAM.png)

By default if no arguments are provided this CLI command will iterate over all of the data

   ```bash
    oceandb ingest-along-track // Ingest all missions across all date ranges
    oceandb ingest-along-track s3a  // Ingest all Sentinel-3A (s3a) mission data
    oceandb ingest-along-track s3a j3 c2 // Ingest multiple missions
    oceandb ingest-along-track j3 --start-date 2019-01-01 --end-date 2020-12-03 // Ingest data from specific missions between start-date and end-date
    oceandb ingest-along-track s6a --end-date 2024-01-01 // Specify only end-date
    oceandb ingest-along-track s6a --start-date 2024-01-01  // Specify only start-datea
  ```



3. **Ingesting Eddy Data** 

   ```bash
    oceandb init-eddy 
    oceandb ingest-eddy // Ingest all missions across all date ranges
   ```
 
4. **Querying SLA Data**
   
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

![Screenshot 2025-12-18 at 12.14.04 PM.png](docs/Screenshot%202025-12-18%20at%2012.14.04%E2%80%AFPM.png)

- **Script path**  
  Select the script you want to run, for example:  
  `src/OceanDB/tests/test_geographic_nearest_neighbor.py`

- **Python interpreter**  
  Ensure the correct virtual environment is selected (the same one where OceanDB was installed).

- **Working directory**  
  Set this to the **repository root** (the directory containing `pyproject.toml`).

- **Environment file (.env)**  
  Set **Paths to .env files** to point to your `.env` file containing PostgreSQL credentials and any other required environment variables.

![Screenshot 2025-12-18 at 12.18.01 PM.png](docs/Screenshot%202025-12-18%20at%2012.18.01%E2%80%AFPM.png)



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
   


