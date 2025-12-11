# OceanDB
OceanDB is a python package for managing oceanic satellite data intelligently.  The python package interfaces with a postgres database enabling efficient geospatial/temporal queries.  OceanDB comes with a simple CLI that allows users to initialize database & ingest data.  

Configuring the .env 
Using the .env.example create an .env populated with 
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
## Installation Instructions
1. **Instalalling**
   With your python environment activated
   ```bash
   pip install OceanDB 
   pip install -e OceanDB // editable install for development
   ```

## OceanDB Initialization Instructions
The OceanDB package provides a CLI for 
1. **Initializing the Database**
   ```bash
   oceandb init // Creates the database tables 
   ```

2. **Ingesting Data** 

With Copernicus Marine Service Along Track downloaded to your computer. ensure that the ALONG_TRACK_DATA_DIRECTORY is set correctly in
the .env file.  ALONG_TRACK_DATA_DIRECTORY should be the file path to the directory at which the SEALEVEL_GLO existis.  so ALONG_TRACK_DATA_DIRECTORY=/path/../../copernicus   



![Screenshot 2025-12-05 at 11.19.07 AM.png](docs/Screenshot%202025-12-05%20at%2011.19.07%E2%80%AFAM.png)

By default if no arguments are provided this CLI command will iterate over all of the data

   ```bash
    oceandb ingest // Ingest all missions across all date ranges
    oceandb ingest -m al  // Ingest all 'al' mission data
    oceandb ingest -m j1 -m j3 // Multiple missions 
    oceandb ingest -m j1 --start-date 2021-02-01 --end-date 2024-03-01  // Ingest all data between start-date & end-date  
  ```

 
4. **Querying SLA Data**
   To query the sea level anomaly for a given satellite mission, time range & radius around a given point
   ```python
   from datetime import datetime
   from OceanDB.AlongTrack import AlongTrack
   
   latitude =  -63.77912
   longitude = 291.794742
   date = datetime(year=2013, month=12, day=31, hour=23)
   along_track = AlongTrack()
   
   sla_geographic = along_track.geographic_points_in_spatialtemporal_window(
    latitude=latitude,
    longitude=longitude,
    date=date,
    missions = ['al']
   )
   ```

## Docker Instructions

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
