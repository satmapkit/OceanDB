# OceanDB
OceanDB is a python package for managing oceanic satellite data intelligently.  The python package interfaces with a postgres database enabling efficient geospatial/temporal queries.  OceanDB comes with a simple cli that allows users to initialize database & ingest data.  

Configuring the .env 
Using the .env.example create an .env populated with 

POSTGRES_HOST=postgres
POSTGRES_USERNAME=postgres
POSTGRES_PASSWORD=postgres
POSTGRES_PORT=5432
POSTGRES_DATABASE=ocean

ALONG_TRACK_DATA_DIRECTORY=/app/data/copernicus
EDDY_DATA_DIRECTORY=/app/data/eddies

COPERNICUS_PASSWORD=copernicus_marine_service_password_placeholder
COPERNICUS_USERNAME=copernicus_marine_service_username

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

TODO -> this is in progress 
1. **Ingesting Data**
   Downloads data from Copernicus Marine Service over given date range & ingests into postgres.  Will take a long time.  
    ```bash
   oceandb ingest --start-date 2021-02-01 --end-date 2024-03-01 --missions all // Ingest all date between start-date & end-date  
    ```
2. **Querying SLA Data**
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
