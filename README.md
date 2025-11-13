# OceanDB
OceanDB is a python package for managing oceanic satellite data intelligently.  The python package interfaces with a postgres database enabling efficient geospatial/temporal queries.  OceanDB comes with a simple cli that allows users to initialize database & ingest data.  


## Build Docker Image
This repository is configured such that we are using Docker to run the python package & a self hosted postgres container.  If Docker is not installed you will need to install it & docker-compose.    

1. **Running Postgres**
   ```bash
   make run_postgres // runs postgres postgis in docker compose
   ```
   
2. **Build OceanDB Python Image**
   ```bash
   make build_image
   ```
## Configuring Database Connection
2. **Edit config.yaml** 

   Copy config.example.yaml to config.yaml, and modify postgres connection values if using an external postgres instance. (If using local docker postgres instance, the default credentials will work)

## Deploying & Initializing OceanDB
To Initialize the database run the following commands
1. **Running Container**
   ```bash
   make shell // Open shell in docker container with Oceandb package installed 
   ```
   Subsequent commands will be run from this shell
2. **Initializing the Database**
   ```bash
   oceandb init // Creates the database tables 
   ```

3. **Ingesting Data**

   Downloads data from Copernicus Marine Service over given date range & ingests into postgres.  Will take a long time.  
    ```bash
   oceandb ingest --start-date 2021-02-01 --end-date 2024-03-01 --missions all // Ingest all date between start-date & end-date
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