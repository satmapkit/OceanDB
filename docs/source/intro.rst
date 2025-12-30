Introduction
============

OceanDB is a python package for managing oceanic satellite data intelligently.  The python package interfaces with a postgres database enabling efficient geospatial/temporal queries.  OceanDB comes with a simple CLI that allows users to initialize the database and ingest data.  

Installation Instructions
-------------------------

1. **Create a Copernicus Marine account (if needed)**

   If you don't already have a Copernicus Marine account, create one. 

1. **Clone OceanDB repository**

2. **Configure the .env file**
   
   Open the OceanDB directory, and copy the example *.env.example* file to *.env*.
   Open the new *.env* file, and edit to set the postgres server and directories to download the data. 

   .. code-block:: bash
 
      POSTGRES_HOST=postgres
      POSTGRES_USERNAME=postgres
      POSTGRES_PASSWORD=postgres
      POSTGRES_PORT=5432
      POSTGRES_DATABASE=ocean
      
      ALONG_TRACK_DATA_DIRECTORY=/app/data/copernicus
      EDDY_DATA_DIRECTORY=/app/data/eddies
      
      COPERNICUS_PASSWORD=copernicus_marine_service_password_placeholder
      COPERNICUS_USERNAME=copernicus_marine_service_username


4. **Setup python environment**
   
   The details depend on how you use python, e.g. from the command line or an IDE like PyCharm. These instructions are specific to PyCharm.
   

1. **Install OceanDB**
   
   With your python environment activated

   .. code-block:: bash
   
      pip install OceanDB 
      pip install -e OceanDB # editable install for development
   

OceanDB Initialization Instructions
-----------------------------------

The OceanDB package provides a CLI for initializing the database and ingesting data.
1. **Initializing the Database**

   .. code-block:: bash
   
      oceandb init // Creates the database tables 

2. **Ingesting Data** 

With Copernicus Marine Service Along Track downloaded to your computer, ensure that the `ALONG_TRACK_DATA_DIRECTORY` environment variable is set correctly in
the *.env* file.  `ALONG_TRACK_DATA_DIRECTORY` should be the file path to the directory at which `SEALEVEL_GLO` exists (e.g. `ALONG_TRACK_DATA_DIRECTORY=/path/to//copernicus`)




By default if no arguments are provided this CLI command will iterate over all of the data

.. code-block:: bash

    oceandb ingest // Ingest all missions across all date ranges
    oceandb ingest -m s3a  // Ingest all Sentinel-3A (s3a) mission data
    oceandb ingest -m s3a -m j3 // Ingest multiple missions
    oceandb ingest -m j3 --start-date 2019-01-01 --end-date 2020-12-3 // Ingest data from specific missions between start-date and end-date  

 
4. **Querying SLA Data**
   
   To query the sea level anomaly for a given satellite mission, time range & radius around a given point

   .. code-block:: python
   
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
   
## Docker Instructions

1. **Running Postgres**
   
   If you want to spin up a postgres development container with docker-compose

   .. code-block:: bash

      make run_postgres // runs postgres postgis in docker compose
   
3. **Build OceanDB Python Image**
   If building a development image 

   .. code-block:: bash

      make build_image
