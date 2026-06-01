OceanDB
=======

OceanDB is a python package for managing oceanic satellite data intelligently. The python package interfaces with a postgres database enabling efficient geospatial/temporal queries. OceanDB comes with a simple CLI that allows users to initialize the database and ingest data.


See the detailed instructions linked below for the full setup and workflow.

Getting Started
---------------

To get started with OceanDB, perform the following actions:

1. Install OceanDB and set up the surrounding tools you need for local development.
   See :doc:`Installation <setup/installation>`.
2. Configure your ``.env`` file and choose how Postgres will run.
   See :doc:`Postgres Setup <setup/postgres>`.
3. Download SLA along-track data and, if needed, eddy data.
   See :doc:`Download Data <setup/download>`.
4. Ingest the downloaded data into Postgres.
   See :doc:`Ingest Data <setup/ingest>`.
5. Build indexes so OceanDB queries perform reliably.
   See :doc:`Index Management <setup/index_management>`.
6. Run OceanDB queries against along-track and eddy datasets.
   See :doc:`Queries <setup/queries>`.
7. Review the dataset and query architecture.
   See :doc:`Architecture <architecture>`.
