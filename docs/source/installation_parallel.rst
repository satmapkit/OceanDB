Simple Installation
===================


Prerequisites
-------------
You will need on your system:

* Docker
* python 3.10 or above
* git


Source Data
-----------
At present, the eddy data is expected to be formatted as follows:

.. code-block:: text

   eddy_data_dir/
   |- META3.2_DT_allsat_Anticyclonic_long_19930101_20220209.nc
   |- META3.2_DT_allsat_Cyclonic_long_19930101_20220209.nc

The along-track data is expected to be formatted as follows:

.. code-block:: text

   along_track_data_dir/
   |- SEALEVEL_GLO_PHY_L3_MY_008_062
      | - cmems_obs-sl_glo_phy-ssh_my_al-l3-duacs_PT1S_202411
      | - cmems_obs-sl_glo_phy-ssh_my_alg-l3-duacs_PT1S_202411
      | - ...
      | - cmems_obs-sl_glo_phy-ssh_my_tpn-l3-duacs_PT1S_202411

    

Create A Shared Development Directory
-------------------------------------

For example:

.. code-block:: text

   satmapkit-dev/
   |- .venv/
   |- OceanDB/
   |- MapInterp/

This keeps one virtual environment and multiple related repositories in the
same parent directory.

Create the directory and virtual environment:

.. code-block:: bash

   mkdir satmapkit-dev
   cd satmapkit-dev
   python -m venv .venv
   source .venv/bin/activate

Clone And Install OceanDB
-------------------------

.. code-block:: bash

   git clone https://github.com/Nazanne/OceanDB.git
   pip install OceanDB

The ``dev`` extras include the documentation tooling used by ``docs/Makefile``.

Configure OceanDB
-----------------

Copy the example environment file:

.. code-block:: bash

   cp OceanDB/.env.example .env

Then edit ``.env`` with your database settings and local data directories:

.. code-block:: text

   POSTGRES_HOST=localhost
   POSTGRES_USERNAME=postgres
   POSTGRES_PASSWORD=postgres
   POSTGRES_PORT=5432
   POSTGRES_DATABASE=ocean

   ALONG_TRACK_DATA_DIRECTORY=/absolute/path/to/along_track
   EDDY_DATA_DIRECTORY=/absolute/path/to/eddy

   COPERNICUS_USERNAME=your_username
   COPERNICUS_PASSWORD=your_password

If you plan to use ``oceandb download``, create a Copernicus Marine account
first.

Start PostgreSQL
----------------

Use the repository's local development database target
(this will require the docker daemon be running):

.. code-block:: bash

   cd OceanDB
   make run_postgres

Initialize OceanDB
------------------

Create the database schema, partitions, and reference data:

.. code-block:: bash

   oceandb init
   oceandb create-indices

Ingest along-track and eddy data
------------------

Create the database schema, partitions, and reference data:

.. code-block:: bash

   oceandb ingest-along-track --start-date 2024-01-01 --end-date 2024-01-31
   oceandb ingest-eddy
   oceandb create-indices


