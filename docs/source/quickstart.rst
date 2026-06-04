Quickstart
==========

For a minimal local setup:

1. Configure your ``.env`` file with your PostgreSQL settings and data directories.
2. Initialize a postgres database.

   .. code-block:: sh

      oceandb init

3. Download the along-track and, if needed, eddy datasets.

   .. code-block:: sh

      oceandb download j2 --start-date 2013-01-01 --end-date 2013-01-31 --yes
      oceandb download-eddy --yes

4. Ingest the downloaded data into OceanDB.

   .. code-block:: sh

      oceandb ingest-along-track j2 --start-date 2013-01-01 --end-date 2013-01-31
      oceandb ingest-eddy

5. Create indices so queries perform reliably.

   .. code-block:: sh

      oceandb index create --default

