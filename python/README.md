
## Installing PostgreSQL

We need to install PostgreSQL with the PostGIS extension. On macOS, there is a [nice simple installer](https://postgresapp.com) which includes the necessary components. Open up the app, and then hit `initialize` and now the PostgreSQL server is running locally on our machine.

Now we need to interact with the server as a client from within python. The [psycopg](https://www.psycopg.org) package is the primary interface we will use. There is a pre-compiled binary available which should work fine, but if not, you can just request `psycopg`.

```
pip install --upgrade pip
pip install "psycopg[binary,pool]"
```

It is also useful to download [pgAdmin](https://www.pgadmin.org), which is a postgresql client.

## Initialize the along-track database

As of 2024-07-12, I am piecing together code snippets and building a class `AlongTrack.py` which will handling building, indexing, populating, and querying the database. The script `create_along_track_db.py` shows basic usage.

## Downloading the data
Start by installing [copernicusmarine](https://help.marine.copernicus.eu/en/articles/7970514-copernicus-marine-toolbox-installation),
```
python3 -m pip install copernicusmarine
```

To download the data, you need to [setup credential](https://help.marine.copernicus.eu/en/articles/8185007-copernicus-marine-toolbox-credentials-configuration).
```
copernicusmarine login
```

I now run the script `copernicus_download_netcdf_data.py` with my own paths set.

## Naming conventions for the database

- snake_case for all variable names
- table names should be snake case and singular, e.g., ocean_basin, ocean_basin_connection, along_track
- verbosity and clarity, avoid abbreviations when possible.
- longitude, latitude, (point, boundary?), e.g., ocean_basin.boundary
- use `date_time` for a reference date
- index names follow the format: {table_name}_{field_name}_idx
- primary key *generally* should follow: {table_name}_id


## Notes

https://www.postgresql.org/docs/current/populate.html
https://www.postgresql.org/docs/16/sql-copy.html
