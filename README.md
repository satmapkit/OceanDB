
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

## Installing PostgreSQL

We now need to install PostgreSQL with the PostGIS extension. On macos, there is a [nice simple installer](https://postgresapp.com) which includes the necessary components. Open up the app, and then hit `initialize` and now the PostgreSQL server is running locally on our machine.

Now we need to interact with the server as a client.
```
pip install psycopg2 psycopg2-binary
```

https://www.postgresql.org/docs/current/populate.html
https://www.postgresql.org/docs/16/sql-copy.html
