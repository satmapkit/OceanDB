import netCDF4 as nc
import psycopg2 as ps
from psycopg2 import sql


class AlongTrackDatabase:
    dbName: str = 'along-track'
    alongTrackTableName: str = 'alongTrack'

    def __init__(self, host, username, port="5432"):
        self.host = host
        self.username = username
        self.port = port

    def connection(self):
        conn = ps.connect(host=self.host,
                          dbname=self.dbName,
                          user=self.username,
                          port=self.port
                          )
        return conn

    def create_database(self):
        pg_conn = ps.connect(dbname="postgres", user=self.username, host=self.host, port=self.port)
        pg_conn.autocommit = True  # Enable autocommit to execute CREATE DATABASE command

        # SQL module usage: https://www.psycopg.org/docs/sql.html#module-psycopg2.sql
        cur = pg_conn.cursor()
        cur.execute(f"SELECT 1 FROM pg_database WHERE datname = '{self.dbName}'")
        exists = cur.fetchone()
        if exists is not None:
            print(f"Database '{self.dbName}' already exists.")
        else:
            # Create the new database
            cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(self.dbName)))
            atdb_conn = self.connection()
            atdb_conn.autocommit = True
            atdb_cur = atdb_conn.cursor()

            atdb_cur.execute(sql.SQL("CREATE EXTENSION IF NOT EXISTS plpgsql;"))
            atdb_cur.execute(sql.SQL("CREATE EXTENSION IF NOT EXISTS postgis;"))

            atdb_cur.close()
            atdb_conn.close()

            print(f"Database '{self.dbName}' created successfully.")

        # Close the cursor and connection
        cur.close()
        pg_conn.close()

    def drop_database(self):
        conn = ps.connect(dbname="postgres", user=self.username, host=self.host, port=self.port)
        conn.autocommit = True  # Enable autocommit to execute CREATE DATABASE command

        # Create a cursor object
        cur = conn.cursor()

        # Create the new database
        cur.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(self.dbName)))

        # Close the cursor and connection
        cur.close()
        conn.close()

        print(f"Database '{self.dbName}' dropped.")

    def create_along_track_table(self):
        query = f"""
        CREATE TABLE IF NOT EXISTS public.{self.alongTrackTableName}
        (
            idx bigint NOT NULL GENERATED ALWAYS AS IDENTITY ( INCREMENT 1 START 1 MINVALUE 1 MAXVALUE 9223372036854775807 CACHE 1 ),
            nme text COLLATE pg_catalog."default",
            track smallint,
            cycle smallint,
            lat double precision,
            lon double precision,
            sla_unfiltered smallint,
            sla_filtered smallint,
            "time" timestamp without time zone,
            dac smallint,
            ocean_tide smallint,
            internal_tide smallint,
            lwe smallint,
            mdt smallint,
            tpa_correction smallint,
            cat_point geometry(Point,4326) GENERATED ALWAYS AS (st_setsrid(st_makepoint(lon, lat), 4326)) STORED,
            CONSTRAINT cop_along_pkey PRIMARY KEY ("time", idx)
        ) PARTITION BY RANGE ("time");
        """

        conn = self.connection()
        cur = conn.cursor()

        cur.execute(query)

        cur.close()
        conn.close()

        print(f"Table '{self.alongTrackTableName} added to database '{self.dbName}'.")
