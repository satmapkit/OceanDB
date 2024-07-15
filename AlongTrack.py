import netCDF4 as nc
import psycopg as pg
from psycopg import sql
from sys import exit

class AlongTrackDatabase:
    dbName: str = 'along_track'
    alongTrackTableName: str = 'alongTrack'

    def __init__(self, host, username, port="5432"):
        self.host = 'localhost'
        self.username = 'postgres'
        self.port = 5432

    def connection(self):
        conn = pg.connect(host=self.host,
                          dbname=self.dbName,
                          user=self.username,
                          port=self.port
                          )
        return conn

    def create_database(self):
        with pg.connect(dbname="postgres", user=self.username, host=self.host, port=self.port) as pg_conn:
            pg_conn.autocommit = True  # Enable autocommit to execute CREATE DATABASE command

            # SQL module usage: https://www.psycopg.org/docs/sql.html#module-psycopg2.sql
            with pg_conn.cursor() as cur:
                cur.execute(f"SELECT 1 FROM pg_database WHERE datname = '{self.dbName}'")
                exists = cur.fetchone()
                if exists is not None:
                    print(f"Database '{self.dbName}' already exists.")
                else:
                    # Create the new database
                    cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(self.dbName)))

                    # Now create a connection to the new db, and add postgis (autocommit set to true!)
                    with self.connection() as atdb_conn:
                        with atdb_conn.cursor() as atdb_cur:
                            atdb_cur.execute(sql.SQL("CREATE EXTENSION IF NOT EXISTS plpgsql;"))
                            atdb_cur.execute(sql.SQL("CREATE EXTENSION IF NOT EXISTS postgis;"))
                    atdb_conn.commit()

            print(f"Database '{self.dbName}' created successfully.")

    def drop_database(self):
        with pg.connect(dbname="postgres", user=self.username, host=self.host, port=self.port) as conn:
            conn.autocommit = True  # Enable autocommit to execute CREATE DATABASE command
            # Create a cursor object
            with conn.cursor() as cur:
                # Create the new database
                cur.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(self.dbName)))

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

        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                conn.commit()

        print(f"Table '{self.alongTrackTableName} added to database '{self.dbName}'.")

    def create_along_track_indices(self):
        query_create_point_index = f"""
            CREATE INDEX IF NOT EXISTS cat_pt_idx
 	        ON public.{self.alongTrackTableName} USING gist
 	        (cat_point)
 	        WITH (buffering=auto);
        """

        query_create_date_index = f"""
            CREATE INDEX IF NOT EXISTS date_idx
            ON public.{self.alongTrackTableName} USING btree
        	(("time"::date) ASC NULLS LAST)
         	WITH (deduplicate_items=True);
        """

        query_create_filename_index = f"""
            CREATE INDEX IF NOT EXISTS nme_alng_idx
         	ON public.{self.alongTrackTableName} USING btree
         	(nme COLLATE pg_catalog."default" ASC NULLS LAST)
         	WITH (deduplicate_items=True);
        """

        # Brian says this may require some additional initialization.
        query_create_combined_point_date_index = f"""
            CREATE INDEX IF NOT EXISTS cat_pt_date_idx
         	ON public.{self.alongTrackTableName} USING gist
         	(cat_point, ("time"::date))
         	WITH (buffering=auto);
        """

        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query_create_point_index)
                cur.execute(query_create_date_index)
                cur.execute(query_create_filename_index)
                conn.commit()

