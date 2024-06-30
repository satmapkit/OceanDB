#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jun 13 07:49:15 2024

@author: briancurtis

Need to install DuckDB

"""

import duckdb

with duckdb.connect("/Users/briancurtis/Documents/Eddy/eddydbs/eddy_duckdb.db") as conn:
# 	try:
# 		conn.sql("create table if not exists test (id integer, testfield varchar);")
# 	except Exception as err:
# 		print(f"Error: {err}")
# 	conn.sql("INSERT INTO test VALUES (42, 'what does it all mean?');")
# 	conn.sql("select * from information_schema.tables").show()
# 	conn.table("test").show()
	try:
		conn.sql('CREATE SEQUENCE IF NOT EXISTS id_sequence START 1;')
		conn.sql('INSTALL spatial;')
		conn.sql('LOAD spatial;')
		conn.sql('''CREATE TABLE IF NOT EXISTS cop_along
(
	id BIGINT DEFAULT nextval('id_sequence'),
	nme VARCHAR,
	track smallint,
	cycle smallint,
	lat double,
	lon double,
	sla_unfiltered smallint,
	sla_filtered smallint,
	"time" timestamp,
	dac smallint,
	ocean_tide smallint,
	internal_tide smallint,
	lwe smallint,
	mdt smallint,
	tpa_correction smallint,
	cat_point geometry
);''') # Stored generated fields are not yet implemented in DuckDB so the point will have to be generated prior to import.
	except Exception as err:
		print(f"Error: {err}")

	try:
		conn.sql('''CREATE INDEX IF NOT EXISTS cat_pt_date
			ON cop_along
			(cat_point);''')
	except Exception as err:
		print(f"Error1: {err}")

	try:
		conn.sql('''
-- Index: cat_pt_date_idx

-- DROP INDEX IF EXISTS cat_pt_date_idx;

CREATE INDEX IF NOT EXISTS cat_pt_date_idx
	ON cop_along
	(cat_point, ("time"::date));
-- Index: cat_pt_idx

-- DROP INDEX IF EXISTS cat_pt_idx;

CREATE INDEX IF NOT EXISTS cat_pt_idx
	ON cop_along
	(cat_point);
-- Index: date_idx

-- DROP INDEX IF EXISTS date_idx;

CREATE INDEX IF NOT EXISTS date_idx
	ON cop_along
	(("time"::date));
-- Index: nme_alng_idx

-- DROP INDEX IF EXISTS nme_alng_idx;

CREATE INDEX IF NOT EXISTS nme_alng_idx
	ON cop_along
	(nme);''')
	except Exception as err:
		print(f"Error2: {err}")

	try:
		conn.sql('''CREATE TABLE
  cop_meta (
 nme text NOT NULL,
 conventions text NULL,
 metadata_conventions text NULL,
 cdm_data_type text NULL,
 comment
   text NULL,
   contact text NULL,
   creator_email text NULL,
   creator_name text NULL,
   creator_url text NULL,
   date_created timestamp,
   date_issued timestamp,
   date_modified timestamp,
   history text NULL,
   institution text NULL,
   keywords text NULL,
   license text NULL,
   platform text NULL,
   processing_level text NULL,
   product_version text NULL,
   project text NULL,
   "references" text NULL,
   software_version text NULL,
   source text NULL,
   ssalto_duacs_comment text NULL,
   summary text NULL,
   title text NULL
  );''')
	except Exception as err:
		print(f"cop_meta error: {err}")




