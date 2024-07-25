import duckdb
import pyarrow as pa
import netCDF4 as nc
from psycopg2 import sql
import struct
from io import BytesIO
import glob
import os
import time
from dateutil.relativedelta import relativedelta

# Connection string
def db_connection():
	conn = duckdb.connect("/Users/briancurtis/Documents/Eddy/eddydbs/eddy_duckdb.db")
	return conn

# Define the function to extract data from NetCDF file
def extract_data_from_netcdf(file_path):
	# Open the NetCDF file
	ds = nc.Dataset(file_path, 'r')

	import_metadata_to_psql(ds)

	# Don't scale most variables so they are stored more efficiently in db
	ds.variables['sla_unfiltered'].set_auto_maskandscale(False)
	ds.variables['sla_filtered'].set_auto_maskandscale(False)
	ds.variables['ocean_tide'].set_auto_maskandscale(False)
	ds.variables['internal_tide'].set_auto_maskandscale(False)
	ds.variables['lwe'].set_auto_maskandscale(False)
	ds.variables['mdt'].set_auto_maskandscale(False)
	ds.variables['dac'].set_auto_maskandscale(False)
	ds.variables['tpa_correction'].set_auto_maskandscale(False)

	time_data = ds.variables['time'] # Extract dates from the dataset and convert them to to standard datetime


	time_data = nc.num2date(time_data[:],time_data.units, only_use_cftime_datetimes=False, only_use_python_datetimes=False)
	time_data = nc.date2num(time_data[:], "microseconds since 2000-01-01 00:00:00") #Convert the standard date back to the 8-byte integer PSQL uses
	lat_data = ds.variables['latitude'][:]
	lon_data = ds.variables['longitude'][:]
	cycle_data = ds.variables['cycle'][:]
	track_data = ds.variables['track'][:]
	sla_un_data = ds.variables['sla_unfiltered'][:]
	sla_f_data = ds.variables['sla_filtered'][:]
	dac_data = ds.variables['dac'][:]
	o_tide_data = ds.variables['ocean_tide'][:]
	i_tide_data = ds.variables['internal_tide'][:]
	lwe_data = ds.variables['lwe'][:]
	mdt_data = ds.variables['mdt'][:]
	tpa_corr_data = ds.variables['tpa_correction'][:]

	ds.close()

	return time_data, lat_data, lon_data, cycle_data, track_data, sla_un_data, sla_f_data, dac_data, o_tide_data, i_tide_data, lwe_data, mdt_data, tpa_corr_data

# Define the function to import data into PostgreSQL using copy_from in binary format
def import_data_to_postgresql(fname, time_data, lat_data, lon_data, cycle_data, track_data, sla_un_data, sla_f_data, dac_data, o_tide_data, i_tide_data, lwe_data, mdt_data, tpa_corr_data):
	conn = db_connection()
	cur = conn.cursor()
	# Create table if not exists
# =============================================================================
# 	create_table_query = '''
# -- Table: public.cop_along

# -- DROP TABLE IF EXISTS public.cop_along;

# CREATE TABLE IF NOT EXISTS public.cop_along
# (
# 	idx bigint NOT NULL GENERATED ALWAYS AS IDENTITY ( INCREMENT 1 START 1 MINVALUE 1 MAXVALUE 9223372036854775807 CACHE 1 ),
# 	nme text COLLATE pg_catalog."default",
# 	track smallint,
# 	cycle smallint,
# 	lat double precision,
# 	lon double precision,
# 	sla_unfiltered smallint,
# 	sla_filtered smallint,
# 	"time" timestamp without time zone,
# 	dac smallint,
# 	ocean_tide smallint,
# 	internal_tide smallint,
# 	lwe smallint,
# 	mdt smallint,
# 	tpa_correction smallint,
# 	cat_point geometry(Point,4326) GENERATED ALWAYS AS (st_setsrid(st_makepoint(lon, lat), 4326)) STORED,
# 	CONSTRAINT cop_along_pkey PRIMARY KEY ("time", idx)
# ) PARTITION BY RANGE ("time");

# ALTER TABLE IF EXISTS public.cop_along
# 	OWNER to postgres;
# -- Index: cat_pt_date

# -- DROP INDEX IF EXISTS public.cat_pt_date;

# CREATE INDEX IF NOT EXISTS cat_pt_date
# 	ON public.cop_along USING gist
# 	(cat_point)
# 	WITH (buffering=auto);
# -- Index: cat_pt_date_idx

# -- DROP INDEX IF EXISTS public.cat_pt_date_idx;

# CREATE INDEX IF NOT EXISTS cat_pt_date_idx
# 	ON public.cop_along USING gist
# 	(cat_point, ("time"::date))
# 	WITH (buffering=auto);
# -- Index: cat_pt_idx

# -- DROP INDEX IF EXISTS public.cat_pt_idx;

# CREATE INDEX IF NOT EXISTS cat_pt_idx
# 	ON public.cop_along USING gist
# 	(cat_point)
# 	WITH (buffering=auto);
# -- Index: date_idx

# -- DROP INDEX IF EXISTS public.date_idx;

# CREATE INDEX IF NOT EXISTS date_idx
# 	ON public.cop_along USING btree
# 	(("time"::date) ASC NULLS LAST)
# 	WITH (deduplicate_items=True);
# -- Index: nme_alng_idx

# -- DROP INDEX IF EXISTS public.nme_alng_idx;

# CREATE INDEX IF NOT EXISTS nme_alng_idx
# 	ON public.cop_along USING btree
# 	(nme COLLATE pg_catalog."default" ASC NULLS LAST)
# 	WITH (deduplicate_items=True);
# 	'''
# 	try:
# 		cur.execute(create_table_query)
# 	except:
# 		print('Could not create database')
# 	conn.commit()
# =============================================================================

	# Prepare the data in binary format
	output = BytesIO()
	output.write(struct.pack('!11sii', b'PGCOPY\n\377\r\n\0', 0, 0))  # Write the PostgreSQL COPY binary header

	fname_binary = str.encode(fname)
	str_len = len(fname_binary)

	# See the Struct docs for explanations of the letter codes used to pack the data: https://docs.python.org/3/library/struct.html
	for i in range(len(time_data)):
		output.write(struct.pack('!h', 14))  # Number of fields
		output.write(struct.pack('!i' + str(str_len) + 's', str_len, fname_binary))
		output.write(struct.pack('!ih', 2, track_data[i]))
		output.write(struct.pack('!ih', 2, cycle_data[i]))
		output.write(struct.pack('!id', 8, lat_data[i]))
		output.write(struct.pack('!id', 8, lon_data[i]))
		output.write(struct.pack('!ih', 2, sla_un_data[i]))
		output.write(struct.pack('!ih', 2, sla_f_data[i]))
		output.write(struct.pack('!iq', 8, time_data[i])) # PSQL stores time as an 8-byte integer
		output.write(struct.pack('!ih', 2, dac_data[i]))
		output.write(struct.pack('!ih', 2, o_tide_data[i]))
		output.write(struct.pack('!ih', 2, i_tide_data[i]))
		output.write(struct.pack('!ih', 2, lwe_data[i]))
		output.write(struct.pack('!ih', 2, mdt_data[i]))
		output.write(struct.pack('!ih', 2, tpa_corr_data))

	output.write(struct.pack('!h', -1))  # Write the COPY binary trailer
	output.seek(0)
	# Connect to the PostgreSQL database
	insert_query = "COPY cop_along ( nme, track, cycle, lat, lon, sla_unfiltered, sla_filtered, time, dac, ocean_tide, internal_tide, lwe, mdt, tpa_correction) FROM STDIN WITH (FORMAT binary);"
	try:
		cur.copy_expert(insert_query, output)
	except Exception as err:
		print(f"Insert failed for {fname}: {err}")
	conn.commit()
	conn.close()
# End import_data_to_postgresql

def import_metadata_to_psql(ds):


	conn = db_connection()
	query = sql.SQL("INSERT INTO {table} ({fields}) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s,%s,%s,%s,%s,%s,%s, %s,%s,%s,%s,%s,%s,%s,%s)").format(
	fields=sql.SQL(',').join([
		sql.Identifier('nme'),
		sql.Identifier('conventions'),
		sql.Identifier('metadata_conventions'),
		sql.Identifier('cdm_data_type'),
		sql.Identifier('comment'),
		sql.Identifier('contact'),
		sql.Identifier('creator_email'),
		sql.Identifier('creator_name'),
		sql.Identifier('creator_url'),
		sql.Identifier('date_created'),
		sql.Identifier('date_issued'),
		sql.Identifier('date_modified'),
		sql.Identifier('history'),
		sql.Identifier('institution'),
		sql.Identifier('keywords'),
		sql.Identifier('license'),
		sql.Identifier('platform'),
		sql.Identifier('processing_level'),
		sql.Identifier('product_version'),
		sql.Identifier('project'),
		sql.Identifier('references'),
		sql.Identifier('software_version'),
		sql.Identifier('source'),
		sql.Identifier('ssalto_duacs_comment'),
		sql.Identifier('summary'),
		sql.Identifier('title'),
	]),
	table=sql.Identifier('cop_meta'),
	)
	curs = conn.cursor()
	data = (fname, ds.Conventions, ds.Metadata_Conventions, ds.cdm_data_type, ds.comment, ds.contact, ds.creator_email, ds.creator_name, ds.creator_url, ds.date_created, ds.date_issued, ds.date_modified, ds.history, ds.institution, ds.keywords, ds.license, ds.platform, ds.processing_level, ds.product_version, ds.project, ds.references, ds.software_version, ds.source, ds.ssalto_duacs_comment, ds.summary, ds.title)

	curs.execute(query, data)
	conn.commit()
	conn.close()

# Create partition. Partition duration is in months. For now only year or monthly partitions will be allowed
def create_partitions(min_date, max_date, partition_duration=12):
	if partition_duration != 12 and partition_duration != 1:
		raise ValueError('Partitions must be either yearly (12) or monthly(1)')
	dates = [min_date, max_date]
	conn = db_connection()
	cur = conn.cursor()
	for date in dates:
		year = date.year
		month = 1
		month_str = '01'
		month_partition_str = ''
		this_year = date.replace(day = 1, month=1, hour=0, minute=0, second=0, microsecond=0)
		next_year = date + relativedelta(years=1)
		next_year = next_year.replace(day = 1, month=1, hour=0, minute=0, second=0, microsecond=0)
		this_month = date.replace(day = 1, hour=0, minute=0, second=0, microsecond=0)
		next_month = date + relativedelta(months=1 )
		next_month = next_month.replace(day = 1, hour=0, minute=0, second=0, microsecond=0)
		to_partition = next_year
		min_partition = this_year

		if partition_duration == 1:
			month = date.month
			if month <= 9:
				month_str = f"{0}{month}"
			else:
				month_str = f"{month}"
			month_partition_str = month_str
			to_partition = next_month
			min_partition = this_month

		partition_name = f"cop_along_{str(year)}{month_partition_str}" # Monthly partions are named cop_along_YYYYMM and yearly are named cop_along_YYYY
		min_partition_date = f"{min_partition}"

		query = f"""CREATE TABLE IF NOT EXISTS {partition_name} PARTITION OF cop_along
			FOR VALUES FROM ('{min_partition_date}') TO ('{to_partition}');"""
# 		print(query)
		if partition_name not in partitions_created:
			try:
				cur.execute(query)
			except Exception as err:
				print(f"Unable to create partition: {err}")
			else:
				partitions_created.append(partition_name)
				conn.commit()
	cur.close()
	conn.close()
	return




# Main function
if __name__ == "__main__":
	directory = '/Users/briancurtis/Documents/Eddy/Along_files2'
	partitions_created = [] # Keep track of partitions created so we can avoid round trips to the database.
	start = time.time()
# 	i = 1
	for filename in glob.glob(directory + '/*.nc'):
		names = [os.path.basename(x) for x in glob.glob(filename)]
		fname = names[0] #filename will be used to link data to metadata
		time_data, lat_data, lon_data, cycle_data, track_data, sla_un_data, sla_f_data, dac_data, o_tide_data, i_tide_data, lwe_data, mdt_data, tpa_corr_data = extract_data_from_netcdf(filename)
		import_start = time.time()
		import_data_to_postgresql(fname, time_data, lat_data, lon_data, cycle_data, track_data, sla_un_data, sla_f_data, dac_data, o_tide_data, i_tide_data, lwe_data, mdt_data, tpa_corr_data)
		import_end = time.time()
		print(f"{fname} import time: {import_end - import_start}")
# 		i += 1
# 		if i == 100:
# 			break
	end = time.time()
	print(f"Script end. Total time: {end - start}")