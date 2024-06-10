import netCDF4 as nc
import psycopg2 as ps
from psycopg2 import sql
import struct
from io import BytesIO
import glob
import os

# Connection string
def db_connection():
	conn = ps.connect(host="localhost",
		dbname="eddy_local",
		user="postgres",
		password="eJ^n$+%Ghwq3#oFW"
	)
	return conn

# Define the function to extract data from NetCDF file
def extract_data_from_netcdf(file_path):
	# Open the NetCDF file
	ds = nc.Dataset(file_path, 'r')

	import_metadata_to_psql(ds)

	# Print out the variables in the file
	print(ds.variables.keys())

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


	# Create table if not exists
# =============================================================================
# 	create_table_query = '''
# 	CREATE TABLE IF NOT EXISTS public.cop_along (
# 		idx bigint NOT NULL GENERATED ALWAYS AS IDENTITY,
# 		nme text NULL,
# 		track smallint NULL,
# 		cycle smallint NULL,
# 		lat double precision NULL,
# 		lon double precision NULL,
# 		sla_unfiltered smallint NULL,
# 		sla_filtered smallint NULL,
# 		"time" timestamp without time zone NULL,
# 		dac smallint NULL,
# 		ocean_tide smallint NULL,
# 		internal_tide smallint NULL,
# 		lwe smallint NULL,
# 		mdt smallint NULL,
# 		tpa_correction smallint NULL,
# 		cat_point geometry (Point, 4326) NULL DEFAULT st_setsrid (st_makepoint (lon, lat), 4326)
# 	);

# 	ALTER TABLE
# 		public.cop_along
# 	ADD
# 		CONSTRAINT cop_along_pkey PRIMARY KEY (idx)
# 	'''
# 	cur.execute(create_table_query)
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
	conn = db_connection()
	cur = conn.cursor()# 	postgreSQL_select_Query = 'SELECT * FROM cop_along limit 10;'
	insert_query = "COPY cop_along ( nme, track, cycle, lat, lon, sla_unfiltered, sla_filtered, time, dac, ocean_tide, internal_tide, lwe, mdt, tpa_correction) FROM STDIN WITH (FORMAT binary);"
	cur.copy_expert(insert_query, output)
	conn.commit()
	conn.close()
# End import_data_to_postgresql

def import_metadata_to_psql(ds):
	# 	create_table_query = '''
	# 		CREATE TABLE
	#   public.cop_meta (
	#     nme text NOT NULL,
	#     conventions text NULL,
	#     metadata_conventions text NULL,
	#     cdm_data_type text NULL,
	#     comment
	#       text NULL,
	#       contact text NULL,
	#       creator_email text NULL,
	#       creator_name text NULL,
	#       creator_url text NULL,
	#       date_created timestamp with time zone NULL,
	#       date_issued timestamp with time zone NULL,
	#       date_modified timestamp with time zone NULL,
	#       history text NULL,
	#       institution text NULL,
	#       keywords text NULL,
	#       license text NULL,
	#       platform text NULL,
	#       processing_level text NULL,
	#       product_version text NULL,
	#       project text NULL,
	#       "references" text NULL,
	#       software_version text NULL,
	#       source text NULL,
	#       ssalto_duacs_comment text NULL,
	#       summary text NULL,
	#       title text NULL
	#   );

	# ALTER TABLE
	#   public.cop_meta
	# ADD
	#   CONSTRAINT cop_meta_pkey PRIMARY KEY (nme)
	# 	'''
	# 	cur.execute(create_table_query)
	#  	conn.commit()

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



# Main function
if __name__ == "__main__":
	directory = '/Users/briancurtis/Documents/Eddy/Along_files2'
	for filename in glob.glob(directory + '/*.nc'):
		names = [os.path.basename(x) for x in glob.glob(filename)]
		fname = names[0] #filename will be used to link data to metadata
		time_data, lat_data, lon_data, cycle_data, track_data, sla_un_data, sla_f_data, dac_data, o_tide_data, i_tide_data, lwe_data, mdt_data, tpa_corr_data = extract_data_from_netcdf(filename)
		import_data_to_postgresql(fname, time_data, lat_data, lon_data, cycle_data, track_data, sla_un_data, sla_f_data, dac_data, o_tide_data, i_tide_data, lwe_data, mdt_data, tpa_corr_data)
		break