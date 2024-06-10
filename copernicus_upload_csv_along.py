import glob
import psycopg
from psycopg import sql

#Uploaded Along Track files that were converted from NetCDF to .csv. 

directory = '/Users/briancurtis/Documents/Eddy/Along_files2'
outdir = '/Users/briancurtis/Documents/Eddy/Along_csv2'
loadfile = outdir + '/alongtrackcop1.csv'
# query = 'COPY cop_along (nme, track, cycle, lat, lon, sla_unfiltered, sla_filtered, time, dac, ocean_tide, internal_tide, lwe, mdt, tpa_correction) FROM STDIN WITH (FORMAT CSV, HEADER)'

with psycopg.connect('host=eddydb-psi.cxintbkhx78d.us-east-2.rds.amazonaws.com port=5432 dbname=eddy user=postgres password=bX3vV5uV2nq(8!V') as conn:
	query = sql.SQL("COPY {table} ({fields}) FROM STDIN WITH (FORMAT CSV, HEADER)").format(
	fields=sql.SQL(',').join([
		sql.Identifier('nme'),
		sql.Identifier('track'),
		sql.Identifier('cycle'),
		sql.Identifier('lat'),
		sql.Identifier('lon'),
		sql.Identifier('sla_unfiltered'),
		sql.Identifier('sla_filtered'),
		sql.Identifier('time'),
		sql.Identifier('dac'),
		sql.Identifier('ocean_tide'),
		sql.Identifier('internal_tide'),
		sql.Identifier('lwe'),
		sql.Identifier('mdt'),
		sql.Identifier('tpa_correction'),
	]),
	table=sql.Identifier('cop_along'),
	)
	i = 5070
	for filename in glob.glob(outdir + '/*.csv'):
	# for filename in glob.glob(loadfile):
		with conn.cursor() as cur:
			with open(filename, 'r') as f:
				# f = f.read()
				with cur.copy(query) as copy:
					while data := f.read(100):
						copy.write(data)
			conn.commit()
		print(i)
		i += 1

print('Done')