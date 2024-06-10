import netCDF4 as nc
import pandas as pd
import glob
import psycopg
from psycopg import sql
import os

#Uploaded Along Track files that were converted from NetCDF to .csv. 

directory = '/Users/briancurtis/Documents/Eddy/Along_files2'
outdir = '/Users/briancurtis/Documents/Eddy/Along_csv2'
# loadfile = outdir + '/alongtrack1.csv'
# query = 'COPY alongtrk (atindex, reference_orbit, lat, lon, mssh, ssha, datetime, eflag, surface_type, dis_to_coast, bathy) FROM STDIN WITH (FORMAT CSV, HEADER)'

# with psycopg.connect('host=eddydb-psi.cxintbkhx78d.us-east-2.rds.amazonaws.com port=5432 dbname=eddy user=postgres password=bX3vV5uV2nq(8!V') as conn:
# 	query = sql.SQL("COPY {table} ({fields}) FROM STDIN WITH (FORMAT CSV, HEADER)").format(
# 	fields=sql.SQL(',').join([
# 		sql.Identifier('atindex'),
# 		sql.Identifier('reference_orbit'),
# 		sql.Identifier('lat'),
# 		sql.Identifier('lon'),
# 		sql.Identifier('mssh'),
# 		sql.Identifier('ssha'),
# 		sql.Identifier('datetime'),
# 		sql.Identifier('eflag'),
# 		sql.Identifier('surface_type'),
# 		sql.Identifier('dis_to_coast'),
# 		sql.Identifier('bathy'),
# 	]),
# 	table=sql.Identifier('alongtrk'),
# 	)
# 	i = 2
# 	for filename in glob.glob(outdir + '/*.csv'):
# 		with conn.cursor() as cur:
# 			with open(filename, 'r') as f:
# 				# f = f.read()
# 				with cur.copy(query) as copy:
# 					while data := f.read(100):
# 						copy.write(data)
# 			conn.commit()
# 		print(i)
# 		i += 1

# Convert downloaded Along Track NetCDF files to .csv

# with conn.cursor() as cur:
# 	cur.execute("SELECT * FROM argo limit 10")
# 	cur.fetchone()
# 	for record in cur:
# 		print(record)
# conn.close()	
# print('all done')

i = 1
# for filename in glob.glob(directory + '/*.nc'):
for filename in glob.glob(directory + '/*.nc'):
	ds = nc.Dataset(filename, 'r')
	names = [os.path.basename(x) for x in glob.glob(filename)]
	fname = names[0] #filename will be used to link data to metadata
	# First read the metadata and insert into table. 
	# cols = list(ds.variables.keys())
	# print(ds.ncattrs())           
	# for attrname in ds.ncattrs():
	# 	print(attrname + ': ' + str(ds.getncattr(attrname)))
	# print(ds.comment)
	# print(cols)
	
	# with psycopg.connect('host=eddydb-psi.cxintbkhx78d.us-east-2.rds.amazonaws.com port=5432 dbname=eddy user=postgres password=bX3vV5uV2nq(8!V') as conn:
	# 	query = sql.SQL("INSERT INTO {table} ({fields}) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s ,%s ,%s ,%s ,%s ,%s ,%s ,%s, %s,%s,%s,%s,%s,%s,%s,%s)").format(
	# 	fields=sql.SQL(',').join([
	# 		sql.Identifier('nme'),
	# 		sql.Identifier('conventions'),
	# 		sql.Identifier('metadata_conventions'),
	# 		sql.Identifier('cdm_data_type'),
	# 		sql.Identifier('comment'),
	# 		sql.Identifier('contact'),
	# 		sql.Identifier('creator_email'),
	# 		sql.Identifier('creator_name'),
	# 		sql.Identifier('creator_url'),
	# 		sql.Identifier('date_created'),
	# 		sql.Identifier('date_issued'),
	# 		sql.Identifier('date_modified'),
	# 		sql.Identifier('history'),
	# 		sql.Identifier('institution'),
	# 		sql.Identifier('keywords'),
	# 		sql.Identifier('license'),
	# 		sql.Identifier('platform'),
	# 		sql.Identifier('processing_level'),
	# 		sql.Identifier('product_version'),
	# 		sql.Identifier('project'),
	# 		sql.Identifier('references'),
	# 		sql.Identifier('software_version'),
	# 		sql.Identifier('source'),
	# 		sql.Identifier('ssalto_duacs_comment'),
	# 		sql.Identifier('summary'),
	# 		sql.Identifier('title'),
	# 	]),
	# 	table=sql.Identifier('cop_meta'),
	# 	)
	# 	with conn.cursor() as curs:
	# 		data = (fname , 
	# 		ds.Conventions , 
	# 		ds.Metadata_Conventions , 
	# 		ds.cdm_data_type , 
	# 		ds.comment , 
	# 		ds.contact , 
	# 		ds.creator_email , 
	# 		ds.creator_name , 
	# 		ds.creator_url , 
	# 		ds.date_created , 
	# 		ds.date_issued , 
	# 		ds.date_modified , 
	# 		ds.history , 
	# 		ds.institution , 
	# 		ds.keywords , 
	# 		ds.license , 
	# 		ds.platform , 
	# 		ds.processing_level , 
	# 		ds.product_version , 
	# 		ds.project , 
	# 		ds.references , 
	# 		ds.software_version , 
	# 		ds.source , 
	# 		ds.ssalto_duacs_comment , 
	# 		ds.summary , 
	# 		ds.title)
	# 		
	# 		curs.execute(query, data)
# 
# 	
# 	# Convert downloaded Along Track NetCDF files to .csv # Importing from CSV is the fastest way to import the data	
# 	
	df_ds = pd.DataFrame()

	# df_ds.columns = ['time', 'longitude', 'latitude', 'cycle', 'track', 'sla_unfiltered', 'sla_filtered', 'dac', 'ocean_tide', 'internal_tide', 'lwe', 'mdt', 'tpa_correction'] # Copernicus
	# df_ds.columns = ['index', 'reference_orbit', 'lat', 'lon', 'mssh' 'ssha' 'time' 'Surface_Type'] # NASA
	# print(ds.variables['Surface_Type'][:])
	ds.variables['sla_unfiltered'].set_auto_maskandscale(False)
	ds.variables['sla_filtered'].set_auto_maskandscale(False)
	ds.variables['ocean_tide'].set_auto_maskandscale(False)
	ds.variables['internal_tide'].set_auto_maskandscale(False)
	ds.variables['lwe'].set_auto_maskandscale(False)
	ds.variables['mdt'].set_auto_maskandscale(False)
	ds.variables['dac'].set_auto_maskandscale(False)
	ds.variables['tpa_correction'].set_auto_maskandscale(False)
	
	df_ds['track'] = ds.variables['track'][:]
	df_ds['cycle'] = ds.variables['cycle'][:]
	df_ds['lat'] = ds.variables['latitude'][:]
	lon = ds.variables['longitude'][:]
	lon = (lon + 180) % 360 - 180
	df_ds['lon'] = lon
	df_ds['sla_unfiltered'] = ds.variables['sla_unfiltered'][:]
	df_ds['sla_filtered'] = ds.variables['sla_filtered'][:]
	time_var = ds.variables['time']
	obtime = nc.num2date(time_var[:],time_var.units, only_use_cftime_datetimes=False, only_use_python_datetimes=False)
	df_ds['time'] = obtime
	df_ds['dac'] = ds.variables['dac'][:]
	df_ds['ocean_tide'] = ds.variables['ocean_tide'][:]
	df_ds['internal_tide'] = ds.variables['internal_tide'][:]
	df_ds['lwe'] = ds.variables['lwe'][:]
	df_ds['mdt'] = ds.variables['mdt'][:]
	df_ds['tpa_correction'] = ds.variables['tpa_correction'][:]
	df_ds['nme'] = fname
	df_ds.set_index('nme', inplace = True)
	# df_ds = df_ds[df_ds['surface_Type'].isin([0]) == True]
	outfile = outdir + '/alongtrackcop' + str(i) + '.csv'
	df_ds.to_csv(outfile)
	ds.close()
	print(i)
	i += 1
print('Done')