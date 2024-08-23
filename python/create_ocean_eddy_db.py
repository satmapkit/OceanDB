from OceanDB.Eddy import Eddy

eddy_db = Eddy(db_name='ocean')

# eddy_db.drop_database()

# Build Database
# eddy_db.create_database()

# eddy_db.create_eddy_table()
# eddy_db.create_eddy_indices()

# Database build complete. Now load data

# # Load Eddy NetCDF files from an existing directory of files
# # Eddy NetCDFs to be imported will be called META3.2_DT_allsat_Anticyclonic_long_19930101_20220209 and META3.2_DT_allsat_Cyclonic_long_19930101_20220209
# eddy_db.insert_eddy_data_from_netcdf_with_tuples()

eddy_db.create_eddy_indices()