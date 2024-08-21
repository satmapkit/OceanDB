from OceanDB.Eddy import Eddy
import os.path
import pandas as pd

eddy_track = 41
cyclonic_type = -1

eddy_db = Eddy(db_name='ocean')

try:
    path = eddy_db.export_file_path
except Exception:
    path = '/Users/briancurtis/Documents/Eddy/along_nc_out/'

header_array = ['along_file_name', 'track', 'cycle', 'latitude', 'longitude', 'sla_unfiltered', 'sla_filtered', 'time', 'dac', 'ocean_tide', 'internal_tide', 'lwe', 'mdt', 'tpa_correction']

meta = dict()
row_meta = dict()
row_meta = [{'var_name': 'sla_unfiltered', 'comment': 'The sea level anomaly is the sea surface height above mean sea surface height; the uncorrected sla can be computed as follows: [uncorrected sla]=[sla from product]+[dac]+[ocean_tide]+[internal_tide]-[lwe]; see the product user manual for details', 'long_name': 'Sea level anomaly not-filtered not-subsampled with dac, ocean_tide and lwe correction applied', 'scale_factor': 0.001, 'standard_name': 'sea_surface_height_above_sea_level', 'units': 'm'}, {'var_name': 'sla_filtered', 'comment': 'The sea level anomaly is the sea surface height above mean sea surface height; the uncorrected sla can be computed as follows: [uncorrected sla]=[sla from product]+[dac]+[ocean_tide]+[internal_tide]-[lwe]; see the product user manual for details', 'long_name': 'Sea level anomaly filtered not-subsampled with dac, ocean_tide and lwe correction applied', 'scale_factor': 0.001, 'standard_name': 'sea_surface_height_above_sea_level', 'units': 'm'}, {'var_name': 'dac', 'comment': 'The sla in this file is already corrected for the dac; the uncorrected sla can be computed as follows: [uncorrected sla]=[sla from product]+[dac]; see the product user manual for details', 'long_name': 'Dynamic Atmospheric Correction', 'scale_factor': 0.001, 'standard_name': None, 'units': 'm'}, {'var_name': 'time', 'comment': '', 'long_name': 'Time of measurement', 'scale_factor': None, 'standard_name': 'time', 'units': 'days since 1950-01-01 00:00:00'}, {'var_name': 'track', 'comment': '', 'long_name': 'Track in cycle the measurement belongs to', 'scale_factor': None, 'standard_name': None, 'units': '1\n'}, {'var_name': 'cycle', 'comment': '', 'long_name': 'Cycle the measurement belongs to', 'scale_factor': None, 'standard_name': None, 'units': '1'}, {'var_name': 'ocean_tide', 'comment': 'The sla in this file is already corrected for the ocean_tide; the uncorrected sla can be computed as follows: [uncorrected sla]=[sla from product]+[ocean_tide]; see the product user manual for details', 'long_name': 'Ocean tide model', 'scale_factor': 0.001, 'standard_name': None, 'units': 'm'}, {'var_name': 'internal_tide', 'comment': 'The sla in this file is already corrected for the internal_tide; the uncorrected sla can be computed as follows: [uncorrected sla]=[sla from product]+[internal_tide]; see the product user manual for details', 'long_name': 'Internal tide correction', 'scale_factor': 0.001, 'standard_name': None, 'units': 'm'}, {'var_name': 'lwe', 'comment': 'The sla in this file is already corrected for the lwe; the uncorrected sla can be computed as follows: [uncorrected sla]=[sla from product]-[lwe]; see the product user manual for details', 'long_name': 'Long wavelength error', 'scale_factor': 0.001, 'standard_name': None, 'units': 'm'}, {'var_name': 'mdt', 'comment': 'The mean dynamic topography is the sea surface height above geoid; it is used to compute the absolute dynamic tyopography adt=sla+mdt', 'long_name': 'Mean dynamic topography', 'scale_factor': 0.001, 'standard_name': 'sea_surface_height_above_geoid', 'units': 'm'}]
encodes = dict()

data = eddy_db.along_points_near_eddy(eddy_track, cyclonic_type)

try:
	df = pd.DataFrame(data, columns=header_array)
except Exception as err:
	print(err)
xrdata = df.to_xarray()
for record in row_meta:
	meta[record['var_name']] = {k: v for k, v in record.items() if v is not None} # Remove empty items from dictionary. Xarray will throw an error is an item is None
for var_name in meta:
	for key in meta[var_name]:
		if key == 'scale_factor':
			encodes[var_name] = {'scale_factor':meta[var_name]['scale_factor']}
		if key != 'var_name' and key != 'scale_factor' and (var_name != 'time' and key != 'units'):
			try:
				xrdata[var_name].attrs[key] = meta[var_name][key]
			except Exception as err:
				print(err)
try:
	xrdata.time.encoding['units'] = 'days since 1950-01-01 00:00:00'
	filename = f"along_track_data_for_eddy_track_{eddy_track}_cyclonic_type_{cyclonic_type}.nc"
	path = os.path.expanduser("~/Documents") + '/' + filename
	xrdata.to_netcdf(path, encoding=encodes)
except Exception as err:
	print(err)

print(filename + ' was downladed to the Documents folder')