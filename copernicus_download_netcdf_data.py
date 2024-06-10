# Import modules
import copernicusmarine
from pprint import pprint

# Define parameters to run the extraction
dataset_id_yearly = "cmems_obs-sl_glo_phy-ssh_my_j1n-l3-duacs_PT1S"

# Define output storage parameters
output_directory = "/Users/briancurtis/Documents/Eddy/Along_files2"

# Call the get function to save data
get_result_annualmean = copernicusmarine.get(
	dataset_id=dataset_id_yearly,
	output_directory=output_directory,
	no_directories=True)

pprint(f"List of saved files: {get_result_annualmean}")