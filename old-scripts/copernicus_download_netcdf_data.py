# Documentation for this API is found here:
# https://help.marine.copernicus.eu/en/articles/8286883-copernicus-marine-toolbox-api-get-original-files#h_26674730f3

# Import modules
import copernicusmarine
from pprint import pprint

dataset_id = ['']*32
dataset_id[0]="cmems_obs-sl_glo_phy-ssh_my_al-l3-duacs_PT1S"
dataset_id[1]="cmems_obs-sl_glo_phy-ssh_my_alg-l3-duacs_PT1S"
dataset_id[2]="cmems_obs-sl_glo_phy-ssh_my_c2-l3-duacs_PT1S"
dataset_id[3]="cmems_obs-sl_glo_phy-ssh_my_c2n-l3-duacs_PT1S"
dataset_id[4]="cmems_obs-sl_glo_phy-ssh_my_e1-l3-duacs_PT1S"
dataset_id[5]="cmems_obs-sl_glo_phy-ssh_my_e1g-l3-duacs_PT1S"
dataset_id[6]="cmems_obs-sl_glo_phy-ssh_my_e2-l3-duacs_PT1S"
dataset_id[7]="cmems_obs-sl_glo_phy-ssh_my_en-l3-duacs_PT1S"
dataset_id[8]="cmems_obs-sl_glo_phy-ssh_my_enn-l3-duacs_PT1S"
dataset_id[9]="cmems_obs-sl_glo_phy-ssh_my_g2-l3-duacs_PT1S"
dataset_id[10]="cmems_obs-sl_glo_phy-ssh_my_h2a-l3-duacs_PT1S"
dataset_id[11]="cmems_obs-sl_glo_phy-ssh_my_h2ag-l3-duacs_PT1S"
dataset_id[12]="cmems_obs-sl_glo_phy-ssh_my_h2b-l3-duacs_PT1S"
dataset_id[13]="cmems_obs-sl_glo_phy-ssh_my_j1-l3-duacs_PT1S"
dataset_id[14]="cmems_obs-sl_glo_phy-ssh_my_j1g-l3-duacs_PT1S"
dataset_id[15]="cmems_obs-sl_glo_phy-ssh_my_j1n-l3-duacs_PT1S"
dataset_id[16]="cmems_obs-sl_glo_phy-ssh_my_j2-l3-duacs_PT1S"
dataset_id[17]="cmems_obs-sl_glo_phy-ssh_my_j2g-l3-duacs_PT1S"
dataset_id[18]="cmems_obs-sl_glo_phy-ssh_my_j2n-l3-duacs_PT1S"
dataset_id[19]="cmems_obs-sl_glo_phy-ssh_my_j3-l3-duacs_PT1S"
dataset_id[20]="cmems_obs-sl_glo_phy-ssh_my_j3n-l3-duacs_PT1S"
dataset_id[21]="cmems_obs-sl_glo_phy-ssh_my_s3a-l3-duacs_PT1S"
dataset_id[22]="cmems_obs-sl_glo_phy-ssh_my_s3b-l3-duacs_PT1S"
dataset_id[23]="cmems_obs-sl_glo_phy-ssh_my_s6a-lr-l3-duacs_PT1S"
dataset_id[24]="cmems_obs-sl_glo_phy-ssh_my_tp-l3-duacs_PT1S"
dataset_id[25]="cmems_obs-sl_glo_phy-ssh_my_tpn-l3-duacs_PT1S"
dataset_id[26]="cmems_obs-sl_glo_phy-ssh_myint_alg-l3-duacs_PT1S"
dataset_id[27]="cmems_obs-sl_glo_phy-ssh_myint_c2n-l3-duacs_PT1S"
dataset_id[28]="cmems_obs-sl_glo_phy-ssh_myint_h2b-l3-duacs_PT1S"
dataset_id[29]="cmems_obs-sl_glo_phy-ssh_myint_j3n-l3-duacs_PT1S"
dataset_id[30]="cmems_obs-sl_glo_phy-ssh_myint_s3a-l3-duacs_PT1S"
dataset_id[31]="cmems_obs-sl_glo_phy-ssh_myint_s3b-l3-duacs_PT1S"
dataset_id[32]="cmems_obs-sl_glo_phy-ssh_myint_s6a-lr-l3-duacs_PT1S"

# Define parameters to run the extraction
dataset_id_yearly = dataset_id[13]

# Define output storage parameters
# output_directory = "/Users/briancurtis/Documents/Eddy/Along_files2"
output_directory = "/Users/jearly/Documents/Data/along-track-data"

# Call the get function to save data
get_result_annualmean = copernicusmarine.get(
	dataset_id=dataset_id_yearly,
	output_directory=output_directory,
	no_directories=True)

pprint(f"List of saved files: {get_result_annualmean}")