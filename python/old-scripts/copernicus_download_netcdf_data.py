# Documentation for this API is found here:
# https://help.marine.copernicus.eu/en/articles/8286883-copernicus-marine-toolbox-api-get-original-files#h_26674730f3
# example metadata:
# 	https://stac.marine.copernicus.eu/metadata/SEALEVEL_GLO_PHY_L3_MY_008_062/cmems_obs-sl_glo_phy-ssh_my_j1n-l3-duacs_PT1S_202112/dataset.stac.json
# NOTE: the dataset version varies, depending on the dataset. The json metadata file provides this information. A correct implementation would probably parse the file and use that information.

# Import modules
import copernicusmarine
from pprint import pprint
import yaml

with open('../config.yaml', 'r') as param_file:
    along_params = yaml.full_load(param_file)

missions = ['tp', 'j1', 'j2', 'j3', 's3a', 's3b', 's6a-lr']

my = {'al': '202112',
      'alg': '202112',
       'c2': '202112',
       'c2n': '202112',
       'e1g': '202112',
       'e1': '202112',
       'e2': '202112',
       'en': '202112',
       'enn': '202112',
       'g2': '202112',
       'h2a': '202112',
       'h2b': '202112',
       'j1g': '202112',
       'j1': '202112',
       'j1n': '202112',
       'j2g': '202112',
       'j2': '202112',
       'j2n': '202112',
       'j3': '202112',
       'j3n': '202207',
       's3a': '202112',
       's3b': '202112',
       's6a-lr': '202207',
       'tp': '202112',
       'tpn': '202112'}

myint = {'alg': '202311',
         'c2n': '202311',
         'h2b': '202311',
         'j3n': '202311',
         's3a': '202311',
         's3b': '202311',
         's6a-lr': '202311'}

dataset_id = []
dataset_version = []
for mission in missions:
    if mission in my:
        dataset_id.append("cmems_obs-sl_glo_phy-ssh_my_" + mission + "-l3-duacs_PT1S")
        dataset_version.append(my[mission])
    if mission in myint:
        dataset_id.append("cmems_obs-sl_glo_phy-ssh_myint_" + mission + "-l3-duacs_PT1S")
        dataset_version.append(myint[mission])

copernicusmarine.login(username=along_params['copernicus_marine']['username'],
                       password=along_params['copernicus_marine']['password'], skip_if_user_logged_in=True)

# Call the get function to save data
for i in range(len(dataset_id)):
    get_result = copernicusmarine.get(
        dataset_id=dataset_id[i],
        output_directory=along_params['copernicus_marine']['nc_files_path'],
        sync=True,
        dataset_version=dataset_version[i],
        force_download=True
    )

# pprint(f"List of saved files: {get_result}")
