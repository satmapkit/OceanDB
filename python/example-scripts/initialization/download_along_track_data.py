# Documentation for this API is found here:
# https://help.marine.copernicus.eu/en/articles/8286883-copernicus-marine-toolbox-api-get-original-files#h_26674730f3
# example metadata:
# 	https://stac.marine.copernicus.eu/metadata/SEALEVEL_GLO_PHY_L3_MY_008_062/cmems_obs-sl_glo_phy-ssh_my_j1n-l3-duacs_PT1S_202112/dataset.stac.json
# NOTE: the dataset version varies, depending on the dataset. The json metadata file provides this information. A correct implementation would probably parse the file and use that information.

# Import modules
import copernicusmarine
from pprint import pprint
import yaml

with open('../../config.yaml', 'r') as param_file:
    along_params = yaml.full_load(param_file)

missions = [
    'al', 'alg', 'c2', 'c2n', 'e1g', 'e1', 'e2', 'en', 'enn',
    'g2', 'h2a', 'h2ag', 'h2b', 'j1g', 'j1', 'j1n', 'j2g', 'j2', 'j2n',
    'j3', 'j3n', 's3a', 's3b', 's6a-lr', 'tp', 'tpn', 'swon', 'swonc'
]

# missions = ['s6a-lr'] # small set for testing
# missions = ['al', 'alg', 'c2', 'c2n', 'e1g', 'e1', 'e2', 'en', 'enn', 'g2', 'h2a', 'h2b', 'j1g', 'j1n', 'j2g', 'j2n', 'j3n', 'tpn'] # the rest
# missions = ['al', 'alg', 'c2', 'c2n', 'e1g', 'e1', 'e2', 'en', 'enn', 'g2', 'h2a', 'h2b', 'j1g', 'j1', 'j1n', 'j2g', 'j2', 'j2n', 'j3', 'j3n', 's3a', 's3b', 's6a-lr', 'tp', 'tpn'] # full set

my = {
    'al':     '202411',
    'alg':    '202411',
    'c2':     '202411',
    'c2n':    '202411',
    'e1g':    '202411',
    'e1':     '202411',
    'e2':     '202411',
    'en':     '202411',
    'enn':    '202411',
    'g2':     '202411',
    'h2a':    '202411',
    'h2ag':   '202411',
    'h2b':    '202411',
    'j1g':    '202411',
    'j1':     '202411',
    'j1n':    '202411',
    'j2g':    '202411',
    'j2':     '202411',
    'j2n':    '202411',
    'j3':     '202411',
    'j3n':    '202411',
    's3a':    '202411',
    's3b':    '202411',
    's6a-lr': '202411',
    'tp':     '202411',
    'tpn':    '202411',
    'swon':   '202411',  
    'swonc':  '202411'   
}

myint = {}

# my = {'al': '202112',
#       'alg': '202112',
#        'c2': '202112',
#        'c2n': '202112',
#        'e1g': '202112',
#        'e1': '202112',
#        'e2': '202112',
#        'en': '202112',
#        'enn': '202112',
#        'g2': '202112',
#        'h2a': '202112',
#        'h2b': '202112',
#        'j1g': '202112',
#        'j1': '202112',
#        'j1n': '202112',
#        'j2g': '202112',
#        'j2': '202112',
#        'j2n': '202112',
#        'j3': '202112',
#        'j3n': '202207',
#        's3a': '202112',
#        's3b': '202112',
#        's6a-lr': '202207',
#        'tp': '202112',
#        'tpn': '202112'}

# myint = {'alg': '202311',
#          'c2n': '202311',
#          'h2b': '202311',
#          'j3n': '202311',
#          's3a': '202311',
#          's3b': '202311',
#          's6a-lr': '202311'}

dataset_id = []
dataset_version = []
for mission in missions:
    if mission in my:
        dataset_id.append("cmems_obs-sl_glo_phy-ssh_my_" + mission + "-l3-duacs_PT1S")
        dataset_version.append(my[mission])
    if mission in myint:
        dataset_id.append("cmems_obs-sl_glo_phy-ssh_myint_" + mission + "-l3-duacs_PT1S")
        dataset_version.append(myint[mission])

# copernicusmarine.login(username=along_params['copernicus_marine']['username'],
#                        password=along_params['copernicus_marine']['password'])

# Call the get function to save data
for i in range(len(dataset_id)):
    get_result = copernicusmarine.get(
        dataset_id=dataset_id[i],
        output_directory=along_params['copernicus_marine']['nc_files_path'],
        sync=True,
        dataset_version=dataset_version[i]
    )

names = {'e1': 'ERS-1 (only for dt)',
'e1g': 'ERS-1 geodetic phase (only for dt)',
'e2': 'ERS-2 (only for dt)',
'tp': 'TOPEX/Poseidon (only for dt)',
'tpn': 'TOPEX/Poseidon on its new orbit (only for dt)',
'g2': 'GFO (only for dt)',
'j1': 'Jason-1 (only for dt)',
'j1n': 'Jason-1 on its new orbit (only for dt)',
'j1g': 'Jason-1 on its geodetic orbit (only for dt)',
'j2': 'OSTM/Jason -2 (only for dt)',
'j2n': 'OSTM/Jason -2 on its interleaved orbit',
'j2g': 'OSTM/Jason-2 on its long repeat orbit (LRO)',
'j3': 'Jason-3',
'j3n': 'Jason-3 on its new (interleaved) orbit',
'en': 'Envisat (only for dt)',
'enn': 'Envisat on its new orbit (only for dt)',
'c2': 'Cryosat-2',
'c2n': 'Cryosat-2 on its new orbit',
'al': 'Saral/AltiKa',
'alg': 'Saral/AltiKa on its geodetic orbit (only for dt)',
'h2a': 'HaiYang-2A (only for dt)',
'h2ag': 'HaiYang-2A on its geodetic orbit (only for dt)',
'h2b': 'HaiYang-2B',
's3a': 'Sentinel-3A',
's3b': 'Sentinel-3B',
's6a-hr': 'Sentinel-6A with SAR mode measurement',
's6a-lr': 'Sentinel-6A with LRM mode measurement',
'swon': 'SWOT-nadir'}
# pprint(f"List of saved files: {get_result}")
