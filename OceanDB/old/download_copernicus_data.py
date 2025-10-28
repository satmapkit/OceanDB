import copernicusmarine
import OceanDB.login


def build_copernicus_datasets():
    """Build a list of Copernicus Marine dataset configurations."""
    missions = [
        "al", "alg", "c2", "c2n", "e1g", "e1", "e2", "en", "enn",
        "g2", "h2a", "h2ag", "h2b",
        "j1g", "j1", "j1n", "j2g", "j2", "j2n",
        "j3", "j3n",
        "s3a", "s3b", "s6a-lr",
        "tp", "tpn",
        "swon", "swonc",
    ]
    my_versions = {m: "202411" for m in missions}
    myint_versions = {
        # Example:
        # "alg": "202311",
        # "c2n": "202311",
        # "s6a-lr": "202311",
    }

    names = {
        "e1": "ERS-1 (only for dt)",
        "e1g": "ERS-1 geodetic phase (only for dt)",
        "e2": "ERS-2 (only for dt)",
        "tp": "TOPEX/Poseidon (only for dt)",
        "tpn": "TOPEX/Poseidon on its new orbit (only for dt)",
        "g2": "GFO (only for dt)",
        "j1": "Jason-1 (only for dt)",
        "j1n": "Jason-1 on its new orbit (only for dt)",
        "j1g": "Jason-1 on its geodetic orbit (only for dt)",
        "j2": "OSTM/Jason-2 (only for dt)",
        "j2n": "OSTM/Jason-2 on its interleaved orbit",
        "j2g": "OSTM/Jason-2 on its long repeat orbit (LRO)",
        "j3": "Jason-3",
        "j3n": "Jason-3 on its new (interleaved) orbit",
        "en": "Envisat (only for dt)",
        "enn": "Envisat on its new orbit (only for dt)",
        "c2": "Cryosat-2",
        "c2n": "Cryosat-2 on its new orbit",
        "al": "Saral/AltiKa",
        "alg": "Saral/AltiKa on its geodetic orbit (only for dt)",
        "h2a": "HaiYang-2A (only for dt)",
        "h2ag": "HaiYang-2A on its geodetic orbit (only for dt)",
        "h2b": "HaiYang-2B",
        "s3a": "Sentinel-3A",
        "s3b": "Sentinel-3B",
        "s6a-hr": "Sentinel-6A with SAR mode measurement",
        "s6a-lr": "Sentinel-6A with LRM mode measurement",
        "swon": "SWOT-nadir",
    }

    all_versions = {
        m: {"my": my_versions.get(m), "myint": myint_versions.get(m)}
        for m in set(my_versions) | set(myint_versions)
    }

    datasets = [
        {
            "mission": m,
            "type": t,
            "version": v,
            "id": f"cmems_obs-sl_glo_phy-ssh_{t}_{m}-l3-duacs_PT1S",
            "name": names.get(m, m),
        }
        for m, versions in all_versions.items()
        for t, v in versions.items()
        if v
    ]

    return datasets

datasets = build_copernicus_datasets()


"""
 {'mission': 's3b',
  'type': 'my',
  'version': '202411',
  'id': 'cmems_obs-sl_glo_phy-ssh_my_s3b-l3-duacs_PT1S',
  'name': 'Sentinel-3B'}

"""

for dataset in datasets:
    get_result = copernicusmarine.get(
        dataset_id=dataset['id'],
        output_directory="/app/data/copernicus",
        sync=True,
        dataset_version=dataset['version']
    )
    break