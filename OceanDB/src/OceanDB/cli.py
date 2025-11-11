import click
from pathlib import Path
from OceanDB.OceanDB_ETL import OceanDBETl, AlongTrackData, AlongTrackMetaData
from OceanDB.OceanDB_Initializer import OceanDBInit
from OceanDB.OceanDB import OceanDB
from OceanDB.utils.logging import get_logger
import netCDF4 as nc
import time

logger = get_logger()


@click.group()
def cli():
    pass

@cli.command()
def process():
    logger.info("OceanDB")


@cli.command()
def init():
    ocean_db_init = OceanDBInit()
    ocean_db_init.create_database()
    ocean_db_init.create_tables()
    ocean_db_init.create_indices()
    ocean_db_init.create_partitions("1990-01-01", "2025-11-01")
    ocean_db_init.validate_schema()
    oceandb_etl = OceanDBETl()
    oceandb_etl.insert_basins_data()
    oceandb_etl.insert_basin_connections_data()

@cli.command()
def insert_basins():
    from OceanDB.OceanDB_ETL import OceanDBETl
    oceandb_etl = OceanDBETl()
    oceandb_etl.insert_basins_data()
    oceandb_etl.insert_basin_connections_data()


@cli.command
def validate():
    ocean_db_init = OceanDBInit()
    ocean_db_init.validate_schema()


@cli.command
def insert(root_dir = Path("data/copernicus/SEALEVEL_GLO_PHY_L3_MY_008_062/cmems_obs-sl_glo_phy-ssh_my_al-l3-duacs_PT1S_202411")):
    """
    Iterate over ALL
    """
    # root_dir = Path("data/copernicus/SEALEVEL_GLO_PHY_L3_MY_008_062/cmems_obs-sl_glo_phy-ssh_my_al-l3-duacs_PT1S_202411")

    # Recursively iterate through all .nc files
    oceandb_etl = OceanDBETl()
    nc_files = root_dir.rglob("*.nc")

    for file in nc_files:
        try:
            start = time.perf_counter()
            oceandb_etl.ingest_along_track_file(file)
            size_mb = file.stat().st_size / (1024 * 1024)
            duration = time.perf_counter() - start
            print(f"✅ {file.name} | {size_mb:.2f} MB | {duration:.2f}s")

        except Exception as ex:
            logger.error(f"❌ {file.name}: {ex}")
