from datetime import datetime

import click
from pathlib import Path

from OceanDB.OceanDB_Copernicus import OceanDBCopernicusMarine
from OceanDB.OceanDB_ETL import OceanDBETl, AlongTrackData, AlongTrackMetaData
from OceanDB.OceanDB_Initializer import OceanDBInit
from OceanDB.config import Config
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
    # ocean_db_init.validate_schema()
    oceandb_etl = OceanDBETl()
    oceandb_etl.insert_basins_data()
    oceandb_etl.insert_basin_connections_data()




@cli.command
def download():
    config = Config()

    along_track_directory = Path(config.ALONG_TRACK_DATA_DIRECTORY)

    # 2️⃣ Check contents
    files = list(along_track_directory.glob("*"))

    if not along_track_directory.exists():
        click.echo(
            f"Directory does not exist: {along_track_directory}\n"
            f"Please create it or update ALONG_TRACK_DATA_DIRECTORY in your .env "
        )
        return


    if not files:
        click.echo(f"Found {len(files)} file(s) in directory. No need to download.")
        click.echo("✔ Data already exists.")

        # 3️⃣ Directory is empty — warn user
        click.echo(
            "\n⚠️  No data found in the directory.\n"
            f"The directory '{along_track_directory}' is empty.\n"
            "Downloading the full dataset requires more than **20 GB** of storage.\n"
        )

        # 4️⃣ Ask the user if they want to continue
        proceed = click.confirm(
            "Do you want to proceed with downloading 20+ GB of data?",
            default=False
        )

        if not proceed:
            click.echo("Download canceled.")
            return

    from OceanDB.OceanDB_Copernicus import OceanDBCopernicusMarine
    oceandb_cm = OceanDBCopernicusMarine()
    # click.echo("\n⬇️  Starting download... (this may take hours)")
    #
    oceandb_cm.sync_copernicus_along_track_data()
    # click.echo("✔ Download complete.")


def parse_date(ctx, param, value) -> datetime:
    """Parse date strings into datetime objects."""
    if value is None:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        raise click.BadParameter(f"Invalid date format: {value}. Use YYYY-MM-DD.")



@cli.command
# @click.option(
#     "--missions",
#     "-m",
#     multiple=True,
#     help="One or more missions to ingest (e.g. -m j3 -m al -m s3a)."
# )
# @click.option(
#     "--start-date",
#     callback=parse_date,
#     help="Start date (YYYY-MM-DD)."
# )
# @click.option(
#     "--end-date",
#     callback=parse_date,
#     help="End date (YYYY-MM-DD)."
# )
# @click.option(
#     "--all",
#     "ingest_all",
#     is_flag=True,
#     help="Ingest all missions and all available date ranges."
# )

def ingest():
    config = Config()

    mission = "al"
    prefix = "SEALEVEL_GLO_PHY_L3_MY_008_062"
    file_structure = f"cmems_obs-sl_glo_phy-ssh_my_{mission}-l3-duacs_PT1S_202411"
    ingest_directory = Path(f"{config.ALONG_TRACK_DATA_DIRECTORY}/{prefix}/{file_structure}")
    nc_files = ingest_directory.rglob("*.nc")

    oceandb_etl = OceanDBETl()

    for file in nc_files:
        print(file)
        print(f"Processing {file.name}")
        start = time.perf_counter()
        oceandb_etl.ingest_along_track_file(file)
        size_mb = file.stat().st_size / (1024 * 1024)
        duration = time.perf_counter() - start
        print(f"✅ {file.name} | {size_mb:.2f} MB | {duration:.2f}s")


        break

# def insert():
#     """
#     Iterate over ALL
#     """
#     config = Config()
#     root_directory = Path(config.ALONG_TRACK_DATA_DIRECTORY)
#     nc_files = root_directory.rglob("*.nc")
#     oceandb_etl = OceanDBETl()
#
#     for file in nc_files:
#         print("The ")
#         oceandb_etl.ingest_along_track_file(file)
#         break
#
#     mission = "al"
#     file_structure = f"cmems_obs-sl_glo_phy-ssh_my_{mission}-l3-duacs_PT1S_202411"
    # # oceandb_etl = OceanDBETl()
    #
    # al_root_dir = Path("data/copernicus/SEALEVEL_GLO_PHY_L3_MY_008_062/cmems_obs-sl_glo_phy-ssh_my_al-l3-duacs_PT1S_202411")
    # # alg_root_dir = Path("data/copernicus/SEALEVEL_GLO_PHY_L3_MY_008_062/cmems_obs-sl_glo_phy-ssh_my_alg-l3-duacs_PT1S_202411")
    # #
    # #
    # # # Recursively iterate through all .nc files
    # # oceandb_etl = OceanDBETl()
    # # al_nc_files = al_root_dir.rglob("*.nc")
    # # alg_nc_files = alg_root_dir.rglob("*.nc")
    # #
    # # nc_files = [*al_nc_files, *alg_nc_files]
    # #
    # for file in nc_files:
    #     # try:
    #     start = time.perf_counter()
    #     oceandb_etl.ingest_along_track_file(file)
    #     size_mb = file.stat().st_size / (1024 * 1024)
    #     duration = time.perf_counter() - start
    #     print(f"✅ {file.name} | {size_mb:.2f} MB | {duration:.2f}s")
    #
    #     # except Exception as ex:
    #     #     logger.error(f"❌ {file.name}: {ex}")
# @cli.command()
# def insert_basins():
#     from OceanDB.OceanDB_ETL import OceanDBETl
#     oceandb_etl = OceanDBETl()
#     oceandb_etl.insert_basins_data()
#     oceandb_etl.insert_basin_connections_data()
#
