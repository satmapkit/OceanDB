from dataclasses import dataclass
import netCDF4 as nc
from OceanDB.OceanDB_ETL import OceanDBETl
import numpy as np

@dataclass
class AlongTrackData:
    """Structured container for extracted along-track variables."""
    time: np.ndarray
    latitude: np.ndarray
    longitude: np.ndarray
    cycle: np.ndarray
    track: np.ndarray
    sla_unfiltered: np.ndarray
    sla_filtered: np.ndarray
    dac: np.ndarray
    ocean_tide: np.ndarray
    internal_tide: np.ndarray
    lwe: np.ndarray
    mdt: np.ndarray
    tpa_correction: np.ndarray
    basin_id: np.ndarray


filepath = "/app/data/copernicus/SEALEVEL_GLO_PHY_L3_MY_008_062/cmems_obs-sl_glo_phy-ssh_my_c2n-l3-duacs_PT1S_202411/2020/08"
filename = "dt_global_c2n_phy_l3_1hz_20200801_20240205.nc"
full_file_path = f'{filepath}/{filename}'

ds = nc.Dataset(full_file_path, 'r')

# self.import_metadata_to_psql(ds, fname)

# Don't scale most variables, so they are stored more efficiently in db
ds.variables['sla_unfiltered'].set_auto_maskandscale(False)
ds.variables['sla_filtered'].set_auto_maskandscale(False)
ds.variables['ocean_tide'].set_auto_maskandscale(False)
ds.variables['internal_tide'].set_auto_maskandscale(False)
ds.variables['lwe'].set_auto_maskandscale(False)
ds.variables['mdt'].set_auto_maskandscale(False)
ds.variables['dac'].set_auto_maskandscale(False)
ds.variables['tpa_correction'].set_auto_maskandscale(False)

time_data = ds.variables['time']  # Extract dates from the dataset and convert them to standard datetime
time_min = time_data[:].min()
time_max = time_data[:].max()
time_min = nc.num2date(time_min, time_data.units, only_use_cftime_datetimes=False,
                       only_use_python_datetimes=False)
time_max = nc.num2date(time_max, time_data.units, only_use_cftime_datetimes=False,
                       only_use_python_datetimes=False)

time_data = nc.num2date(time_data[:], time_data.units, only_use_cftime_datetimes=False,
                        only_use_python_datetimes=False)
time_data = nc.date2num(time_data[:],
                        "microseconds since 2000-01-01 00:00:00")  # Convert the standard date back to the 8-byte integer PSQL uses

oceandb_etl = OceanDBETl()

basin_id = oceandb_etl.basin_mask(ds.variables['latitude'][:], ds.variables['longitude'][:])

along_track_data = AlongTrackData(
    time=time_data,
    latitude=ds.variables["latitude"][:],
    longitude=ds.variables["longitude"][:],
    cycle=ds.variables["cycle"][:],
    track=ds.variables["track"][:],
    sla_unfiltered=ds.variables["sla_unfiltered"][:],
    sla_filtered=ds.variables["sla_filtered"][:],
    dac=ds.variables["dac"][:],
    ocean_tide=ds.variables["ocean_tide"][:],
    internal_tide=ds.variables["internal_tide"][:],
    lwe=ds.variables["lwe"][:],
    mdt=ds.variables["mdt"][:],
    tpa_correction=ds.variables["tpa_correction"][:],
    basin_id=basin_id
)

fname = "along_track"
import pandas as pd
from datetime import datetime, timedelta

EPOCH = datetime(2000, 1, 1)

date_times = [EPOCH + timedelta(microseconds=int(t)) for t in along_track_data.time]

df = pd.DataFrame({
    "file_name": fname,
    "track": along_track_data.track,
    "cycle": along_track_data.cycle,
    "latitude": along_track_data.latitude,
    "longitude": along_track_data.longitude,
    "sla_unfiltered": along_track_data.sla_unfiltered,
    "sla_filtered": along_track_data.sla_filtered,
    "date_time": date_times,
    "dac": along_track_data.dac,
    "ocean_tide": along_track_data.ocean_tide,
    "internal_tide": along_track_data.internal_tide,
    "lwe": along_track_data.lwe,
    "mdt": along_track_data.mdt,
    "tpa_correction": along_track_data.tpa_correction,
    "basin_id": along_track_data.basin_id,
})


from typing import List, Tuple, Any, Iterable
from sqlalchemy.engine import Connection
from sqlalchemy.sql.schema import Table
from sqlalchemy.dialects.postgresql import insert


def upsert_ignore(
    table: Table,
    conn: Connection,
    keys: List[str],
    data_iter: Iterable[Tuple[Any, ...]],
    ) -> None:
    """
    Custom pandas.to_sql 'method' that performs bulk INSERTs into PostgreSQL
    and ignores duplicates based on a unique constraint.

    Parameters
    ----------
    table : sqlalchemy.sql.schema.Table
        SQLAlchemy Table object being written to by pandas.to_sql().
    conn : sqlalchemy.engine.Connection
        Active SQLAlchemy connection or transaction context.
    keys : list of str
        Column names (in order) corresponding to DataFrame columns.
    data_iter : iterable of tuple
        Iterable of row value tuples to insert.

    Returns
    -------
    None
        Executes an INSERT ... ON CONFLICT DO NOTHING for each chunk.
    """
    data = [dict(zip(keys, row)) for row in data_iter]
    stmt = insert(table.table).values(data)
    stmt = stmt.on_conflict_do_nothing(
        index_elements=['date_time', 'latitude', 'longitude']  # your unique columns
    )
    conn.execute(stmt)


df.to_sql(
    name=oceandb_etl.along_track_table_name,
    con=oceandb_etl.get_engine(),
    schema="public",
    if_exists="append",
    index=False,
    chunksize=1000,
    method=upsert_ignore,  # type-safe and IDE-aware now
)

# df.to_sql(
#     name=oceandb_etl.along_track_table_name,
#     con=oceandb_etl.get_engine(),          # SQLAlchemy engine
#     schema="public",
#     if_exists="append",
#     index=False,
#     method="multi",           # uses multi-row inserts
#     chunksize=1000,
#     method=upsert_ignore
# )
#
# df.to_sql(
#     name=oceandb_etl.along_track_table_name,
#     con=oceandb_etl.get_engine(),          # SQLAlchemy engine
#     schema="public",
#     if_exists="append",
#     index=False,
#     method="multi",           # uses multi-row inserts
#     chunksize=1000,
# )



# df.take()
