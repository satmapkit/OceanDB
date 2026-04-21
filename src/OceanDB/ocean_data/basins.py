from functools import cached_property
from importlib import resources
from typing import Any

import netCDF4 as nc
import numpy as np

from OceanDB.OceanDB import OceanDB


class BasinMask:
    """Domain helper for basin mask lookups from packaged mask data."""

    @cached_property
    def data(self) -> Any:
        with resources.files("OceanDB.data").joinpath(
            "basin_masks/new_basin_mask.nc"
        ).open("rb") as f:
            ds = nc.Dataset("inmemory.nc", memory=f.read())
            ds.set_auto_mask(False)
            basin_mask = ds.variables["basinmask"][:]
            ds.close()
            return basin_mask

    def lookup(self, latitude, longitude):
        onesixth = 1 / 6
        i = np.floor((latitude + 90) / onesixth).astype(int)
        j = np.floor((longitude % 360) / onesixth).astype(int)
        return self.data[i, j]

    def basin_ids(self) -> np.ndarray:
        return np.unique(self.data)


class BasinConnections(OceanDB):
    """Domain helper for basin connectivity lookups."""

    @cached_property
    def connection_map(self) -> dict:
        with self.cursor() as cur:
            cur.execute(
                """SELECT DISTINCT basin_id FROM basin_connections ORDER BY basin_id"""
            )
            unique_ids = cur.fetchall()

        basin_ids = [row[0] for row in unique_ids]
        basin_id_params = [{"basin_id": basin_id} for basin_id in basin_ids]

        query = """SELECT array_agg(connected_id) as connected_basin_id
                FROM basin_connections
                WHERE basin_id = %(basin_id)s
                GROUP BY basin_id"""

        basin_id_connection_dict = {}
        with self.cursor() as cur:
            cur.executemany(query, basin_id_params, returning=True)
            i = 0
            while True:
                data = cur.fetchall()
                basin_id_connection_dict[basin_ids[i]] = data[0][0]
                i += 1
                if not cur.nextset():
                    break

        for basin_id in BasinMask().basin_ids():
            if basin_id not in basin_id_connection_dict:
                basin_id_connection_dict[basin_id] = []
            basin_id_connection_dict[basin_id].insert(0, basin_id)

        return basin_id_connection_dict
