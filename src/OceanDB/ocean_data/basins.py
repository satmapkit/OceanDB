from functools import cached_property
from importlib import resources
from typing import Any, overload

import netCDF4 as nc
import numpy as np

from OceanDB.OceanDB import OceanDB


class BasinMask:
    """
    Domain helper for basin mask lookups from packaged mask data.

    0 is land
    1-999 is ocean basin
    1000+ is inland lakes
    """

    @cached_property
    def data(self) -> Any:
        with (
            resources.files("OceanDB.data")
            .joinpath("basin_masks/new_basin_mask.nc")
            .open("rb") as f
        ):
            ds = nc.Dataset("inmemory.nc", memory=f.read())
            ds.set_auto_mask(False)
            basin_mask = ds.variables["basinmask"][:]
            ds.close()
            return basin_mask

    @overload
    def lookup(self, latitude: float, longitude: float) -> int: ...
    @overload
    def lookup[S, T: np.dtype[np.floating]](
        self, latitude: np.ndarray[S, T], longitude: np.ndarray[S, T]
    ) -> np.ndarray[S, T]: ...
    def lookup(self, latitude, longitude):
        onesixth = 1 / 6
        i = np.floor((latitude + 90) / onesixth).astype(int)
        j = np.floor((longitude % 360) / onesixth).astype(int)
        return self.data[i, j]

    def basin_ids(self) -> np.ndarray:
        return np.unique(self.data)

    @overload
    def basin_is_ocean(self, basin_mask: int) -> int: ...
    @overload
    def basin_is_ocean[S, T: np.dtype[np.integer]](
        self, basin_mask: np.ndarray[S, T]
    ) -> np.ndarray[S, np.dtype[np.bool]]: ...
    def basin_is_ocean(self, basin_mask):
        return (basin_mask > 0) & (basin_mask < 1000)

    @overload
    def loc_is_ocean(self, latitude: float, longitude: float) -> int: ...
    @overload
    def loc_is_ocean[S, T: np.dtype[np.floating]](
        self, latitude: np.ndarray[S, T], longitude: np.ndarray[S, T]
    ) -> np.ndarray[S, np.dtype[np.bool]]: ...
    def loc_is_ocean(self, latitude, longitude):
        return self.basin_is_ocean(self.lookup(latitude, longitude))


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
