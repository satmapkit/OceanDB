from functools import cached_property
from importlib import resources
from typing import Any, overload

import netCDF4 as nc
import numpy as np


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


class BasinConnections:
    """Basin connectivity from the packaged, versioned connection table.

    This is the same asset used to populate PostgreSQL. Reading it directly
    allows an indexed backend to classify query centers without a database.
    """

    @cached_property
    def connection_map(self) -> dict[int, list[int]]:
        path = resources.files("OceanDB.data").joinpath(
            "basins/basin_connection_table.txt"
        )
        connections: dict[int, list[int]] = {}
        for line in path.read_text(encoding="utf-8").splitlines():
            if line.startswith("HDR"):
                continue
            basin, separator, adjacent = line.partition(":")
            if not separator:
                raise ValueError(f"Invalid basin connection entry: {line!r}")
            basin_id = int(basin)
            if basin_id in connections:
                raise ValueError(f"Duplicate basin connection entry: {basin_id}")
            connections[basin_id] = [basin_id]
            for connected in adjacent.split(","):
                connected_id = int(connected)
                if connected_id not in connections[basin_id]:
                    connections[basin_id].append(connected_id)

        for basin_id in BasinMask().basin_ids():
            connections.setdefault(int(basin_id), [int(basin_id)])
        return connections
