import netCDF4 as nc
from pathlib import Path
import datetime
from typing import Any, Callable
from OceanDB.config import Config


def duplicate_n_rows(
    input_file: Path,
    output_file: Path,
    offset: int,
    n_rows: int,
    dim_to_cut: str = "obs",
    custom_func: Callable[[nc.Variable[Any], Any], Any] | None = None,
):
    in_data = nc.Dataset(input_file, "r")
    out_data = nc.Dataset(output_file, "w")

    for dimension in in_data.dimensions.values():
        if dimension.name == dim_to_cut:
            size = n_rows
        else:
            size = dimension.size
        out_data.createDimension(dimname=dimension.name, size=size)

    for in_var in in_data.variables.values():
        out_var = out_data.createVariable(
            varname=in_var.name,
            datatype=in_var.datatype,
            dimensions=in_var.dimensions,
            fill_value=in_var.get_fill_value(),
        )
        for attr in in_var.ncattrs():
            if attr == "_FillValue":
                continue
            out_var.setncattr(attr, in_var.getncattr(attr))
        if in_var.dimensions:
            out = in_var[
                [
                    (
                        slice(offset, offset + n_rows)
                        if dim == dim_to_cut
                        else slice(None, None)
                    )
                    for dim in in_var.dimensions
                ]
            ]
        else:
            out = in_var[:]
        if custom_func is not None:
            out = custom_func(in_var, out)
        out_var[:] = out
    out_data.close()


config = Config()

eddy_directory = config.eddy_data_directory
alongtrack_directory = config.along_track_data_directory

alongtrack_prefix = f"{alongtrack_directory}/SEALEVEL_GLO_PHY_L3_MY_008_062/cmems_obs-sl_glo_phy-ssh_my_j2-l3-duacs_PT1S_202411/2013/01/dt_global_j2_phy_l3_1hz_201301"
alongtrack_prefix_out = f"tests/data/along_track/required_underscores_j2_201301"


def offset_eddy_20_years(var: nc.Variable[Any], data) -> Any:
    if var.name != "time":
        return data
    days = (datetime.datetime(1970, 1, 1) - datetime.datetime(1950, 1, 1)).days
    return data + days


paths = {
    f"{eddy_directory}/META3.2_DT_allsat_Cyclonic_long_19930101_20220209.nc": (
        "tests/data/eddy/cyclonic.nc",
        "obs",
        0,
        offset_eddy_20_years,
    ),
    # f"{eddy_directory}/META3.2_DT_allsat_AntiCyclonic_long_19930101_20220209.nc": ("tests/data/eddy/anticyclonic.nc", "obs", 0),
    f"{alongtrack_prefix}01_20240205.nc": (
        f"{alongtrack_prefix_out}01.nc",
        "time",
        0,
        None,
    ),
    f"{alongtrack_prefix}02_20240205.nc": (
        f"{alongtrack_prefix_out}02.nc",
        "time",
        0,
        None,
    ),
    f"{alongtrack_prefix}03_20240205.nc": (
        f"{alongtrack_prefix_out}03.nc",
        "time",
        11582,
        None,
    ),
    f"{alongtrack_prefix}04_20240205.nc": (
        f"{alongtrack_prefix_out}04.nc",
        "time",
        0,
        None,
    ),
    f"{alongtrack_prefix}05_20240205.nc": (
        f"{alongtrack_prefix_out}05.nc",
        "time",
        0,
        None,
    ),
    f"{alongtrack_prefix}06_20240205.nc": (
        f"{alongtrack_prefix_out}06.nc",
        "time",
        9797,
        None,
    ),
    f"{alongtrack_prefix}07_20240205.nc": (
        f"{alongtrack_prefix_out}07.nc",
        "time",
        42292,
        None,
    ),
    f"{alongtrack_prefix}08_20240205.nc": (
        f"{alongtrack_prefix_out}08.nc",
        "time",
        0,
        None,
    ),
    f"{alongtrack_prefix}09_20240205.nc": (
        f"{alongtrack_prefix_out}09.nc",
        "time",
        7994,
        None,
    ),
}


if __name__ == "__main__":
    for inpath, (outpath, dim, offset, custom_func) in paths.items():
        out = duplicate_n_rows(
            Path(inpath),
            Path(outpath),
            offset=offset,
            n_rows=100,
            dim_to_cut=dim,
            custom_func=custom_func,
        )
