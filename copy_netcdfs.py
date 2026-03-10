import netCDF4 as nc
from pathlib import Path


def duplicate_n_rows(
    input_file: Path, output_file: Path, offset: int, n_rows: int, dim_to_cut="obs"
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
        )
        for attr in in_var.ncattrs():
            out_var.setncattr(attr, in_var.getncattr(attr))
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
        out_var[:] = out
    out_data.close()


if __name__ == "__main__":
    from OceanDB.config import Config

    config = Config()

    eddy_directory = config.eddy_data_directory
    alongtrack_directory = config.along_track_data_directory
    paths = {
        f"{eddy_directory}/META3.2_DT_allsat_Cyclonic_long_19930101_20220209.nc": "tests/data/eddy/cyclonic.nc",
        # f"{eddy_directory}/META3.2_DT_allsat_AntiCyclonic_long_19930101_20220209.nc": "tests/data/eddy/anticyclonic.nc",
    }

    for inpath, outpath in paths.items():
        out = duplicate_n_rows(
            Path(inpath),
            Path(outpath),
            offset=0,
            n_rows=100,
        )
