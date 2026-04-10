from typing import Any
import netCDF4 as nc


def compute_date_time(var: nc.Variable, rows: slice) -> Any:
    """
    Given a datetime variable in a netcdf dataset,
    convert the datetime to a format suitable for
    entry into a postgres database.

    :param var:
        The netcdf variable to convert

    :returns:
        numpy array of the datetimes, formatted as
        microseconds since 2000-01-01 00:00:00
    """
    # TODO: improve typehints
    var.set_auto_scale(True)
    date_time = nc.num2date(
        var[rows],
        var.units,
        only_use_cftime_datetimes=False,
        only_use_python_datetimes=False,
    )
    # date_time = nc.date2num(
    #     date_time, "microseconds since 2000-01-01 00:00:00"
    # )  # Convert the standard date back to the 8-byte integer PSQL uses
    return date_time
