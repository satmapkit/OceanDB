"""Generate the literal, test-only source reference (not a production catalog).

From the repository root::

    uv run --no-sync python tests/data/along_track_reference/generate.py \
        --output-directory <directory>

The intended NETCDF3_CLASSIC asset has no downloaded-product identity. Its
packed and physical oracles below are independent literals, never outputs of
the production decoder. This is a synthetic PostgreSQL regression fixture, not
real-data or cross-backend benchmark evidence.
"""

import argparse
import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

import netCDF4 as nc
import numpy as np

SOURCE_NAME = "dt_global_j2_phy_l3_1hz_20240101_20240102.nc"
ASSET_NAME = SOURCE_NAME
BASE_CODE_REVISION = "9d4b76e283aa38898a94bf6420b3c1573e0bad32"
REPO_ROOT = Path(__file__).resolve().parents[3]
ROW_COUNT = 6
TIME_RAW = (
    27028.999989854816,
    27028.999999999978,
    27029.000000000022,
    27029.000019147235,
    27029.00002314815,
    27029.000038580245,
)
TIME_UTC = (
    "2024-01-01T23:59:59.123456Z",
    "2024-01-01T23:59:59.999998Z",
    "2024-01-02T00:00:00.000002Z",
    "2024-01-02T00:00:01.654321Z",
    "2024-01-02T00:00:02.000000Z",
    "2024-01-02T00:00:03.333333Z",
)
LATITUDE_RAW = (59128138, 0, 1000000, -1000000, 12345678, -63705209)
LONGITUDE_RAW = (297241060, 180000000, 180000000, 359999999, 0, 107813165)
# Preserve the decoded microdegree float64 values, including the last latitude.
LATITUDE = (59.128138, 0.0, 1.0, -1.0, 12.345678, -63.705208999999996)
LONGITUDE = (297.24106, 180.0, 180.0, 359.999999, 0.0, 107.813165)
TRACK = (200, 200, 201, 201, 202, 202)
CYCLE = (165, 165, 165, 165, 165, 165)
# Measured once directly from the pinned packaged raster; row 4 is land.
BASIN_ID = (17, 5, 4, 7, 0, 2)
ScienceField = Literal[
    "sla_unfiltered",
    "sla_filtered",
    "dac",
    "ocean_tide",
    "internal_tide",
    "lwe",
    "mdt",
    "tpa_correction",
]


@dataclass(frozen=True)
class ScienceOracle:
    """Independent literals for on-disk, stored, and returned scientific values."""

    raw: tuple[int, ...] | int
    packed: tuple[int | None, ...]
    physical: tuple[float | None, ...]


SCIENCE: dict[ScienceField, ScienceOracle] = {
    "sla_unfiltered": ScienceOracle(
        (173, 32767, -32766, -234, 0, 32000),
        (173, None, None, -234, 0, 32000),
        (0.173, None, None, -0.234, 0.0, 32.0),
    ),
    "sla_filtered": ScienceOracle(
        (148, 199, -32766, 32767, -1, 0),
        (148, 199, None, None, -1, 0),
        (0.148, 0.199, None, None, -0.001, 0.0),
    ),
    "dac": ScienceOracle(
        (86, -89, 32767, -32766, 0, 1001),
        (86, -89, None, None, 0, 1001),
        (0.086, -0.089, None, None, 0.0, 1.001),
    ),
    "ocean_tide": ScienceOracle(
        (255, 277, 297, 317, 32767, -32766),
        (255, 277, 297, 317, None, None),
        (0.255, 0.277, 0.297, 0.317, None, None),
    ),
    "internal_tide": ScienceOracle(
        (0, 1, -2, 32767, -32766, 1234),
        (0, 1, -2, None, None, 1234),
        (0.0, 0.001, -0.002, None, None, 1.234),
    ),
    "lwe": ScienceOracle(
        (28, 28, 32767, 29, -32766, -321),
        (28, 28, None, 29, None, -321),
        (0.028, 0.028, None, 0.029, None, -0.321),
    ),
    "mdt": ScienceOracle(
        (-133, 32767, -114, -32766, -109, 31000),
        (-133, None, -114, None, -109, 31000),
        (-0.133, None, -0.114, None, -0.109, 31.0),
    ),
    "tpa_correction": ScienceOracle(
        32767,
        (None, None, None, None, None, None),
        (None, None, None, None, None, None),
    ),
}
Attribute = str | int | float
TIME_ATTRIBUTES: dict[str, Attribute] = {
    "_FillValue": 9.969209968386869e36,
    "units": "days since 1950-01-01 00:00:00",
    "calendar": "gregorian",
    "standard_name": "time",
    "axis": "T",
}
COORDINATE_ATTRIBUTES: dict[str, Attribute] = {
    "_FillValue": -2147483647,
    "scale_factor": 0.000001,
    "add_offset": 0.0,
}
COUNTER_ATTRIBUTES: dict[str, Attribute] = {"_FillValue": -32767, "units": "1"}
SCIENCE_ATTRIBUTES: dict[str, Attribute] = {
    "_FillValue": 32767,
    "missing_value": -32766,
    "scale_factor": 0.001,
    "add_offset": 0.0,
    "units": "m",
}
# Preserve the original source bytes; the manifest records the fixture's scope.
GLOBAL_ATTRIBUTES = {
    "Conventions": "CF-1.6",
    "title": "Synthetic provisional OceanDB source reference",
    "source": "Literal oracle in tests/data/along_track_reference/generate.py",
}
DEPENDENCY_PATHS = (
    "tests/data/along_track_reference/generate.py",
    "src/OceanDB/data/basin_masks/new_basin_mask.nc",
    "src/OceanDB/data/basins/ocean_basins.csv",
    "src/OceanDB/data/basins/basin_connection_table.txt",
)


@dataclass(frozen=True)
class SourceVariable:
    """Deterministically ordered NetCDF definition with literal raw values."""

    dtype: Literal["i2", "i4", "f8"]
    dimensions: tuple[str, ...]
    raw: tuple[int, ...] | tuple[float, ...] | int
    attributes: dict[str, Attribute]


def source_variables() -> dict[str, SourceVariable]:
    """Return source definitions without consulting a production decoder.

    :returns: Every required variable, in fixed on-disk order.
    """
    variables = {
        "time": SourceVariable("f8", ("time",), TIME_RAW, TIME_ATTRIBUTES),
        "latitude": SourceVariable(
            "i4",
            ("time",),
            LATITUDE_RAW,
            {**COORDINATE_ATTRIBUTES, "units": "degrees_north"},
        ),
        "longitude": SourceVariable(
            "i4",
            ("time",),
            LONGITUDE_RAW,
            {**COORDINATE_ATTRIBUTES, "units": "degrees_east"},
        ),
        "track": SourceVariable("i2", ("time",), TRACK, COUNTER_ATTRIBUTES),
        "cycle": SourceVariable("i2", ("time",), CYCLE, COUNTER_ATTRIBUTES),
    }
    for name, oracle in SCIENCE.items():
        variables[name] = SourceVariable(
            "i2",
            () if isinstance(oracle.raw, int) else ("time",),
            oracle.raw,
            SCIENCE_ATTRIBUTES,
        )
    return variables


def generate(output_directory: Path) -> None:
    """Write the reproducible asset and generator/basin provenance manifest.

    :param output_directory: Directory receiving the NetCDF source and JSON manifest.
    """
    output_directory.mkdir(parents=True, exist_ok=True)
    source_path = output_directory / ASSET_NAME
    with nc.Dataset(source_path, "w", format="NETCDF3_CLASSIC") as ds:
        ds.setncatts(GLOBAL_ATTRIBUTES)
        ds.createDimension("time", ROW_COUNT)
        for name, spec in source_variables().items():
            var = ds.createVariable(
                name,
                spec.dtype,
                spec.dimensions,
                fill_value=spec.attributes["_FillValue"],
            )
            for attribute, value in spec.attributes.items():
                if attribute != "_FillValue":
                    var.setncattr(
                        attribute,
                        (
                            np.asarray(value, dtype=spec.dtype)
                            if attribute == "missing_value"
                            else value
                        ),
                    )
            var.set_auto_maskandscale(False)
            var[...] = np.asarray(spec.raw, dtype=spec.dtype)
    source_bytes = source_path.read_bytes()
    manifest = {
        "status": "SYNTHETIC",
        "scope": "PostgreSQL regression fixture; not real-data or cross-backend benchmark evidence.",
        "origin_base_code_revision": BASE_CODE_REVISION,
        "oracle": "Independent packed/physical literals in generate.py; no production decoder.",
        "source": {
            "path": ASSET_NAME,
            "ingest_filename": SOURCE_NAME,
            "sha256": hashlib.sha256(source_bytes).hexdigest(),
            "size_bytes": len(source_bytes),
            "format": "NETCDF3_CLASSIC",
            "origin": "Synthetic test asset; no upstream Copernicus observation claimed.",
            "mission": "j2",
            "row_count": ROW_COUNT,
            "actual_time_coverage_utc": {"start": TIME_UTC[0], "end": TIME_UTC[-1]},
            "original_row_indices_0based": list(range(ROW_COUNT)),
            "row_identity": "(source SHA256, original zero-based row index); test-only identity.",
            "returned_row_key": ["file_name", "date_time", "latitude", "longitude"],
            "global_attributes": GLOBAL_ATTRIBUTES,
        },
        "variable_metadata": {
            "time": {"dtype": "f8", "dimensions": ["time"], **TIME_ATTRIBUTES},
            "coordinates": {
                "dtype": "i4",
                "dimensions": ["time"],
                **COORDINATE_ATTRIBUTES,
                "units": {"latitude": "degrees_north", "longitude": "degrees_east"},
            },
            "track_cycle": {
                "names": ["track", "cycle"],
                "dtype": "i2",
                "dimensions": ["time"],
                **COUNTER_ATTRIBUTES,
            },
            "science": {
                "dtype": "i2",
                "row_dimensions": ["time"],
                "row_variables": [name for name in SCIENCE if name != "tpa_correction"],
                "scalar_variables": ["tpa_correction"],
                "scalar_dimensions": [],
                **SCIENCE_ATTRIBUTES,
            },
        },
        "policies": {
            "packing": "Store scientific int16 unchanged; expose metres with scale 0.001 exactly once.",
            "missing": "Both science sentinels become SQL NULL and returned NaN; scalar fill broadcasts to all rows.",
            "coordinates": "Already [0, 360) longitude; retain decoded float64 microdegree precision.",
            "basins": "Literal labels measured once from pinned packaged raster; land row 4 retained.",
            "duplicates": "Reject identical or conflicting (date_time, latitude, longitude) with UniqueViolation.",
        },
        "dependency_sha256": {
            path: hashlib.sha256((REPO_ROOT / path).read_bytes()).hexdigest()
            for path in DEPENDENCY_PATHS
        },
    }
    (output_directory / "manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-directory", type=Path, default=Path(__file__).parent)
    generate(parser.parse_args().output_directory)
