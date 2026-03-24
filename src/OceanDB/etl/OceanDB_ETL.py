from dataclasses import dataclass
from dataclasses import asdict
import netCDF4 as nc
import numpy as np
from OceanDB.OceanDB import OceanDB
from typing import Optional
from pathlib import Path

NDArray = np.ndarray


@dataclass
class AlongTrackData:
    """Structured container for extracted along-track variables."""

    file_name: np.ndarray
    mission: np.ndarray
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


@dataclass
class AlongTrackMetaData:
    """Structured representation of NetCDF global metadata."""

    file_name: str
    conventions: Optional[str] = None
    metadata_conventions: Optional[str] = None
    cdm_data_type: Optional[str] = None
    comment: Optional[str] = None
    contact: Optional[str] = None
    creator_email: Optional[str] = None
    creator_name: Optional[str] = None
    creator_url: Optional[str] = None
    date_created: Optional[str] = None
    date_issued: Optional[str] = None
    date_modified: Optional[str] = None
    history: Optional[str] = None
    institution: Optional[str] = None
    keywords: Optional[str] = None
    license: Optional[str] = None
    platform: Optional[str] = None
    processing_level: Optional[str] = None
    product_version: Optional[str] = None
    project: Optional[str] = None
    references: Optional[str] = None
    software_version: Optional[str] = None
    source: Optional[str] = None
    ssalto_duacs_comment: Optional[str] = None
    summary: Optional[str] = None
    title: Optional[str] = None

    def to_dict(self):
        return asdict(self)

    @classmethod
    def from_netcdf(cls, ds: nc.Dataset, file_name: str) -> "AlongTrackMetaData":
        """Create AlongTrackMetaData from a NetCDF4 dataset."""

        if not isinstance(ds, nc.Dataset):
            raise TypeError("AlongTrackMetaData requires a NetCDF Dataset")

        def get(attr: str):
            return getattr(ds, attr, None)

        conventions = getattr(ds, "Conventions", None)

        print(f"CONVENTIONS {conventions}")
        return cls(
            file_name=file_name,
            conventions=get("Conventions"),
            metadata_conventions=get("Metadata_Conventions"),
            cdm_data_type=get("cdm_data_type"),
            comment=get("comment"),
            contact=get("contact"),
            creator_email=get("creator_email"),
            creator_name=get("creator_name"),
            creator_url=get("creator_url"),
            date_created=get("date_created"),
            date_issued=get("date_issued"),
            date_modified=get("date_modified"),
            history=get("history"),
            institution=get("institution"),
            keywords=get("keywords"),
            license=get("license"),
            platform=get("platform"),
            processing_level=get("processing_level"),
            product_version=get("product_version"),
            project=get("project"),
            references=get("references"),
            software_version=get("software_version"),
            source=get("source"),
            ssalto_duacs_comment=get("ssalto_duacs_comment"),
            summary=get("summary"),
            title=get("title"),
        )


@dataclass
class EddyData:
    """Structured container for detected eddy observations."""

    amplitude: NDArray
    cost_association: NDArray
    effective_area: NDArray
    effective_contour_height: NDArray
    effective_contour_latitude: NDArray
    effective_contour_longitude: NDArray
    effective_contour_shape_error: NDArray
    effective_radius: NDArray
    inner_contour_height: NDArray
    latitude: NDArray
    latitude_max: NDArray
    longitude: NDArray
    longitude_max: NDArray
    num_contours: NDArray
    num_point_e: NDArray
    num_point_s: NDArray
    observation_flag: NDArray  # will normalize to bool
    observation_number: NDArray
    speed_area: NDArray
    speed_average: NDArray
    speed_contour_height: NDArray
    speed_contour_latitude: NDArray
    speed_contour_longitude: NDArray
    speed_contour_shape_error: NDArray
    speed_radius: NDArray
    date_time: NDArray
    track: NDArray

    def __post_init__(self) -> None:
        """Normalize and validate eddy data arrays."""
        if self.observation_flag.dtype != bool:
            self.observation_flag = self.observation_flag.astype(bool)

        # Basic shape validation (cheap, very effective)
        n = len(self.latitude)
        for name, value in vars(self).items():
            if len(value) != n:
                raise ValueError(
                    f"EddyData field '{name}' has length {len(value)} != {n}"
                )


class OceanDBETL(OceanDB):
    ocean_basin_table_name: str = "basin"
    ocean_basins_connections_table_name: str = "basin_connection"
    along_track_table_name: str = "along_track"
    along_track_metadata_table_name: str = "along_track_metadata"

    variable_scale_factor: dict = dict()
    variable_add_offset: dict = dict()
    missions = [
        "al",
        "alg",
        "c2",
        "c2n",
        "e1g",
        "e1",
        "e2",
        "en",
        "enn",
        "g2",
        "h2a",
        "h2b",
        "j1g",
        "j1",
        "j1n",
        "j2g",
        "j2",
        "j2n",
        "j3",
        "j3n",
        "s3a",
        "s3b",
        "s6a",
        "tp",
        "tpn",
    ]

    def __init__(self):
        super().__init__()

    @staticmethod
    def along_track_variable_metadata():
        along_track_variable_metadata = [
            {
                "var_name": "sla_unfiltered",
                "comment": "The sea level anomaly is the sea surface height above mean sea surface height; the uncorrected sla can be computed as follows: [uncorrected sla]=[sla from product]+[dac]+[ocean_tide]+[internal_tide]-[lwe]; see the product user manual for details",
                "long_name": "Sea level anomaly not-filtered not-subsampled with dac, ocean_tide and lwe correction applied",
                "scale_factor": 0.001,
                "standard_name": "sea_surface_height_above_sea_level",
                "units": "m",
                "dtype": "int16",
            },
            {
                "var_name": "sla_filtered",
                "comment": "The sea level anomaly is the sea surface height above mean sea surface height; the uncorrected sla can be computed as follows: [uncorrected sla]=[sla from product]+[dac]+[ocean_tide]+[internal_tide]-[lwe]; see the product user manual for details",
                "long_name": "Sea level anomaly filtered not-subsampled with dac, ocean_tide and lwe correction applied",
                "scale_factor": 0.001,
                "add_offset": 0.0,
                "_FillValue": 32767,
                "standard_name": "sea_surface_height_above_sea_level",
                "units": "m",
                "dtype": "int16",
            },
            {
                "var_name": "dac",
                "comment": "The sla in this file is already corrected for the dac; the uncorrected sla can be computed as follows: [uncorrected sla]=[sla from product]+[dac]; see the product user manual for details",
                "long_name": "Dynamic Atmospheric Correction",
                "scale_factor": 0.001,
                "standard_name": None,
                "units": "m",
                "dtype": "int16",
            },
            {
                "var_name": "time",
                "comment": "",
                "long_name": "Time of measurement",
                "scale_factor": None,
                "standard_name": "time",
                "units": "days since 1950-01-01 00:00:00",
                "calendar": "gregorian",
            },
            {
                "var_name": "track",
                "comment": "",
                "long_name": "Track in cycle the measurement belongs to",
                "scale_factor": None,
                "standard_name": None,
                "units": "1\n",
                "dtype": "int16",
            },
            {
                "var_name": "cycle",
                "comment": "",
                "long_name": "Cycle the measurement belongs to",
                "scale_factor": None,
                "standard_name": None,
                "units": "1",
                "dtype": "int16",
            },
            {
                "var_name": "ocean_tide",
                "comment": "The sla in this file is already corrected for the ocean_tide; the uncorrected sla can be computed as follows: [uncorrected sla]=[sla from product]+[ocean_tide]; see the product user manual for details",
                "long_name": "Ocean tide model",
                "scale_factor": 0.001,
                "standard_name": None,
                "units": "m",
                "dtype": "int16",
            },
            {
                "var_name": "internal_tide",
                "comment": "The sla in this file is already corrected for the internal_tide; the uncorrected sla can be computed as follows: [uncorrected sla]=[sla from product]+[internal_tide]; see the product user manual for details",
                "long_name": "Internal tide correction",
                "scale_factor": 0.001,
                "standard_name": None,
                "units": "m",
                "dtype": "int16",
            },
            {
                "var_name": "lwe",
                "comment": "The sla in this file is already corrected for the lwe; the uncorrected sla can be computed as follows: [uncorrected sla]=[sla from product]-[lwe]; see the product user manual for details",
                "long_name": "Long wavelength error",
                "scale_factor": 0.001,
                "standard_name": None,
                "units": "m",
                "dtype": "int16",
            },
            {
                "var_name": "mdt",
                "comment": "The mean dynamic topography is the sea surface height above geoid; it is used to compute the absolute dynamic tyopography adt=sla+mdt",
                "long_name": "Mean dynamic topography",
                "scale_factor": 0.001,
                "standard_name": "sea_surface_height_above_geoid",
                "units": "m",
                "dtype": "int16",
            },
        ]
        return along_track_variable_metadata

    def load_netcdf(self, file: Path) -> nc.Dataset:
        ds = nc.Dataset(file, "r")
        return ds

    def extract_dataset_metadata(
        self, ds: nc.Dataset, file: Path
    ) -> AlongTrackMetaData:
        return AlongTrackMetaData.from_netcdf(ds, file_name=file.name)

    def extract_data_from_netcdf(self, ds: nc.Dataset, file: Path) -> AlongTrackData:
        """
        Parse & transform NetCDF file
        """
        mission = file.name.split("_")[2]
        try:
            ds.variables["sla_unfiltered"].set_auto_scale(False)
            ds.variables["sla_filtered"].set_auto_scale(False)
            ds.variables["ocean_tide"].set_auto_scale(False)
            ds.variables["internal_tide"].set_auto_scale(False)
            ds.variables["lwe"].set_auto_scale(False)
            ds.variables["mdt"].set_auto_scale(False)
            ds.variables["dac"].set_auto_scale(False)
            ds.variables["tpa_correction"].set_auto_scale(False)

            time_data = ds.variables[
                "time"
            ]  # Extract dates from the dataset and convert them to standard datetime
            time_data = nc.num2date(
                time_data[:],
                time_data.units,
                only_use_cftime_datetimes=False,
                only_use_python_datetimes=False,
            )
            time_data = nc.date2num(
                time_data[:], "microseconds since 2000-01-01 00:00:00"
            )  # Convert the standard date back to the 8-byte integer PSQL uses

            basin_id = self.basin_mask(
                ds.variables["latitude"][:], ds.variables["longitude"][:]
            )

            data = AlongTrackData(
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
                basin_id=basin_id,
                mission=mission,
                file_name=file.name,
            )
            ds.close()
            return data

        except Exception as ex:
            print(ex)
