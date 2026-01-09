"""Station management module."""

from pygnss_rt.stations.station import Station, StationManager
from pygnss_rt.stations.coordinates import (
    ecef_to_geodetic,
    geodetic_to_ecef,
    calculate_distance,
)
from pygnss_rt.stations.bswsta import BSWStationFile, StationRecord
from pygnss_rt.stations.file_writers import (
    CRDFileWriter,
    OTLFileWriter,
    ABBFileWriter,
    VELFileWriter,
    StationListWriter,
    StationCoordinate,
    OceanTideLoading,
    StationAbbreviation,
    write_crd_file,
    write_station_list,
)

__all__ = [
    "Station",
    "StationManager",
    "ecef_to_geodetic",
    "geodetic_to_ecef",
    "calculate_distance",
    # BSW station file parser
    "BSWStationFile",
    "StationRecord",
    # File writers
    "CRDFileWriter",
    "OTLFileWriter",
    "ABBFileWriter",
    "VELFileWriter",
    "StationListWriter",
    "StationCoordinate",
    "OceanTideLoading",
    "StationAbbreviation",
    "write_crd_file",
    "write_station_list",
]
