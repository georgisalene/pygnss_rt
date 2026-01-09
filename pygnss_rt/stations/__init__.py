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
    CRDFileReader,
    OTLFileWriter,
    ABBFileWriter,
    VELFileWriter,
    StationListWriter,
    StationXMLWriter,
    StationCoordinate,
    OceanTideLoading,
    StationAbbreviation,
    StationXMLEntry,
    write_crd_file,
    write_station_list,
    crd_to_otl,
    crd_to_station_xml,
    print_station_xml_blocks,
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
    "CRDFileReader",
    "OTLFileWriter",
    "ABBFileWriter",
    "VELFileWriter",
    "StationListWriter",
    "StationXMLWriter",
    "StationCoordinate",
    "OceanTideLoading",
    "StationAbbreviation",
    "StationXMLEntry",
    "write_crd_file",
    "write_station_list",
    # CRD conversion utilities
    "crd_to_otl",
    "crd_to_station_xml",
    "print_station_xml_blocks",
]
