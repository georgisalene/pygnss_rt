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
    # PROCSTNS.LST file support
    ProcStationEntry,
    ProcStationListWriter,
    write_procstns_list,
)
from pygnss_rt.stations.station_info import (
    # Station data and manager (STA.pm port)
    StationData,
    StationInfoManager,
    # WMO meteorological stations
    WMOStation,
    WMOStationParser,
    # Convenience functions
    load_station_info,
    get_nrt_station_list,
    merge_station_files,
)
# Site log parsing (i-BSWSTA ASCII2XML.pm port)
from pygnss_rt.stations.site_log_parser import (
    SiteLogParser,
    SiteLogData,
    SiteIdentification,
    SiteLocation,
    ReceiverInfo,
    AntennaInfo,
    MeteorologicalSensor,
    ContactInfo,
    # Section 5-13 dataclasses
    SurveyedLocalTie,
    FrequencyStandard,
    CollocationInformation,
    RadioInterference,
    MultipathSource,
    SignalObstruction,
    LocalEpisodicEvent,
    MoreInformation,
    parse_site_log,
    parse_site_logs_directory,
)
# Bernese STA file generation (i-BSWSTA DB2BSWSta52.pm port)
from pygnss_rt.stations.sta_file_writer import (
    STAFileWriter,
    STAEvent,
    STAStationInfo,
    write_sta_file,
    write_sta_from_directory,
    # MJD conversion utilities
    datetime_to_mjd,
    mjd_to_datetime,
)
# Site log downloading (i-BSWSTA FTPSiteLog.pm port)
from pygnss_rt.stations.site_log_downloader import (
    SiteLogDownloader,
    SiteLogSource,
    SiteLogDownloadResult,
    download_site_logs,
    download_and_parse_site_logs,
    DEFAULT_SITE_LOG_SOURCES,
    IGS_SITE_LOG_SOURCE,
    EUREF_SITE_LOG_SOURCE,
    OSGB_SITE_LOG_SOURCE,
)
# AutoStation processor (i-BSWSTA call_autoSta_*.pl port)
from pygnss_rt.stations.auto_station import (
    AutoStationProcessor,
    AutoStationConfig,
    AutoStationResult,
    process_station_metadata,
    update_sta_file,
    # Bad stations lists
    IGS_BAD_STATIONS,
    OSGB_BAD_STATIONS,
    DEFAULT_BAD_STATIONS,
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
    # PROCSTNS.LST file support
    "ProcStationEntry",
    "ProcStationListWriter",
    "write_procstns_list",
    # CRD conversion utilities
    "crd_to_otl",
    "crd_to_station_xml",
    "print_station_xml_blocks",
    # Station info manager (STA.pm port)
    "StationData",
    "StationInfoManager",
    # WMO meteorological stations
    "WMOStation",
    "WMOStationParser",
    # Convenience functions
    "load_station_info",
    "get_nrt_station_list",
    "merge_station_files",
    # Site log parsing (i-BSWSTA ASCII2XML.pm port)
    "SiteLogParser",
    "SiteLogData",
    "SiteIdentification",
    "SiteLocation",
    "ReceiverInfo",
    "AntennaInfo",
    "MeteorologicalSensor",
    "ContactInfo",
    # Section 5-13 dataclasses
    "SurveyedLocalTie",
    "FrequencyStandard",
    "CollocationInformation",
    "RadioInterference",
    "MultipathSource",
    "SignalObstruction",
    "LocalEpisodicEvent",
    "MoreInformation",
    "parse_site_log",
    "parse_site_logs_directory",
    # Bernese STA file generation (i-BSWSTA DB2BSWSta52.pm port)
    "STAFileWriter",
    "STAEvent",
    "STAStationInfo",
    "write_sta_file",
    "write_sta_from_directory",
    # MJD conversion utilities
    "datetime_to_mjd",
    "mjd_to_datetime",
    # Site log downloading (i-BSWSTA FTPSiteLog.pm port)
    "SiteLogDownloader",
    "SiteLogSource",
    "SiteLogDownloadResult",
    "download_site_logs",
    "download_and_parse_site_logs",
    "DEFAULT_SITE_LOG_SOURCES",
    "IGS_SITE_LOG_SOURCE",
    "EUREF_SITE_LOG_SOURCE",
    "OSGB_SITE_LOG_SOURCE",
    # AutoStation processor (i-BSWSTA call_autoSta_*.pl port)
    "AutoStationProcessor",
    "AutoStationConfig",
    "AutoStationResult",
    "process_station_metadata",
    "update_sta_file",
    # Bad stations lists
    "IGS_BAD_STATIONS",
    "OSGB_BAD_STATIONS",
    "DEFAULT_BAD_STATIONS",
]
