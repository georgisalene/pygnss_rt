"""Utility modules for date/time handling, logging, monitoring, and GNSS processing."""

from pygnss_rt.utils.dates import (
    GNSSDate,
    mjd_from_date,
    date_from_mjd,
    gps_week_from_mjd,
    mjd_from_gps_week,
    doy_from_date,
    hour_to_alpha,
    alpha_to_hour,
)
from pygnss_rt.utils.logging import (
    get_logger,
    setup_logging,
    # PRINT.pm replacements
    MessageType,
    IGNSSPrinter,
    ignss_print,
    ignss_banner,
)
from pygnss_rt.utils.wmo_format import (
    WMOParser,
    WMOStation,
    format_wmo_file,
)
from pygnss_rt.utils.monitoring import (
    # TIVOLI2.pm replacement - Alert management
    AlertManager,
    AlertLevel,
    AlertType,
    ProcessingAlert,
    EmailConfig,
    AlertStats,
    ALERT_CODES,
    # Convenience functions
    get_alert_manager,
    configure_alerts,
    alert,
    alert_error,
    alert_warning,
    alert_success,
)
from pygnss_rt.utils.compression import (
    # Compression/decompression (Hatanaka, gzip, Z-file, bz2)
    CompressionFormat,
    HatanakaFormat,
    CompressionResult,
    # Hatanaka functions
    decompress_hatanaka,
    compress_hatanaka,
    # General compression
    decompress_file,
    compress_file,
    decompress_z_file,
    detect_compression,
    is_compressed,
    get_uncompressed_name,
    # Batch operations
    decompress_directory,
)
from pygnss_rt.utils.rinex_qc import (
    # RINEX quality checking (teqc equivalent)
    RINEXQualityChecker,
    QualityResult,
    QualityLevel,
    SatelliteStats,
    EpochStats,
    ObservationType,
    # Convenience functions
    check_rinex_quality,
    batch_quality_check,
    is_rinex_usable,
    get_rinex_summary,
)
from pygnss_rt.utils.multi_gnss import (
    # Multi-GNSS constellation support
    GNSSConstellation,
    Satellite,
    # Frequency and signal handling
    GPSSignal,
    GLONASSSignal,
    GalileoSignal,
    BeiDouSignal,
    QZSSSignal,
    IRNSSSignal,
    GLONASSChannel,
    # Observation codes
    ObservationCode,
    # Time systems
    TimeSystem,
    GNSSTime,
    # Configuration
    ConstellationConfig,
    MultiGNSSConfig,
    MultiGNSSObservation,
    # Inter-system biases
    InterSystemBias,
    DifferentialCodeBias,
    # Frequency constants
    SPEED_OF_LIGHT,
    GPS_L1_FREQ,
    GPS_L2_FREQ,
    GPS_L5_FREQ,
    GALILEO_E1_FREQ,
    GALILEO_E5A_FREQ,
    GALILEO_E5B_FREQ,
    GALILEO_E6_FREQ,
    BEIDOU_B1_FREQ,
    BEIDOU_B1C_FREQ,
    BEIDOU_B2A_FREQ,
    BEIDOU_B3_FREQ,
    # Frequency/wavelength functions
    get_frequency,
    get_wavelength,
    get_glonass_frequency,
    get_ionosphere_free_combination,
    get_geometry_free_combination,
    get_wide_lane_combination,
    get_narrow_lane_combination,
    # PRN handling
    parse_prn,
    format_prn,
    convert_prn,
    is_valid_prn,
    # Observation code functions
    convert_obs_code,
    get_constellation_signals,
    # Time system functions
    get_constellation_time_system,
    # Utility functions
    list_constellations,
    get_all_frequencies,
)

__all__ = [
    # Date/time utilities
    "GNSSDate",
    "mjd_from_date",
    "date_from_mjd",
    "gps_week_from_mjd",
    "mjd_from_gps_week",
    "doy_from_date",
    "hour_to_alpha",
    "alpha_to_hour",
    # Logging
    "get_logger",
    "setup_logging",
    # PRINT.pm replacements
    "MessageType",
    "IGNSSPrinter",
    "ignss_print",
    "ignss_banner",
    # WMO format utilities
    "WMOParser",
    "WMOStation",
    "format_wmo_file",
    # TIVOLI2.pm replacement - Alert management
    "AlertManager",
    "AlertLevel",
    "AlertType",
    "ProcessingAlert",
    "EmailConfig",
    "AlertStats",
    "ALERT_CODES",
    "get_alert_manager",
    "configure_alerts",
    "alert",
    "alert_error",
    "alert_warning",
    "alert_success",
    # Compression/decompression (Hatanaka, gzip, Z-file support)
    "CompressionFormat",
    "HatanakaFormat",
    "CompressionResult",
    "decompress_hatanaka",
    "compress_hatanaka",
    "decompress_file",
    "compress_file",
    "decompress_z_file",
    "detect_compression",
    "is_compressed",
    "get_uncompressed_name",
    "decompress_directory",
    # RINEX quality checking (teqc equivalent)
    "RINEXQualityChecker",
    "QualityResult",
    "QualityLevel",
    "SatelliteStats",
    "EpochStats",
    "ObservationType",
    "check_rinex_quality",
    "batch_quality_check",
    "is_rinex_usable",
    "get_rinex_summary",
    # Multi-GNSS constellation support
    "GNSSConstellation",
    "Satellite",
    "GPSSignal",
    "GLONASSSignal",
    "GalileoSignal",
    "BeiDouSignal",
    "QZSSSignal",
    "IRNSSSignal",
    "GLONASSChannel",
    "ObservationCode",
    "TimeSystem",
    "GNSSTime",
    "ConstellationConfig",
    "MultiGNSSConfig",
    "MultiGNSSObservation",
    "InterSystemBias",
    "DifferentialCodeBias",
    # Frequency constants
    "SPEED_OF_LIGHT",
    "GPS_L1_FREQ",
    "GPS_L2_FREQ",
    "GPS_L5_FREQ",
    "GALILEO_E1_FREQ",
    "GALILEO_E5A_FREQ",
    "GALILEO_E5B_FREQ",
    "GALILEO_E6_FREQ",
    "BEIDOU_B1_FREQ",
    "BEIDOU_B1C_FREQ",
    "BEIDOU_B2A_FREQ",
    "BEIDOU_B3_FREQ",
    # Frequency/wavelength functions
    "get_frequency",
    "get_wavelength",
    "get_glonass_frequency",
    "get_ionosphere_free_combination",
    "get_geometry_free_combination",
    "get_wide_lane_combination",
    "get_narrow_lane_combination",
    # PRN handling
    "parse_prn",
    "format_prn",
    "convert_prn",
    "is_valid_prn",
    # Observation code functions
    "convert_obs_code",
    "get_constellation_signals",
    # Time system functions
    "get_constellation_time_system",
    # Utility functions
    "list_constellations",
    "get_all_frequencies",
]
