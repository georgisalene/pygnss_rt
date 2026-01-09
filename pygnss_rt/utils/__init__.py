"""Utility modules for date/time handling, logging, and monitoring."""

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
]
