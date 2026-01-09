"""Database management using DuckDB."""

from pygnss_rt.database.connection import DatabaseManager, init_db
from pygnss_rt.database.models import (
    ProductType,
    ProductTier,
    Product,
    HourlyData,
    Station,
    ProcessingRun,
    ZTDResult,
)
from pygnss_rt.database.products import ProductManager
from pygnss_rt.database.met import MetManager
from pygnss_rt.database.hourly_data import (
    HourlyDataManager,
    HDEntry,
    HDStatus,
    HDStatistics,
)
from pygnss_rt.database.daily_data import (
    DailyDataManager,
    SDEntry,
    SDStatus,
    SDStatistics,
)
from pygnss_rt.database.subhourly_met import (
    SubhourlyMetManager,
    SMEntry,
    SMStatus,
    SMStatistics,
)
from pygnss_rt.database.maintenance import (
    # Maintenance callers (replaces call_*_maintain.pl)
    HDMaintenanceCaller,
    SDMaintenanceCaller,
    SMMaintenanceCaller,
    MaintenanceConfig,
    MaintenanceStats,
    MaintenanceType,
    # Convenience functions
    run_hd_maintenance,
    run_sd_maintenance,
    run_sm_maintenance,
    run_all_maintenance,
)

__all__ = [
    "DatabaseManager",
    "init_db",
    "ProductType",
    "ProductTier",
    "Product",
    "HourlyData",
    "Station",
    "ProcessingRun",
    "ZTDResult",
    "ProductManager",
    "MetManager",
    # Hourly data (HD) management
    "HourlyDataManager",
    "HDEntry",
    "HDStatus",
    "HDStatistics",
    # Daily data (SD) management
    "DailyDataManager",
    "SDEntry",
    "SDStatus",
    "SDStatistics",
    # Subhourly MET (SM) management
    "SubhourlyMetManager",
    "SMEntry",
    "SMStatus",
    "SMStatistics",
    # Maintenance callers (replaces call_*_maintain.pl)
    "HDMaintenanceCaller",
    "SDMaintenanceCaller",
    "SMMaintenanceCaller",
    "MaintenanceConfig",
    "MaintenanceStats",
    "MaintenanceType",
    "run_hd_maintenance",
    "run_sd_maintenance",
    "run_sm_maintenance",
    "run_all_maintenance",
]
