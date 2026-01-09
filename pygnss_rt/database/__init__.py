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
]
