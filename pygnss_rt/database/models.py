"""
Data models for database records.

Uses dataclasses for type-safe data handling.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any


class ProductType(str, Enum):
    """GNSS product types."""

    ORBIT = "orbit"
    ERP = "erp"
    CLOCK = "clock"
    DCB = "dcb"
    IONOSPHERE = "ionosphere"
    ANTENNA = "antenna"


class ProductTier(str, Enum):
    """Product timeliness tiers."""

    FINAL = "final"
    RAPID = "rapid"
    ULTRA = "ultra"
    REALTIME = "realtime"


@dataclass
class Product:
    """GNSS product record."""

    product_type: ProductType
    provider: str
    tier: ProductTier
    mjd: float
    filename: str
    gps_week: int | None = None
    day_of_week: int | None = None
    local_path: str | None = None
    file_size: int | None = None
    checksum: str | None = None
    download_time: datetime | None = None
    id: int | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for database insertion."""
        return {
            "product_type": self.product_type.value,
            "provider": self.provider,
            "tier": self.tier.value,
            "mjd": self.mjd,
            "gps_week": self.gps_week,
            "day_of_week": self.day_of_week,
            "filename": self.filename,
            "local_path": self.local_path,
            "file_size": self.file_size,
            "checksum": self.checksum,
        }


@dataclass
class HourlyData:
    """Hourly observation data record."""

    station_id: str
    mjd: float
    hour: int
    rinex_file: str | None = None
    status: str = "pending"
    created_at: datetime | None = None
    id: int | None = None


@dataclass
class Station:
    """Station metadata."""

    station_id: str
    name: str | None = None
    network: str | None = None
    latitude: float | None = None
    longitude: float | None = None
    height: float | None = None
    use_nrt: bool = True
    active: bool = True
    created_at: datetime | None = None
    id: int | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "station_id": self.station_id,
            "name": self.name,
            "network": self.network,
            "latitude": self.latitude,
            "longitude": self.longitude,
            "height": self.height,
            "use_nrt": self.use_nrt,
            "active": self.active,
        }


@dataclass
class ProcessingRun:
    """Processing run record."""

    run_type: str
    start_mjd: float
    end_mjd: float
    status: str
    start_time: datetime | None = None
    end_time: datetime | None = None
    stations_processed: int = 0
    errors: str | None = None
    id: int | None = None


@dataclass
class ZTDResult:
    """ZTD/IWV result record."""

    station_id: str
    mjd: float
    ztd: float
    ztd_sigma: float | None = None
    zhd: float | None = None
    zwd: float | None = None
    iwv: float | None = None
    iwv_sigma: float | None = None
    temperature: float | None = None
    pressure: float | None = None
    processing_run_id: int | None = None
    created_at: datetime | None = None
    id: int | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "station_id": self.station_id,
            "mjd": self.mjd,
            "ztd": self.ztd,
            "ztd_sigma": self.ztd_sigma,
            "zhd": self.zhd,
            "zwd": self.zwd,
            "iwv": self.iwv,
            "iwv_sigma": self.iwv_sigma,
            "temperature": self.temperature,
            "pressure": self.pressure,
            "processing_run_id": self.processing_run_id,
        }
