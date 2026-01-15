"""
i-GNSS Processing Orchestrator.

Replaces Perl IGNSS.pm - the main orchestration module that:
- Validates processing arguments
- Checks product availability (orbits, clocks, ERP, DCB, BIA, ION)
- Manages data downloads (hourly/daily/subhourly)
- Executes Bernese GNSS Software (BSW) processing
- Organizes results and output

This is the central coordinator for all processing workflows.
"""

from __future__ import annotations

import gzip
import os
import shutil
import subprocess
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any

from pygnss_rt.core.exceptions import ProcessingError
from pygnss_rt.data_access.ftp_client import FTPClient
from pygnss_rt.data_access.ftp_config import FTPServerConfig, load_ftp_config
from pygnss_rt.data_access.product_downloader import (
    ProductDownloader,
    ProductDownloadConfig,
    ProductDownloadResult,
    DownloadStatus,
)
from pygnss_rt.database import DatabaseManager, ProductManager, HourlyDataManager
from pygnss_rt.products.orbit import SP3Reader
from pygnss_rt.utils.dates import GNSSDate, mjd_from_date, gps_week_from_mjd
from pygnss_rt.utils.logging import get_logger, ignss_print, MessageType


logger = get_logger(__name__)


# =============================================================================
# Enums and Types
# =============================================================================

class ProcessingType(str, Enum):
    """Type of processing to perform."""

    DAILY = "daily"
    HOURLY = "hourly"
    SUBHOURLY = "subhourly"


class ProductCategory(str, Enum):
    """GNSS product categories."""

    ORBIT = "eph"
    ERP = "erp"
    CLOCK = "clk"
    DCB = "dcb"
    BIA = "bia"
    ION = "ion"
    IEP = "iep"
    VMF3 = "vmf3"


# =============================================================================
# Configuration Classes
# =============================================================================

@dataclass
class ProductConfig:
    """Configuration for a GNSS product.

    Attributes:
        enabled: Whether this product is required
        provider_id: Product provider ID (e.g., 'IGS', 'CODE')
        product_type: Product type identifier
        category: Product category
    """

    enabled: bool = False
    provider_id: str = ""
    product_type: str = ""
    category: ProductCategory = ProductCategory.ORBIT


@dataclass
class DataSourceConfig:
    """FTP data source configuration.

    Attributes:
        provider_id: Data provider ID
        categories: List of data categories to download
    """

    provider_id: str = ""
    categories: list[str] = field(default_factory=list)


@dataclass
class DatabaseConfig:
    """Database connection configuration.

    Attributes:
        path: Path to DuckDB database file
        driver: Database driver (default: duckdb)
    """

    path: Path | None = None
    driver: str = "duckdb"


@dataclass
class DCMConfig:
    """Data/Campaign Management configuration.

    Replaces Perl DCM hash for campaign archival settings.

    Attributes:
        enabled: Whether DCM is enabled
        dirs_to_delete: List of subdirectories to delete before archiving
        compress_util: Compression utility ('gzip' or 'compress')
        archive_dir: Directory to archive campaigns to
        organization: Directory organization ('yyyy/doy' or flat)
    """

    enabled: bool = False
    dirs_to_delete: list[str] = field(default_factory=lambda: ["RAW", "OBS", "ORX"])
    compress_util: str = "gzip"
    archive_dir: Path | None = None
    organization: str = "yyyy/doy"


@dataclass
class ProcessingConfig:
    """Main processing configuration.

    This replaces the hash-based configuration in Perl IGNSS.pm.

    Attributes:
        proc_type: Processing type (daily/hourly/subhourly)
        gnss_date: Date to process
        campaign_name: Bernese campaign name
        session_id: Session identifier

        # Products
        orbit: Orbit product configuration
        erp: ERP product configuration
        clock: Clock product configuration
        dcb: DCB product configuration
        bia: BIA product configuration
        ion: ION product configuration

        # Data sources
        data_sources: List of FTP data sources

        # Paths
        ftp_config_path: Path to FTP configuration XML
        data_dir: Root data directory
        bsw_campaign_dir: Bernese campaign directory
        pcf_file: Path to PCF file

        # Info files (station/antenna configuration)
        info_sta: Station info file
        info_otl: Ocean tide loading file
        info_ses: Session info file
        info_sel: Station selection file

        # Database
        database: Database configuration

        # Processing options
        use_clockprep: Enable clock preparation
        use_cc2noncc: Enable CC to non-CC conversion
        use_teqc: Enable TEQC processing
    """

    # Core settings
    proc_type: ProcessingType = ProcessingType.DAILY
    gnss_date: GNSSDate | None = None
    campaign_name: str = ""
    session_id: str = ""

    # Product configurations
    orbit: ProductConfig = field(default_factory=ProductConfig)
    erp: ProductConfig = field(default_factory=ProductConfig)
    clock: ProductConfig = field(default_factory=ProductConfig)
    dcb: ProductConfig = field(default_factory=ProductConfig)
    bia: ProductConfig = field(default_factory=ProductConfig)
    ion: ProductConfig = field(default_factory=ProductConfig)
    vmf3: ProductConfig = field(default_factory=lambda: ProductConfig(enabled=True))  # VMF3 enabled by default

    # Data sources
    data_sources: list[DataSourceConfig] = field(default_factory=list)

    # Paths
    ftp_config_path: Path | None = None
    data_dir: Path | None = None
    bsw_campaign_dir: Path | None = None
    pcf_file: Path | None = None

    # Info files
    info_sta: Path | None = None
    info_otl: Path | None = None
    info_ses: Path | None = None
    info_sel: Path | None = None
    bsw_options: Path | None = None

    # Database
    database: DatabaseConfig = field(default_factory=DatabaseConfig)

    # Processing options
    use_clockprep: bool = False
    clockprep_bin: Path | None = None
    use_cc2noncc: bool = False
    cc2noncc_bin: Path | None = None
    p1c1_bias_hist: Path | None = None
    use_teqc: bool = False
    teqc_bin: Path | None = None

    # Control flags
    validate_args: bool = True
    verbose: bool = False

    # Data/Campaign Management (DCM)
    dcm: DCMConfig = field(default_factory=DCMConfig)


@dataclass
class ProcessingResult:
    """Result of a processing run.

    Attributes:
        success: Whether processing succeeded
        proc_type: Type of processing performed
        gnss_date: Date processed
        start_time: Processing start time
        end_time: Processing end time
        stations_requested: Number of stations requested
        stations_processed: Number of stations successfully processed
        files_downloaded: List of downloaded files
        output_files: List of output files produced
        errors: List of error messages
        warnings: List of warning messages
    """

    success: bool = False
    proc_type: ProcessingType = ProcessingType.DAILY
    gnss_date: GNSSDate | None = None
    start_time: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    end_time: datetime | None = None
    stations_requested: int = 0
    stations_processed: int = 0
    files_downloaded: list[str] = field(default_factory=list)
    output_files: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    @property
    def duration_seconds(self) -> float:
        """Get processing duration in seconds."""
        if self.end_time:
            return (self.end_time - self.start_time).total_seconds()
        return 0.0

    @property
    def success_rate(self) -> float:
        """Get station processing success rate."""
        if self.stations_requested == 0:
            return 0.0
        return self.stations_processed / self.stations_requested * 100


# =============================================================================
# Product Checking
# =============================================================================

class ProductChecker:
    """Check and download GNSS products.

    Handles orbit, ERP, clock, DCB, BIA, and ION products.
    Replaces Perl check_orbit, check_ERP, check_clock, check_DCB functions.
    """

    def __init__(
        self,
        config: ProcessingConfig,
        ftp_configs: list[FTPServerConfig] | None = None,
        db_manager: DatabaseManager | None = None,
    ):
        """Initialize product checker.

        Args:
            config: Processing configuration
            ftp_configs: Optional pre-loaded FTP configurations
            db_manager: Optional database manager for tracking
        """
        self.config = config
        self._ftp_configs = ftp_configs
        self._db_manager = db_manager
        self._product_manager: ProductManager | None = None

    def _load_ftp_configs(self) -> list[FTPServerConfig]:
        """Load FTP configurations from XML."""
        if self._ftp_configs:
            return self._ftp_configs

        if self.config.ftp_config_path and self.config.ftp_config_path.exists():
            return load_ftp_config(self.config.ftp_config_path)
        return []

    def _get_ftp_config(self, provider_id: str) -> FTPServerConfig | None:
        """Get FTP configuration for a provider."""
        configs = self._load_ftp_configs()
        for cfg in configs:
            if cfg.id == provider_id:
                return cfg
        return None

    def _get_product_manager(self) -> ProductManager | None:
        """Get or create product manager for DB tracking."""
        if self._product_manager:
            return self._product_manager

        if self._db_manager:
            self._product_manager = ProductManager(self._db_manager)
            return self._product_manager

        # Try to create from config
        if self.config.database.path:
            try:
                db_mgr = DatabaseManager(self.config.database.path)
                self._product_manager = ProductManager(db_mgr)
                return self._product_manager
            except Exception as e:
                logger.warning(f"Could not initialize product manager: {e}")

        return None

    def get_orbit_filename(self, body_pattern: str = "wwwwd") -> str | None:
        """Generate orbit filename based on configuration.

        Args:
            body_pattern: Body pattern from FTP config ('wwwwd', 'wwwwd_hn', 'doy0')

        Returns:
            Orbit filename or None if not configured
        """
        if not self.config.orbit.enabled or not self.config.gnss_date:
            return None

        gd = self.config.gnss_date
        provider = self.config.orbit.provider_id.lower()
        gps_week = gd.gps_week
        dow = gd.dow

        # Handle different body patterns (from Perl get_orbit_filename)
        if body_pattern == "wwwwd":
            body = f"{gps_week}{dow}"
        elif body_pattern == "wwwwd_hn":
            # For hourly products, determine 6-hour block
            hour = getattr(gd, "hour", 0)
            if 3 <= hour < 9:
                hh = "00"
            elif 9 <= hour < 15:
                hh = "06"
            elif 15 <= hour < 21:
                hh = "12"
            elif 21 <= hour < 24:
                hh = "18"
            else:  # hour 0-2, use previous day's 18:00
                hh = "18"
                dow = dow - 1
                if dow < 0:
                    gps_week = gps_week - 1
                    dow = 6
            body = f"{gps_week}{dow}_{hh}"
        elif body_pattern == "doy0":
            body = f"{gd.doy:03d}0"
        else:
            body = f"{gps_week}{dow}"

        return f"{provider}{body}.sp3.Z"

    def get_erp_filename(self, body_pattern: str = "wwww7") -> str | None:
        """Generate ERP filename based on configuration.

        Args:
            body_pattern: Body pattern ('wwww7', 'wwwwd', 'wwwwd_hn')

        Returns:
            ERP filename or None if not configured
        """
        if not self.config.erp.enabled or not self.config.gnss_date:
            return None

        gd = self.config.gnss_date
        provider = self.config.erp.provider_id.lower()
        gps_week = gd.gps_week
        dow = gd.dow

        if body_pattern == "wwww7":
            body = f"{gps_week}7"
        elif body_pattern == "wwwwd":
            body = f"{gps_week}{dow}"
        elif body_pattern == "wwwwd_hn":
            hour = getattr(gd, "hour", 0)
            if 3 <= hour < 9:
                hh = "00"
            elif 9 <= hour < 15:
                hh = "06"
            elif 15 <= hour < 21:
                hh = "12"
            elif 21 <= hour < 24:
                hh = "18"
            else:
                hh = "18"
                dow = dow - 1
                if dow < 0:
                    gps_week = gps_week - 1
                    dow = 6
            body = f"{gps_week}{dow}_{hh}"
        else:
            body = f"{gps_week}7"

        return f"{provider}{body}.erp.Z"

    def get_clock_filename(self, body_pattern: str = "wwwwd") -> str | None:
        """Generate clock filename based on configuration."""
        if not self.config.clock.enabled or not self.config.gnss_date:
            return None

        gd = self.config.gnss_date
        provider = self.config.clock.provider_id.lower()
        gps_week = gd.gps_week
        dow = gd.dow

        if body_pattern == "wwwwd":
            body = f"{gps_week}{dow}"
        else:
            body = f"{gps_week}{dow}"

        return f"{provider}{body}.clk.Z"

    def get_dcb_filename(self, use_actual: bool = False) -> str | None:
        """Generate DCB filename based on configuration.

        Implements the logic from Perl get_DCB_filename - choosing between
        monthly and actual P1C1 DCB files based on date.

        Args:
            use_actual: Force use of actual (current) DCB file

        Returns:
            DCB filename or None if not configured
        """
        if not self.config.dcb.enabled or not self.config.gnss_date:
            return None

        gd = self.config.gnss_date
        now = datetime.now(timezone.utc)

        # DCB update day (assumed 5th of each month per Perl logic)
        update_dom = 5

        if use_actual:
            return "P1C1.DCB"

        # Determine if we should use monthly or actual file
        # Based on processing date relative to update schedule
        proc_mjd = gd.mjd
        now_year = now.year
        now_month = now.month

        # Calculate MJD of this month's update and last month's update
        this_month_update = datetime(now_year, now_month, update_dom, tzinfo=timezone.utc)
        if now_month == 1:
            last_month_update = datetime(now_year - 1, 12, update_dom, tzinfo=timezone.utc)
        else:
            last_month_update = datetime(now_year, now_month - 1, update_dom, tzinfo=timezone.utc)

        this_update_mjd = mjd_from_date(this_month_update.year, this_month_update.month, this_month_update.day)
        last_update_mjd = mjd_from_date(last_month_update.year, last_month_update.month, last_month_update.day)
        now_mjd = mjd_from_date(now.year, now.month, now.day)

        # Logic from Perl: use actual if processing recent data
        if now_mjd >= this_update_mjd or (now_mjd >= last_update_mjd and now_mjd < this_update_mjd):
            if proc_mjd >= last_update_mjd:
                return "P1C1.DCB"

        # Use monthly file
        proc_year = gd.year
        proc_month = gd.date.month
        proc_dom = gd.date.day

        if proc_dom < update_dom:
            # Use previous month
            if proc_month == 1:
                file_year = proc_year - 1
                file_month = 12
            else:
                file_year = proc_year
                file_month = proc_month - 1
        else:
            file_year = proc_year
            file_month = proc_month

        return f"P1C1{file_year % 100:02d}{file_month:02d}.DCB.Z"

    def get_bia_filename(self) -> str | None:
        """Generate BIA (bias) filename based on configuration."""
        if not self.config.bia.enabled or not self.config.gnss_date:
            return None

        gd = self.config.gnss_date
        provider = self.config.bia.provider_id.upper()
        gps_week = gd.gps_week
        dow = gd.dow

        return f"{provider}{gps_week}{dow}.BIA.Z"

    def get_ion_filename(self) -> str | None:
        """Generate ION (ionosphere) filename based on configuration."""
        if not self.config.ion.enabled or not self.config.gnss_date:
            return None

        gd = self.config.gnss_date
        provider = self.config.ion.provider_id.lower()
        gps_week = gd.gps_week
        dow = gd.dow

        return f"{provider}{gps_week}{dow}i.Z"

    def check_product_in_db(
        self,
        category: ProductCategory,
        provider_id: str,
        product_type: str,
    ) -> bool:
        """Check if product is recorded as available in database.

        Args:
            category: Product category
            provider_id: Provider ID
            product_type: Product type

        Returns:
            True if product is recorded as available
        """
        pm = self._get_product_manager()
        if not pm or not self.config.gnss_date:
            return False

        gd = self.config.gnss_date

        try:
            # Check based on category
            if category == ProductCategory.DCB:
                # DCB uses year/month
                status = pm.get_product_status(
                    provider_id=provider_id,
                    product_type=product_type,
                    category=category.value,
                    year=gd.year,
                    month=gd.date.month,
                )
            else:
                # Other products use MJD
                status = pm.get_product_status(
                    provider_id=provider_id,
                    product_type=product_type,
                    category=category.value,
                    mjd=gd.mjd,
                )

            return status == 1  # 1 = available
        except Exception as e:
            logger.debug(f"DB check failed: {e}")
            return False

    def check_product(
        self,
        category: ProductCategory,
        filename: str,
        destination: Path,
        provider_id: str | None = None,
        product_type: str | None = None,
    ) -> bool:
        """Check if product exists locally or download it.

        Args:
            category: Product category
            filename: Product filename
            destination: Local destination directory
            provider_id: Provider ID for DB tracking
            product_type: Product type for DB tracking

        Returns:
            True if product is available
        """
        local_path = destination / filename

        # Also check in GPS week subdirectory
        if self.config.gnss_date:
            gps_week_path = destination / str(self.config.gnss_date.gps_week) / filename
        else:
            gps_week_path = None

        # Check if already exists locally
        if local_path.exists():
            ignss_print(MessageType.INFO, f"Product already available: {filename}")
            return True

        if gps_week_path and gps_week_path.exists():
            ignss_print(MessageType.INFO, f"Product already available: {filename}")
            return True

        # Check database for status
        if provider_id and product_type:
            if self.check_product_in_db(category, provider_id, product_type):
                ignss_print(MessageType.INFO, f"Product recorded in DB: {filename}")
                # File should exist but doesn't - need to download
                pass

        # Try to download
        ignss_print(MessageType.INFO, f"Attempting to download product: {filename}")
        downloaded = self.download_missing_product(
            category=category,
            filename=filename,
            destination=destination,
            provider_id=provider_id,
            product_type=product_type,
        )

        if downloaded:
            ignss_print(MessageType.INFO, f"Successfully downloaded: {filename}")
            return True
        else:
            ignss_print(MessageType.WARNING, f"Could not download product: {filename}")
            return False

    def download_missing_product(
        self,
        category: ProductCategory,
        filename: str,
        destination: Path,
        provider_id: str | None = None,
        product_type: str | None = None,
    ) -> bool:
        """Download a missing product from FTP.

        Replaces Perl download_missing_products and download_missing_dcb_products.

        Args:
            category: Product category
            filename: Product filename
            destination: Local destination directory
            provider_id: Provider ID
            product_type: Product type

        Returns:
            True if download successful
        """
        if not provider_id:
            return False

        # Get FTP configuration
        ftp_config = self._get_ftp_config(provider_id)
        if not ftp_config:
            logger.warning(f"No FTP config for provider: {provider_id}")
            return False

        # Ensure destination exists
        destination.mkdir(parents=True, exist_ok=True)

        # Determine remote path based on category and organization
        if self.config.gnss_date:
            gd = self.config.gnss_date
            if category == ProductCategory.DCB:
                # DCB files organized by year
                remote_subdir = str(gd.year)
            else:
                # Other products organized by GPS week
                remote_subdir = str(gd.gps_week)
        else:
            remote_subdir = ""

        try:
            # Use FTPClient to download
            ftp_client = FTPClient(
                host=ftp_config.url,
                username=ftp_config.username or "anonymous",
                password=ftp_config.password or "anonymous@",
            )

            remote_path = f"{ftp_config.root}/{remote_subdir}/{filename}" if remote_subdir else f"{ftp_config.root}/{filename}"
            local_path = destination / filename

            success = ftp_client.download_file(remote_path, local_path)

            if success:
                # Update database
                self._update_product_db(category, provider_id, product_type, filename)
                return True

            return False

        except Exception as e:
            logger.error(f"Download failed for {filename}: {e}")
            return False

    def _update_product_db(
        self,
        category: ProductCategory,
        provider_id: str,
        product_type: str | None,
        filename: str,
    ) -> None:
        """Update database after successful download.

        Args:
            category: Product category
            provider_id: Provider ID
            product_type: Product type
            filename: Downloaded filename
        """
        pm = self._get_product_manager()
        if not pm or not self.config.gnss_date:
            return

        gd = self.config.gnss_date

        try:
            if category == ProductCategory.DCB:
                pm.update_product_status(
                    provider_id=provider_id,
                    product_type=product_type or "dcb",
                    category=category.value,
                    year=gd.year,
                    month=gd.date.month,
                    status=1,
                )
            else:
                pm.update_product_status(
                    provider_id=provider_id,
                    product_type=product_type or category.value,
                    category=category.value,
                    mjd=gd.mjd,
                    gps_week=gd.gps_week,
                    dow=gd.dow,
                    status=1,
                )
        except Exception as e:
            logger.warning(f"Could not update product DB: {e}")

    def check_orbit(self) -> bool:
        """Check orbit product availability and download if missing.

        Replaces Perl IGNSS::check_orbit.

        Returns:
            True if orbit is available
        """
        if not self.config.orbit.enabled:
            return True

        filename = self.get_orbit_filename()
        if not filename:
            return False

        return self.check_product(
            category=ProductCategory.ORBIT,
            filename=filename,
            destination=self.config.data_dir or Path("."),
            provider_id=self.config.orbit.provider_id,
            product_type=self.config.orbit.product_type,
        )

    def check_erp(self) -> bool:
        """Check ERP product availability and download if missing.

        Replaces Perl IGNSS::check_ERP.

        Returns:
            True if ERP is available
        """
        if not self.config.erp.enabled:
            return True

        filename = self.get_erp_filename()
        if not filename:
            return False

        return self.check_product(
            category=ProductCategory.ERP,
            filename=filename,
            destination=self.config.data_dir or Path("."),
            provider_id=self.config.erp.provider_id,
            product_type=self.config.erp.product_type,
        )

    def check_clock(self) -> bool:
        """Check clock product availability and download if missing.

        Replaces Perl IGNSS::check_clock.

        Returns:
            True if clock is available
        """
        if not self.config.clock.enabled:
            return True

        filename = self.get_clock_filename()
        if not filename:
            return False

        return self.check_product(
            category=ProductCategory.CLOCK,
            filename=filename,
            destination=self.config.data_dir or Path("."),
            provider_id=self.config.clock.provider_id,
            product_type=self.config.clock.product_type,
        )

    def check_dcb(self) -> bool:
        """Check DCB product availability and download if missing.

        Replaces Perl IGNSS::check_DCB.

        Returns:
            True if DCB is available
        """
        if not self.config.dcb.enabled:
            return True

        filename = self.get_dcb_filename()
        if not filename:
            return False

        # DCB files may be in a different directory
        dcb_dir = self.config.data_dir
        if dcb_dir:
            dcb_dir = dcb_dir / "dcb"
            dcb_dir.mkdir(parents=True, exist_ok=True)
        else:
            dcb_dir = Path(".")

        return self.check_product(
            category=ProductCategory.DCB,
            filename=filename,
            destination=dcb_dir,
            provider_id=self.config.dcb.provider_id,
            product_type=self.config.dcb.product_type,
        )

    def check_all_products(self) -> dict[str, bool]:
        """Check availability of all configured products.

        Returns:
            Dictionary of product category -> availability
        """
        results = {}

        if self.config.orbit.enabled:
            results["orbit"] = self.check_orbit()

        if self.config.erp.enabled:
            results["erp"] = self.check_erp()

        if self.config.clock.enabled:
            results["clock"] = self.check_clock()

        if self.config.dcb.enabled:
            results["dcb"] = self.check_dcb()

        if self.config.bia.enabled:
            filename = self.get_bia_filename()
            if filename:
                results["bia"] = self.check_product(
                    ProductCategory.BIA,
                    filename,
                    self.config.data_dir or Path("."),
                    self.config.bia.provider_id,
                    self.config.bia.product_type,
                )

        if self.config.ion.enabled:
            filename = self.get_ion_filename()
            if filename:
                results["ion"] = self.check_product(
                    ProductCategory.ION,
                    filename,
                    self.config.data_dir or Path("."),
                    self.config.ion.provider_id,
                    self.config.ion.product_type,
                )

        return results


# =============================================================================
# PPP-AR Product Download Configuration (mirrors Perl procOrbit, procERP, etc.)
# =============================================================================

@dataclass
class ProcProductConfig:
    """Configuration for a product type (mirrors Perl procOrbit, procERP, etc.).

    This mirrors the Perl hash structure:
        $args{procOrbit} = {
            yesORno => 'yes',
            id      => 'IGS',
            product => 'final',
            ftp     => {a=>'CDDIS', b=>'BKGE_IGS'},
        };
    """

    enabled: bool = True  # yesORno
    id: str = "IGS"  # Product ID (IGS, CODE, etc.)
    product: str = "final"  # Product tier (final, rapid, ultra)
    ftp_servers: list[str] = field(default_factory=lambda: ["CDDIS", "CODE"])

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ProcProductConfig":
        """Create from Perl-style dictionary."""
        ftp = data.get("ftp", {})
        ftp_list = [ftp.get(k) for k in sorted(ftp.keys())] if isinstance(ftp, dict) else []
        return cls(
            enabled=data.get("yesORno", "yes") == "yes",
            id=data.get("id", "IGS"),
            product=data.get("product", "final"),
            ftp_servers=ftp_list or ["CDDIS", "CODE"],
        )


@dataclass
class PPPProductArgs:
    """PPP product download arguments (mirrors Perl %args structure).

    This mirrors the full Perl product configuration from iGNSS_D_PPP_AR_*.pl:
        - procOrbit, procERP, procClock, procION, procP1C1DCB
        - ftpConfigXml
        - dataDir
        - dbOEDC

    Example:
        args = PPPProductArgs(
            proc_orbit=ProcProductConfig(id="IGS", product="final"),
            proc_erp=ProcProductConfig(id="IGS", product="final"),
            proc_clock=ProcProductConfig(id="IGS", product="final"),
            proc_ion=ProcProductConfig(enabled=False),
            ftp_config_xml=Path("/home/user/conf/ftpConfig.xml"),
        )
    """

    proc_orbit: ProcProductConfig = field(default_factory=ProcProductConfig)
    proc_erp: ProcProductConfig = field(default_factory=ProcProductConfig)
    proc_clock: ProcProductConfig = field(default_factory=ProcProductConfig)
    proc_ion: ProcProductConfig = field(default_factory=lambda: ProcProductConfig(enabled=False))
    proc_bia: ProcProductConfig = field(default_factory=lambda: ProcProductConfig(enabled=False))
    proc_dcb: ProcProductConfig = field(default_factory=lambda: ProcProductConfig(enabled=False))
    proc_vmf3: ProcProductConfig = field(default_factory=lambda: ProcProductConfig(enabled=True))  # VMF3 enabled by default

    ftp_config_xml: Path | None = None  # ftpConfig.xml path
    data_dir: Path | None = None  # Root data directory

    @classmethod
    def from_perl_args(cls, args: dict[str, Any]) -> "PPPProductArgs":
        """Create from Perl-style arguments dictionary.

        Args:
            args: Dictionary matching Perl %args structure

        Returns:
            PPPProductArgs instance
        """
        return cls(
            proc_orbit=ProcProductConfig.from_dict(args.get("procOrbit", {})),
            proc_erp=ProcProductConfig.from_dict(args.get("procERP", {})),
            proc_clock=ProcProductConfig.from_dict(args.get("procClock", {})),
            proc_ion=ProcProductConfig.from_dict(args.get("procION", {})),
            proc_bia=ProcProductConfig.from_dict(args.get("procBIA", {})),
            proc_dcb=ProcProductConfig.from_dict(args.get("procP1C1DCB", {})),
            ftp_config_xml=Path(args["ftpConfigXml"]) if "ftpConfigXml" in args else None,
            data_dir=Path(args["dataDir"]["root"]) if "dataDir" in args else None,
        )


# =============================================================================
# PPP-AR Product Downloader
# =============================================================================

class PPPProductDownloader:
    """Download products required for PPP-AR processing.

    Handles downloading products from CODE FTP with i-GNSS naming conventions:
    - Orbits: {ORB}_{YYYYDDD}.PRE (e.g., COD_2024260.PRE)
    - Clocks: {ORB}_{YYYYDDD}.CLK (e.g., COD_2024260.CLK)
    - ERP: {ORB}_{YYYYDDD}.IEP or .ERP
    - BIA/OSB: {ORB}_{YYYYDDD}.BIA (bias files for PPP-AR)
    - ION: {ORB}_{YYYYDDD}.ION (ionosphere files)
    - VMF3: VMF3_{YYYY}MMDD.H{00,06,12,18} (troposphere grids)
    - CRD: COD{YYDDD}.CRD (a priori coordinates)

    This replaces the Perl FTP.pm product download logic used by ORB_IGS script.

    Integration with Perl structure:
        The class can be initialized with PPPProductArgs which mirrors the Perl
        %args structure used in iGNSS_D_PPP_AR_*.pl callers:

        # Perl structure
        $args{procOrbit} = {yesORno=>'yes', id=>'IGS', product=>'final', ftp=>{a=>'CDDIS'}};

        # Python equivalent
        args = PPPProductArgs(
            proc_orbit=ProcProductConfig(id="IGS", product="final", ftp_servers=["CDDIS"]),
        )
        downloader = PPPProductDownloader(config, product_args=args)
    """

    # Default CODE FTP server
    CODE_FTP_HOST = "ftp.aiub.unibe.ch"
    CODE_FTP_USER = "anonymous"
    CODE_FTP_PASS = "anonymous@"

    # Product paths on CODE server
    CODE_ORB_PATH = "/CODE/{year}"  # For daily products
    CODE_ATM_PATH = "/CODE/{year}"  # For atmosphere products

    # Local paths relative to orbDir (from V_ORBDIR)
    LOCAL_ORB_SUBDIR = "ORB"
    LOCAL_ATM_SUBDIR = "ATM"

    def __init__(
        self,
        config: ProcessingConfig,
        orb_dir: Path | None = None,
        ftp_configs: list[FTPServerConfig] | None = None,
        product_args: PPPProductArgs | None = None,
        ftp_config_xml: Path | None = None,
    ):
        """Initialize PPP product downloader.

        Args:
            config: Processing configuration
            orb_dir: Orbit/product directory (V_ORBDIR equivalent)
            ftp_configs: Optional pre-loaded FTP configurations
            product_args: Perl-style product arguments (mirrors %args)
            ftp_config_xml: Path to ftpConfig.xml (overrides product_args setting)
        """
        self.config = config
        self.orb_dir = orb_dir or config.data_dir or Path(".")
        self._ftp_configs = ftp_configs
        self._ftp_client: FTPClient | None = None

        # Perl-style product configuration
        self.product_args = product_args or PPPProductArgs()

        # Load FTP configuration from XML if provided
        self._ftp_config_xml = ftp_config_xml or (product_args.ftp_config_xml if product_args else None)
        self._ftp_config_manager = None
        if self._ftp_config_xml and self._ftp_config_xml.exists():
            from pygnss_rt.data_access.ftp_config import FTPConfigManager
            self._ftp_config_manager = FTPConfigManager(self._ftp_config_xml)

    def _get_ftp_client(self) -> FTPClient:
        """Get or create FTP client for CODE server."""
        if self._ftp_client is None:
            self._ftp_client = FTPClient(
                host=self.CODE_FTP_HOST,
                username=self.CODE_FTP_USER,
                password=self.CODE_FTP_PASS,
                timeout=120,
                passive=True,
            )
        return self._ftp_client

    def _disconnect(self) -> None:
        """Disconnect FTP client."""
        if self._ftp_client:
            try:
                self._ftp_client.disconnect()
            except Exception:
                pass
            self._ftp_client = None

    def _ensure_dir(self, path: Path) -> None:
        """Ensure directory exists."""
        path.mkdir(parents=True, exist_ok=True)

    def _format_yyyyddd(self, date: GNSSDate) -> str:
        """Format date as YYYYDDD (e.g., 2024260)."""
        return f"{date.year}{date.doy:03d}"

    def _format_yyddd(self, date: GNSSDate) -> str:
        """Format date as YYDDD (e.g., 24260)."""
        return f"{date.year % 100:02d}{date.doy:03d}"

    def _format_wwwwd(self, date: GNSSDate) -> str:
        """Format date as WWWWD (GPS week + day of week)."""
        return f"{date.gps_week}{date.day_of_week}"

    def _format_igs_longform(self, date: GNSSDate) -> str:
        """Format date for IGS long-form naming: yyyyddd0000."""
        return f"{date.year}{date.doy:03d}0000"

    def get_orbit_filename(self, orb_id: str = "COD", use_longform: bool = True) -> str:
        """Generate orbit filename for PPP processing.

        Args:
            orb_id: Orbit provider ID (COD, IGS, etc.)
            use_longform: Use IGS long-form naming (default True)

        Returns:
            Filename like COD0OPSFIN_20240260000_01D_05M_ORB.SP3 (longform)
            or COD_2024260.PRE (legacy)
        """
        if not self.config.gnss_date:
            return ""
        if use_longform:
            date_part = self._format_igs_longform(self.config.gnss_date)
            return f"{orb_id}0OPSFIN_{date_part}_01D_05M_ORB.SP3"
        else:
            yyyyddd = self._format_yyyyddd(self.config.gnss_date)
            return f"{orb_id}_{yyyyddd}.PRE"

    def get_clock_filename(self, orb_id: str = "COD", use_longform: bool = True, high_rate: bool = False) -> str:
        """Generate clock filename for PPP processing.

        Args:
            orb_id: Orbit provider ID
            use_longform: Use IGS long-form naming (default True)
            high_rate: Use 5-second clocks instead of 30-second

        Returns:
            Filename like COD0OPSFIN_20240260000_01D_30S_CLK.CLK (longform)
            or COD_2024260.CLK (legacy)
        """
        if not self.config.gnss_date:
            return ""
        if use_longform:
            date_part = self._format_igs_longform(self.config.gnss_date)
            rate = "05S" if high_rate else "30S"
            return f"{orb_id}0OPSFIN_{date_part}_01D_{rate}_CLK.CLK"
        else:
            yyyyddd = self._format_yyyyddd(self.config.gnss_date)
            return f"{orb_id}_{yyyyddd}.CLK"

    def get_erp_filename(self, orb_id: str = "COD", use_longform: bool = True) -> str:
        """Generate ERP/IEP filename for PPP processing.

        Args:
            orb_id: Orbit provider ID
            use_longform: Use IGS long-form naming (default True)

        Returns:
            Filename like COD0OPSFIN_20240260000_01D_01D_ERP.ERP (longform)
            or COD_2024260.IEP (legacy)
        """
        if not self.config.gnss_date:
            return ""
        if use_longform:
            date_part = self._format_igs_longform(self.config.gnss_date)
            return f"{orb_id}0OPSFIN_{date_part}_01D_01D_ERP.ERP"
        else:
            yyyyddd = self._format_yyyyddd(self.config.gnss_date)
            return f"{orb_id}_{yyyyddd}.IEP"

    def get_bia_filename(self, orb_id: str = "COD", use_longform: bool = True) -> str:
        """Generate BIA/OSB filename for PPP-AR processing.

        Args:
            orb_id: Orbit provider ID
            use_longform: Use IGS long-form naming (default True)

        Returns:
            Filename like COD0OPSFIN_20240260000_01D_01D_OSB.BIA (longform)
            or COD_2024260.BIA (legacy)
        """
        if not self.config.gnss_date:
            return ""
        if use_longform:
            date_part = self._format_igs_longform(self.config.gnss_date)
            return f"{orb_id}0OPSFIN_{date_part}_01D_01D_OSB.BIA"
        else:
            yyyyddd = self._format_yyyyddd(self.config.gnss_date)
            return f"{orb_id}_{yyyyddd}.BIA"

    def get_ion_filename(self, orb_id: str = "COD", use_longform: bool = True) -> str:
        """Generate ION filename for PPP processing.

        Args:
            orb_id: Orbit provider ID
            use_longform: Use IGS long-form naming (default True)

        Returns:
            Filename like COD0OPSFIN_20240260000_01D_01H_GIM.ION (longform)
            or COD_2024260.ION (legacy)
        """
        if not self.config.gnss_date:
            return ""
        if use_longform:
            date_part = self._format_igs_longform(self.config.gnss_date)
            return f"{orb_id}0OPSFIN_{date_part}_01D_01H_GIM.ION"
        else:
            yyyyddd = self._format_yyyyddd(self.config.gnss_date)
            return f"{orb_id}_{yyyyddd}.ION"

    def get_crd_filename(self) -> str:
        """Generate a priori coordinate filename.

        Returns:
            Filename like COD24260.CRD
        """
        if not self.config.gnss_date:
            return ""
        yyddd = self._format_yyddd(self.config.gnss_date)
        return f"COD{yyddd}.CRD"

    def get_vmf3_filenames(self) -> list[str]:
        """Generate VMF3 grid filenames for a processing day.

        VMF3 requires 5 files: H00, H06, H12, H18 of current day
        and H00 of next day. Uses VMF3_ naming convention from TU Wien.

        TU Wien VMF3 URL structure:
        https://vmf.geo.tuwien.ac.at/trop_products/GRID/5x5/VMF3/VMF3_OP/{year}/VMF3_YYYYMMDD.H00

        Returns:
            List of VMF3 filenames (without path)
        """
        if not self.config.gnss_date:
            return []

        gd = self.config.gnss_date
        dt = gd.datetime
        year = dt.year
        month = dt.month
        day = dt.day

        files = []

        # Current day files (VMF3_ naming for TU Wien 5x5 grid)
        for hour in ["00", "06", "12", "18"]:
            files.append(f"VMF3_{year}{month:02d}{day:02d}.H{hour}")

        # Next day 00 file (needed for interpolation at end of day)
        from datetime import timedelta
        next_day = dt + timedelta(days=1)
        files.append(f"VMF3_{next_day.year}{next_day.month:02d}{next_day.day:02d}.H00")

        return files

    def download_orbit(
        self,
        orb_id: str = "COD",
        destination: Path | None = None,
        save_bsw_format: bool = True,
    ) -> ProductDownloadResult:
        """Download orbit file from CODE server.

        Downloads from CODE FTP using IGS long-form naming, saves locally
        with BSW-compatible short naming.

        Remote: COD0OPSFIN_20240260000_01D_05M_ORB.SP3.gz
        Local: COD_2024260.PRE (BSW format)

        Args:
            orb_id: Orbit provider ID
            destination: Destination directory
            save_bsw_format: Save with BSW legacy naming (default True)

        Returns:
            Download result
        """
        if not self.config.gnss_date:
            return ProductDownloadResult(
                status=DownloadStatus.UNKNOWN_ERROR,
                error_message="No processing date configured",
            )

        # Remote filename (IGS long-form)
        remote_filename = self.get_orbit_filename(orb_id, use_longform=True)
        # Local filename (BSW legacy format)
        local_filename = self.get_orbit_filename(orb_id, use_longform=False) if save_bsw_format else remote_filename

        dest_dir = destination or (self.orb_dir / self.LOCAL_ORB_SUBDIR)
        self._ensure_dir(dest_dir)
        local_path = dest_dir / local_filename

        # Check if already exists
        if local_path.exists():
            ignss_print(MessageType.INFO, f"Orbit file already available: {local_filename}")
            return ProductDownloadResult(
                status=DownloadStatus.SUCCESS,
                local_path=local_path,
                source="local",
            )

        # Build remote path - IGS long-form uses .gz compression
        year = self.config.gnss_date.year
        remote_path = f"/CODE/{year}/{remote_filename}.gz"

        ignss_print(MessageType.INFO, f"Downloading orbit: {remote_filename} -> {local_filename}")

        try:
            ftp = self._get_ftp_client()
            ftp.connect()

            # Download compressed file
            compressed_local = dest_dir / f"{remote_filename}.gz"
            success = ftp.download(remote_path, compressed_local)

            if success and compressed_local.exists():
                # Decompress to BSW-compatible filename
                import gzip
                import shutil
                with gzip.open(compressed_local, 'rb') as f_in:
                    with open(local_path, 'wb') as f_out:
                        shutil.copyfileobj(f_in, f_out)
                compressed_local.unlink()

                if local_path.exists():
                    ignss_print(MessageType.INFO, f"Downloaded orbit: {local_filename}")
                    return ProductDownloadResult(
                        status=DownloadStatus.SUCCESS,
                        local_path=local_path,
                        remote_path=remote_path,
                        source="CODE",
                    )

            return ProductDownloadResult(
                status=DownloadStatus.NOT_FOUND,
                error_message=f"Failed to download orbit: {remote_filename}",
            )

        except Exception as e:
            logger.error(f"Orbit download failed: {e}")
            return ProductDownloadResult(
                status=DownloadStatus.CONNECTION_ERROR,
                error_message=str(e),
            )

    def download_clock(
        self,
        orb_id: str = "COD",
        destination: Path | None = None,
        high_rate: bool = False,
        save_bsw_format: bool = True,
    ) -> ProductDownloadResult:
        """Download clock file from CODE server.

        Downloads from CODE FTP using IGS long-form naming, saves locally
        with BSW-compatible short naming.

        Remote: COD0OPSFIN_20240260000_01D_30S_CLK.CLK.gz
        Local: COD_2024260.CLK (BSW format)

        Args:
            orb_id: Orbit provider ID
            destination: Destination directory
            high_rate: Use 5-second clocks instead of 30-second
            save_bsw_format: Save with BSW legacy naming (default True)
        """
        if not self.config.gnss_date:
            return ProductDownloadResult(
                status=DownloadStatus.UNKNOWN_ERROR,
                error_message="No processing date configured",
            )

        # Remote filename (IGS long-form)
        remote_filename = self.get_clock_filename(orb_id, use_longform=True, high_rate=high_rate)
        # Local filename (BSW legacy format)
        local_filename = self.get_clock_filename(orb_id, use_longform=False) if save_bsw_format else remote_filename

        dest_dir = destination or (self.orb_dir / self.LOCAL_ORB_SUBDIR)
        self._ensure_dir(dest_dir)
        local_path = dest_dir / local_filename

        if local_path.exists():
            ignss_print(MessageType.INFO, f"Clock file already available: {local_filename}")
            return ProductDownloadResult(
                status=DownloadStatus.SUCCESS,
                local_path=local_path,
                source="local",
            )

        year = self.config.gnss_date.year
        remote_path = f"/CODE/{year}/{remote_filename}.gz"

        ignss_print(MessageType.INFO, f"Downloading clock: {remote_filename} -> {local_filename}")

        try:
            ftp = self._get_ftp_client()
            ftp.connect()

            compressed_local = dest_dir / f"{remote_filename}.gz"
            success = ftp.download(remote_path, compressed_local)

            if success and compressed_local.exists():
                import gzip
                import shutil
                with gzip.open(compressed_local, 'rb') as f_in:
                    with open(local_path, 'wb') as f_out:
                        shutil.copyfileobj(f_in, f_out)
                compressed_local.unlink()

                if local_path.exists():
                    ignss_print(MessageType.INFO, f"Downloaded clock: {local_filename}")
                    return ProductDownloadResult(
                        status=DownloadStatus.SUCCESS,
                        local_path=local_path,
                        remote_path=remote_path,
                        source="CODE",
                    )

            return ProductDownloadResult(
                status=DownloadStatus.NOT_FOUND,
                error_message=f"Failed to download clock: {remote_filename}",
            )

        except Exception as e:
            logger.error(f"Clock download failed: {e}")
            return ProductDownloadResult(
                status=DownloadStatus.CONNECTION_ERROR,
                error_message=str(e),
            )

    def download_erp(
        self,
        orb_id: str = "COD",
        destination: Path | None = None,
        save_bsw_format: bool = True,
    ) -> ProductDownloadResult:
        """Download ERP file from CODE server.

        Downloads from CODE FTP using IGS long-form naming, saves locally
        with BSW-compatible short naming.

        Remote: COD0OPSFIN_20240260000_01D_01D_ERP.ERP.gz
        Local: COD_2024260.IEP (BSW format)

        Args:
            orb_id: Orbit provider ID
            destination: Destination directory
            save_bsw_format: Save with BSW legacy naming (default True)
        """
        if not self.config.gnss_date:
            return ProductDownloadResult(
                status=DownloadStatus.UNKNOWN_ERROR,
                error_message="No processing date configured",
            )

        # Remote filename (IGS long-form)
        remote_filename = self.get_erp_filename(orb_id, use_longform=True)
        # Local filename (BSW legacy format)
        local_filename = self.get_erp_filename(orb_id, use_longform=False) if save_bsw_format else remote_filename

        dest_dir = destination or (self.orb_dir / self.LOCAL_ORB_SUBDIR)
        self._ensure_dir(dest_dir)
        local_path = dest_dir / local_filename

        if local_path.exists():
            ignss_print(MessageType.INFO, f"ERP file already available: {local_filename}")
            return ProductDownloadResult(
                status=DownloadStatus.SUCCESS,
                local_path=local_path,
                source="local",
            )

        year = self.config.gnss_date.year
        remote_path = f"/CODE/{year}/{remote_filename}.gz"

        ignss_print(MessageType.INFO, f"Downloading ERP: {remote_filename} -> {local_filename}")

        try:
            ftp = self._get_ftp_client()
            ftp.connect()

            compressed_local = dest_dir / f"{remote_filename}.gz"
            success = ftp.download(remote_path, compressed_local)

            if success and compressed_local.exists():
                import gzip
                import shutil
                with gzip.open(compressed_local, 'rb') as f_in:
                    with open(local_path, 'wb') as f_out:
                        shutil.copyfileobj(f_in, f_out)
                compressed_local.unlink()

                if local_path.exists():
                    ignss_print(MessageType.INFO, f"Downloaded ERP: {local_filename}")
                    return ProductDownloadResult(
                        status=DownloadStatus.SUCCESS,
                        local_path=local_path,
                        remote_path=remote_path,
                        source="CODE",
                    )

            return ProductDownloadResult(
                status=DownloadStatus.NOT_FOUND,
                error_message=f"Failed to download ERP: {remote_filename}",
            )

        except Exception as e:
            logger.error(f"ERP download failed: {e}")
            return ProductDownloadResult(
                status=DownloadStatus.CONNECTION_ERROR,
                error_message=str(e),
            )

    def download_bia(
        self,
        orb_id: str = "COD",
        destination: Path | None = None,
        save_bsw_format: bool = True,
    ) -> ProductDownloadResult:
        """Download BIA/OSB file from CODE server for PPP-AR.

        Downloads from CODE FTP using IGS long-form naming, saves locally
        with BSW-compatible short naming.

        Remote: COD0OPSFIN_20240260000_01D_01D_OSB.BIA.gz
        Local: COD_2024260.BIA (BSW format)

        BIA files are required for PPP-AR processing to resolve
        ambiguities. These contain satellite phase biases.

        Args:
            orb_id: Orbit provider ID
            destination: Destination directory
            save_bsw_format: Save with BSW legacy naming (default True)
        """
        if not self.config.gnss_date:
            return ProductDownloadResult(
                status=DownloadStatus.UNKNOWN_ERROR,
                error_message="No processing date configured",
            )

        # Remote filename (IGS long-form)
        remote_filename = self.get_bia_filename(orb_id, use_longform=True)
        # Local filename (BSW legacy format)
        local_filename = self.get_bia_filename(orb_id, use_longform=False) if save_bsw_format else remote_filename

        dest_dir = destination or (self.orb_dir / self.LOCAL_ORB_SUBDIR)
        self._ensure_dir(dest_dir)
        local_path = dest_dir / local_filename

        if local_path.exists():
            ignss_print(MessageType.INFO, f"BIA file already available: {local_filename}")
            return ProductDownloadResult(
                status=DownloadStatus.SUCCESS,
                local_path=local_path,
                source="local",
            )

        year = self.config.gnss_date.year
        remote_path = f"/CODE/{year}/{remote_filename}.gz"

        ignss_print(MessageType.INFO, f"Downloading BIA: {remote_filename} -> {local_filename}")

        try:
            ftp = self._get_ftp_client()
            ftp.connect()

            compressed_local = dest_dir / f"{remote_filename}.gz"
            success = ftp.download(remote_path, compressed_local)

            if success and compressed_local.exists():
                import gzip
                import shutil
                with gzip.open(compressed_local, 'rb') as f_in:
                    with open(local_path, 'wb') as f_out:
                        shutil.copyfileobj(f_in, f_out)
                compressed_local.unlink()

                if local_path.exists():
                    ignss_print(MessageType.INFO, f"Downloaded BIA: {local_filename}")
                    return ProductDownloadResult(
                        status=DownloadStatus.SUCCESS,
                        local_path=local_path,
                        remote_path=remote_path,
                        source="CODE",
                    )

            return ProductDownloadResult(
                status=DownloadStatus.NOT_FOUND,
                error_message=f"Failed to download BIA: {remote_filename}",
            )

        except Exception as e:
            logger.error(f"BIA download failed: {e}")
            return ProductDownloadResult(
                status=DownloadStatus.CONNECTION_ERROR,
                error_message=str(e),
            )

    def download_ion(
        self,
        orb_id: str = "COD",
        destination: Path | None = None,
        save_bsw_format: bool = True,
    ) -> ProductDownloadResult:
        """Download ionosphere file from CODE server.

        Downloads from CODE FTP using IGS long-form naming, saves locally
        with BSW-compatible short naming.

        Remote: COD0OPSFIN_20240260000_01D_01H_GIM.ION.gz
        Local: COD_2024260.ION (BSW format)

        Args:
            orb_id: Orbit provider ID
            destination: Destination directory
            save_bsw_format: Save with BSW legacy naming (default True)
        """
        if not self.config.gnss_date:
            return ProductDownloadResult(
                status=DownloadStatus.UNKNOWN_ERROR,
                error_message="No processing date configured",
            )

        # Remote filename (IGS long-form)
        remote_filename = self.get_ion_filename(orb_id, use_longform=True)
        # Local filename (BSW legacy format)
        local_filename = self.get_ion_filename(orb_id, use_longform=False) if save_bsw_format else remote_filename

        dest_dir = destination or (self.orb_dir / self.LOCAL_ATM_SUBDIR)
        self._ensure_dir(dest_dir)
        local_path = dest_dir / local_filename

        if local_path.exists():
            ignss_print(MessageType.INFO, f"ION file already available: {local_filename}")
            return ProductDownloadResult(
                status=DownloadStatus.SUCCESS,
                local_path=local_path,
                source="local",
            )

        year = self.config.gnss_date.year
        remote_path = f"/CODE/{year}/{remote_filename}.gz"

        ignss_print(MessageType.INFO, f"Downloading ION: {remote_filename} -> {local_filename}")

        try:
            ftp = self._get_ftp_client()
            ftp.connect()

            compressed_local = dest_dir / f"{remote_filename}.gz"
            success = ftp.download(remote_path, compressed_local)

            if success and compressed_local.exists():
                import gzip
                import shutil
                with gzip.open(compressed_local, 'rb') as f_in:
                    with open(local_path, 'wb') as f_out:
                        shutil.copyfileobj(f_in, f_out)
                compressed_local.unlink()

                if local_path.exists():
                    ignss_print(MessageType.INFO, f"Downloaded ION: {local_filename}")
                    return ProductDownloadResult(
                        status=DownloadStatus.SUCCESS,
                        local_path=local_path,
                        remote_path=remote_path,
                        source="CODE",
                    )

            return ProductDownloadResult(
                status=DownloadStatus.NOT_FOUND,
                error_message=f"Failed to download ION: {remote_filename}",
            )

        except Exception as e:
            logger.error(f"ION download failed: {e}")
            return ProductDownloadResult(
                status=DownloadStatus.CONNECTION_ERROR,
                error_message=str(e),
            )

    def download_crd(
        self,
        destination: Path | None = None,
        source_dir: Path | None = None,
    ) -> ProductDownloadResult:
        """Download a priori coordinate file.

        CRD files can come from CODE FTP or a local source directory.
        """
        if not self.config.gnss_date:
            return ProductDownloadResult(
                status=DownloadStatus.UNKNOWN_ERROR,
                error_message="No processing date configured",
            )

        filename = self.get_crd_filename()
        dest_dir = destination or self.orb_dir
        self._ensure_dir(dest_dir)
        local_path = dest_dir / filename

        if local_path.exists():
            ignss_print(MessageType.INFO, f"CRD file already available: {filename}")
            return ProductDownloadResult(
                status=DownloadStatus.SUCCESS,
                local_path=local_path,
                source="local",
            )

        # Try local source directory first (from PathConfig or explicit argument)
        if source_dir and source_dir.exists():
            source_file = source_dir / (filename + ".gz")
            if source_file.exists():
                import shutil
                shutil.copy(source_file, dest_dir / (filename + ".gz"))
                import subprocess
                subprocess.run(
                    ["gunzip", "-f", str(dest_dir / (filename + ".gz"))],
                    capture_output=True,
                )
                if local_path.exists():
                    return ProductDownloadResult(
                        status=DownloadStatus.SUCCESS,
                        local_path=local_path,
                        source="local_copy",
                    )

        # Try CODE FTP
        year = self.config.gnss_date.year
        remote_path = f"/BSWUSER54/STA/{year}/{filename}.gz"

        ignss_print(MessageType.INFO, f"Downloading CRD: {filename}")

        try:
            ftp = self._get_ftp_client()
            ftp.connect()

            compressed_local = dest_dir / (filename + ".gz")
            success = ftp.download(remote_path, compressed_local)

            if success and compressed_local.exists():
                import subprocess
                subprocess.run(["gunzip", "-f", str(compressed_local)], capture_output=True)
                if local_path.exists():
                    return ProductDownloadResult(
                        status=DownloadStatus.SUCCESS,
                        local_path=local_path,
                        remote_path=remote_path,
                        source="CODE",
                    )

            return ProductDownloadResult(
                status=DownloadStatus.NOT_FOUND,
                error_message=f"Failed to download CRD: {filename}",
            )

        except Exception as e:
            logger.error(f"CRD download failed: {e}")
            return ProductDownloadResult(
                status=DownloadStatus.CONNECTION_ERROR,
                error_message=str(e),
            )

    def download_vmf3(
        self,
        destination: Path | None = None,
        source_dir: Path | None = None,
    ) -> list[ProductDownloadResult]:
        """Download VMF3 troposphere grid files from TU Wien.

        Downloads VMF3 files from:
        https://vmf.geo.tuwien.ac.at/trop_products/GRID/5x5/VMF3/VMF3_OP/{year}/

        If source_dir is provided, will try local copy first before downloading.

        Args:
            destination: Destination directory for VMF3 files
            source_dir: Optional local source directory (checked first)

        Returns:
            List of ProductDownloadResult for each file
        """
        import requests

        if not self.config.gnss_date:
            return [ProductDownloadResult(
                status=DownloadStatus.UNKNOWN_ERROR,
                error_message="No processing date configured",
            )]

        filenames = self.get_vmf3_filenames()
        dest_dir = destination or self.orb_dir
        self._ensure_dir(dest_dir)

        # VMF3 base URL from TU Wien (5x5 degree grid)
        vmf3_base_url = "https://vmf.geo.tuwien.ac.at/trop_products/GRID/5x5/VMF3/VMF3_OP"

        results = []

        for filename in filenames:
            local_path = dest_dir / filename

            # Check if already exists locally
            if local_path.exists():
                results.append(ProductDownloadResult(
                    status=DownloadStatus.SUCCESS,
                    local_path=local_path,
                    source="local",
                ))
                continue

            # Extract year from filename (VMF3_YYYYMMDD.Hhh)
            file_year = int(filename[5:9])

            # Try local source first if provided
            if source_dir:
                gd = self.config.gnss_date
                doy = gd.doy

                source_path = source_dir / str(file_year) / f"{doy:03d}" / (filename + ".gz")
                if not source_path.exists():
                    source_path = source_dir / str(file_year) / filename

                if source_path.exists():
                    import shutil
                    if source_path.suffix == ".gz":
                        shutil.copy(source_path, dest_dir / (filename + ".gz"))
                        import subprocess
                        subprocess.run(
                            ["gunzip", "-f", str(dest_dir / (filename + ".gz"))],
                            capture_output=True,
                        )
                    else:
                        shutil.copy(source_path, local_path)

                    if local_path.exists():
                        results.append(ProductDownloadResult(
                            status=DownloadStatus.SUCCESS,
                            local_path=local_path,
                            source="local_copy",
                        ))
                        continue

            # Download from TU Wien HTTPS
            url = f"{vmf3_base_url}/{file_year}/{filename}"
            ignss_print(MessageType.INFO, f"Downloading VMF3: {filename}")

            try:
                response = requests.get(url, timeout=60)
                if response.status_code == 200:
                    with open(local_path, 'wb') as f:
                        f.write(response.content)

                    if local_path.exists():
                        ignss_print(MessageType.INFO, f"Downloaded VMF3: {filename}")
                        results.append(ProductDownloadResult(
                            status=DownloadStatus.SUCCESS,
                            local_path=local_path,
                            remote_path=url,
                            source="VMF3_TU_Wien",
                        ))
                        continue
                else:
                    logger.warning(f"VMF3 download failed: HTTP {response.status_code} for {url}")
            except requests.RequestException as e:
                logger.warning(f"VMF3 download error: {e}")

            results.append(ProductDownloadResult(
                status=DownloadStatus.NOT_FOUND,
                error_message=f"VMF3 file not found: {filename}",
            ))

        return results

    def download_all_ppp_products(
        self,
        orb_id: str = "COD",
        orb_dest: Path | None = None,
        atm_dest: Path | None = None,
        sta_dest: Path | None = None,
        vmf_source: Path | None = None,
        crd_source: Path | None = None,
    ) -> dict[str, ProductDownloadResult]:
        """Download all products required for PPP/PPP-AR processing.

        This is the main method that orchestrates downloading all required
        products for a PPP processing run, equivalent to what ORB_IGS does.

        Args:
            orb_id: Orbit provider ID (COD, IGS, etc.)
            orb_dest: Destination for orbit/clock/ERP/BIA files
            atm_dest: Destination for atmosphere (ION) files
            sta_dest: Destination for station files (CRD, VMF)
            vmf_source: Source directory for VMF3 files
            crd_source: Source directory for CRD files

        Returns:
            Dictionary mapping product type to download result
        """
        results = {}

        # Set up destination directories
        orb_dest = orb_dest or (self.orb_dir / self.LOCAL_ORB_SUBDIR)
        atm_dest = atm_dest or (self.orb_dir / self.LOCAL_ATM_SUBDIR)
        sta_dest = sta_dest or self.orb_dir

        ignss_print(
            MessageType.INFO,
            f"Downloading PPP products for {self.config.gnss_date}",
        )

        # Download orbit products
        results["orbit"] = self.download_orbit(orb_id, orb_dest)
        results["clock"] = self.download_clock(orb_id, orb_dest)
        results["erp"] = self.download_erp(orb_id, orb_dest)

        # Download BIA for PPP-AR
        if self.config.bia.enabled:
            results["bia"] = self.download_bia(orb_id, orb_dest)

        # Download ION file
        if self.config.ion.enabled:
            results["ion"] = self.download_ion(orb_id, atm_dest)

        # Download CRD file
        results["crd"] = self.download_crd(sta_dest, crd_source)

        # Download VMF3 files (troposphere mapping functions from TU Wien)
        if self.config.vmf3.enabled:
            vmf_results = self.download_vmf3(sta_dest, vmf_source)
            results["vmf3"] = vmf_results[0] if vmf_results else ProductDownloadResult(
                status=DownloadStatus.NOT_FOUND,
                error_message="No VMF3 files",
            )

        # Log summary
        success_count = sum(1 for r in results.values() if isinstance(r, ProductDownloadResult) and r.success)
        total_count = len(results)
        ignss_print(
            MessageType.INFO,
            f"Downloaded {success_count}/{total_count} PPP products",
        )

        self._disconnect()
        return results

    def __enter__(self) -> "PPPProductDownloader":
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit - disconnect FTP."""
        self._disconnect()


# =============================================================================
# Data Manager
# =============================================================================

class DataManager:
    """Manage RINEX data files for processing.

    Handles hourly, daily, and subhourly data.
    Replaces Perl get_list_of_hourly_files, get_list_of_daily_files,
    get_list_of_available_hourly_files, compose_list_from_list_and_comp.
    """

    def __init__(self, config: ProcessingConfig, db_manager: DatabaseManager | None = None):
        """Initialize data manager.

        Args:
            config: Processing configuration
            db_manager: Optional database manager for availability checks
        """
        self.config = config
        self._db_manager = db_manager

    @staticmethod
    def compose_list_with_compression(
        file_list: list[str],
        compression: str = ".Z",
    ) -> list[str]:
        """Add compression extension to file list.

        Replaces Perl compose_list_from_list_and_comp.

        Args:
            file_list: List of filenames
            compression: Compression extension to add

        Returns:
            List of filenames with compression extension
        """
        return [f"{f}{compression}" for f in file_list]

    def get_requested_files(
        self,
        stations: list[str],
        compression: str = ".Z",
        include_compression: bool = True,
    ) -> list[str]:
        """Get list of files needed for processing.

        Replaces Perl get_list_of_hourly_files and get_list_of_daily_files.

        Args:
            stations: List of station IDs
            compression: Compression extension
            include_compression: Whether to include compression in filename

        Returns:
            List of required filenames
        """
        if not self.config.gnss_date:
            return []

        gd = self.config.gnss_date
        files = []

        if self.config.proc_type == ProcessingType.HOURLY:
            # Hourly file naming: ssssdddhh.yyd (Hatanaka compressed)
            hour = getattr(gd, "hour", 0)
            hour_alpha = self._hour_to_alpha(hour)
            for sta in stations:
                # Short format with hour letter
                filename = f"{sta.lower()}{gd.doy:03d}{hour_alpha}.{gd.year % 100:02d}d"
                if include_compression:
                    filename = f"{filename}{compression}"
                files.append(filename)

        elif self.config.proc_type == ProcessingType.DAILY:
            # Daily file naming: ssssdddn.yyo or ssssdddn.yyd
            for sta in stations:
                # Use session character (0 for daily, or from config)
                session_char = "0"
                filename = f"{sta.lower()}{gd.doy:03d}{session_char}.{gd.year % 100:02d}d"
                if include_compression:
                    filename = f"{filename}{compression}"
                files.append(filename)

        elif self.config.proc_type == ProcessingType.SUBHOURLY:
            # Subhourly file naming (15-minute files)
            hour = getattr(gd, "hour", 0)
            minute = getattr(gd, "minute", 0)
            # Round to nearest 15-minute boundary
            minute_block = (minute // 15) * 15
            for sta in stations:
                filename = f"{sta.lower()}{gd.doy:03d}{hour:02d}{minute_block:02d}.{gd.year % 100:02d}d"
                if include_compression:
                    filename = f"{filename}{compression}"
                files.append(filename)

        return files

    def _hour_to_alpha(self, hour: int) -> str:
        """Convert hour (0-23) to alpha character (a-x).

        Args:
            hour: Hour of day (0-23)

        Returns:
            Corresponding alpha character
        """
        return chr(ord('a') + hour)

    def get_available_files_from_db(
        self,
        stations: list[str],
        compression: str = ".Z",
    ) -> list[str]:
        """Get list of available files by checking database.

        Replaces Perl get_list_of_available_hourly_files.

        Args:
            stations: List of station IDs
            compression: Compression extension

        Returns:
            List of available filenames
        """
        if not self._db_manager or not self.config.gnss_date:
            return []

        available = []
        gd = self.config.gnss_date

        try:
            hd_manager = HourlyDataManager(self._db_manager)

            for sta in stations:
                # Query database for file status
                status = hd_manager.get_file_status(
                    station=sta,
                    mjd=gd.mjd,
                )

                # Only include if status indicates available (not 'Waiting' or 'Too Late')
                if status and status not in ("Waiting", "Too Late"):
                    if self.config.proc_type == ProcessingType.HOURLY:
                        hour = getattr(gd, "hour", 0)
                        hour_alpha = self._hour_to_alpha(hour)
                        filename = f"{sta.lower()}{gd.doy:03d}{hour_alpha}.{gd.year % 100:02d}d{compression}"
                    else:
                        filename = f"{sta.lower()}{gd.doy:03d}0.{gd.year % 100:02d}d{compression}"

                    available.append(filename)

        except Exception as e:
            logger.warning(f"DB check failed: {e}")

        return available

    def get_available_files(
        self,
        requested: list[str],
        check_db: bool = True,
    ) -> tuple[list[str], list[str]]:
        """Check which files are available.

        Args:
            requested: List of requested filenames
            check_db: Whether to also check database

        Returns:
            Tuple of (available files, missing files)
        """
        available = []
        missing = []

        if not self.config.data_dir:
            return [], requested

        for filename in requested:
            found = False

            # Check various possible locations
            search_paths = [
                self.config.data_dir / filename,
            ]

            # Add year/doy organized path
            if self.config.gnss_date:
                gd = self.config.gnss_date
                search_paths.extend([
                    self.config.data_dir / str(gd.year) / f"{gd.year % 100:02d}{gd.doy:03d}" / filename,
                    self.config.data_dir / str(gd.year) / str(gd.doy) / filename,
                    self.config.data_dir / str(gd.year) / filename,
                ])

            for check_path in search_paths:
                if check_path.exists():
                    available.append(filename)
                    found = True
                    break

            if not found:
                missing.append(filename)

        return available, missing

    def calculate_success_rate(
        self,
        available: list[str],
        requested: list[str],
    ) -> float:
        """Calculate download/availability success rate.

        Args:
            available: List of available files
            requested: List of requested files

        Returns:
            Success rate as percentage (0-100)
        """
        if not requested:
            return 0.0
        return len(available) / len(requested) * 100


# =============================================================================
# BSW Executor
# =============================================================================

class BSWExecutor:
    """Execute Bernese GNSS Software processing.

    Handles:
    - PCF file setup
    - BPE (Bernese Processing Engine) execution
    - Result collection
    """

    def __init__(self, config: ProcessingConfig):
        """Initialize BSW executor.

        Args:
            config: Processing configuration
        """
        self.config = config

    def prepare_campaign(self) -> bool:
        """Prepare BSW campaign directory.

        Returns:
            True if preparation successful
        """
        if not self.config.bsw_campaign_dir:
            logger.error("BSW campaign directory not configured")
            return False

        # Create required subdirectories
        subdirs = ["ATM", "BPE", "GRD", "NEQ", "OBS", "ORB", "OUT", "RAW", "SOL", "STA"]
        for subdir in subdirs:
            dir_path = self.config.bsw_campaign_dir / subdir
            dir_path.mkdir(parents=True, exist_ok=True)

        return True

    def run_bpe(self, pcf_file: Path, session: str) -> tuple[bool, str]:
        """Run Bernese Processing Engine.

        Args:
            pcf_file: Path to PCF control file
            session: Session identifier

        Returns:
            Tuple of (success, output/error message)
        """
        bpe_dir = os.environ.get("BPE", "")
        if not bpe_dir:
            return False, "BPE environment variable not set"

        # Build command
        cmd = [
            f"{bpe_dir}/startBPE",
            "-c", str(self.config.bsw_campaign_dir),
            "-pcf", str(pcf_file),
            "-s", session,
        ]

        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=3600,  # 1 hour timeout
            )

            if result.returncode == 0:
                return True, result.stdout
            else:
                return False, result.stderr or result.stdout

        except subprocess.TimeoutExpired:
            return False, "BPE execution timed out"
        except Exception as e:
            return False, str(e)


# =============================================================================
# Main Orchestrator
# =============================================================================

class IGNSSOrchestrator:
    """Main i-GNSS processing orchestrator.

    Replaces Perl IGNSS.pm module.

    This class coordinates the entire processing workflow:
    1. Validate configuration and arguments
    2. Check and download required products
    3. Manage station data files
    4. Execute BSW processing
    5. Organize and archive results

    Usage:
        from pygnss_rt.processing.orchestrator import (
            IGNSSOrchestrator,
            ProcessingConfig,
            ProcessingType,
        )

        config = ProcessingConfig(
            proc_type=ProcessingType.HOURLY,
            gnss_date=GNSSDate.from_ymd(2024, 1, 15),
            campaign_name="PPP_NRT",
            ...
        )

        orchestrator = IGNSSOrchestrator(config)
        result = orchestrator.run()

        if result.success:
            print(f"Processed {result.stations_processed} stations")
    """

    def __init__(self, config: ProcessingConfig):
        """Initialize orchestrator.

        Args:
            config: Processing configuration
        """
        self.config = config
        self.product_checker = ProductChecker(config)
        self.data_manager = DataManager(config)
        self.bsw_executor = BSWExecutor(config)
        self.ppp_downloader: PPPProductDownloader | None = None
        self._result = ProcessingResult(proc_type=config.proc_type)

    def validate_config(self) -> list[str]:
        """Validate processing configuration.

        Returns:
            List of validation errors (empty if valid)

        Raises:
            ProcessingError: If validation is enabled and fails
        """
        errors = []

        # Check required date
        if not self.config.gnss_date:
            errors.append("Processing date (gnss_date) is required")

        # Check FTP configuration
        if self.config.ftp_config_path:
            if not self.config.ftp_config_path.exists():
                errors.append(f"FTP config file not found: {self.config.ftp_config_path}")

        # Check mandatory info files
        mandatory_files = [
            ("info_sta", self.config.info_sta),
            ("info_otl", self.config.info_otl),
            ("info_ses", self.config.info_ses),
            ("info_sel", self.config.info_sel),
        ]

        for name, path in mandatory_files:
            if path and not path.exists():
                errors.append(f"Missing mandatory file: {name} = {path}")

        # Check PCF file
        if self.config.pcf_file and not self.config.pcf_file.exists():
            errors.append(f"PCF file not found: {self.config.pcf_file}")

        # Check processing-specific tools
        if self.config.use_clockprep and self.config.clockprep_bin:
            if not self.config.clockprep_bin.exists():
                errors.append(f"clockprep binary not found: {self.config.clockprep_bin}")

        if self.config.use_cc2noncc:
            if self.config.cc2noncc_bin and not self.config.cc2noncc_bin.exists():
                errors.append(f"cc2noncc binary not found: {self.config.cc2noncc_bin}")
            if self.config.p1c1_bias_hist and not self.config.p1c1_bias_hist.exists():
                errors.append(f"P1C1 bias history not found: {self.config.p1c1_bias_hist}")

        # Log errors
        if errors:
            ignss_print(MessageType.FATAL, "Configuration validation failed")
            for err in errors:
                ignss_print(MessageType.LIST, err)

            if self.config.validate_args:
                raise ProcessingError("Configuration validation failed", details=errors)

        return errors

    def check_products(self) -> dict[str, bool]:
        """Check availability of all required products.

        Returns:
            Dictionary mapping product name to availability status
        """
        ignss_print(MessageType.INFO, "Checking product availability")
        return self.product_checker.check_all_products()

    def download_ppp_products(
        self,
        orb_id: str = "COD",
        orb_dir: Path | None = None,
        vmf_source: Path | None = None,
        crd_source: Path | None = None,
    ) -> dict[str, ProductDownloadResult]:
        """Download all products required for PPP-AR processing.

        This method integrates with the FTP.pm-equivalent download logic,
        downloading all required products from CODE FTP server:
        - Orbit files (.PRE)
        - Clock files (.CLK)
        - Earth rotation parameters (.IEP/.ERP)
        - Bias files for ambiguity resolution (.BIA)
        - Ionosphere files (.ION)
        - A priori coordinates (.CRD)
        - VMF3 troposphere grids

        This replaces the product download portion of the Perl ORB_IGS script.

        Args:
            orb_id: Orbit provider ID (default: "COD")
            orb_dir: Base directory for products (V_ORBDIR equivalent)
            vmf_source: Local source directory for VMF3 files
            crd_source: Local source directory for CRD files

        Returns:
            Dictionary mapping product type to download result

        Example:
            orchestrator = IGNSSOrchestrator(config)
            results = orchestrator.download_ppp_products(
                orb_id="COD",
                orb_dir=Path("/data/products"),
                vmf_source=Path("/home/user/tiga/VMF3"),
                crd_source=Path("/home/user/tiga/CODE_APRIORI"),
            )
            if all(r.success for r in results.values()):
                orchestrator.run()
        """
        ignss_print(MessageType.INFO, "Downloading PPP-AR products")

        # Initialize PPP downloader if not already done
        if self.ppp_downloader is None:
            self.ppp_downloader = PPPProductDownloader(
                config=self.config,
                orb_dir=orb_dir or self.config.data_dir,
            )

        # Set default source directories from PathConfig if not provided
        from pygnss_rt.core.paths import get_paths
        paths = get_paths()
        if vmf_source is None:
            vmf_source = paths.vmf_source_dir
        if crd_source is None:
            crd_source = paths.apriori_source_dir

        # Download all products
        results = self.ppp_downloader.download_all_ppp_products(
            orb_id=orb_id,
            orb_dest=orb_dir / "ORB" if orb_dir else None,
            atm_dest=orb_dir / "ATM" if orb_dir else None,
            sta_dest=orb_dir if orb_dir else None,
            vmf_source=vmf_source,
            crd_source=crd_source,
        )

        # Log results
        success_count = sum(1 for r in results.values() if isinstance(r, ProductDownloadResult) and r.success)
        total_count = len(results)

        if success_count == total_count:
            ignss_print(MessageType.INFO, f"All {total_count} PPP products downloaded successfully")
        else:
            ignss_print(
                MessageType.WARNING,
                f"Downloaded {success_count}/{total_count} PPP products",
            )
            for name, result in results.items():
                if isinstance(result, ProductDownloadResult) and not result.success:
                    ignss_print(MessageType.LIST, f"Missing: {name} - {result.error_message}")

        return results

    def prepare_data(self, stations: list[str]) -> tuple[list[str], list[str]]:
        """Prepare station data for processing.

        Args:
            stations: List of station IDs

        Returns:
            Tuple of (available files, missing files)
        """
        ignss_print(
            MessageType.INFO,
            f"Preparing data for {len(stations)} stations",
        )

        requested = self.data_manager.get_requested_files(stations)
        available, missing = self.data_manager.get_available_files(requested)

        ignss_print(
            MessageType.INFO,
            f"Available: {len(available)}/{len(requested)} files",
        )

        if missing:
            ignss_print(MessageType.WARNING, f"Missing {len(missing)} files")

        return available, missing

    def run_processing(self) -> bool:
        """Execute BSW processing.

        Returns:
            True if processing successful
        """
        if not self.config.pcf_file:
            ignss_print(MessageType.FATAL, "PCF file not configured")
            return False

        # Prepare campaign
        if not self.bsw_executor.prepare_campaign():
            return False

        # Run BPE
        session = self.config.session_id or "0"
        success, output = self.bsw_executor.run_bpe(
            self.config.pcf_file,
            session,
        )

        if success:
            ignss_print(MessageType.INFO, "BSW processing completed successfully")
        else:
            ignss_print(MessageType.FATAL, f"BSW processing failed: {output}")
            self._result.errors.append(output)

        return success

    def run(self, stations: list[str] | None = None) -> ProcessingResult:
        """Run complete processing workflow.

        Args:
            stations: Optional list of station IDs to process

        Returns:
            Processing result
        """
        self._result = ProcessingResult(
            proc_type=self.config.proc_type,
            gnss_date=self.config.gnss_date,
        )

        try:
            # Step 1: Validate configuration
            errors = self.validate_config()
            if errors and self.config.validate_args:
                self._result.errors.extend(errors)
                return self._result

            # Step 2: Check products
            products = self.check_products()
            missing_products = [k for k, v in products.items() if not v]
            if missing_products:
                ignss_print(
                    MessageType.WARNING,
                    f"Missing products: {', '.join(missing_products)}",
                )

            # Step 3: Prepare data
            if stations:
                self._result.stations_requested = len(stations)
                available, missing = self.prepare_data(stations)
                self._result.files_downloaded = available

                if not available:
                    ignss_print(MessageType.FATAL, "No data available for processing")
                    self._result.errors.append("No data available")
                    return self._result

            # Step 4: Run processing
            success = self.run_processing()
            self._result.success = success

            # Step 5: Finalize
            self._result.end_time = datetime.now(timezone.utc)

            if success:
                ignss_print(
                    MessageType.INFO,
                    f"Processing completed in {self._result.duration_seconds:.1f}s",
                )

        except ProcessingError as e:
            self._result.errors.append(str(e))
            ignss_print(MessageType.FATAL, str(e))

        except Exception as e:
            self._result.errors.append(f"Unexpected error: {e}")
            ignss_print(MessageType.FATAL, f"Unexpected error: {e}")
            logger.exception("Processing failed")

        return self._result

    def run_ppp(
        self,
        stations: list[str] | None = None,
        orb_id: str = "COD",
        orb_dir: Path | None = None,
        vmf_source: Path | None = None,
        crd_source: Path | None = None,
        download_products: bool = True,
    ) -> ProcessingResult:
        """Run complete PPP/PPP-AR processing workflow with product download.

        This is the main entry point for PPP-AR processing, integrating:
        1. Configuration validation
        2. Product download from CODE FTP (orbit, clock, ERP, BIA, ION, CRD, VMF3)
        3. Data preparation
        4. BSW/BPE execution
        5. Result finalization

        This replaces the Perl IGNSS.pm + FTP.pm + ORB_IGS integration.

        Args:
            stations: Optional list of station IDs to process
            orb_id: Orbit provider ID (default: "COD")
            orb_dir: Base directory for products (V_ORBDIR equivalent)
            vmf_source: Local source directory for VMF3 files
            crd_source: Local source directory for CRD files
            download_products: Whether to download products before processing

        Returns:
            Processing result

        Example:
            from pygnss_rt.processing.orchestrator import (
                IGNSSOrchestrator,
                ProcessingConfig,
                ProcessingType,
            )
            from pygnss_rt.utils.dates import GNSSDate

            config = ProcessingConfig(
                proc_type=ProcessingType.DAILY,
                gnss_date=GNSSDate.from_ymd(2024, 9, 16),
                campaign_name="PPP54IGS",
                bia=ProductConfig(enabled=True),
                ion=ProductConfig(enabled=True),
            )

            orchestrator = IGNSSOrchestrator(config)
            result = orchestrator.run_ppp(
                orb_id="COD",
                orb_dir=Path("/home/user/GPSDATA/CODE"),
                vmf_source=Path("/home/user/tiga/VMF3"),
                crd_source=Path("/home/user/tiga/CODE_APRIORI"),
            )

            if result.success:
                print(f"Processing completed in {result.duration_seconds:.1f}s")
        """
        self._result = ProcessingResult(
            proc_type=self.config.proc_type,
            gnss_date=self.config.gnss_date,
        )

        try:
            # Step 1: Validate configuration
            ignss_print(MessageType.INFO, "Step 1: Validating configuration")
            errors = self.validate_config()
            if errors and self.config.validate_args:
                self._result.errors.extend(errors)
                return self._result

            # Step 2: Download PPP products
            if download_products:
                ignss_print(MessageType.INFO, "Step 2: Downloading PPP products")
                product_results = self.download_ppp_products(
                    orb_id=orb_id,
                    orb_dir=orb_dir,
                    vmf_source=vmf_source,
                    crd_source=crd_source,
                )

                # Check critical products (orbit, clock, erp)
                critical_products = ["orbit", "clock", "erp"]
                for product_name in critical_products:
                    if product_name in product_results:
                        result = product_results[product_name]
                        if isinstance(result, ProductDownloadResult) and not result.success:
                            error_msg = f"Critical product {product_name} download failed"
                            ignss_print(MessageType.FATAL, error_msg)
                            self._result.errors.append(error_msg)
                            return self._result

            # Step 3: Check products availability
            ignss_print(MessageType.INFO, "Step 3: Checking product availability")
            products = self.check_products()
            missing_products = [k for k, v in products.items() if not v]
            if missing_products:
                ignss_print(
                    MessageType.WARNING,
                    f"Missing products after download: {', '.join(missing_products)}",
                )

            # Step 4: Prepare data
            if stations:
                ignss_print(MessageType.INFO, f"Step 4: Preparing data for {len(stations)} stations")
                self._result.stations_requested = len(stations)
                available, missing = self.prepare_data(stations)
                self._result.files_downloaded = available

                if not available:
                    ignss_print(MessageType.FATAL, "No data available for processing")
                    self._result.errors.append("No data available")
                    return self._result

            # Step 5: Run BSW processing
            ignss_print(MessageType.INFO, "Step 5: Running BSW processing")
            success = self.run_processing()
            self._result.success = success

            # Step 6: Finalize
            self._result.end_time = datetime.now(timezone.utc)

            if success:
                ignss_print(
                    MessageType.INFO,
                    f"PPP processing completed successfully in {self._result.duration_seconds:.1f}s",
                )
            else:
                ignss_print(
                    MessageType.WARNING,
                    f"PPP processing finished with errors in {self._result.duration_seconds:.1f}s",
                )

        except ProcessingError as e:
            self._result.errors.append(str(e))
            ignss_print(MessageType.FATAL, str(e))

        except Exception as e:
            self._result.errors.append(f"Unexpected error: {e}")
            ignss_print(MessageType.FATAL, f"Unexpected error: {e}")
            logger.exception("PPP processing failed")

        return self._result

    # =========================================================================
    # Campaign Management Functions
    # =========================================================================

    def set_now_time(self) -> dict[str, Any]:
        """Set current GMT time attributes.

        Replaces Perl IGNSS::set_now_time.

        Returns:
            Dictionary with current time attributes
        """
        now = datetime.now(timezone.utc)

        self._now_time = {
            "now_year": now.year,
            "now_month": now.month,
            "now_dom": now.day,
            "now_dow": now.weekday(),  # 0=Monday, 6=Sunday
            "now_doy": now.timetuple().tm_yday,
            "now_mjd": mjd_from_date(now.year, now.month, now.day),
        }

        return self._now_time

    def get_session_name(self) -> str:
        """Generate BSW session/campaign name.

        Based on Perl logic in IGNSS::init for creating session names.

        Returns:
            Session name string (7-8 characters)
        """
        if not self.config.gnss_date:
            return ""

        gd = self.config.gnss_date
        y2c = f"{gd.year % 100:02d}"
        doy = f"{gd.doy:03d}"

        if self.config.proc_type == ProcessingType.HOURLY:
            hour = getattr(gd, "hour", 0)
            hour_alpha = chr(ord('A') + hour)  # A-X for hours 0-23

            # Session suffix based on config
            sess_id = self.config.session_id or "NR"
            if sess_id == "NR":
                return f"{y2c}{doy}{hour_alpha}H"
            elif sess_id == "00":
                return f"{y2c}{doy}{hour_alpha}0"
            elif sess_id == "15":
                return f"{y2c}{doy}{hour_alpha}1"
            elif sess_id == "30":
                return f"{y2c}{doy}{hour_alpha}3"
            elif sess_id == "45":
                return f"{y2c}{doy}{hour_alpha}4"
            else:
                return f"{y2c}{doy}{hour_alpha}{sess_id[0] if sess_id else 'H'}"

        elif self.config.proc_type == ProcessingType.DAILY:
            sess_id = self.config.session_id or "00"
            return f"{y2c}{doy}{sess_id}"

        else:  # SUBHOURLY or other
            hour = getattr(gd, "hour", 0)
            hour_alpha = chr(ord('A') + hour)
            sess_id = self.config.session_id or "NR"
            return f"{y2c}{doy}{hour_alpha}{sess_id}"

    def move_campaign(self, destination: Path | str) -> bool:
        """Move campaign to archive location.

        Replaces Perl IGNSS::move_campaign.

        Args:
            destination: Destination directory for campaign

        Returns:
            True if move successful
        """
        destination = Path(destination)
        session = self.get_session_name()

        if not session:
            ignss_print(MessageType.FATAL, "Cannot determine session name for move")
            return False

        # Get BSW campaign directory from environment or config
        bsw_dir = os.environ.get("P", "")
        if not bsw_dir and self.config.bsw_campaign_dir:
            bsw_dir = str(self.config.bsw_campaign_dir.parent)

        if not bsw_dir:
            ignss_print(MessageType.FATAL, "BSW campaign directory (P) not set")
            return False

        source_campaign = Path(bsw_dir) / session

        if not source_campaign.exists():
            ignss_print(MessageType.WARNING, f"Source campaign does not exist: {source_campaign}")
            return False

        # Create destination if needed
        destination.mkdir(parents=True, exist_ok=True)

        dest_campaign = destination / session

        # Remove existing if present
        if dest_campaign.exists():
            ignss_print(MessageType.WARNING, "Campaign will be replaced in archive")
            shutil.rmtree(dest_campaign)

        # Move campaign
        try:
            shutil.move(str(source_campaign), str(dest_campaign))
            ignss_print(MessageType.INFO, f"Campaign archived at: {dest_campaign}")
            return True
        except Exception as e:
            ignss_print(MessageType.FATAL, f"Failed to move campaign: {e}")
            return False

    def clean_campaign(self, dirs_to_delete: list[str] | None = None) -> bool:
        """Remove unnecessary directories from campaign before archiving.

        Replaces Perl IGNSS::clean_campaign.

        Args:
            dirs_to_delete: List of subdirectory names to remove

        Returns:
            True if cleanup successful
        """
        if dirs_to_delete is None:
            dirs_to_delete = self.config.dcm.dirs_to_delete

        session = self.get_session_name()
        if not session:
            return False

        bsw_dir = os.environ.get("P", "")
        if not bsw_dir and self.config.bsw_campaign_dir:
            bsw_dir = str(self.config.bsw_campaign_dir.parent)

        if not bsw_dir:
            return False

        campaign_dir = Path(bsw_dir) / session
        success = True

        for subdir in dirs_to_delete:
            dir_path = campaign_dir / subdir
            if dir_path.exists():
                try:
                    shutil.rmtree(dir_path)
                    logger.debug(f"Removed: {dir_path}")
                except Exception as e:
                    logger.warning(f"Could not remove {dir_path}: {e}")
                    success = False

        return success

    def compress_campaign(self, method: str | None = None) -> bool:
        """Compress campaign files.

        Replaces Perl IGNSS::compress_campaign.

        Args:
            method: Compression method ('gzip' or 'compress')

        Returns:
            True if compression successful
        """
        if method is None:
            method = self.config.dcm.compress_util

        if method not in ("gzip", "compress"):
            ignss_print(MessageType.WARNING, f"Invalid compression method: {method}")
            return False

        session = self.get_session_name()
        if not session:
            return False

        bsw_dir = os.environ.get("P", "")
        if not bsw_dir and self.config.bsw_campaign_dir:
            bsw_dir = str(self.config.bsw_campaign_dir.parent)

        if not bsw_dir:
            return False

        campaign_dir = Path(bsw_dir) / session

        if not campaign_dir.exists():
            return False

        # Compress all files recursively
        compressed_count = 0

        for file_path in campaign_dir.rglob("*"):
            if file_path.is_file() and not file_path.suffix in (".gz", ".Z"):
                try:
                    if method == "gzip":
                        # Use Python gzip
                        with open(file_path, "rb") as f_in:
                            with gzip.open(f"{file_path}.gz", "wb") as f_out:
                                f_out.writelines(f_in)
                        file_path.unlink()
                        compressed_count += 1
                    else:  # compress
                        # Use system compress command
                        result = subprocess.run(
                            ["compress", str(file_path)],
                            capture_output=True,
                        )
                        if result.returncode == 0:
                            compressed_count += 1
                except Exception as e:
                    logger.warning(f"Could not compress {file_path}: {e}")

        ignss_print(MessageType.INFO, f"Compressed {compressed_count} files")
        return True

    def dcm(self) -> bool:
        """Data/Campaign Management - clean, compress, and archive campaign.

        Replaces Perl IGNSS::dcm.
        Performs cleanup, compression, and archival of processed campaign.

        Returns:
            True if DCM successful
        """
        if not self.config.dcm.enabled:
            ignss_print(MessageType.INFO, "DCM not enabled, skipping")
            return True

        ignss_print(MessageType.INFO, "Starting Data/Campaign Management (DCM)")

        # Step 1: Clean campaign (remove unnecessary directories)
        if not self.clean_campaign():
            ignss_print(MessageType.WARNING, "Campaign cleanup had issues")

        # Step 2: Compress campaign files
        if not self.compress_campaign():
            ignss_print(MessageType.WARNING, "Campaign compression had issues")

        # Step 3: Determine archive destination
        archive_dir = self.config.dcm.archive_dir
        if not archive_dir:
            ignss_print(MessageType.WARNING, "No archive directory configured")
            return False

        # Apply organization pattern
        if self.config.gnss_date and self.config.dcm.organization == "yyyy/doy":
            gd = self.config.gnss_date
            archive_dir = archive_dir / str(gd.year) / f"{gd.doy:03d}"
        elif self.config.gnss_date and self.config.dcm.organization == "yyyy":
            gd = self.config.gnss_date
            archive_dir = archive_dir / str(gd.year)

        # Step 4: Move campaign to archive
        session = self.get_session_name()
        ignss_print(MessageType.INFO, f"Campaign {session} to be archived at: {archive_dir}")

        if not self.move_campaign(archive_dir):
            ignss_print(MessageType.FATAL, "Campaign archival failed")
            return False

        ignss_print(MessageType.INFO, "DCM completed successfully")
        return True

    def print_outcome(self) -> None:
        """Print processing outcome banner.

        Replaces Perl IGNSS::print_outcome.
        """
        if self._result.success:
            banner = """
                           #
                        =======
               =========================
===========================================================
                        SUCCESS
===========================================================
               =========================
                        =======
                           #
"""
        else:
            banner = """
                        =======
               =========================
===========================================================
                        FAILURE
===========================================================
               =========================
                        =======
"""
        print(banner)

    def print_processing_time(self) -> None:
        """Print processing time summary.

        Replaces Perl IGNSS::print_processing_time.
        """
        duration = self._result.duration_seconds

        days = int(duration // 86400)
        hours = int((duration % 86400) // 3600)
        minutes = int((duration % 3600) // 60)
        seconds = int(duration % 60)

        ignss_print(
            MessageType.INFO,
            f"TIME FOR THE RUN: {days} {hours} {minutes} {seconds} (day/hour/min/sec)",
        )


# =============================================================================
# Convenience Functions
# =============================================================================

def create_daily_config(
    date: GNSSDate,
    network_id: str = "IGS",
    campaign_name: str = "PPP_NRT",
    data_dir: Path | str | None = None,
) -> ProcessingConfig:
    """Create configuration for daily processing.

    Args:
        date: Processing date
        network_id: Network identifier
        campaign_name: BSW campaign name
        data_dir: Data directory path

    Returns:
        Processing configuration
    """
    return ProcessingConfig(
        proc_type=ProcessingType.DAILY,
        gnss_date=date,
        campaign_name=campaign_name,
        data_dir=Path(data_dir) if data_dir else None,
        orbit=ProductConfig(enabled=True, provider_id="IGS", category=ProductCategory.ORBIT),
        erp=ProductConfig(enabled=True, provider_id="IGS", category=ProductCategory.ERP),
        clock=ProductConfig(enabled=True, provider_id="IGS", category=ProductCategory.CLOCK),
    )


def create_hourly_config(
    date: GNSSDate,
    hour: int = 0,
    network_id: str = "IGS",
    campaign_name: str = "NRDDP_TRO",
    data_dir: Path | str | None = None,
) -> ProcessingConfig:
    """Create configuration for hourly processing.

    Args:
        date: Processing date
        hour: Hour to process (0-23)
        network_id: Network identifier
        campaign_name: BSW campaign name
        data_dir: Data directory path

    Returns:
        Processing configuration
    """
    # Create date with hour
    gd = GNSSDate.from_ymd(date.year, date.date.month, date.date.day)

    return ProcessingConfig(
        proc_type=ProcessingType.HOURLY,
        gnss_date=gd,
        campaign_name=campaign_name,
        session_id=f"{hour:02d}",
        data_dir=Path(data_dir) if data_dir else None,
        orbit=ProductConfig(enabled=True, provider_id="IGS", category=ProductCategory.ORBIT),
        erp=ProductConfig(enabled=True, provider_id="IGS", category=ProductCategory.ERP),
        clock=ProductConfig(enabled=True, provider_id="IGS", category=ProductCategory.CLOCK),
    )


def run_daily_processing(
    date: GNSSDate,
    stations: list[str],
    network_id: str = "IGS",
    **kwargs: Any,
) -> ProcessingResult:
    """Run daily processing for a list of stations.

    Convenience function for quick daily processing.

    Args:
        date: Processing date
        stations: List of station IDs
        network_id: Network identifier
        **kwargs: Additional configuration options

    Returns:
        Processing result
    """
    config = create_daily_config(date, network_id, **kwargs)
    orchestrator = IGNSSOrchestrator(config)
    return orchestrator.run(stations)


def run_hourly_processing(
    date: GNSSDate,
    hour: int,
    stations: list[str],
    network_id: str = "IGS",
    **kwargs: Any,
) -> ProcessingResult:
    """Run hourly processing for a list of stations.

    Convenience function for quick hourly processing.

    Args:
        date: Processing date
        hour: Hour to process (0-23)
        stations: List of station IDs
        network_id: Network identifier
        **kwargs: Additional configuration options

    Returns:
        Processing result
    """
    config = create_hourly_config(date, hour, network_id, **kwargs)
    orchestrator = IGNSSOrchestrator(config)
    return orchestrator.run(stations)
