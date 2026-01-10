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
