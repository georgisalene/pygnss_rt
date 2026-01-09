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

import os
import shutil
import subprocess
from abc import ABC, abstractmethod
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
    """

    def __init__(
        self,
        config: ProcessingConfig,
        ftp_configs: list[FTPServerConfig] | None = None,
    ):
        """Initialize product checker.

        Args:
            config: Processing configuration
            ftp_configs: Optional pre-loaded FTP configurations
        """
        self.config = config
        self._ftp_configs = ftp_configs

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

    def get_orbit_filename(self) -> str | None:
        """Generate orbit filename based on configuration.

        Returns:
            Orbit filename or None if not configured
        """
        if not self.config.orbit.enabled or not self.config.gnss_date:
            return None

        gd = self.config.gnss_date
        provider = self.config.orbit.provider_id.lower()

        # IGS product naming convention
        gps_week = gd.gps_week
        dow = gd.dow

        return f"{provider}{gps_week}{dow}.sp3.Z"

    def get_erp_filename(self) -> str | None:
        """Generate ERP filename based on configuration."""
        if not self.config.erp.enabled or not self.config.gnss_date:
            return None

        gd = self.config.gnss_date
        provider = self.config.erp.provider_id.lower()
        gps_week = gd.gps_week

        return f"{provider}{gps_week}7.erp.Z"

    def get_clock_filename(self) -> str | None:
        """Generate clock filename based on configuration."""
        if not self.config.clock.enabled or not self.config.gnss_date:
            return None

        gd = self.config.gnss_date
        provider = self.config.clock.provider_id.lower()
        gps_week = gd.gps_week
        dow = gd.dow

        return f"{provider}{gps_week}{dow}.clk.Z"

    def get_dcb_filename(self) -> str | None:
        """Generate DCB filename based on configuration."""
        if not self.config.dcb.enabled or not self.config.gnss_date:
            return None

        gd = self.config.gnss_date
        provider = self.config.dcb.provider_id.upper()
        month = gd.date.strftime("%m")
        year = gd.year

        return f"P1C1{year}{month}.DCB.Z"

    def check_product(
        self,
        category: ProductCategory,
        filename: str,
        destination: Path,
    ) -> bool:
        """Check if product exists locally or download it.

        Args:
            category: Product category
            filename: Product filename
            destination: Local destination directory

        Returns:
            True if product is available
        """
        local_path = destination / filename

        # Check if already exists
        if local_path.exists():
            logger.info("Product already available", file=filename)
            return True

        # Try to download
        # (Implementation depends on specific product source)
        logger.warning("Product download not implemented", file=filename)
        return False

    def check_all_products(self) -> dict[str, bool]:
        """Check availability of all configured products.

        Returns:
            Dictionary of product category -> availability
        """
        results = {}

        if self.config.orbit.enabled:
            filename = self.get_orbit_filename()
            if filename:
                results["orbit"] = self.check_product(
                    ProductCategory.ORBIT,
                    filename,
                    self.config.data_dir or Path("."),
                )

        if self.config.erp.enabled:
            filename = self.get_erp_filename()
            if filename:
                results["erp"] = self.check_product(
                    ProductCategory.ERP,
                    filename,
                    self.config.data_dir or Path("."),
                )

        if self.config.clock.enabled:
            filename = self.get_clock_filename()
            if filename:
                results["clock"] = self.check_product(
                    ProductCategory.CLOCK,
                    filename,
                    self.config.data_dir or Path("."),
                )

        return results


# =============================================================================
# Data Manager
# =============================================================================

class DataManager:
    """Manage RINEX data files for processing.

    Handles hourly, daily, and subhourly data.
    """

    def __init__(self, config: ProcessingConfig):
        """Initialize data manager.

        Args:
            config: Processing configuration
        """
        self.config = config

    def get_requested_files(self, stations: list[str]) -> list[str]:
        """Get list of files needed for processing.

        Args:
            stations: List of station IDs

        Returns:
            List of required filenames
        """
        if not self.config.gnss_date:
            return []

        gd = self.config.gnss_date
        files = []

        if self.config.proc_type == ProcessingType.HOURLY:
            # Hourly file naming: ssss_R_yyyydddhhmm_01H_30S_MO.crx.gz
            # Or: ssssdddhh.yyo.Z
            hour = gd.hour if hasattr(gd, "hour") else 0
            for sta in stations:
                # Short format
                filename = f"{sta.lower()}{gd.doy:03d}{hour:d}.{gd.year % 100:02d}o.Z"
                files.append(filename)

        elif self.config.proc_type == ProcessingType.DAILY:
            # Daily file naming: ssssdddn.yyo.Z
            for sta in stations:
                filename = f"{sta.lower()}{gd.doy:03d}0.{gd.year % 100:02d}o.Z"
                files.append(filename)

        return files

    def get_available_files(self, requested: list[str]) -> tuple[list[str], list[str]]:
        """Check which files are available.

        Args:
            requested: List of requested filenames

        Returns:
            Tuple of (available files, missing files)
        """
        available = []
        missing = []

        if not self.config.data_dir:
            return [], requested

        for filename in requested:
            # Check various possible locations
            found = False
            for subdir in ["", self.config.gnss_date.year if self.config.gnss_date else ""]:
                check_path = self.config.data_dir / str(subdir) / filename
                if check_path.exists():
                    available.append(filename)
                    found = True
                    break

            if not found:
                missing.append(filename)

        return available, missing


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
