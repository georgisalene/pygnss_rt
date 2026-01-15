"""
Orchestrator configuration classes.

Contains all configuration dataclasses and enums for the processing orchestrator.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path

from pygnss_rt.utils.dates import GNSSDate


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
    vmf3: ProductConfig = field(default_factory=lambda: ProductConfig(enabled=True))

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
