"""
i-GNSS Processing Orchestrator.

Replaces Perl IGNSS.pm - the main orchestration module that:
- Validates processing arguments
- Checks product availability (orbits, clocks, ERP, DCB, BIA, ION)
- Manages data downloads (hourly/daily/subhourly)
- Executes Bernese GNSS Software (BSW) processing
- Organizes results and output

This is the central coordinator for all processing workflows.

Note: This module has been refactored to split large classes into separate modules:
- processing_config.py: Configuration dataclasses and enums
- product_checker.py: ProductChecker class
- ppp_product_downloader.py: PPPProductDownloader class
- data_manager.py: DataManager class
- bsw_executor.py: BSWExecutor class

This module now contains only the main IGNSSOrchestrator class and convenience functions.
"""

from __future__ import annotations

import gzip
import os
import shutil
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pygnss_rt.core.exceptions import ProcessingError
from pygnss_rt.data_access.product_downloader import ProductDownloadResult
from pygnss_rt.utils.dates import GNSSDate, mjd_from_date
from pygnss_rt.utils.logging import get_logger, ignss_print, MessageType

# Import from new modular structure
from pygnss_rt.processing.processing_config import (
    ProcessingType,
    ProductCategory,
    ProductConfig,
    DataSourceConfig,
    DatabaseConfig,
    DCMConfig,
    ProcessingConfig,
    ProcessingResult,
    ProcProductConfig,
    PPPProductArgs,
)
from pygnss_rt.processing.product_checker import ProductChecker
from pygnss_rt.processing.ppp_product_downloader import PPPProductDownloader
from pygnss_rt.processing.data_manager import DataManager
from pygnss_rt.processing.bsw_executor import BSWExecutor


logger = get_logger(__name__)


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


# =============================================================================
# Re-export all classes for backward compatibility
# =============================================================================

__all__ = [
    # Enums
    "ProcessingType",
    "ProductCategory",
    # Config classes
    "ProductConfig",
    "DataSourceConfig",
    "DatabaseConfig",
    "DCMConfig",
    "ProcessingConfig",
    "ProcessingResult",
    # PPP config classes
    "ProcProductConfig",
    "PPPProductArgs",
    # Main classes
    "ProductChecker",
    "PPPProductDownloader",
    "DataManager",
    "BSWExecutor",
    "IGNSSOrchestrator",
    # Helper functions
    "create_daily_config",
    "create_hourly_config",
    "run_daily_processing",
    "run_hourly_processing",
]
