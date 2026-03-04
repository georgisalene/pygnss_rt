"""
Product checking and downloading for GNSS processing.

This module handles checking and downloading GNSS products (orbits, clocks, ERP, etc.).
Extracted from orchestrator_main.py for better modularity.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from pygnss_rt.data_access.ftp_client import FTPClient
from pygnss_rt.data_access.ftp_config import FTPServerConfig, load_ftp_config
from pygnss_rt.database import DatabaseManager, ProductManager
from pygnss_rt.processing.processing_config import ProcessingConfig, ProductCategory
from pygnss_rt.utils.dates import mjd_from_date
from pygnss_rt.utils.logging import get_logger, ignss_print, MessageType


logger = get_logger(__name__)


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
