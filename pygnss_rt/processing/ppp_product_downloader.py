"""
PPP-AR product downloader for GNSS processing.

This module handles downloading PPP products from CODE FTP and other sources.
Extracted from orchestrator_main.py for better modularity.
"""

from __future__ import annotations

import gzip
import shutil
import subprocess
from datetime import timedelta
from pathlib import Path

from pygnss_rt.data_access.ftp_client import FTPClient
from pygnss_rt.data_access.ftp_config import FTPServerConfig
from pygnss_rt.data_access.product_downloader import (
    ProductDownloadResult,
    DownloadStatus,
)
from pygnss_rt.processing.processing_config import (
    ProcessingConfig,
    PPPProductArgs,
)
from pygnss_rt.utils.dates import GNSSDate
from pygnss_rt.utils.logging import get_logger, ignss_print, MessageType


logger = get_logger(__name__)


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
        next_day = dt + timedelta(days=1)
        files.append(f"VMF3_{next_day.year}{next_day.month:02d}{next_day.day:02d}.H00")

        return files

    def _download_compressed_product(
        self,
        remote_filename: str,
        local_filename: str,
        dest_dir: Path,
        remote_path_template: str,
    ) -> ProductDownloadResult:
        """Template method for downloading compressed products from CODE FTP.

        This reduces code duplication across orbit, clock, ERP, BIA, ION downloads.

        Args:
            remote_filename: Remote filename (IGS long-form)
            local_filename: Local filename (BSW legacy format)
            dest_dir: Destination directory
            remote_path_template: Remote path (e.g., "/CODE/{year}/{filename}.gz")

        Returns:
            ProductDownloadResult
        """
        if not self.config.gnss_date:
            return ProductDownloadResult(
                status=DownloadStatus.UNKNOWN_ERROR,
                error_message="No processing date configured",
            )

        self._ensure_dir(dest_dir)
        local_path = dest_dir / local_filename

        # Check if already exists
        if local_path.exists():
            ignss_print(MessageType.INFO, f"Product already available: {local_filename}")
            return ProductDownloadResult(
                status=DownloadStatus.SUCCESS,
                local_path=local_path,
                source="local",
            )

        # Build remote path
        year = self.config.gnss_date.year
        remote_path = remote_path_template.format(year=year, filename=remote_filename)

        ignss_print(MessageType.INFO, f"Downloading: {remote_filename} -> {local_filename}")

        try:
            ftp = self._get_ftp_client()
            ftp.connect()

            # Download compressed file
            compressed_local = dest_dir / f"{remote_filename}.gz"
            success = ftp.download(remote_path, compressed_local)

            if success and compressed_local.exists():
                # Decompress to BSW-compatible filename
                with gzip.open(compressed_local, 'rb') as f_in:
                    with open(local_path, 'wb') as f_out:
                        shutil.copyfileobj(f_in, f_out)
                compressed_local.unlink()

                if local_path.exists():
                    ignss_print(MessageType.INFO, f"Downloaded: {local_filename}")
                    return ProductDownloadResult(
                        status=DownloadStatus.SUCCESS,
                        local_path=local_path,
                        remote_path=remote_path,
                        source="CODE",
                    )

            return ProductDownloadResult(
                status=DownloadStatus.NOT_FOUND,
                error_message=f"Failed to download: {remote_filename}",
            )

        except Exception as e:
            logger.error(f"Download failed: {e}")
            return ProductDownloadResult(
                status=DownloadStatus.CONNECTION_ERROR,
                error_message=str(e),
            )

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
        remote_filename = self.get_orbit_filename(orb_id, use_longform=True)
        local_filename = self.get_orbit_filename(orb_id, use_longform=False) if save_bsw_format else remote_filename
        dest_dir = destination or (self.orb_dir / self.LOCAL_ORB_SUBDIR)

        return self._download_compressed_product(
            remote_filename=remote_filename,
            local_filename=local_filename,
            dest_dir=dest_dir,
            remote_path_template="/CODE/{year}/{filename}.gz",
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
        remote_filename = self.get_clock_filename(orb_id, use_longform=True, high_rate=high_rate)
        local_filename = self.get_clock_filename(orb_id, use_longform=False) if save_bsw_format else remote_filename
        dest_dir = destination or (self.orb_dir / self.LOCAL_ORB_SUBDIR)

        return self._download_compressed_product(
            remote_filename=remote_filename,
            local_filename=local_filename,
            dest_dir=dest_dir,
            remote_path_template="/CODE/{year}/{filename}.gz",
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
        remote_filename = self.get_erp_filename(orb_id, use_longform=True)
        local_filename = self.get_erp_filename(orb_id, use_longform=False) if save_bsw_format else remote_filename
        dest_dir = destination or (self.orb_dir / self.LOCAL_ORB_SUBDIR)

        return self._download_compressed_product(
            remote_filename=remote_filename,
            local_filename=local_filename,
            dest_dir=dest_dir,
            remote_path_template="/CODE/{year}/{filename}.gz",
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
        remote_filename = self.get_bia_filename(orb_id, use_longform=True)
        local_filename = self.get_bia_filename(orb_id, use_longform=False) if save_bsw_format else remote_filename
        dest_dir = destination or (self.orb_dir / self.LOCAL_ORB_SUBDIR)

        return self._download_compressed_product(
            remote_filename=remote_filename,
            local_filename=local_filename,
            dest_dir=dest_dir,
            remote_path_template="/CODE/{year}/{filename}.gz",
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
        remote_filename = self.get_ion_filename(orb_id, use_longform=True)
        local_filename = self.get_ion_filename(orb_id, use_longform=False) if save_bsw_format else remote_filename
        dest_dir = destination or (self.orb_dir / self.LOCAL_ATM_SUBDIR)

        return self._download_compressed_product(
            remote_filename=remote_filename,
            local_filename=local_filename,
            dest_dir=dest_dir,
            remote_path_template="/CODE/{year}/{filename}.gz",
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
                shutil.copy(source_file, dest_dir / (filename + ".gz"))
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
                    if source_path.suffix == ".gz":
                        shutil.copy(source_path, dest_dir / (filename + ".gz"))
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
            except Exception as e:
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
