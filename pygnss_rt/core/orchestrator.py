"""
Main orchestrator for PyGNSS-RT processing.

Coordinates all processing activities including:
- Product downloads (orbit, ERP, clock, BIA, ION, DCB)
- Station data retrieval (daily, hourly, subhourly)
- BSW processing campaign management
- ZTD/IWV generation
- Result archiving (Delete, Compress, Move)

Replaces Perl IGNSS.pm module.
"""

from __future__ import annotations

import gzip
import os
import shutil
import subprocess
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import TYPE_CHECKING, Any, Optional

from pygnss_rt.core.config import Settings, load_settings
from pygnss_rt.core.exceptions import ProcessingError, ConfigurationError
from pygnss_rt.database.connection import DatabaseManager, init_db
from pygnss_rt.database.products import ProductManager
from pygnss_rt.database.models import ProductType, ProductTier
from pygnss_rt.data_access.downloader import DataDownloader
from pygnss_rt.data_access.ftp_config import FTPConfigManager
from pygnss_rt.stations.station import StationManager
from pygnss_rt.stations.bswsta import BSWStationFile
from pygnss_rt.bsw.environment import BSWEnvironment, load_bsw_environment
from pygnss_rt.bsw.interface import BSWRunner, CampaignManager, CampaignConfig
from pygnss_rt.atmosphere.ztd2iwv import ZTD2IWV, MeteoStationDatabase
from pygnss_rt.utils.dates import GNSSDate
from pygnss_rt.utils.format import hour_to_alpha
from pygnss_rt.utils.logging import get_logger, setup_logging


logger = get_logger(__name__)


class ProcessingType(str, Enum):
    """Processing type enumeration."""
    DAILY = "daily"
    HOURLY = "hourly"
    SUBHOURLY = "subhourly"


@dataclass
class ProcessingArgs:
    """Processing arguments."""

    proc_type: ProcessingType = ProcessingType.HOURLY
    start_date: GNSSDate | None = None
    end_date: GNSSDate | None = None
    stations: list[str] = field(default_factory=list)
    network: str | None = None
    exclude_stations: list[str] = field(default_factory=list)

    # Product configuration
    orbit_provider: str = "IGS"
    orbit_tier: str = "final"
    erp_provider: str = "IGS"
    erp_tier: str = "final"
    clock_provider: str = "IGS"
    clock_tier: str = "final"
    use_clock_from_eph: bool = False

    # BIA/ION/DCB products
    use_bia: bool = False
    bia_provider: str = "CODE"
    bia_tier: str = "rapid"
    use_ion: bool = False
    ion_provider: str = "CODE"
    use_dcb: bool = False
    dcb_provider: str = "CODE"

    # VMF3 troposphere mapping functions
    use_vmf3: bool = True  # Enabled by default for improved troposphere modeling
    vmf3_provider: str = "VMF3"  # TU Wien

    # Processing options
    cron_mode: bool = False
    latency_hours: int = 3
    latency_days: int = 0
    generate_iwv: bool = True
    use_clockprep: bool = False
    use_cc2noncc: bool = False
    remove_if_no_coord: bool = True

    # Session configuration
    session_id: str = "NR"  # 2-char session identifier

    # Archive options
    archive_enabled: bool = True
    archive_dir: Path | None = None
    archive_compression: str = "gzip"
    dirs_to_delete: list[str] = field(default_factory=lambda: ["OBS", "RAW", "ORX", "INP"])


@dataclass
class ProductFiles:
    """Container for product file paths."""
    orbit: Path | None = None
    erp: Path | None = None
    clock: Path | None = None
    bia: Path | None = None
    ion: Path | None = None
    dcb: Path | None = None
    iep: Path | None = None  # IERS format ERP
    vmf3: list[Path] = field(default_factory=list)  # VMF3 grid files (5 per day)


@dataclass
class ProcessingResult:
    """Result from processing a single epoch."""

    mjd: float
    success: bool
    session: str = ""
    stations_processed: int = 0
    stations_available: int = 0
    stations_missing: int = 0
    ztd_count: int = 0
    iwv_count: int = 0
    error: str | None = None
    runtime_seconds: float = 0.0
    campaign_dir: Path | None = None
    archived_to: Path | None = None


class IGNSS:
    """Main processing orchestrator.

    Coordinates the complete GNSS processing workflow including:
    - Product downloads from various FTP sources
    - Station data acquisition
    - BSW processing
    - ZTD to IWV conversion
    - Result archiving

    Note: Class name kept as IGNSS for backward compatibility.
    """

    def __init__(
        self,
        config_path: Path | str | None = None,
        settings: Settings | None = None,
    ):
        """Initialize orchestrator.

        Args:
            config_path: Path to configuration file
            settings: Pre-loaded settings (overrides config_path)
        """
        self.settings = settings or load_settings(config_path)

        # Initialize logging
        setup_logging(
            level=self.settings.logging.level,
            log_dir=self.settings.logging.log_dir,
            log_to_file=self.settings.logging.log_to_file,
            log_to_console=self.settings.logging.log_to_console,
            json_format=self.settings.logging.json_format,
        )

        # Initialize components (lazy loaded)
        self._db: DatabaseManager | None = None
        self._product_manager: ProductManager | None = None
        self._downloader: DataDownloader | None = None
        self._station_manager: StationManager | None = None
        self._bsw_env: BSWEnvironment | None = None
        self._bsw_runner: BSWRunner | None = None
        self._campaign_manager: CampaignManager | None = None
        self._ftp_config: FTPConfigManager | None = None
        self._meteo_db: MeteoStationDatabase | None = None

        # Processing state
        self._current_date: GNSSDate | None = None
        self._now: GNSSDate | None = None

        logger.info("PyGNSS-RT orchestrator initialized")

    @property
    def db(self) -> DatabaseManager:
        """Get database manager."""
        if self._db is None:
            self._db = init_db(self.settings.database.path)
        return self._db

    @property
    def product_manager(self) -> ProductManager:
        """Get product manager."""
        if self._product_manager is None:
            self._product_manager = ProductManager(self.db)
        return self._product_manager

    @property
    def downloader(self) -> DataDownloader:
        """Get data downloader."""
        if self._downloader is None:
            self._downloader = DataDownloader(
                download_dir=self.settings.data.oedc_dir,
                db=self.db,
            )
        return self._downloader

    @property
    def station_manager(self) -> StationManager:
        """Get station manager."""
        if self._station_manager is None:
            self._station_manager = StationManager()
        return self._station_manager

    @property
    def ftp_config(self) -> FTPConfigManager:
        """Get FTP configuration manager."""
        if self._ftp_config is None:
            self._ftp_config = FTPConfigManager()
        return self._ftp_config

    @property
    def bsw_env(self) -> BSWEnvironment:
        """Get BSW environment."""
        if self._bsw_env is None:
            if self.settings.bsw.loadgps_setvar and self.settings.bsw.loadgps_setvar.exists():
                self._bsw_env = load_bsw_environment(self.settings.bsw.loadgps_setvar)
            else:
                self._bsw_env = BSWEnvironment(
                    bsw_root=self.settings.bsw.bsw_root,
                    user_dir=self.settings.bsw.user_dir,
                    exec_dir=self.settings.bsw.exec_dir,
                    queue_dir=self.settings.bsw.queue_dir,
                    temp_dir=self.settings.bsw.temp_dir,
                    campaign_root=self.settings.bsw.campaign_root,
                )
        return self._bsw_env

    @property
    def campaign_manager(self) -> CampaignManager:
        """Get campaign manager."""
        if self._campaign_manager is None:
            self._campaign_manager = CampaignManager(self.bsw_env.campaign_root)
        return self._campaign_manager

    @property
    def bsw_runner(self) -> BSWRunner:
        """Get BSW runner."""
        if self._bsw_runner is None:
            self._bsw_runner = BSWRunner(self.bsw_env, self.campaign_manager)
        return self._bsw_runner

    @property
    def meteo_db(self) -> MeteoStationDatabase:
        """Get meteorological station database."""
        if self._meteo_db is None:
            self._meteo_db = MeteoStationDatabase()
        return self._meteo_db

    def set_now_time(self) -> None:
        """Set current processing time."""
        self._now = GNSSDate.now()
        logger.debug(f"Processing time set to {self._now}")

    def load_station_config(self, station_file: Path | str) -> int:
        """Load station configuration from XML or YAML file.

        Args:
            station_file: Path to station file (XML or YAML)

        Returns:
            Number of stations loaded
        """
        return self.station_manager.load(station_file)

    def load_ftp_config(self, config_path: Path | str) -> int:
        """Load FTP server configuration.

        Args:
            config_path: Path to FTP configuration XML file

        Returns:
            Number of servers loaded
        """
        return self.ftp_config.load(config_path)

    def load_meteo_stations(self, wmo_file: Path | str) -> int:
        """Load meteorological station database.

        Args:
            wmo_file: Path to WMO station file

        Returns:
            Number of stations loaded
        """
        return self.meteo_db.load_wmo_file(wmo_file)

    def _build_session_name(
        self,
        date: GNSSDate,
        args: ProcessingArgs,
    ) -> str:
        """Build session/campaign name for BSW.

        Args:
            date: Processing date
            args: Processing arguments

        Returns:
            Session name (7-8 characters)
        """
        y2c = date.year % 100
        doy = date.doy
        ha = hour_to_alpha(date.hour) if date.hour is not None else "0"

        if args.proc_type == ProcessingType.HOURLY:
            if args.session_id == "NR":
                session = f"{y2c:02d}{doy:03d}{ha.upper()}H"
            else:
                session = f"{y2c:02d}{doy:03d}{ha.upper()}{args.session_id}"
        elif args.proc_type == ProcessingType.SUBHOURLY:
            # Map minute to single character: 00->0, 15->1, 30->3, 45->4
            minute = date.minute if date.minute else 0
            minute_char_map = {0: "0", 15: "1", 30: "3", 45: "4"}
            minute_char = minute_char_map.get(minute, "0")
            session = f"{y2c:02d}{doy:03d}{ha.upper()}{minute_char}"
        else:  # daily
            session = f"{y2c:02d}{doy:03d}{args.session_id}"

        # Validate session length
        if len(session) not in (7, 8):
            raise ConfigurationError(
                f"Invalid session name '{session}' - must be 7-8 characters"
            )

        return session

    def _get_list_of_files(
        self,
        stations: list[str],
        date: GNSSDate,
        args: ProcessingArgs,
        compression: str = ".gz",
    ) -> list[str]:
        """Generate list of expected RINEX files.

        Args:
            stations: List of station IDs
            date: Processing date
            args: Processing arguments
            compression: Compression extension

        Returns:
            List of expected filenames
        """
        files = []
        year = date.year
        doy = date.doy
        hour = date.hour if date.hour is not None else 0
        minute = date.minute if date.minute is not None else 0

        for sta in sorted(stations):
            # RINEX 3 long filename format
            for suffix in ["_S_", "_R_"]:
                if args.proc_type == ProcessingType.DAILY:
                    # Daily: XXXX_S_YYYYDDD0000_01D_30S_MO.crx.gz
                    filename = (
                        f"{sta}{suffix}{year:04d}{doy:03d}0000_01D_30S_MO.crx"
                    )
                elif args.proc_type == ProcessingType.HOURLY:
                    # Hourly: XXXX_S_YYYYDDDHH00_01H_30S_MO.crx.gz
                    filename = (
                        f"{sta}{suffix}{year:04d}{doy:03d}{hour:02d}00_01H_30S_MO.crx"
                    )
                else:  # subhourly
                    # Subhourly: XXXX_S_YYYYDDDHHMI_15M_01S_MO.crx.gz
                    filename = (
                        f"{sta}{suffix}{year:04d}{doy:03d}{hour:02d}{minute:02d}_15M_01S_MO.crx"
                    )

                if compression:
                    filename += compression

                files.append(filename)

        return files

    def _check_available_files(
        self,
        requested_files: list[str],
        stations: list[str],
        date: GNSSDate,
        args: ProcessingArgs,
    ) -> tuple[list[str], list[str]]:
        """Check which files are available in database.

        Args:
            requested_files: List of requested files
            stations: List of station IDs
            date: Processing date
            args: Processing arguments

        Returns:
            Tuple of (available_files, missing_files)
        """
        available = []
        missing = []

        # Query database for each station
        for sta in sorted(stations):
            # Check if station has data for this epoch
            # This would query the HD/SD database tables
            has_data = self._check_station_data_available(sta, date, args.proc_type)

            # Find files for this station
            sta_files = [f for f in requested_files if f.startswith(sta)]

            if has_data:
                available.extend(sta_files)
            else:
                missing.extend(sta_files)

        logger.info(
            f"Data availability: {len(available)} available, {len(missing)} missing"
        )

        return available, missing

    def _check_station_data_available(
        self,
        station: str,
        date: GNSSDate,
        proc_type: ProcessingType,
    ) -> bool:
        """Check if station has data available for epoch.

        Args:
            station: Station ID
            date: Processing date
            proc_type: Processing type

        Returns:
            True if data is available
        """
        # Query appropriate database (HD for hourly, SD for subhourly)
        # For now, assume data is available
        # TODO: Implement actual database query
        return True

    def _ensure_products(
        self,
        date: GNSSDate,
        args: ProcessingArgs,
    ) -> ProductFiles:
        """Ensure required products are available.

        Downloads products if not already available.

        Args:
            date: Processing date
            args: Processing arguments

        Returns:
            ProductFiles with paths to all products
        """
        products = ProductFiles()

        # Download orbit
        logger.info(f"Checking orbit product: {args.orbit_provider}/{args.orbit_tier}")
        orbit_path = self.downloader.download_product(
            ProductType.ORBIT,
            args.orbit_provider,
            ProductTier(args.orbit_tier),
            date,
        )
        products.orbit = orbit_path

        # Download ERP
        logger.info(f"Checking ERP product: {args.erp_provider}/{args.erp_tier}")
        erp_path = self.downloader.download_product(
            ProductType.ERP,
            args.erp_provider,
            ProductTier(args.erp_tier),
            date,
        )
        products.erp = erp_path

        # Download clocks if not from EPH
        if not args.use_clock_from_eph:
            logger.info(f"Checking clock product: {args.clock_provider}/{args.clock_tier}")
            clock_path = self.downloader.download_product(
                ProductType.CLOCK,
                args.clock_provider,
                ProductTier(args.clock_tier),
                date,
            )
            products.clock = clock_path

        # Optional: BIA
        if args.use_bia:
            logger.info(f"Checking BIA product: {args.bia_provider}/{args.bia_tier}")
            bia_path = self.downloader.download_product(
                ProductType.BIA,
                args.bia_provider,
                ProductTier(args.bia_tier),
                date,
            )
            products.bia = bia_path

        # Optional: ION
        if args.use_ion:
            logger.info(f"Checking ION product: {args.ion_provider}")
            ion_path = self.downloader.download_product(
                ProductType.ION,
                args.ion_provider,
                ProductTier.FINAL,
                date,
            )
            products.ion = ion_path

        # Optional: DCB
        if args.use_dcb:
            logger.info(f"Checking DCB product: {args.dcb_provider}")
            dcb_path = self.downloader.download_product(
                ProductType.DCB,
                args.dcb_provider,
                ProductTier.FINAL,
                date,
            )
            products.dcb = dcb_path

        return products

    def _download_station_data(
        self,
        missing_files: list[str],
        date: GNSSDate,
        args: ProcessingArgs,
        temp_dir: Path,
    ) -> list[str]:
        """Download missing station data files.

        Args:
            missing_files: List of files to download
            date: Processing date
            args: Processing arguments
            temp_dir: Temporary directory for downloads

        Returns:
            List of successfully downloaded files
        """
        downloaded = []

        if not missing_files:
            return downloaded

        # Get FTP configuration for data downloads
        data_category = args.proc_type.value
        if args.proc_type == ProcessingType.SUBHOURLY:
            data_category = "subhourly"

        # Try each configured FTP server
        for server_name in self.ftp_config.get_server_names():
            if not missing_files:
                break

            try:
                config = self.ftp_config.get_data_config(server_name, data_category)
                if not config:
                    continue

                # Build list with compression
                files_to_download = [
                    f"{f}{config.compression}" if not f.endswith(config.compression) else f
                    for f in missing_files
                ]

                # Download files
                result = self.downloader.download_files(
                    server_name=server_name,
                    files=files_to_download,
                    destination=temp_dir,
                    year=date.year,
                    doy=date.doy,
                    gps_week=date.gps_week,
                    hour=date.hour,
                )

                if result:
                    # Remove compression from names for tracking
                    for f in result:
                        base_name = f.replace(config.compression, "")
                        downloaded.append(base_name)
                        if base_name in missing_files:
                            missing_files.remove(base_name)

            except Exception as e:
                logger.warning(f"Failed to download from {server_name}: {e}")

        return downloaded

    def _filter_by_coordinates(
        self,
        files: list[str],
        coord_file: Path,
    ) -> tuple[list[str], list[str]]:
        """Filter files by coordinate availability.

        Args:
            files: List of RINEX files
            coord_file: Path to coordinate file (.CRD)

        Returns:
            Tuple of (files_with_coords, files_without_coords)
        """
        with_coords = []
        without_coords = []

        # Load coordinate file
        sta_file = BSWStationFile()
        sta_file.load(coord_file)

        for f in files:
            # Extract 4-char station code from filename
            sta = Path(f).name[:4].upper()

            if sta_file.has_station(sta):
                with_coords.append(f)
            else:
                without_coords.append(f)

        logger.info(
            f"Coordinate filter: {len(with_coords)} with APC, "
            f"{len(without_coords)} without APC"
        )

        return with_coords, without_coords

    def _setup_campaign(
        self,
        session: str,
        date: GNSSDate,
        args: ProcessingArgs,
        files: list[str],
        products: ProductFiles,
    ) -> Path:
        """Set up BSW campaign directory.

        Args:
            session: Session/campaign name
            date: Processing date
            args: Processing arguments
            files: List of RINEX files to process
            products: Product file paths

        Returns:
            Campaign directory path
        """
        campaign_dir = self.bsw_env.campaign_root / session

        # Remove existing campaign if present
        if campaign_dir.exists():
            logger.warning(f"Removing existing campaign: {campaign_dir}")
            shutil.rmtree(campaign_dir)

        # Create campaign directory structure
        subdirs = ["ATM", "BPE", "OBS", "ORB", "ORX", "OUT", "RAW", "SOL", "STA", "GEN", "INP"]
        for subdir in subdirs:
            (campaign_dir / subdir).mkdir(parents=True, exist_ok=True)

        # Copy RINEX data to RAW
        self._copy_rinex_to_campaign(files, campaign_dir / "RAW", date, args)

        # Copy products to ORB/ATM
        self._copy_products_to_campaign(products, campaign_dir, date)

        # Copy auxiliary files
        self._copy_auxiliary_files(campaign_dir, args)

        return campaign_dir

    def _copy_rinex_to_campaign(
        self,
        files: list[str],
        raw_dir: Path,
        date: GNSSDate,
        args: ProcessingArgs,
    ) -> list[str]:
        """Copy and process RINEX files to campaign RAW directory.

        Handles decompression and Hatanaka conversion.

        Args:
            files: List of RINEX files
            raw_dir: Campaign RAW directory
            date: Processing date
            args: Processing arguments

        Returns:
            List of processed files
        """
        processed = []

        for f in files:
            src = Path(f)
            if not src.exists():
                logger.warning(f"File not found: {f}")
                continue

            # Copy to RAW
            dest = raw_dir / src.name.lower()
            shutil.copy2(src, dest)

            # Decompress if needed
            if dest.suffix == ".gz":
                with gzip.open(dest, "rb") as f_in:
                    with open(dest.with_suffix(""), "wb") as f_out:
                        shutil.copyfileobj(f_in, f_out)
                dest.unlink()
                dest = dest.with_suffix("")

            elif dest.suffix == ".Z":
                subprocess.run(["uncompress", str(dest)], check=True)
                dest = dest.with_suffix("")

            # Convert from Hatanaka if needed (.crx -> .rnx)
            if dest.suffix.lower() == ".crx":
                # Run CRX2RNX
                subprocess.run(
                    ["crx2rnx", str(dest)],
                    cwd=raw_dir,
                    check=False,
                )
                rnx_file = dest.with_suffix(".rnx")
                if rnx_file.exists():
                    dest = rnx_file

            processed.append(str(dest))

        return processed

    def _copy_products_to_campaign(
        self,
        products: ProductFiles,
        campaign_dir: Path,
        date: GNSSDate,
    ) -> None:
        """Copy products to campaign directories.

        Args:
            products: Product file paths
            campaign_dir: Campaign directory
            date: Processing date
        """
        orb_dir = campaign_dir / "ORB"
        atm_dir = campaign_dir / "ATM"
        out_dir = campaign_dir / "OUT"

        # Copy orbit
        if products.orbit and products.orbit.exists():
            dest = orb_dir / products.orbit.name.upper()
            shutil.copy2(products.orbit, dest)
            self._decompress_file(dest)

        # Copy ERP
        if products.erp and products.erp.exists():
            dest = orb_dir / products.erp.name.upper()
            shutil.copy2(products.erp, dest)
            self._decompress_file(dest)

        # Copy IEP (if separate)
        if products.iep and products.iep.exists():
            dest = orb_dir / products.iep.name.upper()
            shutil.copy2(products.iep, dest)
            self._decompress_file(dest)

        # Copy clock
        if products.clock and products.clock.exists():
            dest = out_dir / products.clock.name.upper()
            shutil.copy2(products.clock, dest)
            self._decompress_file(dest)

        # Copy BIA
        if products.bia and products.bia.exists():
            dest = orb_dir / products.bia.name.upper()
            shutil.copy2(products.bia, dest)
            self._decompress_file(dest)

        # Copy ION
        if products.ion and products.ion.exists():
            dest = atm_dir / products.ion.name.upper()
            shutil.copy2(products.ion, dest)
            self._decompress_file(dest)

    def _decompress_file(self, path: Path) -> Path:
        """Decompress a file in place.

        Args:
            path: File path

        Returns:
            Decompressed file path
        """
        if path.suffix == ".gz":
            with gzip.open(path, "rb") as f_in:
                with open(path.with_suffix(""), "wb") as f_out:
                    shutil.copyfileobj(f_in, f_out)
            path.unlink()
            return path.with_suffix("")
        elif path.suffix == ".Z":
            subprocess.run(["uncompress", str(path)], check=True)
            return path.with_suffix("")
        return path

    def _copy_auxiliary_files(
        self,
        campaign_dir: Path,
        args: ProcessingArgs,
    ) -> None:
        """Copy auxiliary files to campaign.

        Args:
            campaign_dir: Campaign directory
            args: Processing arguments
        """
        # Copy info files from settings
        sta_dir = campaign_dir / "STA"
        gen_dir = campaign_dir / "GEN"

        # These would be configured in settings
        # For now, just log what would be copied
        logger.debug("Auxiliary files would be copied to STA/GEN directories")

    def _run_clockprep(
        self,
        files: list[str],
        campaign_dir: Path,
    ) -> None:
        """Run clockprep preprocessing.

        Args:
            files: List of RINEX files
            campaign_dir: Campaign directory
        """
        clockprep_bin = self.settings.tools.clockprep_path
        if not clockprep_bin or not clockprep_bin.exists():
            logger.warning("clockprep binary not found, skipping")
            return

        raw_dir = campaign_dir / "RAW"
        report_file = campaign_dir / "STA" / "report_clockprep.txt"

        for f in files:
            src = raw_dir / Path(f).name
            if not src.exists():
                continue

            out = raw_dir / f"clockprep{src.name}"

            try:
                subprocess.run(
                    [str(clockprep_bin), "-i", str(src), "-o", str(out)],
                    capture_output=True,
                    check=True,
                )

                if out.exists():
                    shutil.move(out, src)

            except subprocess.CalledProcessError as e:
                logger.warning(f"clockprep failed for {f}: {e}")

    def _run_cc2noncc(
        self,
        files: list[str],
        campaign_dir: Path,
        hist_file: Path,
    ) -> None:
        """Run cc2noncc preprocessing.

        Args:
            files: List of RINEX files
            campaign_dir: Campaign directory
            hist_file: P1C1 bias history file
        """
        cc2noncc_bin = self.settings.tools.cc2noncc_path
        if not cc2noncc_bin or not cc2noncc_bin.exists():
            logger.warning("cc2noncc binary not found, skipping")
            return

        raw_dir = campaign_dir / "RAW"

        for f in files:
            src = raw_dir / Path(f).name
            if not src.exists():
                continue

            out = raw_dir / f"noncc{src.name}"

            try:
                subprocess.run(
                    [str(cc2noncc_bin), str(src), str(out), str(hist_file)],
                    capture_output=True,
                    check=True,
                )

                if out.exists():
                    shutil.move(out, src)

            except subprocess.CalledProcessError as e:
                logger.warning(f"cc2noncc failed for {f}: {e}")

    def _run_bsw(
        self,
        campaign_dir: Path,
        session: str,
        date: GNSSDate,
        args: ProcessingArgs,
    ) -> bool:
        """Run BSW processing.

        Args:
            campaign_dir: Campaign directory
            session: Session name
            date: Processing date
            args: Processing arguments

        Returns:
            True if processing succeeded
        """
        # Configure BSW run
        bpe_config = {
            "BPE_CAMPAIGN": session,
            "YEAR": date.year,
            "SESSION": session[2:6] if args.proc_type != ProcessingType.DAILY else f"{session[2:5]}0",
            "SYSOUT": session,
            "STATUS": f"{session}.RUN",
        }

        # Add campaign to menu
        self.bsw_runner.add_campaign(session)

        try:
            # Run BPE
            result = self.bsw_runner.run_bpe(
                campaign_dir=campaign_dir,
                bpe_script=self.settings.bsw.bpe_script or "PPP_AR",
                session=session,
                year=date.year,
            )

            return result.success

        finally:
            # Remove campaign from menu
            self.bsw_runner.remove_campaign(session)

    def _check_run_success(self, campaign_dir: Path, session: str) -> bool:
        """Check if BSW run was successful.

        Args:
            campaign_dir: Campaign directory
            session: Session name

        Returns:
            True if run succeeded
        """
        run_file = campaign_dir / "BPE" / f"{session}.RUN"

        if not run_file.exists():
            logger.error(f"RUN file not found: {run_file}")
            return False

        # Check for success message
        content = run_file.read_text()
        if "Session" in content and ": finished" in content:
            return True

        logger.error("BSW run did not finish successfully")
        return False

    def _generate_iwv(
        self,
        campaign_dir: Path,
        date: GNSSDate,
    ) -> int:
        """Generate IWV from ZTD results.

        Args:
            campaign_dir: Campaign directory
            date: Processing date

        Returns:
            Number of IWV records generated
        """
        trp_files = list((campaign_dir / "OUT").glob("*.TRP"))
        if not trp_files:
            logger.warning("No TRP files found for IWV generation")
            return 0

        converter = ZTD2IWV()

        # Load meteorological data if available
        # TODO: Load actual met data

        for trp_file in trp_files:
            from pygnss_rt.atmosphere.ztd2iwv import read_ztd_file

            ztd_data = read_ztd_file(trp_file)

            for record in ztd_data:
                station = self.station_manager.get_station(record["station"])
                if not station or not station.latitude:
                    continue

                converter.process(
                    station_id=record["station"],
                    ztd=record["ztd"],
                    ztd_sigma=record.get("ztd_sigma", 0.001),
                    timestamp=date.datetime,
                    latitude=station.latitude,
                    longitude=station.longitude or 0.0,
                    height=station.height or 0.0,
                )

        # Write output
        output_dir = campaign_dir / "OUT"
        output_file = output_dir / f"IWV_{date.year:04d}{date.doy:03d}.cost716"
        converter.write_cost716_file(output_file)

        return len(converter.results)

    def _archive_campaign(
        self,
        campaign_dir: Path,
        session: str,
        date: GNSSDate,
        args: ProcessingArgs,
    ) -> Path | None:
        """Archive campaign (Delete, Compress, Move).

        Args:
            campaign_dir: Campaign directory
            session: Session name
            date: Processing date
            args: Processing arguments

        Returns:
            Archive destination path
        """
        if not args.archive_enabled:
            return None

        # Delete unnecessary directories
        for dir_name in args.dirs_to_delete:
            dir_path = campaign_dir / dir_name
            if dir_path.exists():
                shutil.rmtree(dir_path)
                logger.debug(f"Removed directory: {dir_path}")

        # Compress remaining files
        if args.archive_compression == "gzip":
            self._compress_directory(campaign_dir)

        # Move to archive location
        if args.archive_dir:
            archive_dest = args.archive_dir / str(date.year) / f"{date.doy:03d}" / session
            archive_dest.parent.mkdir(parents=True, exist_ok=True)

            if archive_dest.exists():
                shutil.rmtree(archive_dest)

            shutil.move(campaign_dir, archive_dest)
            logger.info(f"Campaign archived to: {archive_dest}")
            return archive_dest

        return None

    def _compress_directory(self, directory: Path) -> None:
        """Compress all files in a directory with gzip.

        Args:
            directory: Directory to compress
        """
        for file_path in directory.rglob("*"):
            if file_path.is_file() and file_path.suffix not in (".gz", ".Z"):
                with open(file_path, "rb") as f_in:
                    with gzip.open(f"{file_path}.gz", "wb") as f_out:
                        shutil.copyfileobj(f_in, f_out)
                file_path.unlink()

    def process(self, args: ProcessingArgs) -> list[ProcessingResult]:
        """Run processing for specified parameters.

        Args:
            args: Processing arguments

        Returns:
            List of processing results
        """
        # Set current time
        self.set_now_time()

        # Determine date range
        if args.cron_mode:
            current = GNSSDate.now()
            # Apply latency
            if args.latency_days > 0:
                start = current.add_days(-args.latency_days)
            else:
                start = current.add_hours(-args.latency_hours)
            end = start
        elif args.start_date:
            start = args.start_date
            end = args.end_date or start
        else:
            raise ConfigurationError("No date specified and not in CRON mode")

        # Get stations
        if args.stations:
            stations = args.stations
        else:
            stations = self.station_manager.get_station_ids(
                network=args.network,
                exclude=args.exclude_stations,
            )

        if not stations:
            logger.warning("No stations to process")
            return []

        logger.info(
            f"Starting processing: {start} to {end}, "
            f"type={args.proc_type.value}, stations={len(stations)}"
        )

        # Process each epoch
        results: list[ProcessingResult] = []
        current = start

        while current.mjd <= end.mjd:
            if args.proc_type == ProcessingType.HOURLY:
                for hour in range(24):
                    epoch = GNSSDate(
                        current.year, current.month, current.day, hour
                    )
                    result = self.process_single(epoch, args, stations)
                    results.append(result)
            elif args.proc_type == ProcessingType.SUBHOURLY:
                for hour in range(24):
                    for minute in [0, 15, 30, 45]:
                        epoch = GNSSDate(
                            current.year, current.month, current.day, hour, minute
                        )
                        result = self.process_single(epoch, args, stations)
                        results.append(result)
            else:  # daily
                result = self.process_single(current, args, stations)
                results.append(result)

            current = current.add_days(1)

        # Summary
        success_count = sum(1 for r in results if r.success)
        logger.info(
            f"Processing complete: {success_count}/{len(results)} epochs succeeded"
        )

        return results

    def process_single(
        self,
        date: GNSSDate,
        args: ProcessingArgs,
        stations: list[str],
    ) -> ProcessingResult:
        """Process a single epoch.

        Args:
            date: Processing date/time
            args: Processing arguments
            stations: Stations to process

        Returns:
            Processing result
        """
        import time

        start_time = time.time()
        self._current_date = date

        logger.info(
            f"Processing epoch: MJD={date.mjd:.4f}, "
            f"{date.year}/{date.doy:03d} {date.hour or 0:02d}:{date.minute or 0:02d}"
        )

        try:
            # Build session name
            session = self._build_session_name(date, args)
            logger.info(f"Session: {session}")

            # Ensure products are available
            products = self._ensure_products(date, args)

            # Create temp directory for downloads
            temp_dir = self.settings.data.temp_dir / str(os.getpid())
            temp_dir.mkdir(parents=True, exist_ok=True)

            try:
                # Get list of expected files
                requested_files = self._get_list_of_files(stations, date, args)
                logger.info(f"Requested files: {len(requested_files)}")

                # Check availability
                available, missing = self._check_available_files(
                    requested_files, stations, date, args
                )

                # Download missing files
                if missing:
                    downloaded = self._download_station_data(
                        missing, date, args, temp_dir
                    )
                    available.extend(downloaded)

                if not available:
                    logger.warning("No data available for processing")
                    return ProcessingResult(
                        mjd=date.mjd,
                        success=False,
                        session=session,
                        error="No data available",
                        runtime_seconds=time.time() - start_time,
                    )

                # Filter by coordinates if requested
                if args.remove_if_no_coord and self.settings.bsw.coord_file:
                    available, no_coord = self._filter_by_coordinates(
                        available,
                        self.settings.bsw.coord_file,
                    )

                    if not available:
                        return ProcessingResult(
                            mjd=date.mjd,
                            success=False,
                            session=session,
                            error="No files with a priori coordinates",
                            runtime_seconds=time.time() - start_time,
                        )

                # Set up campaign
                campaign_dir = self._setup_campaign(
                    session, date, args, available, products
                )

                # Run preprocessing if requested
                if args.use_clockprep:
                    self._run_clockprep(available, campaign_dir)

                if args.use_cc2noncc and self.settings.tools.p1c1_hist_file:
                    self._run_cc2noncc(
                        available, campaign_dir, self.settings.tools.p1c1_hist_file
                    )

                # Run BSW processing
                bsw_success = self._run_bsw(campaign_dir, session, date, args)

                if not bsw_success:
                    return ProcessingResult(
                        mjd=date.mjd,
                        success=False,
                        session=session,
                        stations_processed=0,
                        error="BSW processing failed",
                        runtime_seconds=time.time() - start_time,
                        campaign_dir=campaign_dir,
                    )

                # Generate IWV if requested
                iwv_count = 0
                if args.generate_iwv:
                    iwv_count = self._generate_iwv(campaign_dir, date)

                # Archive campaign
                archived_to = None
                if args.archive_enabled:
                    archived_to = self._archive_campaign(
                        campaign_dir, session, date, args
                    )

                return ProcessingResult(
                    mjd=date.mjd,
                    success=True,
                    session=session,
                    stations_processed=len(available),
                    stations_available=len(available),
                    stations_missing=len(missing),
                    iwv_count=iwv_count,
                    runtime_seconds=time.time() - start_time,
                    campaign_dir=campaign_dir if not archived_to else None,
                    archived_to=archived_to,
                )

            finally:
                # Clean up temp directory
                if temp_dir.exists():
                    shutil.rmtree(temp_dir, ignore_errors=True)

        except Exception as e:
            logger.exception(f"Processing failed: {e}")
            return ProcessingResult(
                mjd=date.mjd,
                success=False,
                error=str(e),
                runtime_seconds=time.time() - start_time,
            )

    def close(self) -> None:
        """Close all connections and cleanup."""
        if self._downloader:
            self._downloader.close()
        if self._db:
            self._db.close()

    def __enter__(self) -> "IGNSS":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()


# Convenience function for quick processing
def run_processing(
    config_path: Path | str,
    start_date: GNSSDate,
    end_date: GNSSDate | None = None,
    proc_type: str = "hourly",
    stations: list[str] | None = None,
    **kwargs,
) -> list[ProcessingResult]:
    """Run GNSS processing with simple interface.

    Args:
        config_path: Path to configuration file
        start_date: Start date
        end_date: End date (defaults to start_date)
        proc_type: Processing type (daily, hourly, subhourly)
        stations: List of station IDs
        **kwargs: Additional processing arguments

    Returns:
        List of processing results
    """
    args = ProcessingArgs(
        proc_type=ProcessingType(proc_type),
        start_date=start_date,
        end_date=end_date,
        stations=stations or [],
        **kwargs,
    )

    with IGNSS(config_path) as ignss:
        return ignss.process(args)
