"""
NRDDP TRO (Near Real-Time Double Difference Processing for Troposphere).

Hourly processing for tropospheric parameter estimation combining
stations from all available networks. Produces ZTD/IWV products.

Replaces the Perl caller scripts:
- iGNSS_NRDDP_TRO_54_nrt_direct.pl
- iGNSS_NRDDP_TRO_BSW54_nrt.pl
- iGNSS_NRDDP_TRO_BSW54_direct.pl

Key differences from Daily PPP:
- Hourly processing (1/24 day increment vs 1 day)
- Dynamic NRT coordinates (updated daily: DNR{YY}{DOY}0.CRD)
- All-network station merging (IGS+EUREF+OS+RGP+10 networks)
- NEQ stacking (4-hour accumulation)
- ZTD to IWV conversion output

Usage:
    from pygnss_rt.processing.nrddp_tro import NRDDPTROProcessor, NRDDPTROArgs

    processor = NRDDPTROProcessor()
    results = processor.process(NRDDPTROArgs(
        start_date=GNSSDate(2024, 9, 16),
        cron_mode=True,
        latency_hours=3,
    ))
"""

from __future__ import annotations

import os
import shutil
import subprocess
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pygnss_rt.core.paths import PathConfig, get_paths
from pygnss_rt.processing.bsw_options import (
    BSWOptionsParser,
    BSWOptionsConfig,
    get_option_dirs,
)
from pygnss_rt.processing.neq_stacking import (
    NEQStacker,
    NEQStackingConfig,
    NRDDP_TRO_STACKING,
)
from pygnss_rt.processing.nrt_coordinates import (
    NRTCoordinateManager,
    NRTCoordinateConfig,
    NRDDP_TRO_COORDINATES,
)
from pygnss_rt.processing.station_merger import (
    StationMerger,
    NetworkSource,
    NRDDP_STATION_SOURCES,
)
from pygnss_rt.utils.dates import GNSSDate
from pygnss_rt.utils.logging import get_logger

logger = get_logger(__name__)


@dataclass
class NRDDPTROResult:
    """Result of an NRDDP TRO processing run."""

    session_name: str
    date: GNSSDate
    hour: int
    hour_char: str  # a-x for hours 0-23
    success: bool
    stations_processed: int = 0
    stations_total: int = 0
    start_time: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    end_time: datetime | None = None
    error_message: str = ""
    output_files: list[str] = field(default_factory=list)
    neq_files_stacked: int = 0
    ztd_records: int = 0
    iwv_records: int = 0

    @property
    def duration_seconds(self) -> float:
        """Get processing duration in seconds."""
        if self.end_time:
            return (self.end_time - self.start_time).total_seconds()
        return 0.0

    @property
    def mjdh(self) -> float:
        """Get MJD with hour fraction."""
        return self.date.mjd + (self.hour / 24.0)


@dataclass
class NRDDPTROArgs:
    """Arguments for NRDDP TRO processing."""

    # Date range
    start_date: GNSSDate | None = None
    end_date: GNSSDate | None = None

    # Hour range (0-23)
    start_hour: int = 0
    end_hour: int = 23

    # CRON mode settings
    cron_mode: bool = False
    latency_hours: int = 3  # Hours behind real-time

    # Station configuration
    nrt_only: bool = True  # Only NRT-capable stations
    network_sources: list[NetworkSource] = field(default_factory=lambda: NRDDP_STATION_SOURCES)
    exclude_stations: list[str] = field(default_factory=list)

    # Coordinate configuration
    coord_config: NRTCoordinateConfig = field(default_factory=lambda: NRDDP_TRO_COORDINATES)

    # NEQ stacking configuration
    neq_stacking: NEQStackingConfig = field(default_factory=lambda: NRDDP_TRO_STACKING)

    # Processing options
    generate_iwv: bool = True  # Convert ZTD to IWV
    use_clockprep: bool = True

    # Skip options
    skip_products: bool = False
    skip_data: bool = False
    skip_dcm: bool = False
    skip_iwv: bool = False

    # Output control
    dry_run: bool = False
    verbose: bool = False


def _get_default_paths() -> PathConfig:
    """Get default PathConfig for NRDDP TRO."""
    return get_paths()


def _get_default_data_root() -> Path:
    """Get default data root from PathConfig."""
    paths = _get_default_paths()
    if paths.data_root:
        return paths.data_root
    return Path.home() / "data54"


def _get_default_gpsuser_dir() -> Path:
    """Get default GPSUSER directory from PathConfig."""
    paths = _get_default_paths()
    if paths.gpsuser_dir:
        return paths.gpsuser_dir
    return Path.home() / "GPSUSER54_LANT"


def _get_default_tro_campaign_root() -> Path:
    """Get default TRO campaign root from PathConfig."""
    paths = _get_default_paths()
    if paths.tro_campaign_root:
        return paths.tro_campaign_root
    if paths.data_root:
        return paths.data_root / "campaigns" / "tro"
    return Path.home() / "data54" / "campaigns" / "tro"


@dataclass
class NRDDPTROConfig:
    """Configuration for NRDDP TRO processing."""

    # Directory paths (using PathConfig for defaults)
    pygnss_rt_dir: Path = field(default_factory=lambda: _get_default_paths().pygnss_rt_dir)
    station_data_dir: Path = field(default_factory=lambda: _get_default_paths().station_data_dir)
    data_root: Path = field(default_factory=_get_default_data_root)
    gpsuser_dir: Path = field(default_factory=_get_default_gpsuser_dir)
    campaign_root: Path = field(default_factory=_get_default_tro_campaign_root)

    # Processing configuration
    pcf_file: str = "SMHI_TGX_OCT2025_MGX.PCF"  # TGX-based DD processing chain
    bsw_options_yaml: str = "bsw_configs/iGNSS_NRDDP_TRO_BSW54_direct.yaml"

    # Session naming
    session_suffix: str = "NR"  # e.g., 24260ANR

    # Datum and reference frame
    datum: str = "IGS20"
    antenna_phase_center: str = "ABSOLUTE"
    min_elevation: int = 5

    # Information files
    info_files: dict[str, str] = field(default_factory=lambda: {
        "sessions": "SESSIONS.SES",
        "station": "STATION.STA",
        "ocean_loading": "IGS20_FES2014b_CM.OTL",
        "abbreviations": "ABBREV.ABB",
        "obs_selection": "OBS_SELECTION.SEL",
        "sinex_skeleton": "IGS20_GNSS.SKL",
        "phase_center": "ANTENNA_I20.I20",
    })

    # DCM settings
    dcm_enabled: bool = True
    dcm_dirs_to_delete: list[str] = field(default_factory=lambda: ["RAW", "OBS", "ORB", "ORX"])
    dcm_archive_dir: str = field(default_factory=lambda: str(_get_default_tro_campaign_root()))
    dcm_organization: str = "yyyy/doy"

    # Data sources
    data_organization: dict[str, dict[str, str]] = field(default_factory=lambda: {
        "oedc": {"name": "oedc", "org": "gpsweek"},
        "dcb": {"name": "oedc/dcb", "org": "yyyy"},
        "hd": {"name": "hourlyData", "org": "yyyy/doy"},
    })


class NRDDPTROProcessor:
    """NRDDP TRO processor for hourly tropospheric estimation.

    Manages the complete NRDDP TRO processing workflow:
    1. Merge stations from all networks (IGS, EUREF, OS, RGP, etc.)
    2. Get dynamic NRT coordinates for the day
    3. Stack NEQ files from previous hours
    4. Run hourly Bernese processing
    5. Convert ZTD to IWV
    6. Archive results

    Usage:
        processor = NRDDPTROProcessor()

        # Process in cron mode (3-hour latency)
        results = processor.process(NRDDPTROArgs(
            cron_mode=True,
            latency_hours=3,
        ))

        # Process specific date/hour range
        results = processor.process(NRDDPTROArgs(
            start_date=GNSSDate(2024, 9, 16),
            start_hour=0,
            end_hour=23,
        ))
    """

    def __init__(
        self,
        config: NRDDPTROConfig | None = None,
        config_path: Path | str | None = None,
    ):
        """Initialize NRDDP TRO processor.

        Args:
            config: Full configuration object
            config_path: Path to configuration file
        """
        self.config = config or NRDDPTROConfig()
        self.config_path = Path(config_path) if config_path else None

        self._station_merger: StationMerger | None = None
        self._coord_manager: NRTCoordinateManager | None = None
        self._neq_stacker: NEQStacker | None = None

        if self.config_path:
            self._load_config()

    def _load_config(self) -> None:
        """Load configuration from file."""
        if self.config_path and self.config_path.exists():
            from pygnss_rt.core.config import load_config
            config_dict = load_config(self.config_path)
            # Apply config overrides as needed

    def get_station_merger(self, args: NRDDPTROArgs) -> StationMerger:
        """Get or create station merger.

        Args:
            args: Processing arguments

        Returns:
            Configured StationMerger
        """
        if self._station_merger is None:
            self._station_merger = StationMerger(
                station_data_dir=self.config.station_data_dir,
                verbose=args.verbose,
            )
            # Add sources specified in args
            for source in args.network_sources:
                try:
                    self._station_merger.add_source(source)
                except FileNotFoundError:
                    if args.verbose:
                        print(f"  Warning: Source XML not found for {source.value}")

        return self._station_merger

    def get_coord_manager(self, args: NRDDPTROArgs) -> NRTCoordinateManager:
        """Get or create NRT coordinate manager.

        Args:
            args: Processing arguments

        Returns:
            Configured NRTCoordinateManager
        """
        if self._coord_manager is None:
            self._coord_manager = NRTCoordinateManager(
                config=args.coord_config,
                verbose=args.verbose,
            )
        return self._coord_manager

    def get_neq_stacker(self, args: NRDDPTROArgs) -> NEQStacker:
        """Get or create NEQ stacker.

        Args:
            args: Processing arguments

        Returns:
            Configured NEQStacker
        """
        if self._neq_stacker is None:
            self._neq_stacker = NEQStacker(
                config=args.neq_stacking,
                verbose=args.verbose,
            )
        return self._neq_stacker

    def process(self, args: NRDDPTROArgs) -> list[NRDDPTROResult]:
        """Run NRDDP TRO processing.

        Args:
            args: Processing arguments

        Returns:
            List of results (one per hour processed)
        """
        results = []

        # Determine date/hour range
        if args.cron_mode:
            # Calculate processing time from current time minus latency
            now = datetime.utcnow()
            proc_dt = datetime(now.year, now.month, now.day, now.hour)
            # Subtract latency hours (simplified - should handle day rollover)
            from datetime import timedelta
            proc_dt = proc_dt - timedelta(hours=args.latency_hours)

            proc_date = GNSSDate(proc_dt.year, proc_dt.month, proc_dt.day)
            start_date = proc_date
            end_date = proc_date
            start_hour = proc_dt.hour
            end_hour = proc_dt.hour

            print(f"CRON mode: Processing {proc_date} hour {start_hour} (latency: {args.latency_hours} hours)")
        elif args.start_date and args.end_date:
            start_date = args.start_date
            end_date = args.end_date
            start_hour = args.start_hour
            end_hour = args.end_hour
        else:
            raise ValueError("Must specify either cron_mode or start_date/end_date")

        # Get merged station list (done once per run)
        merger = self.get_station_merger(args)
        stations = merger.get_station_ids(nrt_only=args.nrt_only)

        # Apply exclusions
        exclude_set = {s.lower() for s in args.exclude_stations}
        stations = [s for s in stations if s.lower() not in exclude_set]

        print(f"\nNRDDP TRO Processing")
        print(f"{'=' * 60}")
        print(f"Stations: {len(stations)} (merged from {len(args.network_sources)} networks)")
        if args.verbose:
            stats = merger.get_statistics()
            for source, counts in stats.get("by_source", {}).items():
                print(f"  {source}: {counts['nrt']} NRT stations")

        # Process each date/hour
        current_date = start_date
        while current_date.mjd <= end_date.mjd:
            # Determine hour range for this date
            if current_date.mjd == start_date.mjd:
                h_start = start_hour
            else:
                h_start = 0

            if current_date.mjd == end_date.mjd:
                h_end = end_hour
            else:
                h_end = 23

            # Process each hour
            for hour in range(h_start, h_end + 1):
                result = self._process_single_hour(
                    current_date, hour, stations, args
                )
                results.append(result)

            current_date = current_date.add_days(1)

        # Summary
        success_count = sum(1 for r in results if r.success)
        print(f"\n{'=' * 60}")
        print(f"NRDDP TRO Summary: {success_count}/{len(results)} hours successful")

        return results

    def _process_single_hour(
        self,
        date: GNSSDate,
        hour: int,
        stations: list[str],
        args: NRDDPTROArgs,
    ) -> NRDDPTROResult:
        """Process a single hour.

        Args:
            date: Processing date
            hour: Hour (0-23)
            stations: List of station IDs
            args: Processing arguments

        Returns:
            Processing result
        """
        # Hour character: a=0, b=1, ..., x=23
        hour_char = chr(ord('a') + hour)

        # Session name: YYDOYHSUFFIX (e.g., 24260ANR)
        session_name = (
            f"{date.year % 100:02d}{date.doy:03d}"
            f"{hour_char.upper()}{self.config.session_suffix}"
        )

        result = NRDDPTROResult(
            session_name=session_name,
            date=date,
            hour=hour,
            hour_char=hour_char,
            success=False,
            stations_total=len(stations),
        )

        print(f"\n{'-' * 40}")
        print(f"Processing: {session_name} ({date.year}/{date.doy:03d} {hour:02d}:00 UTC)")
        print(f"{'-' * 40}")

        if args.dry_run:
            print("[DRY RUN MODE]")

        try:
            # Step 1: Get NRT coordinates for this day
            coord_manager = self.get_coord_manager(args)
            try:
                coord_file = coord_manager.get_coordinate_file(date.year, date.doy)
                print(f"  Coordinates: {coord_file.name}")
            except FileNotFoundError as e:
                result.error_message = f"No coordinate file: {e}"
                print(f"  ERROR: {result.error_message}")
                return result

            # Step 2: Get NEQ files to stack
            neq_stacker = self.get_neq_stacker(args)
            mjdh = date.mjd + (hour / 24.0)
            neq_files = neq_stacker.get_neq_files_to_stack(
                current_mjdh=mjdh,
                archive_dir=self.config.campaign_root,
            )
            available_neq = [f for f in neq_files if f.exists]
            result.neq_files_stacked = len(available_neq)
            print(f"  NEQ stacking: {len(available_neq)}/{len(neq_files)} files available")

            # Step 3: Check products
            if not args.skip_products:
                print("  Checking products...")
                if not args.dry_run:
                    products_ok = self._check_products(date, hour, args)
                    if not products_ok:
                        result.error_message = "Missing required products"
                        return result

            # Step 4: Download hourly station data
            if not args.skip_data:
                print(f"  Downloading data for {len(stations)} stations...")
                if not args.dry_run:
                    downloaded = self._download_hourly_data(date, hour, stations, args)
                    if not downloaded:
                        result.error_message = "No station data downloaded"
                        return result

            # Step 5: Setup campaign
            campaign_dir = self._setup_campaign(date, session_name, args)
            if args.verbose:
                print(f"  Campaign: {campaign_dir}")

            # Step 6: Copy NEQ files to campaign
            if available_neq and not args.dry_run:
                campaign_sol = campaign_dir / "SOL"
                copied = neq_stacker.copy_neq_files_to_campaign(
                    neq_files=available_neq,
                    campaign_sol_dir=campaign_sol,
                )
                print(f"  Copied {len(copied)} NEQ files to campaign")

            # Step 7: Run BSW processing
            print("  Running Bernese processing...")
            if not args.dry_run:
                bsw_success = self._run_bsw_processing(
                    date, hour, session_name, campaign_dir, stations, coord_file, args
                )
                if not bsw_success:
                    result.error_message = "BSW processing failed"
                    return result

            # Step 8: Convert ZTD to IWV
            if args.generate_iwv and not args.skip_iwv:
                print("  Converting ZTD to IWV...")
                if not args.dry_run:
                    iwv_count = self._convert_ztd_to_iwv(campaign_dir, date, hour, args)
                    result.iwv_records = iwv_count
                    print(f"  Generated {iwv_count} IWV records")

            # Step 9: DCM archiving
            if not args.skip_dcm and self.config.dcm_enabled:
                print("  Archiving (DCM)...")
                if not args.dry_run:
                    self._run_dcm(date, session_name, campaign_dir)

            result.success = True
            result.stations_processed = len(stations)
            result.end_time = datetime.now(timezone.utc)

            print(f"  Complete: {result.duration_seconds:.1f}s")

        except Exception as e:
            result.error_message = str(e)
            result.end_time = datetime.now(timezone.utc)
            print(f"  ERROR: {e}")

        return result

    def _check_products(
        self,
        date: GNSSDate,
        hour: int,
        args: NRDDPTROArgs,
    ) -> bool:
        """Check if required products are available.

        Port of Perl PROD.pm check_orbit and check_ERP functionality.
        For NRT processing, uses IGS ultra-rapid products (updated 4x daily).

        Args:
            date: Processing date
            hour: Hour
            args: Processing arguments

        Returns:
            True if products available
        """
        from pygnss_rt.data_access.ftp_client import FTPClient

        gps_week = date.gps_week
        dow = date.dow

        # Determine which ultra-rapid product to use based on hour
        # Ultra-rapid products are released at 03, 09, 15, 21 UTC
        # and are named wwwwd_hh for prediction start hour
        if hour < 3:
            # Use previous day's 18:00 product
            ur_hour = 18
            if dow == 0:
                ur_gps_week = gps_week - 1
                ur_dow = 6
            else:
                ur_gps_week = gps_week
                ur_dow = dow - 1
        elif hour < 9:
            ur_hour = 0
            ur_gps_week = gps_week
            ur_dow = dow
        elif hour < 15:
            ur_hour = 6
            ur_gps_week = gps_week
            ur_dow = dow
        elif hour < 21:
            ur_hour = 12
            ur_gps_week = gps_week
            ur_dow = dow
        else:
            ur_hour = 18
            ur_gps_week = gps_week
            ur_dow = dow

        # Product filenames (IGS ultra-rapid)
        orbit_file = f"igu{ur_gps_week}{ur_dow}_{ur_hour:02d}.sp3.Z"
        erp_file = f"igu{ur_gps_week}{ur_dow}_{ur_hour:02d}.erp.Z"

        # Product directory
        prod_dir = self.config.data_root / "oedc" / str(ur_gps_week)
        prod_dir.mkdir(parents=True, exist_ok=True)

        orbit_path = prod_dir / orbit_file
        erp_path = prod_dir / erp_file

        products_ok = True

        # Check orbit
        if not orbit_path.exists():
            logger.info(f"Downloading orbit: {orbit_file}")
            downloaded = self._download_product(
                "IGS_ULTRA",
                f"/IGS/products/{ur_gps_week}/{orbit_file}",
                orbit_path,
            )
            if not downloaded:
                logger.warning(f"Could not download orbit: {orbit_file}")
                products_ok = False
        else:
            logger.debug(f"Orbit available: {orbit_file}")

        # Check ERP
        if not erp_path.exists():
            logger.info(f"Downloading ERP: {erp_file}")
            downloaded = self._download_product(
                "IGS_ULTRA",
                f"/IGS/products/{ur_gps_week}/{erp_file}",
                erp_path,
            )
            if not downloaded:
                logger.warning(f"Could not download ERP: {erp_file}")
                # ERP is less critical - don't fail processing
        else:
            logger.debug(f"ERP available: {erp_file}")

        # For NRT processing, also check for clock products if needed
        # (optional - some processing chains don't need them)

        return products_ok

    def _download_product(
        self,
        source_name: str,
        remote_path: str,
        local_path: Path,
    ) -> bool:
        """Download a GNSS product file.

        Args:
            source_name: Source identifier
            remote_path: Remote file path
            local_path: Local destination path

        Returns:
            True if download successful
        """
        from pygnss_rt.data_access.ftp_client import FTPClient

        # Product sources (from PROD.pm)
        product_sources = {
            "IGS_ULTRA": {
                "host": "igs-ftp.bkg.bund.de",
                "user": "anonymous",
                "passwd": "anonymous@",
            },
            "IGS_RAPID": {
                "host": "igs-ftp.bkg.bund.de",
                "user": "anonymous",
                "passwd": "anonymous@",
            },
            "CODE": {
                "host": "ftp.aiub.unibe.ch",
                "user": "anonymous",
                "passwd": "anonymous@",
            },
            "CDDIS": {
                "host": "gdc.cddis.eosdis.nasa.gov",
                "user": "anonymous",
                "passwd": "anonymous@",
            },
        }

        source = product_sources.get(source_name, product_sources["IGS_ULTRA"])

        try:
            client = FTPClient(
                host=source["host"],
                username=source["user"],
                password=source["passwd"],
            )

            success = client.download_file(remote_path, local_path)
            client.close()

            if success:
                # Verify file is not empty
                if local_path.exists() and local_path.stat().st_size > 0:
                    return True
                else:
                    local_path.unlink(missing_ok=True)
                    return False

            return False

        except Exception as e:
            logger.warning(f"Product download failed: {source_name} {remote_path}: {e}")
            return False

    def _check_product_availability_db(
        self,
        date: GNSSDate,
        product_type: str,
    ) -> bool:
        """Check product availability in database.

        Port of Perl PROD.pm database tracking functionality.

        Args:
            date: Processing date
            product_type: Product type (eph, erp, clk)

        Returns:
            True if product is recorded as available
        """
        # This would connect to the database to check status
        # For now, we rely on file system checks
        return False

    def _download_hourly_data(
        self,
        date: GNSSDate,
        hour: int,
        stations: list[str],
        args: NRDDPTROArgs,
    ) -> bool:
        """Download hourly RINEX data from multiple FTP sources.

        Port of Perl FTP.pm package HD (Hourly Data) functionality.
        Downloads from OSGB, CDDIS, BKGE, and other configured sources.

        Args:
            date: Processing date
            hour: Hour
            stations: Station list
            args: Processing arguments

        Returns:
            True if sufficient data downloaded
        """
        from pygnss_rt.data_access.ftp_client import FTPClient
        from pygnss_rt.data_access.ftp_config import load_ftp_config

        # Load FTP configuration
        ftp_config_path = self.config.pygnss_rt_dir / "config" / "ftp_config.xml"
        if not ftp_config_path.exists():
            ftp_config_path = self.config.pygnss_rt_dir / "callers" / "ftp_config.xml"

        ftp_configs = []
        if ftp_config_path.exists():
            try:
                ftp_configs = load_ftp_config(ftp_config_path)
            except Exception as e:
                logger.warning(f"Failed to load FTP config: {e}")

        # Hourly data directory organization
        y4c = str(date.year)
        doy = f"{date.doy:03d}"
        hour_char = chr(ord('a') + hour)

        # Destination directory
        dest_dir = (
            self.config.data_root
            / self.config.data_organization["hd"]["name"]
            / y4c
            / doy
        )
        dest_dir.mkdir(parents=True, exist_ok=True)

        # Generate expected filenames for stations
        # Hourly format: ssssdddhh.yyd.Z (e.g., abcd001a.24d.Z)
        expected_files = {}
        for sta in stations:
            filename = f"{sta.lower()}{doy}{hour_char}.{date.year % 100:02d}d.Z"
            expected_files[sta.lower()] = filename

        downloaded_count = 0
        already_have = 0

        # Check what we already have
        for sta, filename in expected_files.items():
            local_path = dest_dir / filename
            if local_path.exists():
                already_have += 1

        logger.info(
            "Hourly data status",
            date=f"{y4c}/{doy}",
            hour=hour,
            stations=len(stations),
            already_have=already_have,
        )

        # Skip download if we have enough data (>80%)
        if already_have >= len(stations) * 0.8:
            logger.info("Sufficient data already available, skipping download")
            return True

        # Define hourly data sources (from FTP.pm package HD)
        hourly_sources = [
            {
                "name": "OSGB",
                "host": "ftp.osgb.org.uk",
                "path": f"/gps/hourly/{y4c}/{doy}",
                "user": "anonymous",
                "passwd": "anonymous@",
            },
            {
                "name": "CDDIS",
                "host": "gdc.cddis.eosdis.nasa.gov",
                "path": f"/gps/data/hourly/{y4c}/{doy}/{hour:02d}",
                "user": "anonymous",
                "passwd": "anonymous@",
            },
            {
                "name": "BKGE",
                "host": "igs-ftp.bkg.bund.de",
                "path": f"/IGS/obs/{y4c}/{doy}",
                "user": "anonymous",
                "passwd": "anonymous@",
            },
            {
                "name": "TUDELFT",
                "host": "gnss1.tudelft.nl",
                "path": f"/rinex/{y4c}/{doy}",
                "user": "anonymous",
                "passwd": "anonymous@",
            },
        ]

        # Override with FTP config if available
        for cfg in ftp_configs:
            if cfg.id in ["OSGB_HD", "CDDIS_HD", "BKGE_HD"]:
                for source in hourly_sources:
                    if source["name"] == cfg.id.replace("_HD", ""):
                        source["host"] = cfg.url
                        if cfg.username:
                            source["user"] = cfg.username
                        if cfg.password:
                            source["passwd"] = cfg.password

        # Try each source
        for source in hourly_sources:
            if downloaded_count + already_have >= len(stations) * 0.9:
                break  # Have enough data

            try:
                client = FTPClient(
                    host=source["host"],
                    username=source["user"],
                    password=source["passwd"],
                )

                # Get list of available files
                available_files = client.list_files(source["path"])
                available_set = set(f.lower() for f in available_files)

                # Download missing files
                for sta, filename in expected_files.items():
                    local_path = dest_dir / filename
                    if local_path.exists():
                        continue

                    if filename.lower() in available_set:
                        remote_path = f"{source['path']}/{filename}"
                        try:
                            success = client.download_file(remote_path, local_path)
                            if success:
                                downloaded_count += 1
                                logger.debug(
                                    f"Downloaded {filename} from {source['name']}"
                                )
                        except Exception as e:
                            logger.debug(
                                f"Failed to download {filename} from {source['name']}: {e}"
                            )

                client.close()

            except Exception as e:
                logger.warning(f"Failed to connect to {source['name']}: {e}")
                continue

        total_available = downloaded_count + already_have
        logger.info(
            "Hourly download complete",
            downloaded=downloaded_count,
            already_have=already_have,
            total=total_available,
            required=len(stations),
        )

        # Return True if we have at least 50% of stations
        return total_available >= len(stations) * 0.5

    def _setup_campaign(
        self,
        date: GNSSDate,
        session_name: str,
        args: NRDDPTROArgs,
    ) -> Path:
        """Setup BSW campaign directory.

        Args:
            date: Processing date
            session_name: Session name
            args: Processing arguments

        Returns:
            Path to campaign directory
        """
        # Build campaign path with yyyy/doy organization
        campaign_dir = (
            self.config.campaign_root
            / str(date.year)
            / f"{date.doy:03d}"
            / session_name
        )

        if not args.dry_run:
            for subdir in ["ATM", "BPE", "GRD", "OBS", "ORB", "ORX", "OUT", "RAW", "SOL", "STA"]:
                (campaign_dir / subdir).mkdir(parents=True, exist_ok=True)

        return campaign_dir

    def _run_bsw_processing(
        self,
        date: GNSSDate,
        hour: int,
        session_name: str,
        campaign_dir: Path,
        stations: list[str],
        coord_file: Path,
        args: NRDDPTROArgs,
    ) -> bool:
        """Run Bernese GNSS Software processing.

        Executes the Bernese Processing Engine (BPE) for hourly processing.
        Port of the BSW execution from Perl IGNSS.pm.

        Args:
            date: Processing date
            hour: Hour
            session_name: Session name
            campaign_dir: Campaign directory
            stations: Station list
            coord_file: NRT coordinate file path
            args: Processing arguments

        Returns:
            True if processing succeeded
        """
        # Build BSW arguments
        bsw_args = self._build_bsw_args(
            date, hour, session_name, coord_file, stations, args
        )

        if args.verbose:
            print(f"    PCF: {self.config.pcf_file}")
            print(f"    Options: {self.config.bsw_options_yaml}")
            print(f"    Coordinate: {coord_file}")

        # Get BSW environment variables
        bpe_dir = os.environ.get("BPE", "")
        u_dir = os.environ.get("U", "")
        p_dir = os.environ.get("P", "")

        if not bpe_dir:
            logger.error("BPE environment variable not set")
            return False

        # Prepare session file naming
        y2c = f"{date.year % 100:02d}"
        doy = f"{date.doy:03d}"
        hour_char = chr(ord('A') + hour)

        # Copy PCF file to campaign
        pcf_source = self.config.gpsuser_dir / "PCF" / self.config.pcf_file
        pcf_dest = campaign_dir / "BPE" / self.config.pcf_file

        if pcf_source.exists():
            shutil.copy2(pcf_source, pcf_dest)

        # Prepare station list file
        sta_list_file = campaign_dir / "STA" / "STATION.LST"
        self._write_station_list(sta_list_file, stations)

        # Copy coordinate file to campaign
        coord_dest = campaign_dir / "STA" / coord_file.name
        if coord_file.exists():
            shutil.copy2(coord_file, coord_dest)

        # Copy info files to campaign
        self._copy_info_files(campaign_dir)

        # Build BPE command
        # Format: startBPE -c CAMPAIGN -s SESSION -pcf PCF_FILE [options]
        bpe_cmd = [
            f"{bpe_dir}/startBPE",
            "-c", str(campaign_dir),
            "-s", f"{doy}{hour_char}",
            "-pcf", self.config.pcf_file,
        ]

        # Add processing options from XML configuration
        if bsw_args.get("optDirs"):
            for key, value in bsw_args["optDirs"].items():
                if value:
                    bpe_cmd.extend([f"-{key}", str(value)])

        logger.info(
            "Starting BPE",
            session=session_name,
            campaign=str(campaign_dir),
        )

        try:
            # Set up environment for BSW
            env = os.environ.copy()
            env["CAMPAIGN"] = str(campaign_dir)
            env["SESSION"] = f"{doy}{hour_char}"
            env["SESS_ID"] = self.config.session_suffix

            # Run BPE with timeout (45 minutes for hourly processing)
            result = subprocess.run(
                bpe_cmd,
                capture_output=True,
                text=True,
                timeout=2700,  # 45 minutes
                env=env,
                cwd=str(campaign_dir),
            )

            if result.returncode == 0:
                logger.info("BPE completed successfully", session=session_name)

                # Check for output files
                tro_file = self._find_tro_output(campaign_dir, date, hour)
                if tro_file:
                    logger.info("TRO output found", file=str(tro_file))
                    return True
                else:
                    logger.warning("BPE completed but no TRO output found")
                    return True  # Still consider success if BPE completed

            else:
                logger.error(
                    "BPE failed",
                    session=session_name,
                    returncode=result.returncode,
                    stderr=result.stderr[:500] if result.stderr else "No stderr",
                )

                # Check for error summary in OUT directory
                self._log_bpe_errors(campaign_dir, session_name)

                return False

        except subprocess.TimeoutExpired:
            logger.error("BPE timed out", session=session_name, timeout=2700)
            return False
        except FileNotFoundError:
            logger.error("BPE executable not found", path=f"{bpe_dir}/startBPE")
            return False
        except Exception as e:
            logger.exception("BPE execution failed", error=str(e))
            return False

    def _write_station_list(self, list_file: Path, stations: list[str]) -> None:
        """Write station list file for BSW.

        Args:
            list_file: Output file path
            stations: List of station IDs
        """
        list_file.parent.mkdir(parents=True, exist_ok=True)
        with open(list_file, "w") as f:
            for sta in sorted(stations):
                f.write(f"{sta.upper():4s}\n")

    def _copy_info_files(self, campaign_dir: Path) -> None:
        """Copy required info files to campaign directory.

        Args:
            campaign_dir: Campaign directory
        """
        sta_dir = campaign_dir / "STA"
        sta_dir.mkdir(parents=True, exist_ok=True)

        gpsuser_sta = self.config.gpsuser_dir / "STA"

        for key, filename in self.config.info_files.items():
            source = gpsuser_sta / filename
            if source.exists():
                shutil.copy2(source, sta_dir / filename)
            else:
                logger.debug(f"Info file not found: {source}")

    def _find_tro_output(
        self,
        campaign_dir: Path,
        date: GNSSDate,
        hour: int,
    ) -> Path | None:
        """Find TRO output file in campaign.

        Args:
            campaign_dir: Campaign directory
            date: Processing date
            hour: Hour

        Returns:
            Path to TRO file if found
        """
        atm_dir = campaign_dir / "ATM"
        if not atm_dir.exists():
            return None

        y2c = f"{date.year % 100:02d}"
        doy = f"{date.doy:03d}"
        hour_char = chr(ord('A') + hour)

        # Common TRO filename patterns
        patterns = [
            f"*{y2c}{doy}{hour_char}*.TRO",
            f"*{doy}{hour_char}*.TRO",
            "*.TRO",
        ]

        for pattern in patterns:
            matches = list(atm_dir.glob(pattern))
            if matches:
                # Return most recent
                return max(matches, key=lambda p: p.stat().st_mtime)

        return None

    def _log_bpe_errors(self, campaign_dir: Path, session_name: str) -> None:
        """Log BPE error information from output files.

        Args:
            campaign_dir: Campaign directory
            session_name: Session name for logging
        """
        out_dir = campaign_dir / "OUT"
        if not out_dir.exists():
            return

        # Check for error/warning files
        for error_file in out_dir.glob("*.ERR"):
            try:
                content = error_file.read_text(errors="ignore")[:1000]
                logger.error(
                    "BPE error file",
                    file=error_file.name,
                    content=content,
                )
            except Exception:
                pass

        # Check protocol file
        for prt_file in out_dir.glob("*.PRT"):
            try:
                content = prt_file.read_text(errors="ignore")
                # Look for error lines
                error_lines = [
                    line for line in content.split("\n")
                    if "ERROR" in line.upper() or "FATAL" in line.upper()
                ]
                if error_lines:
                    logger.error(
                        "BPE protocol errors",
                        session=session_name,
                        errors=error_lines[:10],
                    )
            except Exception:
                pass

    def _build_bsw_args(
        self,
        date: GNSSDate,
        hour: int,
        session_name: str,
        coord_file: Path,
        stations: list[str],
        args: NRDDPTROArgs,
    ) -> dict[str, Any]:
        """Build BSW processing arguments.

        Args:
            date: Processing date
            hour: Hour
            session_name: Session name
            coord_file: NRT coordinate file
            stations: Station list
            args: Processing arguments

        Returns:
            Dictionary of BSW arguments
        """
        y4c = str(date.year)
        y2c = f"{date.year % 100:02d}"
        doy = f"{date.doy:03d}"
        hour_char = chr(ord('a') + hour)

        # Session string for hourly: DOYH (e.g., 260A)
        session_str = f"{doy}{hour_char.upper()}"

        # Get coordinate args
        coord_manager = self.get_coord_manager(args)
        coord_args = coord_manager.build_bsw_args(date.year, date.doy)

        bsw_args = {
            # Processing type
            "procType": "hourly",
            # Date/time components
            "y4c": y4c,
            "y2c": y2c,
            "doy": doy,
            "ha": hour_char.upper(),
            "hour": hour,
            # Session info
            "session": session_name,
            "sessID2char": self.config.session_suffix,
            # PCF and options
            "PCF_FILE": self.config.pcf_file,
            "bswOpt": str(self.config.pygnss_rt_dir / self.config.bsw_options_yaml),
            # Option directories mapping
            "optDirs": get_option_dirs("nrddp"),
            # Datum and reference
            "datum": self.config.datum,
            "ABS_REL": self.config.antenna_phase_center,
            "opt_MINEL": self.config.min_elevation,
            # Coordinate files (dynamic NRT)
            "infoCRD": coord_args["infoCRD"],
            "infoCRA": coord_args["infoCRA"],
            "remIfNoCoord": coord_args["remIfNoCoord"],
            # Station list
            "stations": stations,
            # NEQ stacking
            "COMBNEQ": {
                "yesORno": "yes" if args.neq_stacking.enabled else "no",
                "n2stack": args.neq_stacking.n_hours_to_stack,
                "nameScheme": (
                    args.neq_stacking.name_scheme.value
                    if hasattr(args.neq_stacking.name_scheme, 'value')
                    else str(args.neq_stacking.name_scheme)
                ),
            },
            # Archive files (none for NRDDP TRO)
            "archFiles": {},
            # DCM settings
            "DCM": {
                "yesORno": "yes" if self.config.dcm_enabled else "no",
                "dir2del": self.config.dcm_dirs_to_delete,
                "compUtil": "gzip",
                "mv2dir": self.config.dcm_archive_dir,
                "org": self.config.dcm_organization,
            },
            # Data directories
            "dataDir": {
                "root": str(self.config.data_root),
                **self.config.data_organization,
            },
        }

        return bsw_args

    def _convert_ztd_to_iwv(
        self,
        campaign_dir: Path,
        date: GNSSDate,
        hour: int,
        args: NRDDPTROArgs,
    ) -> int:
        """Convert ZTD to IWV.

        Port of Perl ZTD2IWV.pm functionality. Uses meteorological
        data from nearby WMO stations for pressure/temperature extrapolation.

        Args:
            campaign_dir: Campaign directory
            date: Processing date
            hour: Hour
            args: Processing arguments

        Returns:
            Number of IWV records generated
        """
        from pygnss_rt.atmosphere.ztd2iwv import (
            ZTD2IWV,
            MeteoStationDatabase,
            read_tro_file,
        )
        from pygnss_rt.stations.coordinates import ecef_to_geodetic

        # Find TRO file in campaign
        tro_file = self._find_tro_output(campaign_dir, date, hour)
        if not tro_file:
            logger.warning("No TRO file found for IWV conversion")
            return 0

        # Load meteorological station database
        wmo_file = self.config.station_data_dir / "wmo_stations.dat"
        met_db = None
        if wmo_file.exists():
            met_db = MeteoStationDatabase()
            met_db.load_wmo_file(wmo_file)
            logger.debug(f"Loaded {len(met_db)} WMO stations")

        # Initialize converter
        converter = ZTD2IWV(
            tm_method="bevis",  # Use Bevis (1992) relation for mean temperature
            met_database=met_db,
        )

        # Load meteorological observations for this hour
        # Met data file naming: synop_YYDOYHH.dat
        y2c = f"{date.year % 100:02d}"
        doy = f"{date.doy:03d}"
        met_file = self.config.data_root / "met" / f"synop_{y2c}{doy}{hour:02d}.dat"
        if met_file.exists():
            converter.load_met_data(met_file)
            logger.debug(f"Loaded met data from {met_file}")

        # Read TRO file
        try:
            coordinates, ztd_records = read_tro_file(tro_file)
        except Exception as e:
            logger.error(f"Failed to read TRO file: {e}")
            return 0

        logger.info(
            f"Processing {len(ztd_records)} ZTD records from {len(coordinates)} stations"
        )

        # Process each ZTD record
        iwv_count = 0
        for record in ztd_records:
            station = record["station"].upper()

            # Get coordinates for this station
            if station not in coordinates:
                logger.debug(f"No coordinates for station {station}")
                continue

            coord = coordinates[station]
            x, y, z = coord["X"], coord["Y"], coord["Z"]

            # Convert ECEF to geodetic
            lat, lon, height = ecef_to_geodetic(x, y, z)

            # Build timestamp
            from datetime import datetime
            timestamp = datetime(
                record["year"],
                1,  # Will be overridden by doy
                1,
                record["hour"],
                record["minute"],
                record["second"],
            )
            # Adjust for day of year
            from datetime import timedelta
            timestamp = datetime(record["year"], 1, 1) + timedelta(
                days=record["doy"] - 1,
                hours=record["hour"],
                minutes=record["minute"],
                seconds=record["second"],
            )

            try:
                result = converter.process(
                    station_id=station.lower(),
                    ztd=record["ztd"],
                    ztd_sigma=record["ztd_sigma"],
                    timestamp=timestamp,
                    latitude=lat,
                    longitude=lon,
                    height=height,
                )
                iwv_count += 1
            except Exception as e:
                logger.debug(f"Failed to convert ZTD for {station}: {e}")
                continue

        # Write output files
        if iwv_count > 0:
            session_name = f"{y2c}{doy}{chr(ord('A') + hour)}{self.config.session_suffix}"

            # COST-716 format output
            cost_file = campaign_dir / "ATM" / f"{session_name}.COST716"
            converter.write_cost716_file(
                cost_file,
                project="NRDDP-TRO",
                processing_center="PYGNSS",
                status="TEST",
            )

            # CSV output for easy processing
            csv_file = campaign_dir / "ATM" / f"{session_name}_IWV.csv"
            converter.write_csv(csv_file)

            # Detailed log file
            log_file = campaign_dir / "OUT" / f"IWV_CONV_{session_name}.log"
            converter.write_iwv_log(log_file)

            logger.info(
                f"IWV conversion complete",
                records=iwv_count,
                cost_file=str(cost_file),
            )

        return iwv_count

    def _run_dcm(
        self,
        date: GNSSDate,
        session_name: str,
        campaign_dir: Path,
    ) -> None:
        """Run Delete/Compress/Move archiving.

        Args:
            date: Processing date
            session_name: Session name
            campaign_dir: Campaign directory
        """
        if not self.config.dcm_enabled:
            return

        # Delete specified directories
        for dir_name in self.config.dcm_dirs_to_delete:
            dir_path = campaign_dir / dir_name
            if dir_path.exists():
                shutil.rmtree(dir_path)

        # Compress remaining files (would use gzip)

        # Move to final archive location (already in place with yyyy/doy org)


def create_nrddp_tro_config(
    paths: PathConfig | None = None,
    data_root: str | None = None,
    pygnss_rt_dir: str | None = None,
    gpsuser_dir: str | None = None,
) -> NRDDPTROConfig:
    """Create NRDDP TRO configuration.

    Args:
        paths: PathConfig instance (uses global instance if None)
        data_root: Override data root directory
        pygnss_rt_dir: Override pygnss_rt directory
        gpsuser_dir: Override GPSUSER directory

    Returns:
        NRDDPTROConfig instance
    """
    if paths is None:
        paths = get_paths()

    return NRDDPTROConfig(
        data_root=Path(data_root) if data_root else _get_default_data_root(),
        pygnss_rt_dir=Path(pygnss_rt_dir) if pygnss_rt_dir else paths.pygnss_rt_dir,
        station_data_dir=paths.station_data_dir,
        gpsuser_dir=Path(gpsuser_dir) if gpsuser_dir else _get_default_gpsuser_dir(),
    )
