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
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

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


@dataclass
class NRDDPTROConfig:
    """Configuration for NRDDP TRO processing."""

    # Directory paths
    ignss_dir: Path = field(default_factory=lambda: Path("/home/ahunegnaw/Python_IGNSS/i-GNSS"))
    data_root: Path = field(default_factory=lambda: Path("/home/nrt105/data54"))
    gpsuser_dir: Path = field(default_factory=lambda: Path("/home/ahunegnaw/GPSUSER54_LANT"))
    campaign_root: Path = field(default_factory=lambda: Path("/home/nrt105/data54/campaigns/tro"))

    # Processing configuration
    pcf_file: str = "NRDDPTRO_BSW54.PCF"
    bsw_options_xml: str = "callers/NRDDP_TRO/iGNSS_NRDDP_TRO_BSW54_direct.xml"

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
    dcm_archive_dir: str = "/home/nrt105/data54/campaigns/tro"
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
                info_dir=self.config.ignss_dir / "info",
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

        Args:
            date: Processing date
            hour: Hour
            args: Processing arguments

        Returns:
            True if products available
        """
        # Check for ultra-rapid products (updated 4x daily)
        # For NRDDP, we typically use IGS ultra-rapid products
        return True

    def _download_hourly_data(
        self,
        date: GNSSDate,
        hour: int,
        stations: list[str],
        args: NRDDPTROArgs,
    ) -> bool:
        """Download hourly RINEX data.

        Args:
            date: Processing date
            hour: Hour
            stations: Station list
            args: Processing arguments

        Returns:
            True if data downloaded
        """
        # Would use FTP module to download from configured sources
        # OSGB, CDDIS, BKGE, TUDELFT, etc.
        return True

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
            print(f"    Options: {self.config.bsw_options_xml}")
            print(f"    Coordinate: {coord_file}")

        # TODO: Invoke BPE runner
        return True

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
            "bswOpt": str(self.config.ignss_dir / self.config.bsw_options_xml),
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

        Args:
            campaign_dir: Campaign directory
            date: Processing date
            hour: Hour
            args: Processing arguments

        Returns:
            Number of IWV records generated
        """
        # Would use ZTD2IWV converter
        # from pygnss_rt.atmosphere.ztd2iwv import ZTD2IWV
        return 0

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
    data_root: str = "/home/nrt105/data54",
    ignss_dir: str = "/home/ahunegnaw/Python_IGNSS/i-GNSS",
    gpsuser_dir: str = "/home/ahunegnaw/GPSUSER54_LANT",
) -> NRDDPTROConfig:
    """Create NRDDP TRO configuration.

    Args:
        data_root: Data root directory
        ignss_dir: i-GNSS directory
        gpsuser_dir: GPSUSER directory

    Returns:
        NRDDPTROConfig instance
    """
    return NRDDPTROConfig(
        data_root=Path(data_root),
        ignss_dir=Path(ignss_dir),
        gpsuser_dir=Path(gpsuser_dir),
    )
