"""
Daily PPP (Precise Point Positioning) processor.

Unified implementation for daily PPP processing across all networks.
Replaces the 5 Perl caller scripts:
- iGNSS_D_PPP_AR_IG_IGS54_direct_NRT.pl
- iGNSS_D_PPP_AR_EU_IGS54_direct_NRT.pl
- iGNSS_D_PPP_AR_GB_IGS54_direct_NRT.pl
- iGNSS_D_PPP_AR_RG_IGS54_direct_NRT.pl
- iGNSS_D_PPP_AR_SS_IGS54_direct_NRT.pl
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pygnss_rt.core.paths import PathConfig, get_paths
from pygnss_rt.processing.networks import (
    NetworkID,
    NetworkProfile,
    get_network_profile,
)
from pygnss_rt.processing.neq_stacking import (
    NEQStackingConfig,
    NO_STACKING,
)
from pygnss_rt.utils.dates import GNSSDate

# Import mixins for concern-based separation
from pygnss_rt.processing.daily_ppp_stations import StationMixin
from pygnss_rt.processing.daily_ppp_products import ProductMixin
from pygnss_rt.processing.daily_ppp_campaign import CampaignMixin
from pygnss_rt.processing.daily_ppp_bsw import BSWMixin
from pygnss_rt.processing.daily_ppp_dcm import DCMMixin


@dataclass
class DailyPPPResult:
    """Result of a daily PPP processing run."""

    network_id: str
    session_name: str
    date: GNSSDate
    success: bool
    stations_processed: int = 0
    stations_total: int = 0
    start_time: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    end_time: datetime | None = None
    error_message: str = ""
    output_files: list[str] = field(default_factory=list)

    @property
    def duration_seconds(self) -> float:
        """Get processing duration in seconds."""
        if self.end_time:
            return (self.end_time - self.start_time).total_seconds()
        return 0.0


@dataclass
class DailyPPPArgs:
    """Arguments for daily PPP processing."""

    # Network selection
    network_id: NetworkID | str

    # Date range (single day for cron, range for manual)
    start_date: GNSSDate | None = None
    end_date: GNSSDate | None = None

    # CRON mode settings
    cron_mode: bool = False
    latency_days: int = 21

    # Station overrides
    stations: list[str] = field(default_factory=list)  # Override stations
    exclude_stations: list[str] = field(default_factory=list)  # Additional exclusions

    # Processing options
    use_clockprep: bool = True
    use_cc2noncc: bool = False
    systems: str = "GRE"  # GNSS systems: G, GE, GR, GRE
    tro_gradient_interval: str | None = None  # Troposphere gradient interval (e.g., "12 00 00")

    # NEQ stacking configuration (for hourly processing)
    neq_stacking: NEQStackingConfig | None = None

    # Skip options (for debugging/partial runs)
    skip_products: bool = False  # Skip product download
    skip_data: bool = False  # Skip station data download
    skip_dcm: bool = False  # Skip DCM archiving

    # Local RINEX source (alternative to network downloads)
    local_rinex_dir: Path | str | None = None  # Directory with local RINEX files
    local_only: bool = False  # If True, only use local files (no network downloads)
    min_stations_pct: float = 0.5  # Minimum station download percentage (0.2 = 20%)

    # Output control
    dry_run: bool = False
    verbose: bool = False


class DailyPPPProcessor(StationMixin, ProductMixin, CampaignMixin, BSWMixin, DCMMixin):
    """Daily PPP processor for GNSS networks.

    Manages the complete daily PPP processing workflow:
    1. Load network configuration
    2. Get station list from XML
    3. Download required products (orbit, ERP, clock)
    4. Download station RINEX data
    5. Run Bernese GNSS Software processing
    6. Archive results (DCM: Delete, Compress, Move)

    This class uses a mixin pattern to separate concerns:
    - StationMixin: Station management and validation
    - ProductMixin: Product downloads and management
    - CampaignMixin: Campaign setup and file copying
    - BSWMixin: BSW execution and processing
    - DCMMixin: DCM archiving

    Usage:
        processor = DailyPPPProcessor(config_path="config/settings.yaml")

        # Process single network in cron mode
        result = processor.process(DailyPPPArgs(
            network_id="IG",
            cron_mode=True,
            latency_days=21,
        ))

        # Process specific date range
        result = processor.process(DailyPPPArgs(
            network_id="EU",
            start_date=GNSSDate(2024, 7, 7),
            end_date=GNSSDate(2024, 7, 7),
        ))
    """

    def __init__(
        self,
        config_path: Path | str | None = None,
        paths: PathConfig | None = None,
    ):
        """Initialize daily PPP processor.

        Args:
            config_path: Path to configuration file
            paths: PathConfig instance (uses global instance if None)
        """
        self.config_path = Path(config_path) if config_path else None
        self.paths = paths or get_paths()

        # For backward compatibility, expose these as properties
        self.ignss_dir = str(self.paths.pygnss_rt_dir)
        self.data_root = str(self.paths.data_root) if self.paths.data_root else ""
        self.gpsuser_dir = str(self.paths.gpsuser_dir) if self.paths.gpsuser_dir else ""

        self._config: dict[str, Any] = {}
        self._load_config()

    def _load_config(self) -> None:
        """Load configuration from file."""
        if self.config_path and self.config_path.exists():
            from pygnss_rt.core.config import load_config

            self._config = load_config(self.config_path)

    def get_profile(self, network_id: NetworkID | str) -> NetworkProfile:
        """Get network profile with any config overrides.

        Args:
            network_id: Network identifier

        Returns:
            NetworkProfile for the specified network
        """
        return get_network_profile(network_id, paths=self.paths)

    def _build_session_name(
        self,
        profile: NetworkProfile,
        date: GNSSDate,
        systems: str | None = None,
    ) -> str:
        """Build campaign/session name with systems suffix.

        Base format: YYDOYID (e.g., 25310IG)
        Systems suffix is always appended:
          - G   -> _G
          - GE  -> _GE
          - GR  -> _GR
          - GRE -> _GRE
        """
        base = f"{date.year % 100:02d}{date.doy:03d}{profile.session_id}"
        normalized = BSWMixin._normalize_systems(systems)
        return f"{base}_{normalized}"

    def process(self, args: DailyPPPArgs) -> list[DailyPPPResult]:
        """Run daily PPP processing.

        Args:
            args: Processing arguments

        Returns:
            List of results (one per date processed)
        """
        profile = self.get_profile(args.network_id)
        results = []

        # Determine date range
        if args.cron_mode:
            # Calculate processing date from current time minus latency
            proc_date = GNSSDate.now().add_days(-args.latency_days)
            start_date = proc_date
            end_date = proc_date
            print(f"CRON mode: Processing date {proc_date} (latency: {args.latency_days} days)")
        elif args.start_date and args.end_date:
            start_date = args.start_date
            end_date = args.end_date
        else:
            raise ValueError("Must specify either cron_mode or start_date/end_date")

        # Process each date
        current = start_date
        while current.mjd <= end_date.mjd:
            result = self._process_single_day(profile, current, args)
            results.append(result)
            current = current.add_days(1)

        return results

    def _process_single_day(
        self,
        profile: NetworkProfile,
        date: GNSSDate,
        args: DailyPPPArgs,
    ) -> DailyPPPResult:
        """Process a single day for a network.

        Args:
            profile: Network profile
            date: Processing date
            args: Processing arguments

        Returns:
            Processing result
        """
        # Build session name (YYDOYID[_SYS] format)
        session_name = self._build_session_name(profile, date, args.systems)

        result = DailyPPPResult(
            network_id=profile.network_id.value,
            session_name=session_name,
            date=date,
            success=False,
        )

        print(f"\n{'=' * 60}")
        print(f"Daily PPP Processing: {profile.description}")
        print(f"Session: {session_name}")
        print(f"Date: {date} (DOY {date.doy})")
        print(f"{'=' * 60}")

        if args.dry_run:
            print("\n[DRY RUN MODE - No actual processing]")

        try:
            # Step 1: Get station list
            stations = self._get_stations(profile, args)
            result.stations_total = len(stations)
            print(f"\nStations to process: {len(stations)}")
            if args.verbose and stations:
                print(f"  {', '.join(stations[:10])}" + ("..." if len(stations) > 10 else ""))

            if not stations:
                result.error_message = "No stations to process"
                return result

            # Step 1b: Validate BLQ (ocean loading) coverage
            # All stations must have entries in the BLQ file
            blq_valid, missing_blq = self._validate_blq_coverage(profile, stations, args)
            if not blq_valid:
                result.error_message = (
                    f"BLQ validation failed: {len(missing_blq)} stations missing ocean loading data. "
                    f"Missing: {', '.join(sorted(missing_blq))}. "
                    f"Add these stations to the BLQ file using http://holt.oso.chalmers.se/loading/"
                )
                return result

            # Step 1c: Auto-update STA file with missing station info
            if not args.dry_run:
                self._update_station_info(profile, stations, args)

            # Step 2: Check for required products
            if args.skip_products:
                print("\nSkipping product check (--skip-products)")
            else:
                print("\nChecking products...")
                products_ok = self._check_products(profile, date, args)
                if not products_ok and not args.dry_run:
                    result.error_message = "Missing required products"
                    return result

            # Step 3: Download station data
            if args.skip_data:
                print("\nSkipping station data download (--skip-data)")
            else:
                print("\nDownloading station data...")
                if not args.dry_run:
                    downloaded = self._download_station_data(profile, date, stations, args)
                    if not downloaded:
                        result.error_message = "No station data downloaded"
                        return result

            # Step 4: Check alignment files (for non-IGS networks)
            if profile.requires_igs_alignment:
                print("\nChecking IGS alignment files...")
                if not args.dry_run:
                    alignment_ok = self._check_alignment_files(profile, date)
                    if not alignment_ok:
                        result.error_message = "IGS alignment files not available"
                        return result

            # Step 5: Setup campaign directory
            print("\nSetting up campaign...")
            campaign_dir = self._setup_campaign(profile, date, session_name, stations, args)
            if args.verbose:
                print(f"  Campaign dir: {campaign_dir}")

            # Step 5b: Validate all stations exist in STA file (STRICT)
            # This ensures Bernese won't run with missing station equipment info
            print("\nValidating station information...")
            if not args.dry_run:
                sta_valid, missing, error_type = self._validate_stations_in_sta(profile, campaign_dir, args)
                if not sta_valid:
                    if error_type == "STA_FILE_NOT_FOUND":
                        result.error_message = "STA file not found - cannot process without station information"
                    else:
                        result.error_message = f"Stations missing from STA file: {', '.join(missing)}"
                    return result

            # Step 6: Run BSW processing
            print("\nRunning Bernese processing...")
            if not args.dry_run:
                bsw_success = self._run_bsw_processing(
                    profile, date, session_name, campaign_dir, args
                )
                if not bsw_success:
                    result.error_message = "BSW processing failed"
                    return result

            # Step 7: DCM (Delete, Compress, Move)
            if args.skip_dcm:
                print("\nSkipping DCM archiving (--skip-dcm)")
            elif profile.dcm_enabled:
                print("\nArchiving results (DCM)...")
                if not args.dry_run:
                    self._run_dcm(profile, date, session_name, campaign_dir)

            result.success = True
            result.stations_processed = len(stations)  # Simplified
            result.end_time = datetime.now(timezone.utc)

            # Step 8: Update quality database with PPP-AR solutions
            if not args.dry_run:
                print("\nUpdating quality database...")
                try:
                    from pygnss_rt.frontend.post_processing_hook import update_quality_db
                    campaign_parent = str(campaign_dir.parent)
                    db_success = update_quality_db(
                        date.year,
                        date.doy,
                        campaign_parent,
                        verbose=args.verbose,
                        preferred_session=session_name,
                    )
                    if db_success:
                        print("  Quality database updated successfully")
                    else:
                        print("  Warning: Quality database update returned no records")
                except ImportError:
                    print("  Warning: post_processing_hook not available, skipping DB update")
                except Exception as e:
                    print(f"  Warning: Quality database update failed: {e}")

            print(f"\nProcessing complete: {result.stations_processed} stations")
            print(f"Duration: {result.duration_seconds:.1f} seconds")

        except Exception as e:
            result.error_message = str(e)
            result.end_time = datetime.now(timezone.utc)
            print(f"\nERROR: {e}")

        return result


def process_all_networks(
    args_base: DailyPPPArgs,
    networks: list[NetworkID | str] | None = None,
) -> dict[str, list[DailyPPPResult]]:
    """Process multiple networks in sequence.

    Args:
        args_base: Base processing arguments (network_id will be overridden)
        networks: List of networks to process (default: all)

    Returns:
        Dictionary of network_id -> results
    """
    if networks is None:
        networks = [NetworkID.IG, NetworkID.EU, NetworkID.GB, NetworkID.RG, NetworkID.SS]

    processor = DailyPPPProcessor()
    all_results = {}

    for network in networks:
        # Create args copy with specific network
        args = DailyPPPArgs(
            network_id=network,
            start_date=args_base.start_date,
            end_date=args_base.end_date,
            cron_mode=args_base.cron_mode,
            latency_days=args_base.latency_days,
            stations=args_base.stations,
            exclude_stations=args_base.exclude_stations,
            use_clockprep=args_base.use_clockprep,
            use_cc2noncc=args_base.use_cc2noncc,
            systems=args_base.systems,
            skip_products=args_base.skip_products,
            skip_data=args_base.skip_data,
            skip_dcm=args_base.skip_dcm,
            dry_run=args_base.dry_run,
            verbose=args_base.verbose,
        )

        results = processor.process(args)
        network_key = network.value if isinstance(network, NetworkID) else network
        all_results[network_key] = results

    return all_results
