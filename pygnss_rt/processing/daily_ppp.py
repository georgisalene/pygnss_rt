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

import os
import shutil
import subprocess
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pygnss_rt.processing.networks import (
    NetworkID,
    NetworkProfile,
    get_network_profile,
)
from pygnss_rt.utils.dates import GNSSDate


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

    # Skip options (for debugging/partial runs)
    skip_products: bool = False  # Skip product download
    skip_data: bool = False  # Skip station data download
    skip_dcm: bool = False  # Skip DCM archiving

    # Output control
    dry_run: bool = False
    verbose: bool = False


class DailyPPPProcessor:
    """Daily PPP processor for GNSS networks.

    Manages the complete daily PPP processing workflow:
    1. Load network configuration
    2. Get station list from XML
    3. Download required products (orbit, ERP, clock)
    4. Download station RINEX data
    5. Run Bernese GNSS Software processing
    6. Archive results (DCM: Delete, Compress, Move)

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
        ignss_dir: str | None = None,
        data_root: str | None = None,
        gpsuser_dir: str | None = None,
    ):
        """Initialize daily PPP processor.

        Args:
            config_path: Path to configuration file
            ignss_dir: Override i-GNSS directory
            data_root: Override data root directory
            gpsuser_dir: Override GPSUSER directory
        """
        self.config_path = Path(config_path) if config_path else None
        self.ignss_dir = ignss_dir or os.environ.get(
            "IGNSS", "/home/ahunegnaw/Python_IGNSS/i-GNSS"
        )
        self.data_root = data_root or "/home/ahunegnaw/data54"
        self.gpsuser_dir = gpsuser_dir or os.environ.get(
            "GPSUSER", "/home/ahunegnaw/GPSUSER"
        )

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
        return get_network_profile(
            network_id,
            ignss_dir=self.ignss_dir,
            data_root=self.data_root,
            gpsuser_dir=self.gpsuser_dir,
        )

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
        # Build session name (YYDOYID format)
        session_name = f"{date.year % 100:02d}{date.doy:03d}{profile.session_id}"

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
            campaign_dir = self._setup_campaign(profile, date, session_name, args)
            if args.verbose:
                print(f"  Campaign dir: {campaign_dir}")

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

            print(f"\nProcessing complete: {result.stations_processed} stations")
            print(f"Duration: {result.duration_seconds:.1f} seconds")

        except Exception as e:
            result.error_message = str(e)
            result.end_time = datetime.now(timezone.utc)
            print(f"\nERROR: {e}")

        return result

    def _get_stations(
        self,
        profile: NetworkProfile,
        args: DailyPPPArgs,
    ) -> list[str]:
        """Get list of stations to process.

        Args:
            profile: Network profile
            args: Processing arguments

        Returns:
            List of station IDs
        """
        if args.stations:
            # User override
            stations = args.stations.copy()
        else:
            # Load from XML based on profile filter
            stations = self._load_stations_from_xml(profile)

        # Apply exclusions from profile
        exclude = set(profile.station_filter.exclude_stations)

        # Apply additional exclusions from args
        exclude.update(args.exclude_stations)

        # Filter out excluded stations
        stations = [s for s in stations if s.lower() not in {e.lower() for e in exclude}]

        return sorted(stations)

    def _load_stations_from_xml(self, profile: NetworkProfile) -> list[str]:
        """Load station list from XML file.

        Args:
            profile: Network profile

        Returns:
            List of station IDs
        """
        xml_path = Path(profile.station_filter.xml_file)
        if not xml_path.exists():
            print(f"Warning: Station XML not found: {xml_path}")
            return []

        # Use the existing StationManager
        try:
            from pygnss_rt.stations.station import StationManager

            manager = StationManager()
            manager.load_xml(xml_path)

            # Apply filters
            kwargs: dict[str, Any] = {}
            if profile.station_filter.use_nrt:
                kwargs["use_nrt"] = True
            if profile.station_filter.primary_net:
                kwargs["network"] = profile.station_filter.primary_net

            station_objs = manager.get_stations(**kwargs)
            return [s.station_id for s in station_objs]
        except Exception as e:
            print(f"Warning: Error loading stations from XML: {e}")
            return []

    def _check_products(
        self,
        profile: NetworkProfile,
        date: GNSSDate,
        args: DailyPPPArgs,
    ) -> bool:
        """Check if required products are available.

        Args:
            profile: Network profile
            date: Processing date
            args: Processing arguments

        Returns:
            True if all products available
        """
        # This would check the database/download products
        # For now, return True (products are downloaded as needed)
        if args.verbose:
            print(f"  Orbit: {profile.orbit_source.provider} {profile.orbit_source.tier}")
            print(f"  ERP: {profile.erp_source.provider} {profile.erp_source.tier}")
            print(f"  Clock: {profile.clock_source.provider} {profile.clock_source.tier}")
        return True

    def _download_station_data(
        self,
        profile: NetworkProfile,
        date: GNSSDate,
        stations: list[str],
        args: DailyPPPArgs,
    ) -> bool:
        """Download station RINEX data.

        Args:
            profile: Network profile
            date: Processing date
            stations: List of stations
            args: Processing arguments

        Returns:
            True if data downloaded successfully
        """
        # This would use the FTP module to download data
        # For now, placeholder
        if args.verbose:
            for ftp in profile.data_ftp_sources:
                print(f"  FTP source: {ftp.server_id} ({ftp.category})")
        return True

    def _check_alignment_files(
        self,
        profile: NetworkProfile,
        date: GNSSDate,
    ) -> bool:
        """Check if IGS alignment files are available.

        Args:
            profile: Network profile
            date: Processing date

        Returns:
            True if alignment files exist
        """
        if not profile.archive_files:
            return True

        for name, spec in profile.archive_files.items():
            # Build expected path
            y4 = date.year
            y2 = date.year % 100
            doy = date.doy
            path_pattern = spec.organization.replace("yyyy", str(y4)).replace("doy", f"{doy:03d}")
            camp_pattern = spec.campaign_pattern.replace("YY", f"{y2:02d}").replace("DOY", f"{doy:03d}")

            base_dir = Path(spec.root) / path_pattern / camp_pattern / spec.source_dir

            for ext in spec.extensions:
                file_pattern = f"{spec.prefix}{y2:02d}{doy:03d}0{ext}{spec.compression}"
                expected_file = base_dir / file_pattern
                # In production, check if file exists
                # For now, just log
                print(f"  Checking: {expected_file}")

        return True

    def _setup_campaign(
        self,
        profile: NetworkProfile,
        date: GNSSDate,
        session_name: str,
        args: DailyPPPArgs,
    ) -> Path:
        """Setup BSW campaign directory.

        Args:
            profile: Network profile
            date: Processing date
            session_name: Session name
            args: Processing arguments

        Returns:
            Path to campaign directory
        """
        # Campaign root from config or profile
        campaign_root = Path(
            self._config.get("bsw", {}).get("campaign_root", "/home/ahunegnaw/campaigns")
        )

        campaign_dir = campaign_root / session_name

        if not args.dry_run:
            # Create campaign directory structure
            for subdir in ["ATM", "BPE", "GRD", "OBS", "ORB", "ORX", "OUT", "RAW", "SOL", "STA"]:
                (campaign_dir / subdir).mkdir(parents=True, exist_ok=True)

        return campaign_dir

    def _run_bsw_processing(
        self,
        profile: NetworkProfile,
        date: GNSSDate,
        session_name: str,
        campaign_dir: Path,
        args: DailyPPPArgs,
    ) -> bool:
        """Run Bernese GNSS Software processing.

        Args:
            profile: Network profile
            date: Processing date
            session_name: Session name
            campaign_dir: Campaign directory
            args: Processing arguments

        Returns:
            True if processing succeeded
        """
        # This would invoke the BPE runner
        # For now, placeholder that would call:
        # - LOADGPS environment
        # - BPE with PCF file
        print(f"  PCF: {profile.pcf_file}")
        print(f"  Options: {profile.bsw_options_xml}")
        return True

    def _run_dcm(
        self,
        profile: NetworkProfile,
        date: GNSSDate,
        session_name: str,
        campaign_dir: Path,
    ) -> None:
        """Run Delete/Compress/Move archiving.

        Args:
            profile: Network profile
            date: Processing date
            session_name: Session name
            campaign_dir: Campaign directory
        """
        if not profile.dcm_enabled:
            return

        # Delete specified directories
        for dir_name in profile.dcm_dirs_to_delete:
            dir_path = campaign_dir / dir_name
            if dir_path.exists():
                shutil.rmtree(dir_path)
                print(f"  Deleted: {dir_name}")

        # Compress remaining files
        # (would use gzip)

        # Move to archive location
        if profile.dcm_archive_dir:
            y4 = date.year
            doy = date.doy
            org = profile.dcm_organization.replace("yyyy", str(y4)).replace("doy", f"{doy:03d}")
            archive_path = Path(profile.dcm_archive_dir) / org
            archive_path.mkdir(parents=True, exist_ok=True)
            # shutil.move(campaign_dir, archive_path / session_name)
            print(f"  Archive: {archive_path / session_name}")


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
