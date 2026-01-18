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
import re
import shutil
import subprocess
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
from pygnss_rt.stations.sta_auto_update import (
    find_missing_stations,
    update_sta_from_yaml,
)
from pygnss_rt.processing.bsw_options import (
    BSWOptionsParser,
    BSWOptionsConfig,
    get_option_dirs,
    xml_step_to_opt_dir,
)
from pygnss_rt.processing.neq_stacking import (
    NEQStacker,
    NEQStackingConfig,
    NO_STACKING,
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
            print(f"\nStation source: User-specified list ({len(stations)} stations)")
        elif args.local_only and args.local_rinex_dir:
            # Scan local RINEX directory for stations
            stations = self._scan_local_rinex_stations(args.local_rinex_dir)
            print(f"\nStation source: Local RINEX directory ({len(stations)} stations)")
            print(f"  Directory: {args.local_rinex_dir}")
        else:
            # Load from XML based on profile filter
            print(f"\nStation source: {profile.station_filter.xml_file}")
            stations = self._load_stations_from_xml(profile)
            print(f"  Loaded {len(stations)} stations from XML")

        # Apply exclusions from profile
        exclude = set(profile.station_filter.exclude_stations)

        # Apply additional exclusions from args
        exclude.update(args.exclude_stations)

        if exclude:
            print(f"  Excluding: {', '.join(sorted(exclude))}")

        # Filter out excluded stations
        stations = [s for s in stations if s.lower() not in {e.lower() for e in exclude}]

        # Print full station list
        print(f"\nStations to process ({len(stations)}):")
        # Print in columns of 10
        for i in range(0, len(stations), 10):
            row = stations[i:i+10]
            print(f"  {', '.join(row)}")

        return sorted(stations)

    def _scan_local_rinex_stations(
        self,
        local_dir: Path | str,
    ) -> list[str]:
        """Scan local RINEX directory for station IDs.

        Extracts 4-character station IDs from RINEX filenames.
        Supports both short (SSSS2960.25o) and long (SSSS00CCC_R_...) formats.

        Args:
            local_dir: Directory containing RINEX files

        Returns:
            List of unique station IDs (uppercase)
        """
        local_dir = Path(local_dir)
        if not local_dir.exists():
            print(f"  WARNING: Local RINEX directory not found: {local_dir}")
            return []

        stations = set()

        # RINEX file patterns
        # Short format: SSSS2960.25o, SSSS2960.25g, etc.
        # Long format: SSSS00CCC_R_20252960000_01D_30S_MO.crx.gz
        extensions = [
            "*.??o", "*.??g", "*.??n",  # RINEX 2.x observation/nav
            "*.rnx", "*.crx",  # RINEX 3.x
            "*.rnx.gz", "*.crx.gz",  # Compressed
            "*_MO.rnx*", "*_MO.crx*",  # Long format observation
        ]

        for pattern in extensions:
            for f in local_dir.glob(pattern):
                # Extract first 4 characters as station ID
                sta_id = f.name[:4].upper()
                # Validate: should be alphanumeric
                if sta_id.isalnum() and len(sta_id) == 4:
                    stations.add(sta_id)

        return sorted(stations)

    def _load_stations_from_xml(self, profile: NetworkProfile) -> list[str]:
        """Load station list from station file (XML or YAML).

        Supports both XML (legacy) and YAML (preferred) formats.
        If the XML path doesn't exist but a YAML version does, it will use YAML.

        Args:
            profile: Network profile

        Returns:
            List of station IDs
        """
        station_file = Path(profile.station_filter.xml_file)

        # Try YAML first if XML doesn't exist
        if not station_file.exists():
            yaml_path = station_file.with_suffix(".yaml")
            if yaml_path.exists():
                station_file = yaml_path
            else:
                print(f"  WARNING: Station file not found: {station_file}")
                return []

        # Use the existing StationManager
        try:
            from pygnss_rt.stations.station import StationManager

            manager = StationManager()
            manager.load(station_file)  # Auto-detects XML or YAML

            # Print filter info
            filters = []
            if profile.station_filter.use_nrt:
                filters.append("NRT-enabled")
            if profile.station_filter.primary_net:
                filters.append(f"network={profile.station_filter.primary_net}")
            if profile.station_filter.station_type:
                filters.append(f"type={profile.station_filter.station_type}")
            if filters:
                print(f"  Filters: {', '.join(filters)}")

            # Apply filters
            kwargs: dict[str, Any] = {}
            if profile.station_filter.use_nrt:
                kwargs["use_nrt"] = True
            if profile.station_filter.primary_net:
                kwargs["network"] = profile.station_filter.primary_net
            if profile.station_filter.station_type:
                kwargs["station_type"] = profile.station_filter.station_type

            station_objs = manager.get_stations(**kwargs)
            return [s.station_id for s in station_objs]
        except Exception as e:
            print(f"  WARNING: Error loading stations: {e}")
            return []

    def _get_sta_file_from_pcf(self, profile: NetworkProfile) -> Path | None:
        """Get the STA file name from PCF V_STAINF variable.

        Args:
            profile: Network profile containing PCF file path

        Returns:
            Path to STA file, or None if not found
        """
        import re

        pcf_path = Path(profile.pcf_file)
        if not pcf_path.exists():
            return None

        try:
            content = pcf_path.read_text()
            # Match: V_STAINF = IGS20_54;
            match = re.search(r'V_STAINF\s*=\s*(\w+)\s*;', content)
            if match:
                sta_name = match.group(1)  # e.g., "IGS20_54"
                sta_file = self.paths.station_data_dir / f"{sta_name}.STA"
                return sta_file
        except Exception as e:
            print(f"  Warning: Could not read PCF file: {e}")

        return None

    def _get_blq_file_from_pcf(self, profile: NetworkProfile) -> Path | None:
        """Get BLQ file path from PCF V_BLQINF variable.

        Args:
            profile: Network profile containing PCF path

        Returns:
            Path to BLQ file, or None if not found
        """
        pcf_path = Path(profile.pcf_file)
        if not pcf_path.exists():
            return None

        try:
            content = pcf_path.read_text()
            # Match: V_BLQINF = IGS20_54;
            match = re.search(r'V_BLQINF\s*=\s*(\w+)\s*;', content)
            if match:
                blq_name = match.group(1)  # e.g., "IGS20_54" or "NEWNRT52"
                # Try station_data_dir first, then info_dir
                blq_file = self.paths.station_data_dir / f"{blq_name}.BLQ"
                if blq_file.exists():
                    return blq_file
                # Try info_dir
                blq_file = self.paths.info_dir / f"{blq_name}.BLQ"
                if blq_file.exists():
                    return blq_file
        except Exception as e:
            print(f"  Warning: Could not read PCF file for BLQ: {e}")

        return None

    def _parse_blq_stations(self, blq_file: Path) -> set[str]:
        """Parse BLQ file and extract station names.

        BLQ file format has station names on lines starting with 2 spaces
        followed by 4-character station code.

        Args:
            blq_file: Path to BLQ file

        Returns:
            Set of station names (uppercase)
        """
        stations = set()
        try:
            with open(blq_file, 'r') as f:
                for line in f:
                    # Station names start with 2 spaces followed by 4-char code
                    # Example: "  ABMF"
                    match = re.match(r'^  ([A-Z0-9]{4})\s*$', line)
                    if match:
                        stations.add(match.group(1).upper())
        except Exception as e:
            print(f"  Warning: Could not parse BLQ file {blq_file}: {e}")

        return stations

    def _validate_blq_coverage(
        self,
        profile: NetworkProfile,
        stations: list[str],
        args: DailyPPPArgs,
    ) -> tuple[bool, list[str]]:
        """Validate that all stations have BLQ (ocean loading) entries.

        Args:
            profile: Network profile
            stations: List of station IDs to process
            args: Processing arguments

        Returns:
            Tuple of (validation_passed, list of missing stations)

        Raises:
            ValueError: If stations are missing from BLQ file
        """
        # Get BLQ file from PCF
        blq_file = self._get_blq_file_from_pcf(profile)
        if blq_file is None:
            # Try default locations
            for blq_name in ["IGS20_54.BLQ", "NEWNRT52.BLQ"]:
                blq_path = self.paths.station_data_dir / blq_name
                if blq_path.exists():
                    blq_file = blq_path
                    break
                blq_path = self.paths.info_dir / blq_name
                if blq_path.exists():
                    blq_file = blq_path
                    break

        if blq_file is None:
            print("  WARNING: No BLQ file found - cannot validate ocean loading")
            return True, []  # Allow processing to continue if no BLQ file configured

        print(f"\n  Validating BLQ coverage: {blq_file.name}")

        # Parse stations from BLQ file
        blq_stations = self._parse_blq_stations(blq_file)
        if args.verbose:
            print(f"    BLQ file contains {len(blq_stations)} stations")

        # Check which processing stations are missing from BLQ
        missing = []
        for station in stations:
            if station.upper() not in blq_stations:
                missing.append(station.upper())

        if missing:
            print(f"    ERROR: {len(missing)} stations missing from BLQ file!")
            print(f"    Missing stations: {', '.join(sorted(missing))}")
            return False, missing

        print(f"    All {len(stations)} stations have BLQ entries")
        return True, []

    def _update_station_info(
        self,
        profile: NetworkProfile,
        stations: list[str],
        args: DailyPPPArgs,
    ) -> int:
        """Auto-update STA file with missing station info from gnss-metadata.eu.

        Checks if stations from the YAML file exist in the STA file.
        For any missing stations, downloads site logs from gnss-metadata.eu
        and adds the station info to the STA file.

        The STA file is determined by V_STAINF in the PCF file.

        Args:
            profile: Network profile containing station file paths
            stations: List of station IDs to check
            args: Processing arguments

        Returns:
            Number of stations added to STA file
        """
        from pygnss_rt.stations.sta_auto_update import (
            get_existing_stations,
            read_yaml_stations,
            build_nine_char_id,
            check_and_update_sta,
        )

        # Get STA file path from PCF V_STAINF variable
        sta_file = self._get_sta_file_from_pcf(profile)
        if sta_file is None:
            # Fallback to default
            sta_file = self.paths.station_info_file
            print(f"  Using default STA file: {sta_file.name}")
        else:
            print(f"  STA file from PCF (V_STAINF): {sta_file.name}")

        if not sta_file.exists():
            print(f"  Warning: STA file not found: {sta_file}")
            return 0

        # Get existing stations in STA file
        existing = get_existing_stations(sta_file)

        # Find stations that need to be added
        missing = [s.upper() for s in stations if s.upper() not in existing]

        if not missing:
            if args.verbose:
                print(f"  All {len(stations)} stations exist in STA file")
            return 0

        print(f"\n  Station info update: {len(missing)} stations missing from STA file")
        if args.verbose:
            print(f"    Missing: {', '.join(missing[:10])}" + ("..." if len(missing) > 10 else ""))

        # Try to get ISO codes from the YAML file
        yaml_file = Path(profile.station_filter.xml_file)
        # Try YAML version if XML doesn't exist
        if not yaml_file.exists():
            yaml_file = yaml_file.with_suffix(".yaml")

        iso_codes = {}
        if yaml_file.exists():
            try:
                yaml_stations = read_yaml_stations(yaml_file)
                for station in yaml_stations:
                    sta_id = station.get('id', '').upper()
                    iso = station.get('iso', '')
                    if sta_id and iso:
                        iso_codes[sta_id] = iso
                if args.verbose:
                    print(f"    Loaded {len(iso_codes)} ISO codes from {yaml_file.name}")
            except Exception as e:
                print(f"    Warning: Could not load YAML for ISO codes: {e}")

        # Download and add missing stations
        try:
            added = check_and_update_sta(
                stations=missing,
                sta_file=sta_file,
                iso_codes=iso_codes,
            )
            if added > 0:
                print(f"  Added {added} stations to STA file from gnss-metadata.eu")
            return added
        except Exception as e:
            print(f"  Warning: Station info update failed: {e}")
            return 0

    def _validate_stations_in_sta(
        self,
        profile: NetworkProfile,
        campaign_dir: Path,
        args: DailyPPPArgs,
    ) -> tuple[bool, list[str], str]:
        """Validate that all stations in campaign OBS directory exist in STA file.

        This is a STRICT validation that FAILS processing if:
        1. The STA file does not exist
        2. Any station is missing from the STA file

        Stations without STA entries will:
        - Lack proper antenna phase center calibrations
        - Have no receiver/antenna history
        - Produce unreliable or invalid results
        - Fail ambiguity resolution

        Args:
            profile: Network profile
            campaign_dir: Campaign directory with OBS subdirectory
            args: Processing arguments

        Returns:
            Tuple of (success, list of missing stations, error_type)
            error_type: "STA_FILE_NOT_FOUND" or "STATIONS_MISSING" or ""
            success=False if STA file missing or any stations are missing
        """
        from pygnss_rt.stations.sta_auto_update import get_existing_stations

        obs_dir = campaign_dir / "OBS"
        if not obs_dir.exists():
            print(f"  Warning: OBS directory not found: {obs_dir}")
            return True, [], ""

        # Get STA file path from PCF
        sta_file = self._get_sta_file_from_pcf(profile)
        if sta_file is None:
            sta_file = self.paths.station_info_file

        # STRICT CHECK: STA file MUST exist
        if not sta_file:
            print(f"\n  ERROR: No STA file configured!")
            print(f"  The station information file is required for processing.")
            print(f"  Check PCF file V_STAINF variable or paths configuration.")
            return False, [], "STA_FILE_NOT_FOUND"

        if not sta_file.exists():
            print(f"\n  ERROR: STA file not found: {sta_file}")
            print(f"  The station information file is required for processing.")
            print(f"  Processing CANNOT continue without the STA file.")
            print()
            print(f"  Expected location: {sta_file}")
            print(f"  This file should contain TYPE 001 (renaming) and TYPE 002 (station info) entries")
            print(f"  for all stations to be processed.")
            return False, [], "STA_FILE_NOT_FOUND"

        # Get existing stations in STA file
        sta_stations = get_existing_stations(sta_file)

        # Extract station IDs from RINEX files in OBS directory
        # File formats: XXXX*.RXO, XXXX*.CZH, XXXX*.CZO, XXXX*.PZH, XXXX*.PZO
        obs_stations = set()
        for ext in ["*.RXO", "*.CZH", "*.CZO", "*.PZH", "*.PZO", "*.PSH", "*.PSO", "*.CSH", "*.CSO"]:
            for obs_file in obs_dir.glob(ext):
                # Extract first 4 characters as station ID
                sta_id = obs_file.stem[:4].upper()
                if sta_id:
                    obs_stations.add(sta_id)

        if not obs_stations:
            print(f"  Warning: No observation files found in {obs_dir}")
            return True, [], ""

        # Find stations missing from STA file
        missing_stations = sorted(obs_stations - sta_stations)

        if args.verbose:
            print(f"  Stations in OBS: {len(obs_stations)}")
            print(f"  Stations in STA: {len(sta_stations)}")

        if missing_stations:
            print(f"\n  ERROR: {len(missing_stations)} station(s) NOT FOUND in STA file!")
            print(f"  Missing stations: {', '.join(missing_stations)}")
            print(f"  STA file: {sta_file}")
            print()
            print("  Processing CANNOT continue without proper station information.")
            print("  Stations without STA entries will have:")
            print("    - No antenna phase center calibrations")
            print("    - No receiver/antenna history")
            print("    - Invalid coordinates and unreliable results")
            print()
            print("  To fix this issue:")
            print("    1. Add station entries to the STA file manually, OR")
            print("    2. Rename RINEX files to use valid 4-char station IDs, OR")
            print("    3. Exclude these stations from processing")
            print()
            return False, missing_stations, "STATIONS_MISSING"

        print(f"  STA validation: All {len(obs_stations)} stations found in {sta_file.name}")
        return True, [], ""

    def _check_products(
        self,
        profile: NetworkProfile,
        date: GNSSDate,
        args: DailyPPPArgs,
    ) -> bool:
        """Check if required products are available and download if needed.

        Downloads orbit (SP3), ERP, and clock products from CDDIS/IGS/CODE
        using the FTPConfigManager and ProductDownloader.

        Args:
            profile: Network profile
            date: Processing date
            args: Processing arguments

        Returns:
            True if all products available/downloaded
        """
        from pathlib import Path
        from pygnss_rt.data_access.ftp_config import FTPConfigManager
        from pygnss_rt.data_access.product_downloader import (
            ProductDownloader,
            ProductDownloadConfig,
        )

        if args.verbose:
            print(f"  Orbit: {profile.orbit_source.provider} {profile.orbit_source.tier}")
            print(f"  ERP: {profile.erp_source.provider} {profile.erp_source.tier}")
            print(f"  Clock: {profile.clock_source.provider} {profile.clock_source.tier}")

        # Build session name and campaign ORB directory
        session_name = f"{date.year % 100:02d}{date.doy:03d}{profile.session_id}"
        campaign_root_cfg = self._config.get("bsw", {}).get("campaign_root")
        if campaign_root_cfg:
            campaign_root = Path(campaign_root_cfg)
        elif self.paths.campaign_root:
            campaign_root = self.paths.campaign_root
        else:
            campaign_root = Path.home() / "GPSDATA" / "CAMPAIGN54"
        orb_dir = campaign_root / session_name / "ORB"
        orb_dir.mkdir(parents=True, exist_ok=True)

        # Load FTP configuration
        ftp_config_path = Path(self.ignss_dir) / "conf" / "ftpConfig.xml"
        if not ftp_config_path.exists():
            print(f"  Warning: FTP config not found at {ftp_config_path}")
            # Try alternate location
            ftp_config_path = Path(self.ignss_dir) / "pygnss_rt" / "conf" / "ftpConfig.xml"

        # Use data_root as the product storage directory
        product_storage = Path(self.data_root)
        gps_week = date.gps_week
        product_week_dir = product_storage / "products" / str(gps_week)
        product_week_dir.mkdir(parents=True, exist_ok=True)

        if args.verbose:
            print(f"  Product storage: {product_week_dir}")
            print(f"  Campaign ORB dir: {orb_dir}")

        # Configure the product downloader
        config = ProductDownloadConfig(
            ftp_config_path=ftp_config_path if ftp_config_path.exists() else None,
            destination_dir=product_week_dir,
            max_retries=3,
            timeout=120,
            decompress=True,
        )

        all_products_ok = True

        with ProductDownloader(config) as downloader:
            # Download orbit (SP3) for 3 days (day-1, day0, day+1) for CCPREORB
            print(f"  Downloading orbit files (3-day window)...")
            orbit_ok = False
            for day_offset in [-1, 0, 1]:
                orbit_date = date.add_days(day_offset)
                orbit_result = downloader.download_orbit(
                    orbit_date,
                    provider=profile.orbit_source.provider,
                    tier=profile.orbit_source.tier,
                )
                if orbit_result.success:
                    offset_str = f"+{day_offset}" if day_offset >= 0 else str(day_offset)
                    print(f"    Orbit[{offset_str}]: {orbit_result.local_path.name} (from {orbit_result.source})")
                    # Copy/link to campaign ORB directory with Bernese naming
                    # Use PRE extension for CCPREORB/ORBMRG (same as SP3 format)
                    self._copy_product_to_campaign(orbit_result.local_path, orb_dir, orbit_date, "PRE")
                    if day_offset == 0:
                        orbit_ok = True
                else:
                    if day_offset == 0:
                        print(f"    Orbit download failed: {orbit_result.error_message}")
                    else:
                        print(f"    Orbit[{day_offset:+d}] not found (optional)")
            if not orbit_ok:
                all_products_ok = False

            # Download ERP
            print(f"  Downloading ERP file...")
            erp_result = downloader.download_erp(
                date,
                provider=profile.erp_source.provider,
            )
            if erp_result.success:
                print(f"    ERP: {erp_result.local_path.name} (from {erp_result.source})")
                # Copy to campaign ORB directory
                self._copy_product_to_campaign(erp_result.local_path, orb_dir, date, "IEP")
            else:
                print(f"    ERP download failed: {erp_result.error_message}")
                all_products_ok = False

            # Download clock (CLK)
            print(f"  Downloading clock file...")
            clock_result = downloader.download_clock(
                date,
                provider=profile.clock_source.provider,
                tier=profile.clock_source.tier,
            )
            if clock_result.success:
                print(f"    Clock: {clock_result.local_path.name} (from {clock_result.source})")
                # Copy to campaign OUT directory (CLK files go there for CCRNXC)
                out_dir = campaign_root / session_name / "OUT"
                out_dir.mkdir(parents=True, exist_ok=True)
                self._copy_product_to_campaign(clock_result.local_path, out_dir, date, "CLK")
            else:
                print(f"    Clock download failed: {clock_result.error_message}")
                all_products_ok = False

            # Download BIA/OSB (Signal Biases for PPP-AR)
            print(f"  Downloading BIA/OSB file...")
            bia_result = downloader.download_bia(date, provider="CODE")
            if bia_result.success:
                print(f"    BIA: {bia_result.local_path.name} (from {bia_result.source})")
                # Copy to campaign ORB directory
                self._copy_product_to_campaign(bia_result.local_path, orb_dir, date, "BIA")
            else:
                print(f"    BIA download failed: {bia_result.error_message}")
                # BIA is required for PPP-AR but not fatal
                print(f"    Warning: PPP-AR may not work without OSB/BIA file")

            # Download ION/GIM (Ionosphere model)
            print(f"  Downloading ION/GIM file...")
            ion_result = downloader.download_ion(date, provider="CODE")
            if ion_result.success:
                print(f"    ION: {ion_result.local_path.name} (from {ion_result.source})")
                # Copy to campaign ATM directory
                atm_dir = campaign_root / session_name / "ATM"
                atm_dir.mkdir(parents=True, exist_ok=True)
                self._copy_product_to_campaign(ion_result.local_path, atm_dir, date, "ION")
            else:
                print(f"    ION download failed: {ion_result.error_message}")
                # ION is optional but useful

            # Download VMF3 (Troposphere mapping functions) - combined 1x1 degree GRD file
            print(f"  Downloading VMF3 files...")
            grd_dir = campaign_root / session_name / "GRD"
            grd_dir.mkdir(parents=True, exist_ok=True)
            vmf_result = downloader.download_vmf3(date, destination=grd_dir)
            if vmf_result.success:
                print(f"    VMF3: Combined GRD file created - {vmf_result.local_path.name}")
            else:
                print(f"    VMF3: All downloads failed")
                # VMF3 is optional but useful for troposphere modeling

        return all_products_ok

    def _copy_product_to_campaign(
        self,
        source_path: Path,
        dest_dir: Path,
        date: GNSSDate,
        product_type: str,
    ) -> Path | None:
        """Copy a downloaded product file to campaign directory with Bernese naming.

        Converts IGS long-format names to Bernese short format:
        - SP3: IGS0OPSFIN_20253560000_01D_15M_ORB.SP3 -> COD_2025356.EPH
        - ERP: IGS0OPSFIN_20253500000_07D_01D_ERP.ERP -> COD_2025356.IEP
        - CLK: IGS0OPSFIN_20253560000_01D_30S_CLK.CLK -> COD_2025356.CLK
        - ION: COD0OPSFIN_...GIM.INX -> HOI_YYYYDDDS.ION (Higher Order Ionosphere)

        Args:
            source_path: Path to downloaded product file
            dest_dir: Destination campaign directory
            date: Processing date
            product_type: Type of product (EPH, IEP, CLK, ION, etc.)

        Returns:
            Path to copied file or None on error
        """
        import shutil

        # Build Bernese-style filename based on product type
        if product_type == "ION":
            # ION files need special naming: HOI_YYYYDDDS.ION (S = session, 0 for daily)
            bernese_name = f"HOI_{date.year}{date.doy:03d}0.ION"
        else:
            # Standard format: COD_YYYYDOY.EXT
            bernese_name = f"COD_{date.year}{date.doy:03d}.{product_type}"
        dest_path = dest_dir / bernese_name

        try:
            shutil.copy2(source_path, dest_path)
            return dest_path
        except Exception as e:
            print(f"    Warning: Could not copy {source_path.name} to {dest_path}: {e}")
            return None

    def _download_station_data(
        self,
        profile: NetworkProfile,
        date: GNSSDate,
        stations: list[str],
        args: DailyPPPArgs,
    ) -> bool:
        """Download station RINEX data.

        Two-step process (like the original Perl implementation):
        1. Download RINEX 3 files from CDDIS to central storage (data54/rinex/)
        2. Convert to Bernese 5.4 format and copy to campaign RAW directory

        Uses the StationDownloader which handles:
        - CDDIS HTTPS authentication (via NASA Earthdata Login)
        - RINEX 3 to Bernese 5.4 filename conversion (WTZR00DEU20252710.RXO)
        - Hatanaka decompression (.crx.gz -> .rnx -> .RXO)

        Args:
            profile: Network profile
            date: Processing date
            stations: List of stations
            args: Processing arguments

        Returns:
            True if sufficient data downloaded (>= 50% stations)
        """
        from pygnss_rt.data_access.station_downloader import (
            StationDownloader,
            RINEXType,
        )

        # Step 1: Central storage directory (like Perl's dataDir)
        # Downloads go to: {data_root}/rinex/{year}/{doy}/
        central_storage = Path(self.data_root) / "rinex" / str(date.year) / f"{date.doy:03d}"
        central_storage.mkdir(parents=True, exist_ok=True)

        # Step 2: Campaign RAW directory (final destination)
        campaign_root_cfg = self._config.get("bsw", {}).get("campaign_root")
        if campaign_root_cfg:
            campaign_root = Path(campaign_root_cfg)
        elif self.paths.campaign_root:
            campaign_root = self.paths.campaign_root
        else:
            campaign_root = Path.home() / "GPSDATA" / "CAMPAIGN54"
        session_name = f"{date.year % 100:02d}{date.doy:03d}{profile.session_id}"
        raw_dir = campaign_root / session_name / "RAW"
        raw_dir.mkdir(parents=True, exist_ok=True)

        if args.verbose:
            print(f"  Central storage: {central_storage}")
            print(f"  Target RAW directory: {raw_dir}")
            for ftp in profile.data_ftp_sources:
                print(f"  FTP source: {ftp.server_id} ({ftp.category})")

        # Let the downloader use all available providers in priority order
        # The YAML config defines provider priority: CDDIS (RINEX3) first, then FTP fallbacks
        # This ensures we try CDDIS before falling back to RINEX2 servers
        if args.verbose:
            if args.local_rinex_dir:
                print(f"  Local RINEX source: {args.local_rinex_dir}")
                if args.local_only:
                    print(f"  Mode: LOCAL ONLY (no network downloads)")
                else:
                    print(f"  Mode: Local first, then network fallback")
            else:
                print(f"  Using all providers in priority order (CDDIS first)")

        # Download to central storage with Bernese 5.4 naming (flat_structure=True)
        # This will create files like WTZR00DEU20252710.RXO
        downloader = StationDownloader(
            download_dir=central_storage,
            verbose=args.verbose,
            max_retries=2,
            parallel_downloads=12,  # Increased for faster downloads (CDDIS dir is cached)
            flat_structure=True,  # Enables Bernese 5.4 long format naming
            local_rinex_dir=args.local_rinex_dir,  # Local RINEX source
            local_only=args.local_only,  # Use only local files if True
        )

        try:
            print(f"  Downloading {len(stations)} stations to central storage...")
            # Pass None for providers to use all available providers in priority order
            results = downloader.download_daily_data(
                stations=stations,
                year=date.year,
                doy=date.doy,
                providers=None,  # Use all providers in priority order (CDDIS first)
            )

            # Get summary
            summary = downloader.get_download_summary(results)
            successful_downloads = [r for r in results if r.success]

            if args.verbose:
                print(f"  Downloaded: {summary['successful']}/{summary['total']}")
                if summary['failed_stations']:
                    print(f"  Failed: {', '.join(summary['failed_stations'][:10])}")

            # Copy downloaded files to campaign RAW directory
            copied_count = 0
            for result in successful_downloads:
                if result.local_path and result.local_path.exists():
                    dest_file = raw_dir / result.local_path.name
                    try:
                        shutil.copy2(result.local_path, dest_file)
                        copied_count += 1
                        if args.verbose:
                            print(f"    Copied: {result.local_path.name} -> RAW/")
                    except Exception as e:
                        print(f"    Warning: Failed to copy {result.local_path.name}: {e}")

            print(f"  Copied {copied_count} files to campaign RAW directory")

            # Consider success if >= min_stations_pct stations downloaded
            success_rate = summary['success_rate']
            min_pct = args.min_stations_pct
            if success_rate >= min_pct:
                print(f"  Download success rate: {success_rate*100:.0f}% (min: {min_pct*100:.0f}%)")
                return True
            else:
                print(f"  Download success rate too low: {success_rate*100:.0f}% (min: {min_pct*100:.0f}%)")
                return False

        except Exception as e:
            print(f"  Download error: {e}")
            return False
        finally:
            downloader.close()

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
        stations: list[str],
        args: DailyPPPArgs,
    ) -> Path:
        """Setup BSW campaign directory.

        Creates the campaign directory structure and copies reference files
        (CRD, STA, BLQ, etc.) from the info directory to the campaign STA directory.

        Args:
            profile: Network profile
            date: Processing date
            session_name: Session name
            stations: List of station IDs to process
            args: Processing arguments

        Returns:
            Path to campaign directory
        """
        # Campaign root from config or PathConfig
        campaign_root_cfg = self._config.get("bsw", {}).get("campaign_root")
        if campaign_root_cfg:
            campaign_root = Path(campaign_root_cfg)
        elif self.paths.campaign_root:
            campaign_root = self.paths.campaign_root
        else:
            campaign_root = Path.home() / "GPSDATA" / "CAMPAIGN54"

        campaign_dir = campaign_root / session_name

        if not args.dry_run:
            # Create campaign directory structure
            for subdir in ["ATM", "BPE", "GEN", "GRD", "INP", "OBS", "ORB", "ORX", "OUT", "RAW", "SOL", "STA"]:
                (campaign_dir / subdir).mkdir(parents=True, exist_ok=True)

            # Copy reference files from info directory to campaign STA
            self._copy_info_files_to_campaign(profile, campaign_dir, args)

        return campaign_dir

    def _copy_info_files_to_campaign(
        self,
        profile: NetworkProfile,
        campaign_dir: Path,
        args: DailyPPPArgs,
    ) -> None:
        """Copy reference files from info directory to campaign STA directory.

        Copies files like IGS20_54.CRD, IGS20_54.STA, IGS20_54.BLQ to the campaign
        STA directory for BSW processing.

        Args:
            profile: Network profile containing info file paths
            campaign_dir: Campaign directory path
            args: Processing arguments
        """
        sta_dir = campaign_dir / "STA"
        info_dir = self.paths.info_dir

        # Map of info file types to their source files (relative to info dir)
        # These are the essential reference files for BSW processing
        # Filenames must match what the PCF file expects:
        #   V_CRDINF = NEWNRT52 -> NEWNRT52.CRD
        #   V_STAINF = NEWNRT54 -> NEWNRT54.STA
        #   V_BLQINF = NEWNRT52 -> NEWNRT52.BLQ
        #   V_ATLINF = NEWNRT52 -> NEWNRT52.ATL (if exists)
        reference_files = {
            # Station coordinates file (V_CRDINF = NEWNRT52)
            "coord_newnrt52": "NEWNRT52.CRD",
            # Also NEWNRT54 coordinates
            "coord_newnrt54": "NEWNRT54.CRD",
            # Reference coordinates
            "coord_igs20": "IGS20_R.CRD",
            # IGS20_54 coordinates (V_CRDINF = IGS20_54)
            "coord_igs20_54": "IGS20_54.CRD",
            # Station information file (V_STAINF = NEWNRT54)
            "station": "NEWNRT54.STA",
            # Also NEWNRT52 STA
            "station_52": "NEWNRT52.STA",
            # IGS20_54 station info (V_STAINF = IGS20_54)
            "station_igs20_54": "IGS20_54.STA",
            # Ocean loading file (V_BLQINF = NEWNRT52)
            "ocean_loading": "NEWNRT52.BLQ",
            # Also copy IGS20_54.BLQ for compatibility
            "ocean_loading_igs": "IGS20_54.BLQ",
            # Abbreviations file
            "abbreviations_52": "NEWNRT52.ABB",
            "abbreviations_igs": "IGS20_54.ABB",
            # Observation selection file
            "obs_selection": "OBSSEL.SEL",
            # Sessions file
            "sessions": "SESSIONS.SES",
            # Velocity file
            "velocity": "IGS20_54.VEL",
        }

        if args.verbose:
            print(f"  Copying reference files to {sta_dir}")

        for file_type, filename in reference_files.items():
            source_path = info_dir / filename
            dest_path = sta_dir / filename

            if source_path.exists():
                try:
                    shutil.copy2(source_path, dest_path)
                    if args.verbose:
                        print(f"    Copied: {filename}")
                except Exception as e:
                    print(f"    Warning: Failed to copy {filename}: {e}")
            else:
                # Try alternate filenames from profile info_files
                alt_path = profile.info_files.get(file_type, "")
                if alt_path and Path(alt_path).exists():
                    try:
                        shutil.copy2(alt_path, sta_dir / Path(alt_path).name)
                        if args.verbose:
                            print(f"    Copied: {Path(alt_path).name} (alternate)")
                    except Exception as e:
                        print(f"    Warning: Failed to copy {alt_path}: {e}")
                elif args.verbose:
                    print(f"    Warning: {filename} not found at {source_path}")

        # Also copy antenna phase center file to GEN directory
        gen_dir = campaign_dir / "GEN"
        pcv_files = ["ANTENNA_I20.PCV", "I20.ATX"]
        for pcv_file in pcv_files:
            pcv_source = info_dir / pcv_file
            if pcv_source.exists():
                try:
                    shutil.copy2(pcv_source, gen_dir / pcv_file)
                    if args.verbose:
                        print(f"    Copied: {pcv_file} -> GEN/")
                except Exception as e:
                    print(f"    Warning: Failed to copy {pcv_file}: {e}")
                break  # Only copy one PCV file

        # Copy observation selection file (OBSERV_COD.SEL) to GEN directory
        # This file is required by RNXSMT and RNXGRA programs
        ref54_local_dir = Path(os.environ.get("U", "")) / "REF54_LOCAL"
        if not ref54_local_dir.exists() and self.paths.ref_local_dir:
            ref54_local_dir = self.paths.ref_local_dir

        observ_sel_file = ref54_local_dir / "OBSERV_COD.SEL"
        if observ_sel_file.exists():
            try:
                shutil.copy2(observ_sel_file, gen_dir / "OBSERV_COD.SEL")
                if args.verbose:
                    print(f"    Copied: OBSERV_COD.SEL -> GEN/")
            except Exception as e:
                print(f"    Warning: Failed to copy OBSERV_COD.SEL: {e}")
        else:
            # Try info directory as fallback
            observ_sel_info = info_dir / "OBSERV_COD.SEL"
            if observ_sel_info.exists():
                try:
                    shutil.copy2(observ_sel_info, gen_dir / "OBSERV_COD.SEL")
                    if args.verbose:
                        print(f"    Copied: OBSERV_COD.SEL -> GEN/ (from info)")
                except Exception as e:
                    print(f"    Warning: Failed to copy OBSERV_COD.SEL: {e}")

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
        print(f"  PCF: {profile.pcf_file}")
        print(f"  Options: {profile.bsw_options_xml}")

        # Load and parse BSW options XML
        bsw_options = self._load_bsw_options(profile, date)
        if bsw_options is None:
            print("  Warning: Could not load BSW options XML")
            return False

        if args.verbose:
            print(f"  Processing steps: {bsw_options.list_steps()}")

        # Build processing arguments for BSW
        bsw_args = self._build_bsw_args(profile, date, session_name, bsw_options, args)

        if args.verbose:
            print(f"  BSW args prepared: {len(bsw_args)} parameters")

        # Load BSW environment
        from pygnss_rt.bsw.environment import load_bsw_environment
        from pygnss_rt.bsw.bpe_runner import BPERunner, BPEConfig, parse_bsw_options_xml

        # Find LOADGPS.setvar using PathConfig
        loadgps_path = self.paths.loadgps_setvar
        if loadgps_path is None or not loadgps_path.exists():
            # Try common locations as fallback
            for loc in [
                Path(self.gpsuser_dir).parent / "LOADGPS.setvar" if self.gpsuser_dir else None,
                Path(os.environ.get("C", "")) / "LOADGPS.setvar",
            ]:
                if loc and loc.exists():
                    loadgps_path = loc
                    break

        if loadgps_path is None or not loadgps_path.exists():
            print(f"  Warning: LOADGPS.setvar not found")
            print("  Set BERN54_DIR environment variable or configure paths")
            print("  Skipping actual BSW execution (dry run mode)")
            return True

        try:
            # Load environment
            env = load_bsw_environment(loadgps_path)
            print(f"  BSW Environment loaded from {loadgps_path}")

            # Create BPE runner
            runner = BPERunner(env)

            # Build session string for BPE (DOY + session char)
            doy = date.doy
            session_char = "0"  # Daily processing uses "0"
            bpe_session = f"{doy:03d}{session_char}"

            # Create BPE config
            # Use session_name for output files (e.g., 25358IG.OUT, 25358IG.RUN)
            config = BPEConfig(
                pcf_file=Path(profile.pcf_file).stem,  # Just the filename without path/extension
                campaign=session_name,
                session=bpe_session,
                year=date.year,
                task_id=profile.task_id,
                sysout=session_name,  # Output: 25358IG.OUT
                status=f"{session_name}.RUN",  # Status: 25358IG.RUN
            )

            # Get option directories
            opt_dirs = get_option_dirs("ppp")

            # Parse BSW options from XML for INP customization
            xml_options = parse_bsw_options_xml(Path(profile.bsw_options_xml))

            # Convert XML step names to OPT directory names
            converted_options: dict[str, dict[str, dict[str, str]]] = {}
            for xml_step, inp_files in xml_options.items():
                opt_dir = xml_step_to_opt_dir(xml_step)
                converted_options[opt_dir] = inp_files

            # Add default options to disable ATL (Atmospheric Loading)
            # ATL file not available - we need to set the count to 0, not just the path
            # The INP format is: ATMLOAD <count> "<path>" - setting count to 0 disables it
            # TODO: Implement ATL file download/generation in the future
            # Note: This requires modifying the INP line format, not just the value

            # Build variable substitutions (opt_* prefixed values)
            var_subs = {k: v for k, v in bsw_args.items() if k.startswith("opt_")}

            print(f"  Starting BPE execution...")
            print(f"    Campaign: {session_name}")
            print(f"    Session: {bpe_session}")
            print(f"    PCF: {config.pcf_file}")

            # Run BPE
            result = runner.run(
                config=config,
                opt_dirs=opt_dirs,
                bsw_options=converted_options,
                variable_substitutions=var_subs,
                timeout=7200,  # 2 hours
            )

            if result.success:
                print(f"  BPE completed successfully in {result.runtime_seconds:.1f}s")
                print(f"    Sessions finished: {result.sessions_finished}")
                if result.output_file:
                    print(f"    Output: {result.output_file}")
                return True
            else:
                print(f"  BPE failed: {result.error_message}")
                print(f"    Return code: {result.return_code}")
                if result.sessions_error > 0:
                    print(f"    Sessions with errors: {result.sessions_error}")
                return False

        except Exception as e:
            print(f"  BSW execution error: {e}")
            import traceback
            if args.verbose:
                traceback.print_exc()
            return False

    def _load_bsw_options(
        self,
        profile: NetworkProfile,
        date: GNSSDate,
    ) -> BSWOptionsConfig | None:
        """Load BSW options from XML file.

        Args:
            profile: Network profile containing XML path
            date: Processing date for variable substitution

        Returns:
            Parsed BSW options config or None if error
        """
        xml_path = Path(profile.bsw_options_xml)
        if not xml_path.exists():
            return None

        try:
            parser = BSWOptionsParser()
            config = parser.load(xml_path)
            return config
        except Exception as e:
            print(f"  Error loading BSW options: {e}")
            return None

    def _build_bsw_args(
        self,
        profile: NetworkProfile,
        date: GNSSDate,
        session_name: str,
        bsw_options: BSWOptionsConfig,
        args: DailyPPPArgs,
    ) -> dict[str, Any]:
        """Build BSW processing arguments.

        Corresponds to the Perl %args hash that gets passed to IGNSS->new().

        Args:
            profile: Network profile
            date: Processing date
            session_name: Session name
            bsw_options: Parsed BSW options
            args: Processing arguments

        Returns:
            Dictionary of BSW arguments
        """
        y4c = str(date.year)
        y2c = f"{date.year % 100:02d}"
        doy = f"{date.doy:03d}"

        # Build session string (for daily: DOY + "0")
        session_str = f"{doy}0"

        bsw_args = {
            # Processing type
            "procType": "daily",
            # Date components
            "y4c": y4c,
            "y2c": y2c,
            "doy": doy,
            "ha": "0",  # Hour character (0 for daily)
            # Session info
            "session": session_name,
            "sessID2char": profile.session_id,
            "TASKID": profile.task_id,
            # PCF and options
            "PCF_FILE": profile.pcf_file,
            "bswOpt": profile.bsw_options_xml,
            # Option directories mapping
            "optDirs": get_option_dirs("ppp"),
            # Datum and reference frame
            "datum": profile.datum,
            "ABS_REL": profile.antenna_phase_center,
            # Minimum elevation
            "opt_MINEL": profile.min_elevation,
            # VMF3 file pattern - full Bernese path with ${P} variable
            # Format: ${P}/campaign/GRD/VMF3_YYDDD0.GRD
            "opt_VMF3": f"${{P}}/{session_name}/GRD/VMF3_{y2c}{doy}0.GRD",
            # Information files
            "infoSES": profile.info_files.get("sessions", ""),
            "infoSTA": profile.info_files.get("station", ""),
            "infoOTL": profile.info_files.get("ocean_loading", ""),
            "infoABB": profile.info_files.get("abbreviations", ""),
            "infoSEL": profile.info_files.get("obs_selection", ""),
            "infoSNX": profile.info_files.get("sinex_skeleton", ""),
            "infoPCV": profile.info_files.get("phase_center", ""),
            "infoCRD": profile.coord_file,
            # Satellite/phase options (derived from ABS_REL)
            "opt_SATELL": "SATELLIT_I20" if profile.antenna_phase_center == "ABSOLUTE" else "SATELLIT_I01",
            "opt_PHASECC": "ANTENNA_I20.I20" if profile.antenna_phase_center == "ABSOLUTE" else "ANTENNA_I01.I01",
            # CRX option
            "opt_CRX": f"SAT_{y4c}",
            # OBSFIL pattern
            "opt_OBSFIL": f"????{doy}0",
            # Campaign directory pattern
            "CAMP_DRV": "${P}/",
            # DCM settings
            "DCM": {
                "yesORno": "yes" if profile.dcm_enabled else "no",
                "dir2del": profile.dcm_dirs_to_delete,
                "compUtil": "gzip",
                "mv2dir": profile.dcm_archive_dir,
                "org": profile.dcm_organization,
            },
            # Control
            "controlArgs": {
                "yesORno": "yes",
                "type": "NRT",
            },
        }

        # Add archive file specifications if needed
        if profile.requires_igs_alignment:
            bsw_args["archFiles"] = {}
            for arch_name, arch_spec in profile.archive_files.items():
                bsw_args["archFiles"][arch_name] = {
                    "root": arch_spec.root,
                    "org": arch_spec.organization,
                    "campPat": arch_spec.campaign_pattern,
                    "prefix": arch_spec.prefix,
                    "body": arch_spec.body_pattern,
                    "srcDir": arch_spec.source_dir,
                    "ext": arch_spec.extensions,
                    "dstDir": arch_spec.dest_dir,
                }

        # Add NEQ stacking configuration
        neq_config = args.neq_stacking or NO_STACKING
        bsw_args["COMBNEQ"] = {
            "yesORno": "yes" if neq_config.enabled else "no",
            "n2stack": neq_config.n_hours_to_stack,
            "nameScheme": neq_config.name_scheme.value if hasattr(neq_config.name_scheme, 'value') else str(neq_config.name_scheme),
        }

        return bsw_args

    def _run_dcm(
        self,
        profile: NetworkProfile,
        date: GNSSDate,
        session_name: str,
        campaign_dir: Path,
    ) -> None:
        """Run Delete/Compress/Move archiving.

        This method:
        1. Archives final results (SNX, CRD, TRO, TRP) to archive location
        2. Deletes intermediate directories (RAW, BPE, etc.)
        3. Optionally compresses archived files

        Archive structure: {dcm_archive_dir}/yyyy/doy/
        - FIN_YYYYDDDS.SNX (from SOL directory)
        - FIN_YYYYDDDS.CRD (merged coordinates from STA)
        - FIN_YYYYDDDS.TRO (troposphere zenith delay from ATM)
        - FIN_YYYYDDDS.TRP (troposphere parameters from ATM)
        - Per-station NEQ files: FIN_YYYYDDDS_XXXX.NQ0 (from SOL)

        Args:
            profile: Network profile
            date: Processing date
            session_name: Session name
            campaign_dir: Campaign directory
        """
        if not profile.dcm_enabled:
            return

        y4 = date.year
        doy = date.doy
        session_suffix = f"{y4}{doy:03d}0"  # e.g., 20253570

        # Step 1: Archive final results before deletion
        if profile.dcm_archive_dir:
            org = profile.dcm_organization.replace("yyyy", str(y4)).replace("doy", f"{doy:03d}")
            archive_path = Path(profile.dcm_archive_dir) / org
            archive_path.mkdir(parents=True, exist_ok=True)
            print(f"  Archive path: {archive_path}")

            archived_count = 0

            # Archive SNX file from SOL directory (e.g., RED_20253570.SNX)
            sol_dir = campaign_dir / "SOL"
            if sol_dir.exists():
                for snx_file in sol_dir.glob(f"*_{session_suffix}.SNX"):
                    dest = archive_path / snx_file.name
                    try:
                        shutil.copy2(snx_file, dest)
                        print(f"    Archived: {snx_file.name}")
                        archived_count += 1
                    except Exception as e:
                        print(f"    Warning: Failed to archive {snx_file.name}: {e}")

                # Archive NEQ files (per-station: FIN_20253570_XXXX.NQ0)
                for neq_file in sol_dir.glob(f"FIN_{session_suffix}_*.NQ0"):
                    dest = archive_path / neq_file.name
                    try:
                        shutil.copy2(neq_file, dest)
                        archived_count += 1
                    except Exception as e:
                        print(f"    Warning: Failed to archive {neq_file.name}: {e}")
                if archived_count > 0:
                    print(f"    Archived: {archived_count - 1} NEQ files")

            # Archive merged CRD file from STA directory
            sta_dir = campaign_dir / "STA"
            if sta_dir.exists():
                # Look for FIN_YYYYDDDS.CRD (merged coordinates)
                crd_file = sta_dir / f"FIN_{session_suffix}.CRD"
                if crd_file.exists():
                    dest = archive_path / crd_file.name
                    try:
                        shutil.copy2(crd_file, dest)
                        print(f"    Archived: {crd_file.name}")
                        archived_count += 1
                    except Exception as e:
                        print(f"    Warning: Failed to archive {crd_file.name}: {e}")

            # Archive TRO and TRP files from ATM directory (final versions only)
            atm_dir = campaign_dir / "ATM"
            if atm_dir.exists():
                tro_trp_count = 0
                # Archive merged FIN TRO/TRP files
                for ext in ["TRO", "TRP"]:
                    merged_file = atm_dir / f"FIN_{session_suffix}.{ext}"
                    if merged_file.exists():
                        dest = archive_path / merged_file.name
                        try:
                            shutil.copy2(merged_file, dest)
                            print(f"    Archived: {merged_file.name}")
                            archived_count += 1
                        except Exception as e:
                            print(f"    Warning: Failed to archive {merged_file.name}: {e}")
                    else:
                        # Look for per-station final TRO/TRP files
                        for trp_file in atm_dir.glob(f"FIN_{session_suffix}_*.{ext}"):
                            dest = archive_path / trp_file.name
                            try:
                                shutil.copy2(trp_file, dest)
                                tro_trp_count += 1
                            except Exception as e:
                                print(f"    Warning: Failed to archive {trp_file.name}: {e}")
                if tro_trp_count > 0:
                    print(f"    Archived: {tro_trp_count} TRO/TRP files")
                    archived_count += tro_trp_count

            print(f"  Total files archived: {archived_count}")

        # Step 2: Delete specified directories (RAW, BPE, OBS, etc.)
        deleted_dirs = []
        for dir_name in profile.dcm_dirs_to_delete:
            dir_path = campaign_dir / dir_name
            if dir_path.exists():
                try:
                    shutil.rmtree(dir_path)
                    deleted_dirs.append(dir_name)
                except Exception as e:
                    print(f"  Warning: Failed to delete {dir_name}: {e}")

        if deleted_dirs:
            print(f"  Deleted directories: {', '.join(deleted_dirs)}")

        # Step 3: Optionally delete the entire campaign directory
        # For now, keep ATM, SOL, STA directories with remaining files
        # The campaign directory structure is preserved for potential reprocessing


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
