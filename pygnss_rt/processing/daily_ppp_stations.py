"""Station management mixin for DailyPPPProcessor."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any, TYPE_CHECKING

from pygnss_rt.processing.networks import NetworkProfile

if TYPE_CHECKING:
    from pygnss_rt.processing.daily_ppp import DailyPPPArgs


class StationMixin:
    """Station management methods for DailyPPPProcessor."""

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
