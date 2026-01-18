"""
Station adder module.

Adds new stations from an input file to YAML station configuration files.
Supports multiple input formats and validates against existing stations.

Usage:
    from pygnss_rt.stations.station_adder import StationAdder

    adder = StationAdder()
    added = adder.add_stations(
        input_file="new_stations.txt",
        target_yaml="eurefgh.yaml",
    )
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from pygnss_rt.core.paths import PathConfig, get_paths


# Country code mappings (3-letter to full name and 2-letter ISO)
COUNTRY_MAP = {
    # Europe
    "AUT": {"name": "Austria", "iso2": "AT"},
    "BEL": {"name": "Belgium", "iso2": "BE"},
    "BGR": {"name": "Bulgaria", "iso2": "BG"},
    "CHE": {"name": "Switzerland", "iso2": "CH"},
    "CZE": {"name": "Czech Republic", "iso2": "CZ"},
    "DEU": {"name": "Germany", "iso2": "DE"},
    "DNK": {"name": "Denmark", "iso2": "DK"},
    "ESP": {"name": "Spain", "iso2": "ES"},
    "EST": {"name": "Estonia", "iso2": "EE"},
    "FIN": {"name": "Finland", "iso2": "FI"},
    "FRA": {"name": "France", "iso2": "FR"},
    "GBR": {"name": "United Kingdom", "iso2": "GB"},
    "GRC": {"name": "Greece", "iso2": "GR"},
    "HRV": {"name": "Croatia", "iso2": "HR"},
    "HUN": {"name": "Hungary", "iso2": "HU"},
    "IRL": {"name": "Ireland", "iso2": "IE"},
    "ISL": {"name": "Iceland", "iso2": "IS"},
    "ITA": {"name": "Italy", "iso2": "IT"},
    "LTU": {"name": "Lithuania", "iso2": "LT"},
    "LUX": {"name": "Luxembourg", "iso2": "LU"},
    "LVA": {"name": "Latvia", "iso2": "LV"},
    "NLD": {"name": "Netherlands", "iso2": "NL"},
    "NOR": {"name": "Norway", "iso2": "NO"},
    "POL": {"name": "Poland", "iso2": "PL"},
    "PRT": {"name": "Portugal", "iso2": "PT"},
    "ROU": {"name": "Romania", "iso2": "RO"},
    "RUS": {"name": "Russia", "iso2": "RU"},
    "SVK": {"name": "Slovakia", "iso2": "SK"},
    "SVN": {"name": "Slovenia", "iso2": "SI"},
    "SWE": {"name": "Sweden", "iso2": "SE"},
    "UKR": {"name": "Ukraine", "iso2": "UA"},
    # Other regions
    "USA": {"name": "United States", "iso2": "US"},
    "CAN": {"name": "Canada", "iso2": "CA"},
    "AUS": {"name": "Australia", "iso2": "AU"},
    "NZL": {"name": "New Zealand", "iso2": "NZ"},
    "JPN": {"name": "Japan", "iso2": "JP"},
    "CHN": {"name": "China", "iso2": "CN"},
    "KOR": {"name": "South Korea", "iso2": "KR"},
    "ARG": {"name": "Argentina", "iso2": "AR"},
    "BRA": {"name": "Brazil", "iso2": "BR"},
    "CHL": {"name": "Chile", "iso2": "CL"},
    "ZAF": {"name": "South Africa", "iso2": "ZA"},
}


@dataclass
class NewStation:
    """New station data to be added."""

    id: str  # 4-character station ID
    long_id: str  # 9-character ID (e.g., ARLO00BEL)
    x: float  # ECEF X coordinate
    y: float  # ECEF Y coordinate
    z: float  # ECEF Z coordinate
    name: str = ""  # Station name (optional)
    domes: str = ""  # DOMES number (optional)
    country_code: str = ""  # 3-letter country code

    @property
    def country_info(self) -> dict[str, str]:
        """Get country name and ISO2 code from country code."""
        if self.country_code and self.country_code.upper() in COUNTRY_MAP:
            return COUNTRY_MAP[self.country_code.upper()]
        # Try to extract from long_id (last 3 chars)
        if len(self.long_id) >= 9:
            code = self.long_id[-3:].upper()
            if code in COUNTRY_MAP:
                return COUNTRY_MAP[code]
        return {"name": "Unknown", "iso2": "XX"}

    def to_yaml_dict(
        self,
        primary_net: str = "EUREF",
        provider: str = "EUREF",
        station_type: str = "EUREF",
        use_nrt: bool = True,
    ) -> dict[str, Any]:
        """Convert to YAML station dictionary format.

        Args:
            primary_net: Primary network name
            provider: Data provider
            station_type: Station type
            use_nrt: Enable NRT processing

        Returns:
            Dictionary in YAML station format
        """
        country = self.country_info
        return {
            "id": self.id.lower(),
            "domes": self.domes or self.long_id,
            "name": self.name or self.id.capitalize(),
            "primary_net": primary_net,
            "provider": provider,
            "type": station_type,
            "use_nrt": use_nrt,
            "country": f"({country['name']}) [{country['iso2']}]",
            "iso": self.country_code.upper() if self.country_code else self.long_id[-3:].upper(),
            "coordinates": {"x": self.x, "y": self.y, "z": self.z},
        }


class StationAdder:
    """Add new stations to YAML configuration files."""

    def __init__(self, paths: PathConfig | None = None):
        """Initialize station adder.

        Args:
            paths: PathConfig instance (uses global instance if None)
        """
        self.paths = paths or get_paths()

    def parse_input_file(self, input_file: Path | str) -> list[NewStation]:
        """Parse input file containing new station data.

        Supports multiple formats:
        1. Simple format: STATION_ID  X  Y  Z
        2. Extended format with long ID mapping and coordinates
        3. YAML format

        Args:
            input_file: Path to input file

        Returns:
            List of NewStation objects
        """
        input_path = Path(input_file)
        if not input_path.exists():
            raise FileNotFoundError(f"Input file not found: {input_path}")

        content = input_path.read_text()

        # Try YAML format first
        if input_path.suffix in [".yaml", ".yml"]:
            return self._parse_yaml_format(content)

        # Try the custom format (like new_sta)
        return self._parse_custom_format(content)

    def _parse_yaml_format(self, content: str) -> list[NewStation]:
        """Parse YAML format input file."""
        data = yaml.safe_load(content)
        stations = []

        if isinstance(data, dict) and "stations" in data:
            for stn in data["stations"]:
                coords = stn.get("coordinates", {})
                stations.append(NewStation(
                    id=stn.get("id", "").upper(),
                    long_id=stn.get("long_id", stn.get("domes", "")),
                    x=coords.get("x", 0),
                    y=coords.get("y", 0),
                    z=coords.get("z", 0),
                    name=stn.get("name", ""),
                    domes=stn.get("domes", ""),
                    country_code=stn.get("country_code", ""),
                ))

        return stations

    def _parse_custom_format(self, content: str) -> list[NewStation]:
        """Parse custom format (like new_sta file).

        Expected format:
        - Lines with long ID mapping: station_id  LONG_ID (e.g., arlo  ARLO00BEL)
        - Lines with coordinates: STATION_ID  X  Y  Z
        """
        stations = []
        long_id_map = {}
        coord_map = {}

        for line in content.strip().split("\n"):
            line = line.strip()
            if not line or line.startswith("#") or line.startswith("$"):
                continue

            # Skip header lines
            if "LONG_NAME_MAP" in line or line in ["{", "}"]:
                continue

            parts = line.split()
            if len(parts) >= 2:
                # Check if this is a long ID mapping (2 parts, second is 9+ chars)
                if len(parts) == 2 and len(parts[1]) >= 9:
                    station_id = parts[0].upper()
                    long_id = parts[1].upper()
                    long_id_map[station_id] = long_id

                # Check if this is coordinates (4 parts, last 3 are numbers)
                elif len(parts) >= 4:
                    try:
                        station_id = parts[0].upper()
                        x = float(parts[1])
                        y = float(parts[2])
                        z = float(parts[3])
                        coord_map[station_id] = (x, y, z)
                    except ValueError:
                        continue

        # Combine mappings and coordinates
        for station_id, (x, y, z) in coord_map.items():
            long_id = long_id_map.get(station_id, f"{station_id}00XXX")
            country_code = long_id[-3:] if len(long_id) >= 9 else ""

            stations.append(NewStation(
                id=station_id,
                long_id=long_id,
                x=x,
                y=y,
                z=z,
                name="",
                domes=long_id,
                country_code=country_code,
            ))

        return stations

    def get_existing_stations(self, yaml_file: Path | str) -> set[str]:
        """Get set of existing station IDs from YAML file.

        Args:
            yaml_file: Path to YAML file

        Returns:
            Set of station IDs (uppercase)
        """
        yaml_path = Path(yaml_file)
        if not yaml_path.exists():
            return set()

        with open(yaml_path, 'r') as f:
            data = yaml.safe_load(f)

        if not data or "stations" not in data:
            return set()

        return {stn.get("id", "").upper() for stn in data["stations"]}

    def add_stations(
        self,
        input_file: Path | str,
        target_yaml: Path | str,
        primary_net: str = "EUREF",
        provider: str = "EUREF",
        station_type: str = "EUREF",
        use_nrt: bool = True,
        dry_run: bool = False,
    ) -> tuple[int, list[str], list[str]]:
        """Add new stations from input file to target YAML.

        Args:
            input_file: Path to input file with new stations
            target_yaml: Target YAML file (name or full path)
            primary_net: Primary network name
            provider: Data provider
            station_type: Station type
            use_nrt: Enable NRT processing
            dry_run: If True, don't actually modify the file

        Returns:
            Tuple of (num_added, list_of_added_ids, list_of_skipped_ids)
        """
        input_path = Path(input_file)

        # Resolve target YAML path
        target_path = Path(target_yaml)
        if not target_path.is_absolute():
            target_path = self.paths.station_data_dir / target_yaml

        if not target_path.exists():
            raise FileNotFoundError(f"Target YAML not found: {target_path}")

        # Parse new stations
        new_stations = self.parse_input_file(input_path)
        if not new_stations:
            print("No stations found in input file")
            return 0, [], []

        # Get existing stations
        existing = self.get_existing_stations(target_path)

        # Filter out duplicates
        to_add = []
        skipped = []
        for stn in new_stations:
            if stn.id.upper() in existing:
                skipped.append(stn.id.upper())
            else:
                to_add.append(stn)

        if not to_add:
            print("All stations already exist in target file")
            return 0, [], list(skipped)

        if dry_run:
            print(f"[DRY RUN] Would add {len(to_add)} stations:")
            for stn in to_add:
                print(f"  {stn.id}: {stn.x:.4f}, {stn.y:.4f}, {stn.z:.4f}")
            return len(to_add), [s.id.upper() for s in to_add], skipped

        # Load existing YAML
        with open(target_path, 'r') as f:
            data = yaml.safe_load(f)

        if data is None:
            data = {"metadata": {}, "stations": []}

        # Add new stations
        for stn in to_add:
            station_dict = stn.to_yaml_dict(
                primary_net=primary_net,
                provider=provider,
                station_type=station_type,
                use_nrt=use_nrt,
            )
            data["stations"].append(station_dict)

        # Update station count in header comment
        new_count = len(data["stations"])

        # Write updated YAML
        # First read the file to preserve header comments
        original_content = target_path.read_text()
        header_lines = []
        for line in original_content.split("\n"):
            if line.startswith("#"):
                # Update station count if present
                if "Stations:" in line:
                    line = f"# Stations: {new_count}"
                header_lines.append(line)
            else:
                break

        # Write with preserved header
        with open(target_path, 'w') as f:
            # Write header comments
            for line in header_lines:
                f.write(line + "\n")
            f.write("\n")

            # Write metadata and stations
            yaml.dump(
                data,
                f,
                default_flow_style=None,
                allow_unicode=True,
                sort_keys=False,
            )

        added_ids = [s.id.upper() for s in to_add]
        return len(to_add), added_ids, skipped

    def list_yaml_files(self) -> list[Path]:
        """List available YAML station files.

        Returns:
            List of YAML file paths
        """
        return sorted(self.paths.station_data_dir.glob("*.yaml"))


def add_stations_interactive(
    input_file: str,
    target_yaml: str | None = None,
    dry_run: bool = False,
) -> None:
    """Interactive station addition.

    Args:
        input_file: Path to input file
        target_yaml: Target YAML file (optional, will prompt if not provided)
        dry_run: If True, don't actually modify files
    """
    adder = StationAdder()

    # Parse input file first
    try:
        new_stations = adder.parse_input_file(input_file)
    except Exception as e:
        print(f"Error parsing input file: {e}")
        return

    print(f"\nFound {len(new_stations)} stations in input file:")
    for stn in new_stations:
        country = stn.country_info
        print(f"  {stn.id}: {country['name']} ({stn.x:.4f}, {stn.y:.4f}, {stn.z:.4f})")

    # List available YAML files if target not specified
    if target_yaml is None:
        print("\nAvailable YAML files:")
        yaml_files = adder.list_yaml_files()
        for i, path in enumerate(yaml_files, 1):
            existing = len(adder.get_existing_stations(path))
            print(f"  {i}. {path.name} ({existing} stations)")
        print("\nSpecify target with --target option")
        return

    # Add stations
    num_added, added, skipped = adder.add_stations(
        input_file=input_file,
        target_yaml=target_yaml,
        dry_run=dry_run,
    )

    print(f"\nResults:")
    print(f"  Added: {num_added} stations")
    if added:
        print(f"    {', '.join(added)}")
    if skipped:
        print(f"  Skipped (already exist): {len(skipped)} stations")
        print(f"    {', '.join(skipped)}")


if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        add_stations_interactive(sys.argv[1], sys.argv[2] if len(sys.argv) > 2 else None)
    else:
        print("Usage: python station_adder.py <input_file> [target_yaml]")
