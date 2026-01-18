"""
BLQ (Ocean Loading) file checker.

Checks that all stations in the configuration have entries in the BLQ file.
Outputs missing stations with coordinates in the format needed for the
Chalmers Ocean Loading Provider (http://holt.oso.chalmers.se/loading/).

Usage:
    from pygnss_rt.stations.blq_checker import BLQChecker

    checker = BLQChecker()
    missing = checker.find_missing_stations()
    checker.print_report(missing)
    checker.print_ocean_loading_format(missing)
"""

from __future__ import annotations

import math
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from pygnss_rt.core.paths import PathConfig, get_paths


@dataclass
class StationInfo:
    """Station information with coordinates."""

    id: str
    name: str
    domes: str
    x: float
    y: float
    z: float
    source_file: str

    @property
    def lat_lon_height(self) -> tuple[float, float, float]:
        """Convert ECEF to geodetic coordinates (WGS84).

        Returns:
            Tuple of (latitude_deg, longitude_deg, height_m)
        """
        return ecef_to_llh(self.x, self.y, self.z)


def ecef_to_llh(x: float, y: float, z: float) -> tuple[float, float, float]:
    """Convert ECEF coordinates to geodetic (lat, lon, height).

    Uses WGS84 ellipsoid parameters.

    Args:
        x: ECEF X coordinate in meters
        y: ECEF Y coordinate in meters
        z: ECEF Z coordinate in meters

    Returns:
        Tuple of (latitude_deg, longitude_deg, height_m)
    """
    # WGS84 parameters
    a = 6378137.0  # semi-major axis
    f = 1 / 298.257223563  # flattening
    b = a * (1 - f)  # semi-minor axis
    e2 = (a**2 - b**2) / a**2  # first eccentricity squared

    # Calculate longitude
    lon = math.atan2(y, x)

    # Iterative calculation for latitude and height
    p = math.sqrt(x**2 + y**2)
    lat = math.atan2(z, p * (1 - e2))  # initial guess

    # Iterate to convergence
    for _ in range(10):
        N = a / math.sqrt(1 - e2 * math.sin(lat)**2)
        h = p / math.cos(lat) - N
        lat = math.atan2(z, p * (1 - e2 * N / (N + h)))

    N = a / math.sqrt(1 - e2 * math.sin(lat)**2)
    h = p / math.cos(lat) - N

    return math.degrees(lat), math.degrees(lon), h


class BLQChecker:
    """Check BLQ file coverage for configured stations.

    Compares stations defined in YAML configuration files against
    entries in BLQ (ocean loading) files to find missing stations.
    """

    def __init__(self, paths: PathConfig | None = None):
        """Initialize BLQ checker.

        Args:
            paths: PathConfig instance (uses global instance if None)
        """
        self.paths = paths or get_paths()

    def get_blq_stations(self, blq_files: list[Path] | None = None) -> set[str]:
        """Parse BLQ files and extract station names.

        Args:
            blq_files: List of BLQ files to parse. If None, uses default locations.

        Returns:
            Set of station IDs (uppercase) found in BLQ files
        """
        if blq_files is None:
            blq_files = self._get_default_blq_files()

        stations = set()
        for blq_file in blq_files:
            if blq_file.exists():
                stations.update(self._parse_blq_file(blq_file))

        return stations

    def _get_default_blq_files(self) -> list[Path]:
        """Get default BLQ file paths."""
        blq_files = []

        # Check station_data_dir
        if self.paths.station_data_dir:
            for name in ["IGS20_54.BLQ", "NEWNRT52.BLQ"]:
                path = self.paths.station_data_dir / name
                if path.exists():
                    blq_files.append(path)

        # Check info_dir
        if self.paths.info_dir:
            for name in ["IGS20_54.BLQ", "NEWNRT52.BLQ"]:
                path = self.paths.info_dir / name
                if path.exists() and path not in blq_files:
                    blq_files.append(path)

        return blq_files

    def _parse_blq_file(self, blq_file: Path) -> set[str]:
        """Parse a single BLQ file and extract station names.

        BLQ format has station names on lines starting with 2 spaces
        followed by 4-character station code.

        Args:
            blq_file: Path to BLQ file

        Returns:
            Set of station IDs (uppercase)
        """
        stations = set()
        try:
            with open(blq_file, 'r') as f:
                for line in f:
                    # Station names: 2 spaces + 4-char code
                    match = re.match(r'^  ([A-Z0-9]{4})', line)
                    if match:
                        stations.add(match.group(1).upper())
        except Exception as e:
            print(f"Warning: Could not parse BLQ file {blq_file}: {e}")

        return stations

    def get_yaml_stations(self, yaml_dir: Path | None = None) -> dict[str, StationInfo]:
        """Parse YAML station files and extract station info.

        Args:
            yaml_dir: Directory containing YAML files. If None, uses station_data_dir.

        Returns:
            Dictionary mapping station ID to StationInfo
        """
        if yaml_dir is None:
            yaml_dir = self.paths.station_data_dir

        stations = {}
        yaml_files = list(yaml_dir.glob("*.yaml"))

        for yaml_file in yaml_files:
            try:
                file_stations = self._parse_yaml_file(yaml_file)
                # Update, preferring first occurrence
                for stn_id, info in file_stations.items():
                    if stn_id not in stations:
                        stations[stn_id] = info
            except Exception as e:
                print(f"Warning: Could not parse {yaml_file}: {e}")

        return stations

    def _parse_yaml_file(self, yaml_file: Path) -> dict[str, StationInfo]:
        """Parse a single YAML station file.

        Args:
            yaml_file: Path to YAML file

        Returns:
            Dictionary mapping station ID to StationInfo
        """
        stations = {}

        with open(yaml_file, 'r') as f:
            data = yaml.safe_load(f)

        if not data or 'stations' not in data:
            return stations

        for stn in data['stations']:
            stn_id = stn.get('id', '').upper()
            coords = stn.get('coordinates', {})

            if not stn_id:
                continue

            x = coords.get('x', 0)
            y = coords.get('y', 0)
            z = coords.get('z', 0)

            if x and y and z:
                stations[stn_id] = StationInfo(
                    id=stn_id,
                    name=stn.get('name', ''),
                    domes=stn.get('domes', ''),
                    x=x,
                    y=y,
                    z=z,
                    source_file=yaml_file.name,
                )

        return stations

    def find_missing_stations(
        self,
        blq_files: list[Path] | None = None,
        yaml_dir: Path | None = None,
    ) -> dict[str, StationInfo]:
        """Find stations that are missing from BLQ files.

        Args:
            blq_files: BLQ files to check against
            yaml_dir: Directory with YAML station files

        Returns:
            Dictionary of missing stations (ID -> StationInfo)
        """
        blq_stations = self.get_blq_stations(blq_files)
        yaml_stations = self.get_yaml_stations(yaml_dir)

        missing = {}
        for stn_id, info in yaml_stations.items():
            if stn_id not in blq_stations:
                missing[stn_id] = info

        return missing

    def print_report(
        self,
        missing: dict[str, StationInfo] | None = None,
        verbose: bool = False,
    ) -> None:
        """Print a report of missing BLQ stations.

        Args:
            missing: Missing stations dict. If None, will find them.
            verbose: Print additional details
        """
        if missing is None:
            missing = self.find_missing_stations()

        blq_stations = self.get_blq_stations()
        yaml_stations = self.get_yaml_stations()

        print(f"\n{'=' * 70}")
        print("BLQ (OCEAN LOADING) COVERAGE CHECK")
        print(f"{'=' * 70}")
        print(f"\nStations in BLQ files: {len(blq_stations)}")
        print(f"Stations in YAML files: {len(yaml_stations)}")
        print(f"Stations missing from BLQ: {len(missing)}")

        if not missing:
            print("\nAll stations have BLQ entries!")
            return

        print(f"\n{'=' * 70}")
        print(f"MISSING STATIONS ({len(missing)})")
        print(f"{'=' * 70}")
        print(f"\n{'Station':<8} {'Lat':>10} {'Lon':>11} {'Height':>10}  {'DOMES':<12} Name")
        print("-" * 70)

        for stn_id in sorted(missing.keys()):
            info = missing[stn_id]
            lat, lon, h = info.lat_lon_height
            name = info.name[:25] if info.name else ""
            print(f"{stn_id:<8} {lat:>10.5f} {lon:>11.5f} {h:>10.2f}  {info.domes:<12} {name}")

    def print_ocean_loading_format(
        self,
        missing: dict[str, StationInfo] | None = None,
    ) -> None:
        """Print missing stations in format for ocean loading website.

        Output format is suitable for http://holt.oso.chalmers.se/loading/

        Args:
            missing: Missing stations dict. If None, will find them.
        """
        if missing is None:
            missing = self.find_missing_stations()

        if not missing:
            return

        print(f"\n{'=' * 70}")
        print("FORMAT FOR http://holt.oso.chalmers.se/loading/")
        print("(Copy this to the 'station list' input box)")
        print(f"{'=' * 70}\n")

        for stn_id in sorted(missing.keys()):
            info = missing[stn_id]
            lat, lon, h = info.lat_lon_height
            # Format: STATION_NAME  latitude  longitude  height
            print(f"{stn_id}  {lat:.6f}  {lon:.6f}  {h:.3f}")

    def get_ocean_loading_format(
        self,
        missing: dict[str, StationInfo] | None = None,
    ) -> str:
        """Get missing stations formatted for ocean loading website.

        Args:
            missing: Missing stations dict. If None, will find them.

        Returns:
            String with stations in ocean loading format
        """
        if missing is None:
            missing = self.find_missing_stations()

        lines = []
        for stn_id in sorted(missing.keys()):
            info = missing[stn_id]
            lat, lon, h = info.lat_lon_height
            lines.append(f"{stn_id}  {lat:.6f}  {lon:.6f}  {h:.3f}")

        return "\n".join(lines)

    def check_stations(
        self,
        stations: list[str],
        blq_files: list[Path] | None = None,
    ) -> tuple[bool, list[str]]:
        """Check if specific stations have BLQ entries.

        Args:
            stations: List of station IDs to check
            blq_files: BLQ files to check against

        Returns:
            Tuple of (all_present, list_of_missing)
        """
        blq_stations = self.get_blq_stations(blq_files)

        missing = []
        for stn in stations:
            if stn.upper() not in blq_stations:
                missing.append(stn.upper())

        return len(missing) == 0, missing


def check_blq_coverage(verbose: bool = False) -> dict[str, StationInfo]:
    """Convenience function to check BLQ coverage.

    Args:
        verbose: Print detailed output

    Returns:
        Dictionary of missing stations
    """
    checker = BLQChecker()
    missing = checker.find_missing_stations()
    checker.print_report(missing, verbose=verbose)
    checker.print_ocean_loading_format(missing)
    return missing


if __name__ == "__main__":
    # Run standalone
    check_blq_coverage(verbose=True)
