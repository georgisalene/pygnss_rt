"""
Baseline (BSL) File Parser

Parses Bernese-style BSL baseline definition files.
Example file: BSL_20253020.BSL.gz

File structure:
- Each line contains a station pair forming a baseline
- Format: STATION1  STATION2

Example:
0531             ENTZ
BRUX             EUSK
WARN             WSRT
"""

import os
import gzip
from pathlib import Path
from dataclasses import dataclass
from typing import Optional
import math

from pygnss_rt.stations.coordinates import xyz_to_llh


@dataclass
class Baseline:
    """A baseline between two GNSS stations"""
    station1: str
    station2: str
    # Coordinates (filled in from CRD file)
    lat1: Optional[float] = None
    lon1: Optional[float] = None
    lat2: Optional[float] = None
    lon2: Optional[float] = None
    # Baseline length in km
    length_km: Optional[float] = None

    def compute_length(self) -> float:
        """Compute baseline length using Haversine formula"""
        if None in (self.lat1, self.lon1, self.lat2, self.lon2):
            return 0.0

        R = 6371  # Earth radius in km

        lat1_rad = math.radians(self.lat1)
        lat2_rad = math.radians(self.lat2)
        dlat = math.radians(self.lat2 - self.lat1)
        dlon = math.radians(self.lon2 - self.lon1)

        a = math.sin(dlat/2)**2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlon/2)**2
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))

        self.length_km = R * c
        return self.length_km


def parse_bsl_file(filepath: str) -> list[Baseline]:
    """
    Parse a Bernese BSL baseline file.

    Args:
        filepath: Path to the BSL file (can be .gz compressed)

    Returns:
        List of Baseline objects
    """
    baselines = []

    if not os.path.exists(filepath):
        return baselines

    # Handle gzip compressed files
    if filepath.endswith('.gz'):
        open_func = lambda f: gzip.open(f, 'rt')
    else:
        open_func = lambda f: open(f, 'r')

    with open_func(filepath) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue

            parts = line.split()
            if len(parts) >= 2:
                station1 = parts[0].strip()
                station2 = parts[1].strip()

                # Skip if stations are the same
                if station1 != station2:
                    baselines.append(Baseline(
                        station1=station1,
                        station2=station2
                    ))

    return baselines


def get_bsl_path(base_dir: str, year: int, doy: int) -> Optional[str]:
    """
    Get the path to a BSL file.

    Args:
        base_dir: Base directory for DD results
        year: Year
        doy: Day of year

    Returns:
        Path to BSL file if it exists, None otherwise
    """
    patterns = [
        f"{base_dir}/{year}/{doy}/STA/BSL_{year}{doy:03d}0.BSL.gz",
        f"{base_dir}/{year}/{doy:03d}/STA/BSL_{year}{doy:03d}0.BSL.gz",
        f"{base_dir}/{year}/{doy}/STA/BSL_{year}{doy:03d}0.BSL",
        f"{base_dir}/{year}/{doy:03d}/STA/BSL_{year}{doy:03d}0.BSL",
    ]

    for path in patterns:
        if os.path.exists(path):
            return path

    return None


def parse_crd_for_baselines(crd_path: str) -> dict[str, tuple[float, float, float]]:
    """
    Parse a CRD file and return station coordinates.

    Args:
        crd_path: Path to the CRD file (can be .gz compressed)

    Returns:
        Dictionary mapping station_id to (lat, lon, height)
    """
    coords = {}

    if not os.path.exists(crd_path):
        return coords

    # Handle gzip compressed files
    if crd_path.endswith('.gz'):
        open_func = lambda f: gzip.open(f, 'rt')
    else:
        open_func = lambda f: open(f, 'r')

    with open_func(crd_path) as f:
        in_data = False
        for line in f:
            if 'NUM  STATION NAME' in line:
                in_data = True
                continue

            if not in_data or not line.strip():
                continue

            parts = line.split()
            if len(parts) >= 5:
                try:
                    # Station name is second field, take first 4 chars for matching
                    station_full = parts[1]
                    station_4char = station_full[:4].upper()

                    # Find XYZ coordinates - collect consecutive floats
                    # Note: Any coordinate can be small depending on station location:
                    #   - Y small: near prime meridian (e.g., LROC, HERS)
                    #   - Z small: near equator (e.g., MBAR, NKLG)
                    #   - X small: near 90°E/W (e.g., NRIL, GUAT)
                    #   - X,Z small: Galapagos (GALA, GLPS)
                    float_values = []
                    for p in parts[2:]:
                        try:
                            val = float(p)
                            float_values.append(val)
                            if len(float_values) == 3:
                                break
                        except ValueError:
                            # Non-float encountered, reset if we haven't found 3 yet
                            if len(float_values) < 3:
                                float_values = []
                            continue

                    # Validate using vector magnitude (Earth radius ~6.371 million meters)
                    # Valid range: 6.0 to 6.8 million meters to account for altitude variations
                    xyz_values = float_values
                    if len(xyz_values) == 3:
                        magnitude = math.sqrt(sum(v**2 for v in xyz_values))
                        is_valid_earth_coord = 6000000 < magnitude < 6800000
                    else:
                        is_valid_earth_coord = False

                    if is_valid_earth_coord:
                        lat, lon, height = xyz_to_llh(*xyz_values)
                        # Store with both full name and 4-char code
                        coords[station_full.upper()] = (lat, lon, height)
                        if station_4char != station_full.upper():
                            coords[station_4char] = (lat, lon, height)

                except (ValueError, IndexError):
                    continue

    return coords


def load_baselines_with_coords(bsl_path: str, crd_path: str) -> list[Baseline]:
    """
    Load baselines and populate with coordinates from CRD file.

    Args:
        bsl_path: Path to BSL file
        crd_path: Path to CRD file

    Returns:
        List of Baseline objects with coordinates filled in
    """
    baselines = parse_bsl_file(bsl_path)
    coords = parse_crd_for_baselines(crd_path)

    for bl in baselines:
        # Try to match station names (handle 4-char vs full names)
        sta1_upper = bl.station1.upper()
        sta2_upper = bl.station2.upper()

        # Try exact match first, then 4-char match
        for sta_key in [sta1_upper, sta1_upper[:4]]:
            if sta_key in coords:
                bl.lat1, bl.lon1, _ = coords[sta_key]
                break

        for sta_key in [sta2_upper, sta2_upper[:4]]:
            if sta_key in coords:
                bl.lat2, bl.lon2, _ = coords[sta_key]
                break

        # Compute length if we have both coordinates
        if bl.lat1 is not None and bl.lat2 is not None:
            bl.compute_length()

    return baselines


def get_baseline_statistics(baselines: list[Baseline]) -> dict:
    """
    Compute statistics for a set of baselines.

    Returns:
        Dictionary with statistics
    """
    valid_baselines = [b for b in baselines if b.length_km is not None and b.length_km > 0]

    if not valid_baselines:
        return {
            'total_baselines': len(baselines),
            'valid_baselines': 0,
            'min_length_km': 0,
            'max_length_km': 0,
            'mean_length_km': 0,
            'unique_stations': 0
        }

    lengths = [b.length_km for b in valid_baselines]
    stations = set()
    for b in baselines:
        stations.add(b.station1)
        stations.add(b.station2)

    return {
        'total_baselines': len(baselines),
        'valid_baselines': len(valid_baselines),
        'min_length_km': min(lengths),
        'max_length_km': max(lengths),
        'mean_length_km': sum(lengths) / len(lengths),
        'unique_stations': len(stations)
    }


if __name__ == '__main__':
    # Test with example files
    import sys

    if len(sys.argv) >= 3:
        bsl_path = sys.argv[1]
        crd_path = sys.argv[2]
    else:
        # Default test paths
        bsl_path = "/home/ahunegnaw/GPSRESULTS/ahunegnaw/OCT2025_MGX/2025/302/STA/BSL_20253020.BSL.gz"
        crd_path = "/home/ahunegnaw/GPSRESULTS/ahunegnaw/OCT2025_MGX/2025/302/STA/F10_20253020.CRD.gz"

    print(f"Parsing BSL: {bsl_path}")
    print(f"Parsing CRD: {crd_path}")
    print()

    baselines = load_baselines_with_coords(bsl_path, crd_path)
    stats = get_baseline_statistics(baselines)

    print(f"Total baselines: {stats['total_baselines']}")
    print(f"Valid baselines (with coords): {stats['valid_baselines']}")
    print(f"Unique stations: {stats['unique_stations']}")
    print(f"Length range: {stats['min_length_km']:.1f} - {stats['max_length_km']:.1f} km")
    print(f"Mean length: {stats['mean_length_km']:.1f} km")
    print()

    print("Sample baselines:")
    for bl in baselines[:10]:
        if bl.length_km:
            print(f"  {bl.station1} - {bl.station2}: {bl.length_km:.1f} km")
        else:
            print(f"  {bl.station1} - {bl.station2}: (no coords)")
