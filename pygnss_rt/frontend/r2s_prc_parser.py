"""
R2S PRC File Parser - Coordinate Repeatability

Parses Bernese R2S_*.PRC.gz protocol files to extract coordinate repeatability
from PART 9: SLIDING 7-SESSION COMPARISON OF STATION COORDINATES.

Example output format:
 ----------------------------------------------------
                       Weekday     Repeatability (mm)
 Station        #Days  0123456      N      E      U
 ----------------------------------------------------
 0531               7  XXXXXXX     1.23   0.89   2.45
 BOR1               7  XXXXXXX     0.98   0.76   1.87
 ----------------------------------------------------
 Total             38              1.05   0.82   2.10
 ----------------------------------------------------

Author: Addisu Hunegnaw
Date: January 2026
"""

import os
import gzip
import math
from pathlib import Path
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class StationRepeatabilityR2S:
    """Coordinate repeatability for a single station from R2S PRC file"""
    station: str
    n_days: int
    weekday_pattern: str  # e.g., "XXXXXXX" or "X X XXX"
    north_mm: float
    east_mm: float
    up_mm: float

    @property
    def wrms_3d(self) -> float:
        """Calculate 3D WRMS from NEU components"""
        return math.sqrt(self.north_mm**2 + self.east_mm**2 + self.up_mm**2)


@dataclass
class CoordinateRepeatability:
    """Complete coordinate repeatability results from R2S PRC file"""
    year: int
    doy: int
    total_stations: int
    stations: list[StationRepeatabilityR2S] = field(default_factory=list)
    total_north_mm: float = 0.0
    total_east_mm: float = 0.0
    total_up_mm: float = 0.0

    @property
    def total_wrms_3d(self) -> float:
        """Calculate total 3D WRMS"""
        return math.sqrt(self.total_north_mm**2 + self.total_east_mm**2 + self.total_up_mm**2)


def parse_r2s_prc_file(filepath: str) -> Optional[CoordinateRepeatability]:
    """
    Parse R2S PRC file to extract coordinate repeatability from Part 9.

    Args:
        filepath: Path to R2S_*.PRC.gz file

    Returns:
        CoordinateRepeatability object or None if parsing fails
    """
    if not os.path.exists(filepath):
        return None

    # Extract year and DOY from filename
    # Format: R2S_20253070.PRC.gz -> year=2025, doy=307
    filename = os.path.basename(filepath)
    try:
        # Handle formats like R2S_20253070.PRC.gz or R2S_2021096A.PRC.gz
        name_part = filename.replace('.PRC.gz', '').replace('.PRC', '')
        date_part = name_part.split('_')[1]
        year = int(date_part[:4])
        doy = int(date_part[4:7])
    except (IndexError, ValueError):
        year = 0
        doy = 0

    # Read file
    if filepath.endswith('.gz'):
        open_func = lambda f: gzip.open(f, 'rt', errors='replace')
    else:
        open_func = lambda f: open(f, 'r', errors='replace')

    result = CoordinateRepeatability(year=year, doy=doy, total_stations=0)

    try:
        with open_func(filepath) as f:
            content = f.read()
    except Exception:
        return None

    # Find PART 9 section
    part9_marker = "PART 9: SLIDING 7-SESSION COMPARISON OF STATION COORDINATES"
    if part9_marker not in content:
        return None

    # Extract Part 9 section
    part9_start = content.find(part9_marker)
    # Find next PART or end of file
    next_part = content.find("PART ", part9_start + len(part9_marker))
    if next_part == -1:
        part9_content = content[part9_start:]
    else:
        part9_content = content[part9_start:next_part]

    lines = part9_content.split('\n')

    in_data_section = False
    header_found = False

    for line in lines:
        line_stripped = line.strip()

        # Look for "Total number of stations"
        if "Total number of stations:" in line:
            try:
                result.total_stations = int(line.split(':')[1].strip())
            except (ValueError, IndexError):
                pass
            continue

        # Look for header line
        if "Station" in line and "#Days" in line and "N" in line and "E" in line and "U" in line:
            header_found = True
            continue

        # Skip separator lines
        if line_stripped.startswith('----'):
            if header_found:
                in_data_section = True
            continue

        # Parse data lines
        if in_data_section and line_stripped:
            # Check for Total line
            if line_stripped.startswith('Total'):
                parts = line_stripped.split()
                if len(parts) >= 5:
                    try:
                        # Format: Total  38  0.00  0.00  0.00
                        # or: Total  38  1.05  0.82  2.10
                        result.total_north_mm = float(parts[-3])
                        result.total_east_mm = float(parts[-2])
                        result.total_up_mm = float(parts[-1])
                    except (ValueError, IndexError):
                        pass
                break  # End of data section

            # Parse station line
            # Format: STATIONNAME  #Days  WeekdayPattern  N  E  U
            # Example: 0531               7  XXXXXXX     1.23   0.89   2.45
            # Note: Station name can be 4 chars, weekday pattern is 7 chars with X or space
            parts = line_stripped.split()
            if len(parts) >= 5:
                try:
                    station = parts[0]
                    n_days = int(parts[1])

                    # Find weekday pattern (7 chars of X and spaces)
                    # It's after n_days and before the 3 float values
                    # The last 3 parts are N, E, U values
                    north_mm = float(parts[-3])
                    east_mm = float(parts[-2])
                    up_mm = float(parts[-1])

                    # Weekday pattern is between n_days and NEU values
                    # Could be multiple parts if there are spaces
                    if len(parts) == 5:
                        weekday_pattern = "X" * n_days  # Approximate
                    else:
                        weekday_pattern = parts[2] if len(parts) > 5 else ""

                    result.stations.append(StationRepeatabilityR2S(
                        station=station,
                        n_days=n_days,
                        weekday_pattern=weekday_pattern,
                        north_mm=north_mm,
                        east_mm=east_mm,
                        up_mm=up_mm
                    ))
                except (ValueError, IndexError):
                    continue

    return result if result.stations else None


def get_r2s_prc_path(base_dir: str, year: int, doy: int) -> Optional[str]:
    """
    Get the path to an R2S PRC file.

    Args:
        base_dir: Base directory for DD results
        year: Year
        doy: Day of year

    Returns:
        Path to R2S PRC file if it exists, None otherwise
    """
    patterns = [
        f"{base_dir}/{year}/{doy}/OUT/R2S_{year}{doy:03d}0.PRC.gz",
        f"{base_dir}/{year}/{doy:03d}/OUT/R2S_{year}{doy:03d}0.PRC.gz",
        f"{base_dir}/{year}/{doy}/OUT/R2S_{year}{doy:03d}0.PRC",
        f"{base_dir}/{year}/{doy:03d}/OUT/R2S_{year}{doy:03d}0.PRC",
    ]

    for path in patterns:
        if os.path.exists(path):
            return path

    return None


def load_r2s_repeatability_range(base_dir: str, year: int, start_doy: int, end_doy: int) -> dict[int, CoordinateRepeatability]:
    """
    Load coordinate repeatability data for a range of DOYs.

    Args:
        base_dir: Base directory for DD results
        year: Year
        start_doy: Starting DOY
        end_doy: Ending DOY

    Returns:
        Dictionary mapping DOY to CoordinateRepeatability
    """
    results = {}

    for doy in range(start_doy, end_doy + 1):
        path = get_r2s_prc_path(base_dir, year, doy)
        if path:
            data = parse_r2s_prc_file(path)
            if data:
                results[doy] = data

    return results


if __name__ == '__main__':
    # Test with example file
    import sys

    if len(sys.argv) > 1:
        test_file = sys.argv[1]
    else:
        test_file = "/home/ahunegnaw/GPSRESULTS/ahunegnaw/OCT2025_MGX/2025/307/OUT/R2S_20253070.PRC.gz"

    print(f"Parsing: {test_file}")
    print()

    result = parse_r2s_prc_file(test_file)

    if result:
        print(f"Year: {result.year}, DOY: {result.doy}")
        print(f"Total stations: {result.total_stations}")
        print(f"Parsed stations: {len(result.stations)}")
        print()

        print("Station Repeatabilities (mm):")
        print("-" * 60)
        print(f"{'Station':<12} {'#Days':>6} {'N':>8} {'E':>8} {'U':>8} {'3D':>8}")
        print("-" * 60)

        for sta in result.stations[:10]:  # Show first 10
            print(f"{sta.station:<12} {sta.n_days:>6} {sta.north_mm:>8.2f} {sta.east_mm:>8.2f} {sta.up_mm:>8.2f} {sta.wrms_3d:>8.2f}")

        if len(result.stations) > 10:
            print(f"  ... and {len(result.stations) - 10} more stations")

        print("-" * 60)
        print(f"{'Total':<12} {result.total_stations:>6} {result.total_north_mm:>8.2f} {result.total_east_mm:>8.2f} {result.total_up_mm:>8.2f} {result.total_wrms_3d:>8.2f}")
    else:
        print("Failed to parse file or no Part 9 data found")
