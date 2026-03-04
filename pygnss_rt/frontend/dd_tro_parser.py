"""
Double Difference (DD) TRO File Parser

Parses Bernese-style SINEX-TRO troposphere files from DD processing.
Example file: F10_20253030.TRO.gz

File structure:
- SINEX-TRO format with +TROP/SOLUTION block
- ZTD estimates in mm with timestamps

Data format:
*STATION__ _____EPOCH____ TROTOT STDDEV
 0531      2025:303:00000 2283.2    2.7
"""

import os
import gzip
from datetime import datetime, timedelta
from dataclasses import dataclass
from typing import Optional


@dataclass
class DDTropoEstimate:
    """DD troposphere estimate from Bernese TRO file"""
    station_4char: str   # 4-char station code (e.g., ZIM2)
    timestamp: datetime  # UTC timestamp
    ztd_m: float         # Zenith Total Delay in meters
    sigma_m: float       # Standard deviation in meters
    year: int            # Year
    doy: int             # Day of year


def parse_sinex_epoch(epoch_str: str) -> datetime:
    """
    Parse SINEX epoch format YYYY:DOY:SSSSS to datetime.

    Args:
        epoch_str: Epoch string like "2025:303:00000"

    Returns:
        datetime object
    """
    parts = epoch_str.split(':')
    year = int(parts[0])
    doy = int(parts[1])
    seconds = int(parts[2])

    # Create datetime from year and DOY
    dt = datetime(year, 1, 1) + timedelta(days=doy - 1, seconds=seconds)
    return dt


def parse_dd_tro_file(filepath: str, year: int, doy: int) -> list[DDTropoEstimate]:
    """
    Parse a DD TRO troposphere file.

    Args:
        filepath: Path to the TRO file (can be .gz compressed)
        year: Year of the solution
        doy: Day of year

    Returns:
        List of DDTropoEstimate with troposphere estimates
    """
    estimates = []

    # Handle gzip compressed files
    if filepath.endswith('.gz'):
        open_func = lambda f: gzip.open(f, 'rt')
    else:
        open_func = lambda f: open(f, 'r')

    with open_func(filepath) as f:
        in_solution_section = False
        for line in f:
            line = line.rstrip('\n')

            # Skip empty lines
            if not line.strip():
                continue

            # Look for solution section start
            if line.strip() == '+TROP/SOLUTION':
                in_solution_section = True
                continue

            # End of solution section
            if line.strip() == '-TROP/SOLUTION':
                in_solution_section = False
                continue

            # Skip comment lines
            if line.startswith('*'):
                continue

            # Parse data lines in solution section
            if in_solution_section and line.startswith(' '):
                try:
                    # Format: " STATION__ _____EPOCH____ TROTOT STDDEV"
                    # Example: " 0531      2025:303:00000 2283.2    2.7"
                    station = line[1:11].strip()
                    epoch_str = line[11:25].strip()

                    # Parse remaining values
                    values = line[25:].split()
                    if len(values) >= 2:
                        ztd_mm = float(values[0])
                        sigma_mm = float(values[1])

                        # Convert mm to meters
                        ztd_m = ztd_mm / 1000.0
                        sigma_m = sigma_mm / 1000.0

                        # Parse timestamp
                        timestamp = parse_sinex_epoch(epoch_str)

                        # Extract year and DOY from epoch string (YYYY:DOY:SSSSS)
                        # This is critical: files may contain entries for the next day
                        epoch_parts = epoch_str.split(':')
                        epoch_year = int(epoch_parts[0])
                        epoch_doy = int(epoch_parts[1])

                        estimates.append(DDTropoEstimate(
                            station_4char=station,
                            timestamp=timestamp,
                            ztd_m=ztd_m,
                            sigma_m=sigma_m,
                            year=epoch_year,
                            doy=epoch_doy
                        ))
                except (ValueError, IndexError):
                    continue

    return estimates


def get_dd_tro_path(base_dir: str, year: int, doy: int,
                    prefix: str = 'F10') -> Optional[str]:
    """
    Get the path to a DD TRO file.

    Args:
        base_dir: Base directory for DD results
        year: Year
        doy: Day of year
        prefix: File prefix (F10 for final, R10 for rapid, P1 for preliminary)

    Returns:
        Path to TRO file if it exists, None otherwise
    """
    # Try different path patterns
    patterns = [
        f"{base_dir}/{year}/{doy}/ATM/{prefix}_{year}{doy:03d}0.TRO.gz",
        f"{base_dir}/{year}/{doy:03d}/ATM/{prefix}_{year}{doy:03d}0.TRO.gz",
    ]

    for path in patterns:
        if os.path.exists(path):
            return path

    return None


def get_dd_ztd_for_station(station_4char: str,
                            dd_estimates: list[DDTropoEstimate],
                            target_time: datetime = None,
                            tolerance_minutes: int = 10
                            ) -> Optional[DDTropoEstimate]:
    """
    Find DD ZTD estimate for a specific station and time.

    Args:
        station_4char: 4-character station code (case-insensitive)
        dd_estimates: List of DD troposphere estimates
        target_time: Target time to match (optional)
        tolerance_minutes: Time tolerance in minutes

    Returns:
        DDTropoEstimate if found, None otherwise
    """
    station_upper = station_4char.upper()

    matching = [e for e in dd_estimates if e.station_4char.upper() == station_upper]

    if not matching:
        return None

    if target_time is None:
        # Return first match
        return matching[0]

    # Find closest in time
    tolerance = timedelta(minutes=tolerance_minutes)
    best_match = None
    min_diff = timedelta.max

    for estimate in matching:
        diff = abs(estimate.timestamp - target_time)
        if diff < min_diff and diff <= tolerance:
            min_diff = diff
            best_match = estimate

    return best_match


def get_dd_ztd_timeseries(station_4char: str,
                          dd_estimates: list[DDTropoEstimate]
                          ) -> list[DDTropoEstimate]:
    """
    Get all DD ZTD estimates for a specific station.

    Args:
        station_4char: 4-character station code (case-insensitive)
        dd_estimates: List of DD troposphere estimates

    Returns:
        List of DDTropoEstimate for the station, sorted by time
    """
    station_upper = station_4char.upper()
    matching = [e for e in dd_estimates if e.station_4char.upper() == station_upper]
    return sorted(matching, key=lambda e: e.timestamp)
