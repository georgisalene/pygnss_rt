#!/usr/bin/env python3
"""
PPP-AR TRO File Parser

Parses Bernese-style SINEX-TRO troposphere files from PPP-AR processing.
Example file: FIN_20252990_0531.TRO (individual station files in ATM directory)

File structure:
- SINEX-TRO format with +TROP/SOLUTION block
- ZTD estimates in mm with timestamps

Data format:
*STATION__ _____EPOCH____ TROTOT STDDEV  TGNTOT STDDEV  TGETOT STDDEV
 0531      2025:299:00000 2207.1    0.9  -0.082  0.045   0.284  0.042

Author: Addisu Hunegnaw
Date: January 2026
"""

import os
import glob
import gzip
from datetime import datetime, timedelta
from dataclasses import dataclass
from typing import Optional, List

from pygnss_rt.frontend.bernese_parser import find_ig_campaign_dir


@dataclass
class PPPARTropoEstimate:
    """PPP-AR troposphere estimate from Bernese TRO file"""
    station_4char: str   # 4-char station code (e.g., 0531)
    timestamp: datetime  # UTC timestamp
    ztd_m: float         # Zenith Total Delay in meters
    sigma_m: float       # Standard deviation in meters
    grad_n_m: float      # North gradient in meters
    grad_n_sigma_m: float
    grad_e_m: float      # East gradient in meters
    grad_e_sigma_m: float
    year: int            # Year
    doy: int             # Day of year
    hour: int            # Hour of day


def parse_sinex_epoch(epoch_str: str) -> tuple:
    """
    Parse SINEX epoch format YYYY:DOY:SSSSS to datetime and hour.

    Args:
        epoch_str: Epoch string like "2025:299:00000"

    Returns:
        Tuple of (datetime, year, doy, hour)
    """
    parts = epoch_str.split(':')
    year = int(parts[0])
    doy = int(parts[1])
    seconds = int(parts[2])
    hour = seconds // 3600

    # Create datetime from year and DOY
    dt = datetime(year, 1, 1) + timedelta(days=doy - 1, seconds=seconds)
    return dt, year, doy, hour


def parse_pppar_tro_file(filepath: str) -> List[PPPARTropoEstimate]:
    """
    Parse a PPP-AR TRO troposphere file.

    Args:
        filepath: Path to the TRO file (can be .gz compressed)

    Returns:
        List of PPPARTropoEstimate with troposphere estimates
    """
    estimates = []

    # Handle gzip compressed files
    if filepath.endswith('.gz'):
        open_func = lambda f: gzip.open(f, 'rt')
    else:
        open_func = lambda f: open(f, 'r')

    if not os.path.exists(filepath):
        return estimates

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
                    # Format: " STATION__ _____EPOCH____ TROTOT STDDEV  TGNTOT STDDEV  TGETOT STDDEV"
                    # Example: " 0531      2025:299:00000 2207.1    0.9  -0.082  0.045   0.284  0.042"
                    station = line[1:11].strip()
                    epoch_str = line[11:25].strip()

                    # Parse remaining values
                    values = line[25:].split()
                    if len(values) >= 2:
                        ztd_mm = float(values[0])
                        sigma_mm = float(values[1])

                        # Parse gradients if available
                        grad_n_m = float(values[2]) / 1000.0 if len(values) >= 3 else 0.0
                        grad_n_sigma = float(values[3]) / 1000.0 if len(values) >= 4 else 0.0
                        grad_e_m = float(values[4]) / 1000.0 if len(values) >= 5 else 0.0
                        grad_e_sigma = float(values[5]) / 1000.0 if len(values) >= 6 else 0.0

                        # Convert mm to meters for ZTD
                        ztd_m = ztd_mm / 1000.0
                        sigma_m = sigma_mm / 1000.0

                        # Parse timestamp
                        timestamp, year, doy, hour = parse_sinex_epoch(epoch_str)

                        estimates.append(PPPARTropoEstimate(
                            station_4char=station,
                            timestamp=timestamp,
                            ztd_m=ztd_m,
                            sigma_m=sigma_m,
                            grad_n_m=grad_n_m,
                            grad_n_sigma_m=grad_n_sigma,
                            grad_e_m=grad_e_m,
                            grad_e_sigma_m=grad_e_sigma,
                            year=year,
                            doy=doy,
                            hour=hour
                        ))
                except (ValueError, IndexError):
                    continue

    return estimates


def get_pppar_tro_files(campaign_path: str, year: int, doy: int) -> List[str]:
    """
    Get all PPP-AR TRO files for a given day.

    Args:
        campaign_path: Path to campaign directory (e.g., /home/.../CAMPAIGN54)
        year: Year
        doy: Day of year

    Returns:
        List of paths to TRO files
    """
    session_path = find_ig_campaign_dir(campaign_path, year, doy)
    if not session_path:
        return []
    session_dir = f"{session_path}/ATM"

    # Pattern for PPP-AR TRO files: FIN_YYYYDDD0_STATION.TRO
    pattern = f"{session_dir}/FIN_{year}{doy:03d}0_*.TRO"

    files = glob.glob(pattern)
    return sorted(files)


def parse_pppar_tro_day(campaign_path: str, year: int, doy: int) -> List[PPPARTropoEstimate]:
    """
    Parse all PPP-AR TRO files for a given day.

    Args:
        campaign_path: Path to campaign directory
        year: Year
        doy: Day of year

    Returns:
        List of all troposphere estimates for the day
    """
    all_estimates = []

    files = get_pppar_tro_files(campaign_path, year, doy)

    for filepath in files:
        estimates = parse_pppar_tro_file(filepath)
        all_estimates.extend(estimates)

    return all_estimates


def get_pppar_ztd_for_station(campaign_path: str, station: str,
                              year: int, doy: int) -> List[PPPARTropoEstimate]:
    """
    Get PPP-AR ZTD time series for a specific station.

    Args:
        campaign_path: Path to campaign directory
        station: 4-char station code
        year: Year
        doy: Day of year

    Returns:
        List of troposphere estimates for the station
    """
    session_path = find_ig_campaign_dir(campaign_path, year, doy)
    if not session_path:
        return []
    filepath = f"{session_path}/ATM/FIN_{year}{doy:03d}0_{station.upper()}.TRO"

    return parse_pppar_tro_file(filepath)


if __name__ == '__main__':
    # Test with actual file
    campaign = '/home/ahunegnaw/GPSDATA/CAMPAIGN54'
    year = 2025
    doy = 299

    print(f"Parsing PPP-AR TRO files for DOY {doy}...")

    # Get all files
    files = get_pppar_tro_files(campaign, year, doy)
    print(f"Found {len(files)} TRO files")

    # Parse one station
    estimates = get_pppar_ztd_for_station(campaign, '0531', year, doy)
    print(f"\nStation 0531: {len(estimates)} estimates")

    if estimates:
        print("\nFirst 5 estimates:")
        print(f"{'Epoch':<20} {'ZTD(mm)':>10} {'Sigma':>8} {'Grad_N':>8} {'Grad_E':>8}")
        print("-" * 60)
        for e in estimates[:5]:
            print(f"{e.timestamp.strftime('%Y-%m-%d %H:%M'):20} {e.ztd_m*1000:>10.1f} {e.sigma_m*1000:>8.1f} "
                  f"{e.grad_n_m*1000:>8.3f} {e.grad_e_m*1000:>8.3f}")
