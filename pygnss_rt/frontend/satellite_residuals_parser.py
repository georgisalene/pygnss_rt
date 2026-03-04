"""
Satellite Residuals Parser

Parses Bernese RES_*.SUM.gz files containing satellite-specific phase residual RMS
values per baseline (for DD processing) and per station (for PPP processing).

The RES.SUM file shows:
- Baseline name and session
- RMS values per satellite (columns for GPS, GLONASS, Galileo PRNs)
- Total RMS and observation counts

Also parses TRP files for convergence time analysis (for PPP-AR).

Author: Addisu Hunegnaw
Date: January 2026
"""

import os
import gzip
import re
from pathlib import Path
from dataclasses import dataclass, field
from typing import Optional
from datetime import datetime, timedelta


@dataclass
class SatelliteResidual:
    """Residual data for a single satellite on a specific baseline/station."""
    prn: str  # e.g., "G01", "E05", "R12"
    system: str  # GPS, GLONASS, Galileo
    rms_mm: float  # RMS residual in mm
    obs_count: Optional[int] = None


@dataclass
class BaselineResiduals:
    """Residual data for a baseline (DD) or station (PPP)."""
    name: str  # Baseline name (e.g., "BRUX-WARN") or station name
    session: str
    total_rms_mm: float
    satellite_residuals: dict[str, float] = field(default_factory=dict)  # PRN -> RMS
    obs_counts: dict[str, int] = field(default_factory=dict)  # PRN -> count


@dataclass
class SessionSatelliteStats:
    """Statistics for all satellites in a session."""
    session: str
    year: int
    doy: int
    baselines: list[BaselineResiduals] = field(default_factory=list)
    # Aggregate statistics per satellite
    satellite_mean_rms: dict[str, float] = field(default_factory=dict)
    satellite_max_rms: dict[str, float] = field(default_factory=dict)
    satellite_total_obs: dict[str, int] = field(default_factory=dict)


def prn_to_system(prn_num: int) -> tuple[str, str]:
    """Convert numeric PRN to system name and formatted PRN string.

    PRN numbering in Bernese:
    - 1-32: GPS (G01-G32)
    - 101-132: GLONASS (R01-R32)
    - 201-250: Galileo (E01-E50)
    - 301-350: BeiDou (C01-C50)
    """
    if 1 <= prn_num <= 32:
        return "GPS", f"G{prn_num:02d}"
    elif 101 <= prn_num <= 132:
        return "GLONASS", f"R{prn_num - 100:02d}"
    elif 201 <= prn_num <= 250:
        return "Galileo", f"E{prn_num - 200:02d}"
    elif 301 <= prn_num <= 350:
        return "BeiDou", f"C{prn_num - 300:02d}"
    else:
        return "Unknown", f"X{prn_num:03d}"


def parse_res_sum_file(filepath: str) -> Optional[SessionSatelliteStats]:
    """
    Parse a Bernese RES_*.SUM.gz file containing satellite residuals.

    Args:
        filepath: Path to the RES.SUM file (can be .gz compressed)

    Returns:
        SessionSatelliteStats object with parsed data, or None if parsing fails
    """
    if not os.path.exists(filepath):
        return None

    # Extract year/doy from filename (e.g., RES_20253020.SUM.gz)
    filename = os.path.basename(filepath)
    match = re.search(r'RES_(\d{4})(\d{3})', filename)
    if match:
        year = int(match.group(1))
        doy = int(match.group(2))
    else:
        year, doy = 0, 0

    # Handle gzip compressed files
    if filepath.endswith('.gz'):
        open_func = lambda f: gzip.open(f, 'rt')
    else:
        open_func = lambda f: open(f, 'r')

    stats = SessionSatelliteStats(
        session=f"{year}{doy:03d}",
        year=year,
        doy=doy
    )

    prn_columns = []  # List of PRN numbers from header
    in_data_section = False

    try:
        with open_func(filepath) as f:
            for line in f:
                line = line.rstrip()

                # Parse header line with PRN columns
                if line.strip().startswith('BASELINE') and 'SESS' in line:
                    # Parse PRN columns from header
                    parts = line.split()
                    prn_columns = []
                    for part in parts[2:]:  # Skip BASELINE and SESS
                        if part == 'TOT':
                            break
                        try:
                            prn_num = int(part)
                            prn_columns.append(prn_num)
                        except ValueError:
                            continue
                    in_data_section = True
                    continue

                # Skip separator lines
                if '---' in line:
                    continue

                # Parse TOTAL RMS line
                if 'TOTAL RMS:' in line:
                    parts = line.split()
                    idx = parts.index('RMS:') + 1
                    for i, prn_num in enumerate(prn_columns):
                        if idx + i < len(parts):
                            try:
                                rms = float(parts[idx + i])
                                system, prn_str = prn_to_system(prn_num)
                                stats.satellite_mean_rms[prn_str] = rms
                            except ValueError:
                                pass
                    continue

                # Parse TOT OBS line for observation counts
                if 'TOT OBS (1E0):' in line or line.strip().startswith('TOT OBS'):
                    parts = line.split()
                    # Find where numeric data starts
                    for start_idx, part in enumerate(parts):
                        try:
                            int(part)
                            break
                        except ValueError:
                            continue
                    else:
                        continue

                    for i, prn_num in enumerate(prn_columns):
                        if start_idx + i < len(parts):
                            try:
                                count = int(parts[start_idx + i])
                                system, prn_str = prn_to_system(prn_num)
                                stats.satellite_total_obs[prn_str] = count
                            except ValueError:
                                pass
                    continue

                # Parse baseline data lines
                if in_data_section and prn_columns and line.strip():
                    parts = line.split()
                    if len(parts) < 3:
                        continue

                    baseline_name = parts[0]

                    # Skip header-like lines
                    if baseline_name in ('BASELINE', 'TOTAL', 'TOT'):
                        continue

                    try:
                        session = parts[1]

                        baseline = BaselineResiduals(
                            name=baseline_name,
                            session=session,
                            total_rms_mm=0.0
                        )

                        # Parse satellite RMS values
                        for i, prn_num in enumerate(prn_columns):
                            if 2 + i < len(parts):
                                try:
                                    rms = float(parts[2 + i])
                                    system, prn_str = prn_to_system(prn_num)
                                    baseline.satellite_residuals[prn_str] = rms

                                    # Track max RMS per satellite
                                    if prn_str not in stats.satellite_max_rms:
                                        stats.satellite_max_rms[prn_str] = rms
                                    else:
                                        stats.satellite_max_rms[prn_str] = max(
                                            stats.satellite_max_rms[prn_str], rms
                                        )
                                except ValueError:
                                    pass  # Empty or non-numeric value

                        # Get total RMS (last column)
                        try:
                            baseline.total_rms_mm = float(parts[-1])
                        except ValueError:
                            pass

                        if baseline.satellite_residuals:
                            stats.baselines.append(baseline)

                    except (IndexError, ValueError):
                        continue

    except Exception as e:
        print(f"Error parsing {filepath}: {e}")
        return None

    return stats


@dataclass
class ConvergenceInfo:
    """Convergence information for a station from TRP file."""
    station: str
    epochs: list[datetime] = field(default_factory=list)
    sigma_u: list[float] = field(default_factory=list)  # ZTD sigma over time
    total_u: list[float] = field(default_factory=list)  # Total ZTD over time
    convergence_time_hours: Optional[float] = None  # Time to reach stable solution


def parse_trp_for_convergence(filepath: str, sigma_threshold: float = 0.001) -> dict[str, ConvergenceInfo]:
    """
    Parse a TRP file to extract convergence information per station.

    Convergence is estimated by tracking when the ZTD sigma falls below a threshold
    and remains stable.

    Args:
        filepath: Path to the TRP file (can be .gz compressed)
        sigma_threshold: Sigma threshold for considering solution converged (meters)

    Returns:
        Dictionary mapping station name to ConvergenceInfo
    """
    if not os.path.exists(filepath):
        return {}

    if filepath.endswith('.gz'):
        open_func = lambda f: gzip.open(f, 'rt')
    else:
        open_func = lambda f: open(f, 'r')

    stations = {}

    try:
        with open_func(filepath) as f:
            for line in f:
                line = line.rstrip()

                # Skip header lines
                if not line or line.startswith('-') or 'STATION NAME' in line:
                    continue
                if 'TROPOSPHERE' in line or 'SOURCE' in line or 'PRIORI' in line:
                    continue
                if 'MAPPING' in line or 'TABULAR' in line or 'MINIMUM' in line:
                    continue

                parts = line.split()
                if len(parts) < 15:
                    continue

                try:
                    station = parts[0]
                    flag = parts[1]

                    # Parse datetime
                    year = int(parts[2])
                    month = int(parts[3])
                    day = int(parts[4])
                    hour = int(parts[5])
                    minute = int(parts[6])
                    second = int(parts[7])

                    epoch = datetime(year, month, day, hour, minute, second)

                    # Parse values (format varies slightly)
                    # MOD_U, CORR_U, SIGMA_U, TOTAL_U, CORR_N, SIGMA_N, CORR_E, SIGMA_E
                    # Positions after datetime may vary based on whether there's an end time

                    # Find the numeric values after the datetime
                    value_start = 8
                    # Check if there's an end datetime (8 more fields)
                    if len(parts) > 16 and parts[8].isdigit():
                        value_start = 16

                    mod_u = float(parts[value_start])
                    corr_u = float(parts[value_start + 1])
                    sigma_u = float(parts[value_start + 2])
                    total_u = float(parts[value_start + 3])

                    if station not in stations:
                        stations[station] = ConvergenceInfo(station=station)

                    stations[station].epochs.append(epoch)
                    stations[station].sigma_u.append(sigma_u)
                    stations[station].total_u.append(total_u)

                except (IndexError, ValueError):
                    continue

        # Calculate convergence time for each station
        for station, info in stations.items():
            if len(info.epochs) >= 2 and len(info.sigma_u) >= 2:
                # Find when sigma drops below threshold and stays there
                first_epoch = info.epochs[0]
                for i, sigma in enumerate(info.sigma_u):
                    if sigma < sigma_threshold:
                        # Check if it stays below threshold
                        if all(s < sigma_threshold * 1.5 for s in info.sigma_u[i:]):
                            convergence_epoch = info.epochs[i]
                            info.convergence_time_hours = (
                                convergence_epoch - first_epoch
                            ).total_seconds() / 3600
                            break

    except Exception as e:
        print(f"Error parsing TRP file {filepath}: {e}")

    return stations


def get_res_sum_path(base_dir: str, year: int, doy: int) -> Optional[str]:
    """
    Get the path to a RES.SUM file.

    Args:
        base_dir: Base directory for DD/PPP results
        year: Year
        doy: Day of year

    Returns:
        Path to RES.SUM file if it exists, None otherwise
    """
    patterns = [
        f"{base_dir}/{year}/{doy:03d}/OUT/RES_{year}{doy:03d}0.SUM.gz",
        f"{base_dir}/{year}/{doy}/OUT/RES_{year}{doy:03d}0.SUM.gz",
        f"{base_dir}/{year}/{doy:03d}/OUT/RES_{year}{doy:03d}0.SUM",
    ]

    for path in patterns:
        if os.path.exists(path):
            return path

    return None


def get_trp_path(base_dir: str, year: int, doy: int) -> Optional[str]:
    """
    Get the path to a TRP file.

    Args:
        base_dir: Base directory for PPP results
        year: Year
        doy: Day of year

    Returns:
        Path to TRP file if it exists, None otherwise
    """
    patterns = [
        f"{base_dir}/{year}/{doy:03d}/ATM/F10_{year}{doy:03d}0.TRP.gz",
        f"{base_dir}/{year}/{doy}/ATM/F10_{year}{doy:03d}0.TRP.gz",
        f"{base_dir}/{year}/{doy:03d}/ATM/F10_{year}{doy:03d}0.TRP",
    ]

    for path in patterns:
        if os.path.exists(path):
            return path

    return None


def aggregate_satellite_stats(sessions: list[SessionSatelliteStats]) -> dict:
    """
    Aggregate satellite statistics across multiple sessions.

    Args:
        sessions: List of session statistics

    Returns:
        Dictionary with aggregated satellite performance metrics
    """
    satellite_rms_values = {}  # PRN -> list of RMS values
    satellite_obs_total = {}  # PRN -> total observation count

    for session in sessions:
        for prn, rms in session.satellite_mean_rms.items():
            if prn not in satellite_rms_values:
                satellite_rms_values[prn] = []
            satellite_rms_values[prn].append(rms)

        for prn, count in session.satellite_total_obs.items():
            if prn not in satellite_obs_total:
                satellite_obs_total[prn] = 0
            satellite_obs_total[prn] += count

    # Calculate aggregate statistics
    aggregate = {}
    for prn, rms_list in satellite_rms_values.items():
        if rms_list:
            aggregate[prn] = {
                'mean_rms': sum(rms_list) / len(rms_list),
                'max_rms': max(rms_list),
                'min_rms': min(rms_list),
                'std_rms': (
                    (sum((x - sum(rms_list)/len(rms_list))**2 for x in rms_list) / len(rms_list))**0.5
                    if len(rms_list) > 1 else 0
                ),
                'session_count': len(rms_list),
                'total_obs': satellite_obs_total.get(prn, 0),
                'system': prn[0]  # G, R, E, C
            }

    return aggregate


def identify_problematic_satellites(
    aggregate_stats: dict,
    rms_threshold: float = 2.0,
    consistency_threshold: float = 0.3
) -> list[dict]:
    """
    Identify satellites that consistently show high residuals.

    Args:
        aggregate_stats: Aggregated satellite statistics from aggregate_satellite_stats()
        rms_threshold: RMS threshold in mm to flag as high
        consistency_threshold: Standard deviation threshold to flag as inconsistent

    Returns:
        List of problematic satellites with their issues
    """
    problematic = []

    for prn, stats in aggregate_stats.items():
        issues = []

        if stats['mean_rms'] > rms_threshold:
            issues.append(f"High mean RMS ({stats['mean_rms']:.2f} mm)")

        if stats['max_rms'] > rms_threshold * 1.5:
            issues.append(f"Very high max RMS ({stats['max_rms']:.2f} mm)")

        if stats['std_rms'] > consistency_threshold:
            issues.append(f"Inconsistent performance (std={stats['std_rms']:.2f})")

        if issues:
            problematic.append({
                'prn': prn,
                'system': stats['system'],
                'mean_rms': stats['mean_rms'],
                'max_rms': stats['max_rms'],
                'std_rms': stats['std_rms'],
                'issues': issues
            })

    # Sort by mean RMS descending
    problematic.sort(key=lambda x: x['mean_rms'], reverse=True)

    return problematic


if __name__ == '__main__':
    # Test with example file
    import sys

    if len(sys.argv) >= 2:
        res_path = sys.argv[1]
    else:
        res_path = "/home/ahunegnaw/GPSRESULTS/ahunegnaw/OCT2025_MGX/2025_with_clu/302/OUT/RES_20253020.SUM.gz"

    print(f"Parsing: {res_path}")
    stats = parse_res_sum_file(res_path)

    if stats:
        print(f"\nSession: {stats.session} (Year: {stats.year}, DOY: {stats.doy})")
        print(f"Number of baselines: {len(stats.baselines)}")
        print(f"Number of satellites with data: {len(stats.satellite_mean_rms)}")

        print("\nSatellite Mean RMS (top 10 highest):")
        sorted_sats = sorted(stats.satellite_mean_rms.items(), key=lambda x: x[1], reverse=True)
        for prn, rms in sorted_sats[:10]:
            obs = stats.satellite_total_obs.get(prn, 0)
            print(f"  {prn}: {rms:.2f} mm (obs: {obs})")

        print("\nSample baselines:")
        for bl in stats.baselines[:5]:
            print(f"  {bl.name}: Total RMS = {bl.total_rms_mm:.2f} mm, "
                  f"Satellites: {len(bl.satellite_residuals)}")
    else:
        print("Failed to parse file")

    # Test TRP parsing
    trp_path = "/home/ahunegnaw/GPSRESULTS/ahunegnaw/OCT2025_MGX/2025_with_clu/302/ATM/F10_20253020.TRP.gz"
    if os.path.exists(trp_path):
        print(f"\n\nParsing TRP: {trp_path}")
        convergence = parse_trp_for_convergence(trp_path)

        print(f"Number of stations: {len(convergence)}")
        for station, info in list(convergence.items())[:5]:
            print(f"  {station}: {len(info.epochs)} epochs, "
                  f"convergence: {info.convergence_time_hours:.1f}h"
                  if info.convergence_time_hours else f"  {station}: {len(info.epochs)} epochs")
