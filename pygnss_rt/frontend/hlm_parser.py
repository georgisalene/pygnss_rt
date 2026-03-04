"""
Helmert Transformation (HLM) File Parser

Parses Bernese HLM output files containing Helmert transformation results.
Example file: HLM_20253030.OUT.gz

Data extracted:
- Station residuals (N, E, U) with reference/non-reference flags
- Statistics: RMS, IQR, MEAN, MEDIAN, MIN, MAX
- Transformation parameters: Translation (N, E, U), Rotation (around N, E, U axes)
- Overall RMS of transformation
"""

import math
import os
import re
import gzip
from pathlib import Path
from dataclasses import dataclass, field
from typing import Optional
from datetime import datetime


@dataclass
class HLMStationResidual:
    """Station residual from Helmert transformation"""
    station_4char: str   # 4-char station code
    flag1: str           # First flag (W = weighted)
    flag2: str           # Second flag (W = weighted, A = added)
    north_mm: float      # North residual in mm
    east_mm: float       # East residual in mm
    up_mm: float         # Up residual in mm
    is_reference: bool   # True if reference station (no V marker)
    year: int            # Year
    doy: int             # Day of year


@dataclass
class HLMStatistics:
    """Statistics from Helmert transformation"""
    rms_north: float = 0.0
    rms_east: float = 0.0
    rms_up: float = 0.0
    iqr_north: float = 0.0
    iqr_east: float = 0.0
    iqr_up: float = 0.0
    mean_north: float = 0.0
    mean_east: float = 0.0
    mean_up: float = 0.0
    median_north: float = 0.0
    median_east: float = 0.0
    median_up: float = 0.0
    min_north: float = 0.0
    min_east: float = 0.0
    min_up: float = 0.0
    max_north: float = 0.0
    max_east: float = 0.0
    max_up: float = 0.0
    overall_rms_3d: float = 0.0
    overall_iqr_3d: float = 0.0
    overall_max_3d: float = 0.0
    max_3d_station: str = ""


@dataclass
class HLMTransformParams:
    """Helmert transformation parameters"""
    trans_n_mm: float = 0.0      # Translation in N (mm)
    trans_n_sigma: float = 0.0   # Sigma for N translation
    trans_e_mm: float = 0.0      # Translation in E (mm)
    trans_e_sigma: float = 0.0   # Sigma for E translation
    trans_u_mm: float = 0.0      # Translation in U (mm)
    trans_u_sigma: float = 0.0   # Sigma for U translation
    rot_n_arcsec: float = 0.0    # Rotation around N-axis (arcsec)
    rot_n_sigma: float = 0.0     # Sigma for N rotation
    rot_e_arcsec: float = 0.0    # Rotation around E-axis (arcsec)
    rot_e_sigma: float = 0.0     # Sigma for E rotation
    rot_u_arcsec: float = 0.0    # Rotation around U-axis (arcsec)
    rot_u_sigma: float = 0.0     # Sigma for U rotation
    rms_transformation: float = 0.0  # RMS of transformation (mm)


@dataclass
class HLMResult:
    """Complete HLM result for a single day"""
    year: int
    doy: int
    residuals: list = field(default_factory=list)  # List of HLMStationResidual
    ref_stats: HLMStatistics = field(default_factory=HLMStatistics)  # Reference stations only
    all_stats: HLMStatistics = field(default_factory=HLMStatistics)  # All stations
    transform_params: HLMTransformParams = field(default_factory=HLMTransformParams)
    num_parameters: int = 0
    num_stations: int = 0
    num_coordinates: int = 0
    accepted_stations: int = 0
    verified_stations: int = 0
    rejected_stations: int = 0


def _compute_stats_from_residuals(residuals: list) -> HLMStatistics:
    """Compute RMS/IQR/MEAN/MEDIAN/MIN/MAX statistics from a list of residuals."""
    if not residuals:
        return HLMStatistics()

    norths = [r.north_mm for r in residuals]
    easts = [r.east_mm for r in residuals]
    ups = [r.up_mm for r in residuals]
    n = len(residuals)

    def _rms(vals):
        return math.sqrt(sum(v * v for v in vals) / len(vals))

    def _iqr(vals):
        s = sorted(vals)
        q1 = s[len(s) // 4]
        q3 = s[3 * len(s) // 4]
        return q3 - q1

    def _median(vals):
        s = sorted(vals)
        mid = len(s) // 2
        if len(s) % 2 == 0:
            return (s[mid - 1] + s[mid]) / 2
        return s[mid]

    stats = HLMStatistics(
        rms_north=_rms(norths),
        rms_east=_rms(easts),
        rms_up=_rms(ups),
        iqr_north=_iqr(norths),
        iqr_east=_iqr(easts),
        iqr_up=_iqr(ups),
        mean_north=sum(norths) / n,
        mean_east=sum(easts) / n,
        mean_up=sum(ups) / n,
        median_north=_median(norths),
        median_east=_median(easts),
        median_up=_median(ups),
        min_north=min(norths),
        min_east=min(easts),
        min_up=min(ups),
        max_north=max(norths),
        max_east=max(easts),
        max_up=max(ups),
    )

    # Compute 3D stats
    rms_3d_vals = [math.sqrt(r.north_mm**2 + r.east_mm**2 + r.up_mm**2)
                   for r in residuals]
    stats.overall_rms_3d = math.sqrt(sum(v * v for v in rms_3d_vals) / len(rms_3d_vals))
    stats.overall_iqr_3d = _iqr(rms_3d_vals)
    stats.overall_max_3d = max(rms_3d_vals)
    max_idx = rms_3d_vals.index(stats.overall_max_3d)
    stats.max_3d_station = residuals[max_idx].station_4char

    return stats


def parse_rotation_value(line: str) -> tuple[float, float]:
    """
    Parse rotation value from HLM file.
    Format: "ROTATION AROUND X-AXIS:  [- ] D  M  S.ssssss +-  S.ssssss"

    Returns: (rotation_arcsec, sigma_arcsec)
    """
    try:
        # Extract the part after the colon
        parts = line.split(':')[1].strip()

        # Check for negative sign
        is_negative = parts.startswith('-')
        if is_negative:
            parts = parts[1:].strip()

        # Split by +-
        value_part, sigma_part = parts.split('+-')
        value_tokens = value_part.split()

        # Parse D M S
        deg = int(value_tokens[0])
        mins = int(value_tokens[1])
        secs = float(value_tokens[2].strip('"'))

        # Convert to arcseconds
        total_arcsec = deg * 3600 + mins * 60 + secs
        if is_negative:
            total_arcsec = -total_arcsec

        # Parse sigma (already in arcseconds)
        sigma = float(sigma_part.strip().strip('"'))

        return total_arcsec, sigma
    except Exception:
        return 0.0, 0.0


def parse_hlm_file(filepath: str, year: int, doy: int) -> HLMResult:
    """
    Parse a Helmert transformation output file.

    Args:
        filepath: Path to the HLM file (can be .gz compressed)
        year: Year of the solution
        doy: Day of year

    Returns:
        HLMResult with all parsed data
    """
    result = HLMResult(year=year, doy=doy)

    # Handle gzip compressed files
    if filepath.endswith('.gz'):
        open_func = lambda f: gzip.open(f, 'rt')
    else:
        open_func = lambda f: open(f, 'r')

    with open_func(filepath) as f:
        lines = f.readlines()

    in_station_table = False
    in_all_stats = False
    parsing_ref_stats = False

    i = 0
    while i < len(lines):
        line = lines[i].rstrip('\n')

        # Detect station residual table
        if '| NUM |' in line and 'NAME' in line and 'RESIDUALS' in line:
            in_station_table = True
            i += 1
            continue

        # Parse station residuals
        if in_station_table and line.startswith(' |'):
            # Check for statistics lines
            if 'RMS / COMPONENT' in line:
                if '| ALL |' in line:
                    in_all_stats = True
                else:
                    parsing_ref_stats = True
                # Parse RMS
                match = re.search(r'([-\d.]+)\s+([-\d.]+)\s+([-\d.]+)', line[40:])
                if match:
                    if in_all_stats:
                        result.all_stats.rms_north = float(match.group(1))
                        result.all_stats.rms_east = float(match.group(2))
                        result.all_stats.rms_up = float(match.group(3))
                    else:
                        result.ref_stats.rms_north = float(match.group(1))
                        result.ref_stats.rms_east = float(match.group(2))
                        result.ref_stats.rms_up = float(match.group(3))
            elif '| IQR' in line or 'IQR' in line:
                match = re.search(r'([-\d.]+)\s+([-\d.]+)\s+([-\d.]+)', line[40:])
                if match:
                    if in_all_stats:
                        result.all_stats.iqr_north = float(match.group(1))
                        result.all_stats.iqr_east = float(match.group(2))
                        result.all_stats.iqr_up = float(match.group(3))
                    else:
                        result.ref_stats.iqr_north = float(match.group(1))
                        result.ref_stats.iqr_east = float(match.group(2))
                        result.ref_stats.iqr_up = float(match.group(3))
            elif '| MEAN' in line or 'MEAN' in line:
                match = re.search(r'([-\d.]+)\s+([-\d.]+)\s+([-\d.]+)', line[40:])
                if match:
                    if in_all_stats:
                        result.all_stats.mean_north = float(match.group(1))
                        result.all_stats.mean_east = float(match.group(2))
                        result.all_stats.mean_up = float(match.group(3))
                    else:
                        result.ref_stats.mean_north = float(match.group(1))
                        result.ref_stats.mean_east = float(match.group(2))
                        result.ref_stats.mean_up = float(match.group(3))
            elif '| MEDIAN' in line or 'MEDIAN' in line:
                match = re.search(r'([-\d.]+)\s+([-\d.]+)\s+([-\d.]+)', line[40:])
                if match:
                    if in_all_stats:
                        result.all_stats.median_north = float(match.group(1))
                        result.all_stats.median_east = float(match.group(2))
                        result.all_stats.median_up = float(match.group(3))
                    else:
                        result.ref_stats.median_north = float(match.group(1))
                        result.ref_stats.median_east = float(match.group(2))
                        result.ref_stats.median_up = float(match.group(3))
            elif '| MIN' in line or 'MIN' in line:
                match = re.search(r'([-\d.]+)\s+([-\d.]+)\s+([-\d.]+)', line[40:])
                if match:
                    if in_all_stats:
                        result.all_stats.min_north = float(match.group(1))
                        result.all_stats.min_east = float(match.group(2))
                        result.all_stats.min_up = float(match.group(3))
                    else:
                        result.ref_stats.min_north = float(match.group(1))
                        result.ref_stats.min_east = float(match.group(2))
                        result.ref_stats.min_up = float(match.group(3))
            elif '| MAX' in line or 'MAX' in line:
                match = re.search(r'([-\d.]+)\s+([-\d.]+)\s+([-\d.]+)', line[40:])
                if match:
                    if in_all_stats:
                        result.all_stats.max_north = float(match.group(1))
                        result.all_stats.max_east = float(match.group(2))
                        result.all_stats.max_up = float(match.group(3))
                    else:
                        result.ref_stats.max_north = float(match.group(1))
                        result.ref_stats.max_east = float(match.group(2))
                        result.ref_stats.max_up = float(match.group(3))
            elif re.match(r'\s*\|\s+\d+\s+\|', line):
                # Station residual line
                # Format: |   1 | 0531             | W A |       0.14     -0.30      3.80 | V |
                try:
                    parts = line.split('|')
                    if len(parts) >= 5:
                        station = parts[2].strip()
                        flags = parts[3].strip().split()
                        residuals = parts[4].strip().split()
                        is_ref = 'V' not in line.split('|')[-2]

                        if len(residuals) >= 3 and len(flags) >= 2:
                            result.residuals.append(HLMStationResidual(
                                station_4char=station,
                                flag1=flags[0],
                                flag2=flags[1],
                                north_mm=float(residuals[0]),
                                east_mm=float(residuals[1]),
                                up_mm=float(residuals[2]),
                                is_reference=is_ref,
                                year=year,
                                doy=doy
                            ))
                except (ValueError, IndexError):
                    pass

        # Parse overall RMS/IQR/MAX line
        # Note: 'OVERALL' contains 'ALL' as substring, so use in_all_stats
        # flag instead of string matching to determine which stats block
        if 'OVERALL RMS/IQR/MAX(3D)' in line:
            match = re.search(r'([\d.]+)\s+([\d.]+)\s+([\d.]+)\s+(\w+)', line)
            if match:
                if in_all_stats:
                    result.all_stats.overall_rms_3d = float(match.group(1))
                    result.all_stats.overall_iqr_3d = float(match.group(2))
                    result.all_stats.overall_max_3d = float(match.group(3))
                    result.all_stats.max_3d_station = match.group(4)
                else:
                    result.ref_stats.overall_rms_3d = float(match.group(1))
                    result.ref_stats.overall_iqr_3d = float(match.group(2))
                    result.ref_stats.overall_max_3d = float(match.group(3))
                    result.ref_stats.max_3d_station = match.group(4)

        # Parse number of parameters/stations/coordinates
        if 'NUMBER OF PARAMETERS' in line:
            match = re.search(r':\s+(\d+)', line)
            if match:
                result.num_parameters = int(match.group(1))
        elif 'NUMBER OF STATIONS' in line:
            match = re.search(r':\s+(\d+)', line)
            if match:
                result.num_stations = int(match.group(1))
        elif 'NUMBER OF COORDINATES' in line:
            match = re.search(r':\s+(\d+)', line)
            if match:
                result.num_coordinates = int(match.group(1))

        # Parse RMS of transformation
        elif 'RMS OF TRANSFORMATION' in line:
            match = re.search(r':\s+([\d.]+)', line)
            if match:
                result.transform_params.rms_transformation = float(match.group(1))

        # Parse transformation parameters
        elif 'TRANSLATION IN  N' in line:
            match = re.search(r':\s+([-\d.]+)\s+\+-\s+([\d.]+)', line)
            if match:
                result.transform_params.trans_n_mm = float(match.group(1))
                result.transform_params.trans_n_sigma = float(match.group(2))
        elif 'TRANSLATION IN  E' in line:
            match = re.search(r':\s+([-\d.]+)\s+\+-\s+([\d.]+)', line)
            if match:
                result.transform_params.trans_e_mm = float(match.group(1))
                result.transform_params.trans_e_sigma = float(match.group(2))
        elif 'TRANSLATION IN  U' in line:
            match = re.search(r':\s+([-\d.]+)\s+\+-\s+([\d.]+)', line)
            if match:
                result.transform_params.trans_u_mm = float(match.group(1))
                result.transform_params.trans_u_sigma = float(match.group(2))
        elif 'ROTATION AROUND N-AXIS' in line:
            rot, sigma = parse_rotation_value(line)
            result.transform_params.rot_n_arcsec = rot
            result.transform_params.rot_n_sigma = sigma
        elif 'ROTATION AROUND E-AXIS' in line:
            rot, sigma = parse_rotation_value(line)
            result.transform_params.rot_e_arcsec = rot
            result.transform_params.rot_e_sigma = sigma
        elif 'ROTATION AROUND U-AXIS' in line:
            rot, sigma = parse_rotation_value(line)
            result.transform_params.rot_u_arcsec = rot
            result.transform_params.rot_u_sigma = sigma

        # Parse accepted/verified/rejected counts
        elif 'ACCEPTED STATIONS' in line:
            match = re.search(r':\s+(\d+)', line)
            if match:
                result.accepted_stations = int(match.group(1))
        elif 'VERIFIED STATIONS' in line:
            match = re.search(r':\s+(\d+)', line)
            if match:
                result.verified_stations = int(match.group(1))
        elif 'REJECTED STATIONS' in line:
            match = re.search(r':\s+(\d+)', line)
            if match:
                result.rejected_stations = int(match.group(1))

        # Check for end of station table
        if in_station_table and '---' in line and 'ALL' not in lines[i-1] if i > 0 else True:
            if in_all_stats:
                in_station_table = False
                in_all_stats = False

        i += 1

    # Compute all_stats from all residuals (ref + verified) if not
    # already populated from the file (Bernese only outputs stats for
    # reference stations in the table).
    if result.residuals and result.all_stats.rms_north == 0.0:
        result.all_stats = _compute_stats_from_residuals(result.residuals)

    return result


def get_hlm_path(base_dir: str, year: int, doy: int) -> Optional[str]:
    """
    Get the path to an HLM file.

    Args:
        base_dir: Base directory for DD results
        year: Year
        doy: Day of year

    Returns:
        Path to HLM file if it exists, None otherwise
    """
    patterns = [
        f"{base_dir}/{year}/{doy}/OUT/HLM_{year}{doy:03d}0.OUT.gz",
        f"{base_dir}/{year}/{doy:03d}/OUT/HLM_{year}{doy:03d}0.OUT.gz",
        f"{base_dir}/{year}/{doy}/OUT/HLM_{year}{doy:03d}0.OUT",
        f"{base_dir}/{year}/{doy:03d}/OUT/HLM_{year}{doy:03d}0.OUT",
    ]

    for path in patterns:
        if os.path.exists(path):
            return path

    return None


def load_hlm_timeseries(base_dir: str, year: int, start_doy: int, end_doy: int) -> list[HLMResult]:
    """
    Load HLM results for a range of days.

    Args:
        base_dir: Base directory for DD results
        year: Year
        start_doy: Starting day of year
        end_doy: Ending day of year

    Returns:
        List of HLMResult objects, one per day
    """
    results = []

    for doy in range(start_doy, end_doy + 1):
        hlm_path = get_hlm_path(base_dir, year, doy)
        if hlm_path:
            try:
                result = parse_hlm_file(hlm_path, year, doy)
                results.append(result)
            except Exception as e:
                print(f"Error parsing HLM for DOY {doy}: {e}")

    return results
