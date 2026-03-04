"""
Bernese Output Parser for Quality Metrics

Parses GPSEST output files (EDL_*.OUT) to extract:
- Coordinate solutions (X, Y, Z) with RMS
- A posteriori RMS of unit weight
- Processing statistics (observations, parameters, DOF)
- Hourly ZTD estimates
"""

import re
import os
import glob
from pathlib import Path
from dataclasses import dataclass
from typing import Optional
from datetime import datetime

from .db_models import (PPPSolution, ProcessingStats, QualityDatabase,
                        StationResidual, DataAvailability, ReceiverClock)
from .config import CAMPAIGN_PATH, DB_PATH

from pygnss_rt.utils.dates import doy_to_mjd


def find_ig_campaign_dir(
    campaign_path: str,
    year: int,
    doy: int,
    preferred_session: str | None = None,
) -> str | None:
    """Find IG campaign directory for a day, supporting optional suffixes.

    Matches (in priority order):
    - preferred_session if explicitly provided
    - YYDOYIG_GRE (preferred: full multi-GNSS)
    - YYDOYIG_GE
    - YYDOYIG_G
    - YYDOYIG (plain, no suffix)
    - any other YYDOYIG_* variant
    """
    session_id = f"{year % 100:02d}{doy:03d}"
    base = Path(campaign_path)

    if preferred_session:
        preferred = base / preferred_session
        if preferred.is_dir():
            return str(preferred)

    # Prefer multi-GNSS suffixes in order: GRE > GE > G > plain
    for suffix in ("_GRE", "_GE", "_G", ""):
        candidate = base / f"{session_id}IG{suffix}"
        if candidate.is_dir():
            return str(candidate)

    # Fallback: any other IG* variant
    candidates = sorted(
        p for p in base.glob(f"{session_id}IG*")
        if p.is_dir()
    )
    if candidates:
        return str(candidates[0])

    return None


def parse_fin_crd(filepath: str, year: int, doy: int) -> list[dict]:
    """
    Parse FIN_*.CRD file containing final combined network coordinates.

    Format:
    NUM  STATION NAME           X (M)          Y (M)          Z (M)     FLAG     SYSTEM
      1  BRUX              4027881.29722   306998.84602  4919499.07546    W      GRE

    Returns list of dicts with station coordinates.
    """
    results = []

    try:
        with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
            lines = f.readlines()
    except Exception as e:
        print(f"Error reading {filepath}: {e}")
        return results

    # Skip header lines, find data section
    in_data = False
    for line in lines:
        line = line.strip()
        if not line:
            continue

        # Check if this is the header line
        if 'STATION NAME' in line and 'X (M)' in line:
            in_data = True
            continue

        if not in_data:
            continue

        # Parse data line: NUM  STATION  X  Y  Z  FLAG  SYSTEM
        parts = line.split()
        if len(parts) >= 5:
            try:
                num = int(parts[0])
                station_id = parts[1]
                x = float(parts[2])
                y = float(parts[3])
                z = float(parts[4])

                results.append({
                    'station_id': station_id,
                    'year': year,
                    'doy': doy,
                    'x': x,
                    'y': y,
                    'z': z,
                    # FIN_CRD doesn't have formal errors - use nominal 1mm
                    'x_rms': 0.001,
                    'y_rms': 0.001,
                    'z_rms': 0.001
                })
            except (ValueError, IndexError):
                continue

    return results


def parse_fin_out(filepath: str, year: int, doy: int) -> dict:
    """
    Parse a single FIN_*.OUT file (combined network solution output).

    Extracts coordinates from lines ending with #CRD:
    Format: Sol Station Typ Correction EstimatedValue RMS APriori Unit ... #CRD

    Example:
       1 ZIM2                   X   -1.18773    4331299.58879    0.00067    4331300.77652 meters ... #CRD

    Returns dict with station coordinates and RMS.
    """
    result = {
        'station_id': None,
        'year': year,
        'doy': doy,
        'x': None,
        'y': None,
        'z': None,
        'x_rms': None,
        'y_rms': None,
        'z_rms': None
    }

    try:
        with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read()
    except Exception as e:
        print(f"Error reading {filepath}: {e}")
        return result

    # Find lines ending with #CRD (coordinate estimates)
    for line in content.split('\n'):
        if '#CRD' not in line:
            continue

        parts = line.split()
        if len(parts) < 7:
            continue

        try:
            # Format: Sol Station Typ Correction EstimatedValue RMS APriori Unit ...
            # Index:   0    1      2      3           4          5     6      7
            station = parts[1]
            coord_type = parts[2].upper()  # X, Y, or Z
            estimated_value = float(parts[4])
            rms = float(parts[5])

            if result['station_id'] is None:
                result['station_id'] = station

            if coord_type == 'X':
                result['x'] = estimated_value
                result['x_rms'] = rms
            elif coord_type == 'Y':
                result['y'] = estimated_value
                result['y_rms'] = rms
            elif coord_type == 'Z':
                result['z'] = estimated_value
                result['z_rms'] = rms
        except (ValueError, IndexError):
            continue

    return result


def parse_fin_out_all(out_dir: str, year: int, doy: int) -> list[dict]:
    """
    Parse all FIN_*.OUT files in a directory for a given day.

    Args:
        out_dir: Path to OUT directory containing FIN_*.OUT files
        year: 4-digit year
        doy: Day of year

    Returns:
        List of dicts with station coordinates
    """
    results = []

    # Pattern: FIN_YYYYDDD0_STATION.OUT
    pattern = os.path.join(out_dir, f"FIN_{year}{doy:03d}0_*.OUT")
    files = glob.glob(pattern)

    if not files:
        print(f"No FIN_*.OUT files found matching {pattern}")
        return results

    for filepath in files:
        parsed = parse_fin_out(filepath, year, doy)

        # Only add if we got valid coordinates
        if (parsed['station_id'] and
            parsed['x'] is not None and
            parsed['y'] is not None and
            parsed['z'] is not None):
            results.append(parsed)

    return results


def parse_gpsest_output(filepath: str) -> dict:
    """
    Parse a GPSEST output file (EDL_*.OUT) and extract quality metrics.

    Returns dict with:
    - station_id: str
    - year: int
    - doy: int
    - coordinates: {x, y, z, x_rms, y_rms, z_rms}
    - stats: {rms_unit_weight, chi2_dof, num_obs, num_params, dof}
    - ztd_hourly: list of {hour, ztd, ztd_rms}
    """
    result = {
        'station_id': None,
        'year': None,
        'doy': None,
        'coordinates': {},
        'stats': {},
        'ztd_hourly': [],
        'lat': None,
        'lon': None,
        'height': None
    }

    # Extract station and date from filename: EDL_20252960_BRUX.OUT
    # Format: EDL_YYYYSSSS_STATION.OUT where SSSS is session (DOY + session#, e.g., 2960 = DOY 296, session 0)
    filename = os.path.basename(filepath)
    match = re.match(r'EDL_(\d{4})(\d{3})\d_(\w+)\.OUT', filename)
    if match:
        result['year'] = int(match.group(1))
        result['doy'] = int(match.group(2))  # Extract only 3-digit DOY
        result['station_id'] = match.group(3)

    try:
        with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read()
            lines = content.split('\n')
    except Exception as e:
        print(f"Error reading {filepath}: {e}")
        return result

    # Parse A posteriori RMS of unit weight
    rms_match = re.search(r'A posteriori RMS of unit weight\s+([\d.]+)\s*m', content)
    if rms_match:
        result['stats']['rms_unit_weight'] = float(rms_match.group(1))

    # Parse Chi^2/DOF
    chi2_match = re.search(r'Chi\*\*2/DOF\s+([\d.]+)', content)
    if chi2_match:
        result['stats']['chi2_dof'] = float(chi2_match.group(1))

    # Parse observation statistics
    obs_match = re.search(r'Total number of observations\s+(\d+)', content)
    if obs_match:
        result['stats']['num_observations'] = int(obs_match.group(1))

    params_match = re.search(r'Total number of adjusted parameters\s+(\d+)', content)
    if params_match:
        result['stats']['num_parameters'] = int(params_match.group(1))

    dof_match = re.search(r'Degree of freedom \(DOF\)\s+(\d+)', content)
    if dof_match:
        result['stats']['dof'] = int(dof_match.group(1))

    # Parse coordinates (X, Y, Z with RMS)
    # Format: Sol Station name         Typ Correction  Estimated value  RMS error   A priori value Unit
    # Example:   1 BRUX                   X   -0.16134    4027881.13420    0.07218    4027881.29554 meters
    coord_pattern = r'^\s*\d+\s+(\w+)\s+([XYZ])\s+([-\d.]+)\s+([-\d.]+)\s+([-\d.]+)\s+([-\d.]+)\s+meters'

    for line in lines:
        match = re.match(coord_pattern, line)
        if match:
            station = match.group(1)
            coord_type = match.group(2).lower()
            # Group 3 = Correction, Group 4 = Estimated value, Group 5 = RMS error
            estimated = float(match.group(4))
            rms = float(match.group(5))

            if result['station_id'] is None:
                result['station_id'] = station

            result['coordinates'][coord_type] = estimated
            result['coordinates'][f'{coord_type}_rms'] = rms

    # Parse a priori coordinates for lat/lon/height
    # Format: num  Station name     obs e/f/h        X (m)     Y (m)     Z (m)    Latitude    Longitude    Height
    apriori_pattern = r'^\s*\d+\s+(\w+)\s+\w\s+\w+\s+([-\d.]+)\s+([-\d.]+)\s+([-\d.]+)\s+([-\d.]+)\s+([-\d.]+)\s+([-\d.]+)'
    for line in lines:
        match = re.match(apriori_pattern, line)
        if match and result['station_id'] and match.group(1) == result['station_id']:
            result['lat'] = float(match.group(5))
            result['lon'] = float(match.group(6))
            result['height'] = float(match.group(7))
            break

    # Parse hourly ZTD estimates (Troposphere U type)
    # Format: STATION                   U   Correction  Estimated value  RMS error   A priori  meters  YYYY-MM-DD HH:MM:SS ... #TRP
    # Example:  BRUX                   U        0.07261          2.28841    0.09125          2.21579 meters  2025-10-23 01:00:00
    # Note: Lines may have leading whitespace
    ztd_pattern = r'^\s*(\w+)\s+U\s+([-\d.]+)\s+([-\d.]+)\s+([-\d.]+)\s+([-\d.]+)\s+meters\s+(\d{4})-(\d{2})-(\d{2})\s+(\d{2}):(\d{2}):(\d{2}).*#TRP'

    for line in lines:
        match = re.search(ztd_pattern, line)
        if match:
            station = match.group(1)
            if station == result['station_id']:
                # Group 2 = Correction, Group 3 = Estimated value (ZTD), Group 4 = RMS error
                ztd_value = float(match.group(3))  # Estimated value = ZTD in meters
                ztd_rms = float(match.group(4))    # RMS error
                hour = int(match.group(9))

                result['ztd_hourly'].append({
                    'hour': hour,
                    'ztd': ztd_value,
                    'ztd_rms': ztd_rms
                })

    return result


def parse_campaign_day(
    campaign_path: str,
    year: int,
    doy: int,
    db: Optional[QualityDatabase] = None,
    preferred_session: str | None = None,
) -> list[dict]:
    """
    Parse all GPSEST output files for a campaign day.

    Args:
        campaign_path: Path to campaign directory (e.g., /home/.../CAMPAIGN54)
        year: 4-digit year
        doy: Day of year
        db: Optional database connection to save results
        preferred_session: Exact session directory name to prioritize

    Returns:
        List of parsed results
    """
    # Construct session directory path
    campaign_dir = find_ig_campaign_dir(
        campaign_path,
        year,
        doy,
        preferred_session=preferred_session,
    )
    out_dir = os.path.join(campaign_dir, "OUT") if campaign_dir else ""

    if not os.path.exists(out_dir):
        print(f"Output directory not found: {out_dir}")
        return []

    # Find all EDL output files
    # Filename format: EDL_YYYYSSSS_STATION.OUT where SSSS = DOY + session (e.g., 2960 = DOY 296, session 0)
    pattern = os.path.join(out_dir, f"EDL_{year}{doy:03d}?_*.OUT")
    files = glob.glob(pattern)

    if not files:
        print(f"No EDL output files found in {out_dir}")
        return []

    results = []
    mjd = doy_to_mjd(year, doy)

    for filepath in files:
        print(f"Parsing: {os.path.basename(filepath)}")
        parsed = parse_gpsest_output(filepath)

        if parsed['station_id'] and parsed['coordinates']:
            parsed['mjd'] = mjd
            results.append(parsed)

            # Save to database if connection provided
            if db:
                save_to_database(db, parsed, year, doy, mjd)

    print(f"Parsed {len(results)} station solutions for {year}/{doy:03d}")
    return results


def save_to_database(db: QualityDatabase, parsed: dict,
                     year: int, doy: int, mjd: float):
    """Save parsed results to PostgreSQL database"""

    coords = parsed.get('coordinates', {})
    stats = parsed.get('stats', {})
    station_id = parsed['station_id']

    # Save PPP solution
    if coords.get('x') and coords.get('y') and coords.get('z'):
        solution = PPPSolution(
            station_id=station_id,
            year=year,
            doy=doy,
            mjd=mjd,
            x=coords['x'],
            y=coords['y'],
            z=coords['z'],
            x_rms=coords.get('x_rms', 0),
            y_rms=coords.get('y_rms', 0),
            z_rms=coords.get('z_rms', 0),
            lat=parsed.get('lat'),
            lon=parsed.get('lon'),
            height=parsed.get('height')
        )
        db.insert_solution(solution)

    # Save processing stats
    if stats.get('rms_unit_weight'):
        proc_stats = ProcessingStats(
            station_id=station_id,
            year=year,
            doy=doy,
            mjd=mjd,
            rms_unit_weight=stats['rms_unit_weight'],
            chi2_dof=stats.get('chi2_dof'),
            num_observations=stats.get('num_observations', 0),
            num_parameters=stats.get('num_parameters'),
            dof=stats.get('dof')
        )
        db.insert_stats(proc_stats)

    # NOTE: ZTD from EDL single-station PPP OUT files is NOT saved here.
    # ZTD should be ingested from the combined FIN_*.TRO files (DD network
    # solution) via ingest_all.py, which provides much more precise estimates
    # (RMS ~1mm vs ~70-100mm from EDL single-station PPP).


def prn_to_constellation(prn: int) -> str:
    """Convert PRN number to constellation name"""
    if 1 <= prn <= 32:
        return 'GPS'
    elif 101 <= prn <= 128:
        return 'GLONASS'
    elif 201 <= prn <= 236:
        return 'GALILEO'
    else:
        return 'UNKNOWN'


def parse_edl_sum(filepath: str, year: int, doy: int) -> list[dict]:
    """
    Parse EDL_*.SUM file containing per-station, per-satellite residual RMS.

    File format (EDL_20253100.SUM):
     BASELINE  SESS     1     2     3  ...  TOT
     -------------------------------------------------------
     0531      3100   1.2   1.6   1.1  ...  2.3
     BRUX      3100   2.7   2.7   2.9  ...  1.9

    Returns list of dicts with station/satellite residuals.
    """
    results = []

    try:
        with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
            lines = f.readlines()
    except Exception as e:
        print(f"Error reading {filepath}: {e}")
        return results

    # Find header line with PRN numbers
    prns = []
    data_start = 0
    for i, line in enumerate(lines):
        if 'BASELINE' in line and 'SESS' in line:
            # Parse PRN numbers from header
            parts = line.split()
            for part in parts[2:]:  # Skip 'BASELINE' and 'SESS'
                try:
                    prn = int(part)
                    prns.append(prn)
                except ValueError:
                    if part == 'TOT':
                        break
            data_start = i + 2  # Skip header and separator line
            break

    if not prns:
        print(f"Could not find PRN header in {filepath}")
        return results

    # Parse data lines
    for line in lines[data_start:]:
        line = line.strip()
        if not line or line.startswith('-') or 'TOTAL' in line or 'TOT OBS' in line:
            continue

        parts = line.split()
        if len(parts) < 3:
            continue

        station_id = parts[0]
        # Skip numeric station IDs (these are 4-digit codes, not stations)
        if station_id.isdigit() and len(station_id) == 4:
            station_id = station_id  # Keep as is, e.g., "0531"

        # Parse residuals for each PRN
        for idx, prn in enumerate(prns):
            try:
                rms_str = parts[idx + 2]  # Skip station and session
                rms = float(rms_str)
                if rms > 0:  # Skip empty/zero values
                    results.append({
                        'year': year,
                        'doy': doy,
                        'station_id': station_id,
                        'prn': prn,
                        'constellation': prn_to_constellation(prn),
                        'rms': rms
                    })
            except (ValueError, IndexError):
                continue

    return results


def parse_chk_sum(filepath: str, year: int, doy: int) -> list[dict]:
    """
    Parse CHK_*.SUM file containing satellite observation statistics.

    File format (CHK_20253100.SUM):
    PRN | % Observations   Difference   | # Observations   Difference   | RMS
        | before   after   abs      rel | before    after  abs      rel | bef    aft
    ----+-------------------------------+-------------------------------+-----------
      1 |   0.99    0.99    0.00   0.00 |   1600    1600       0   0.00 |  2.4   2.4

    Returns list of dicts with data availability statistics.
    """
    results = []

    try:
        with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
            lines = f.readlines()
    except Exception as e:
        print(f"Error reading {filepath}: {e}")
        return results

    in_data = False
    for line in lines:
        # Skip empty lines and header/footer
        if not line.strip():
            continue
        if '---+---' in line or '----+---' in line:
            in_data = True
            continue
        if 'TOT |' in line:
            break  # End of data

        if not in_data:
            continue

        # Parse data line
        # Format: PRN |  pct_bef  pct_aft  diff_abs diff_rel | cnt_bef cnt_aft diff_abs diff_rel | rms_bef rms_aft
        try:
            # Remove pipe characters and split
            parts = line.replace('|', ' ').split()
            if len(parts) < 11:
                continue

            prn = int(parts[0])
            obs_pct_before = float(parts[1])
            obs_pct_after = float(parts[2])
            obs_count_before = int(parts[5])
            obs_count_after = int(parts[6])
            rms_before = float(parts[9])
            rms_after = float(parts[10])

            obs_rejected = obs_count_before - obs_count_after
            rejection_rate = (obs_rejected / obs_count_before * 100) if obs_count_before > 0 else 0.0

            results.append({
                'year': year,
                'doy': doy,
                'prn': prn,
                'constellation': prn_to_constellation(prn),
                'obs_pct_before': obs_pct_before,
                'obs_pct_after': obs_pct_after,
                'obs_count_before': obs_count_before,
                'obs_count_after': obs_count_after,
                'obs_rejected': obs_rejected,
                'rejection_rate': rejection_rate,
                'rms_before': rms_before,
                'rms_after': rms_after
            })
        except (ValueError, IndexError):
            continue

    return results


def parse_clk_rinex(filepath: str, year: int, doy: int) -> list[dict]:
    """
    Parse RINEX clock file (FIN_*.CLK) containing receiver clock estimates.

    File format:
    AR BRUX 2025 11 06 00 00  0.000000  2   -0.606459328545E+02  0.156128010051E-09

    Where:
    - AR = Receiver clock record type
    - BRUX = Station name
    - 2025 11 06 00 00 0.000000 = timestamp (YYYY MM DD HH MM SS.SSSSSS)
    - 2 = number of data values
    - -0.606459328545E+02 = clock offset (seconds)
    - 0.156128010051E-09 = clock sigma (seconds)

    Returns list of dicts with receiver clock data.
    """
    results = []

    try:
        with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
            lines = f.readlines()
    except Exception as e:
        print(f"Error reading {filepath}: {e}")
        return results

    for line in lines:
        if not line.startswith('AR '):
            continue

        try:
            # Parse AR record
            parts = line.split()
            if len(parts) < 9:
                continue

            station_id = parts[1]
            # Parse timestamp
            yr = int(parts[2])
            month = int(parts[3])
            day = int(parts[4])
            hour = int(parts[5])
            minute = int(parts[6])
            second = float(parts[7])

            # Calculate epoch as seconds of day
            epoch = hour * 3600 + minute * 60 + second

            # Parse clock offset and sigma
            n_values = int(parts[8])
            clock_offset = float(parts[9])
            clock_sigma = float(parts[10]) if n_values >= 2 and len(parts) > 10 else None

            results.append({
                'station_id': station_id,
                'year': year,
                'doy': doy,
                'epoch': epoch,
                'clock_offset': clock_offset,
                'clock_sigma': clock_sigma
            })
        except (ValueError, IndexError):
            continue

    return results


def parse_clock_data(
    campaign_path: str,
    year: int,
    doy: int,
    db: Optional['QualityDatabase'] = None,
    preferred_session: str | None = None,
) -> dict:
    """
    Parse receiver clock data from RINEX clock files for a campaign day.

    Args:
        campaign_path: Path to campaign directory
        year: 4-digit year
        doy: Day of year
        db: Optional database connection to save results
        preferred_session: Exact session directory name to prioritize

    Returns:
        Dict with count of parsed records
    """
    campaign_dir = find_ig_campaign_dir(
        campaign_path,
        year,
        doy,
        preferred_session=preferred_session,
    )
    out_dir = os.path.join(campaign_dir, "OUT") if campaign_dir else ""

    counts = {'receiver_clocks': 0}

    if not os.path.exists(out_dir):
        return counts

    # Parse FIN_*.CLK files for receiver clocks
    clk_pattern = os.path.join(out_dir, f"FIN_{year}{doy:03d}0_*.CLK")
    clk_files = glob.glob(clk_pattern)

    for filepath in clk_files:
        clocks = parse_clk_rinex(filepath, year, doy)
        counts['receiver_clocks'] += len(clocks)

        if db:
            for clk_data in clocks:
                clk = ReceiverClock(**clk_data)
                db.insert_receiver_clock(clk)

        print(f"  Parsed {len(clocks)} clock records from {os.path.basename(filepath)}")

    return counts


def parse_qc_data(
    campaign_path: str,
    year: int,
    doy: int,
    db: Optional['QualityDatabase'] = None,
    preferred_session: str | None = None,
) -> dict:
    """
    Parse QC data (station residuals and data availability) for a campaign day.

    Args:
        campaign_path: Path to campaign directory
        year: 4-digit year
        doy: Day of year
        db: Optional database connection to save results
        preferred_session: Exact session directory name to prioritize

    Returns:
        Dict with counts of parsed records
    """
    campaign_dir = find_ig_campaign_dir(
        campaign_path,
        year,
        doy,
        preferred_session=preferred_session,
    )
    out_dir = os.path.join(campaign_dir, "OUT") if campaign_dir else ""

    counts = {'station_residuals': 0, 'data_availability': 0}

    if not os.path.exists(out_dir):
        return counts

    # Parse EDL_*.SUM for station residuals
    edl_sum_pattern = os.path.join(out_dir, f"EDL_{year}{doy:03d}?.SUM")
    edl_sum_files = glob.glob(edl_sum_pattern)

    # Also try without session suffix
    edl_sum_pattern2 = os.path.join(out_dir, f"EDL_{year}{doy:03d}.SUM")
    if os.path.exists(edl_sum_pattern2):
        edl_sum_files.append(edl_sum_pattern2)

    for filepath in edl_sum_files:
        residuals = parse_edl_sum(filepath, year, doy)
        counts['station_residuals'] += len(residuals)

        if db:
            for res_data in residuals:
                res = StationResidual(**res_data)
                db.insert_station_residual(res)

        print(f"  Parsed {len(residuals)} station residuals from {os.path.basename(filepath)}")
        break  # Only process first matching file

    # Parse CHK_*.SUM for data availability
    chk_sum_pattern = os.path.join(out_dir, f"CHK_{year}{doy:03d}?.SUM")
    chk_sum_files = glob.glob(chk_sum_pattern)

    for filepath in chk_sum_files:
        availability = parse_chk_sum(filepath, year, doy)
        counts['data_availability'] += len(availability)

        if db:
            for da_data in availability:
                da = DataAvailability(**da_data)
                db.insert_data_availability(da)

        print(f"  Parsed {len(availability)} data availability records from {os.path.basename(filepath)}")
        break  # Only process first matching file

    return counts


def populate_database(campaign_path: str, year: int,
                      start_doy: int, end_doy: int):
    """
    Populate database with quality metrics for a range of days.

    Args:
        campaign_path: Path to campaign directory
        year: Year to process
        start_doy: Starting day of year
        end_doy: Ending day of year
    """
    db = QualityDatabase(DB_PATH)

    try:
        db.create_tables()
        print(f"Processing {year} DOY {start_doy} to {end_doy}")

        for doy in range(start_doy, end_doy + 1):
            parse_campaign_day(campaign_path, year, doy, db)

    finally:
        db.close()

    print("Database population complete")


def parse_mpr_sum(filepath: str, year: int, doy: int) -> list[dict]:
    """
    Parse MPR_*.SUM (MAUPRP summary) file for per-station observation quality metrics.

    Extracts: station_id, total observations, RMS, cycle slips, deleted obs, marked obs.

    MPR format:
     SESS FIL OK?  ST1  ST2 L(KM)   #OBS.    RMS    DX     DY     DZ    #SL   #DL   #MA  MAXL3      MIN. SLIP
     3100   1 OK   0531         0   68559     10      5    -81    -30     0   736   113      0              0

    Returns list of dicts with keys matching ObservationQuality fields.
    """
    results = []

    if not os.path.exists(filepath):
        return results

    with open(filepath, 'r') as f:
        lines = f.readlines()

    in_data = False
    for line in lines:
        stripped = line.strip()

        # Detect header separator line
        if stripped.startswith('----') and len(stripped) > 30:
            in_data = True
            continue

        # Stop at footer separator or summary
        if in_data and (stripped.startswith('----') or stripped.startswith('Tot:')):
            break

        if not in_data:
            continue

        # Parse data line
        parts = stripped.split()
        if len(parts) < 14:
            continue

        # For PPP (no ST2/baseline), split gives:
        # [0]=SESS [1]=FIL [2]=OK? [3]=ST1 [4]=L(KM) [5]=#OBS [6]=RMS
        # [7]=DX [8]=DY [9]=DZ [10]=#SL [11]=#DL [12]=#MA [13]=MAXL3 [14]=MIN.SLIP
        try:
            status = parts[2]  # OK or NOT
            station_id = parts[3]
            n_obs = int(parts[5])
            rms = float(parts[6])  # mm
            cycle_slips = int(parts[10])
            deleted = int(parts[11])
            marked = int(parts[12])

            # Compute cycle slip rate (per 1000 observations)
            slip_rate = (cycle_slips / n_obs * 1000) if n_obs > 0 else 0.0

            # Compute completeness: expected ~86400/30=2880 epochs for 30s data, ~82 sats
            # Use total obs vs expected. For multi-GNSS with ~80 sats, ~2880 epochs:
            # expected_obs ~= 80 * 2880 = 230400 (theoretical max)
            # Use a simpler approach: ratio of actual obs to the max station in the file
            # We'll compute completeness relative to 86400 epochs (1s) or normalize later

            # Quality level based on RMS and cycle slips
            if rms <= 8 and slip_rate < 1.0:
                quality = 'EXCELLENT'
            elif rms <= 12 and slip_rate < 5.0:
                quality = 'GOOD'
            elif rms <= 15 and slip_rate < 10.0:
                quality = 'ACCEPTABLE'
            else:
                quality = 'POOR'

            results.append({
                'station_id': station_id,
                'year': year,
                'doy': doy,
                'total_observations': n_obs,
                'cycle_slips': cycle_slips,
                'cycle_slip_rate': round(slip_rate, 3),
                'rms': rms,
                'deleted_obs': deleted,
                'marked_obs': marked,
                'quality_level': quality,
                'status': status,
            })
        except (ValueError, IndexError):
            continue

    return results


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 4:
        print("Usage: python bernese_parser.py <year> <start_doy> <end_doy>")
        print("Example: python bernese_parser.py 2025 296 296")
        sys.exit(1)

    year = int(sys.argv[1])
    start_doy = int(sys.argv[2])
    end_doy = int(sys.argv[3])

    populate_database(CAMPAIGN_PATH, year, start_doy, end_doy)
