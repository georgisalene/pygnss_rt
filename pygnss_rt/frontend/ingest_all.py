#!/usr/bin/env python3
"""
Unified Data Ingestion Script

Ingests all processing results to the database:
- Coordinates from FIN_*.OUT files (Estimated value with sub-mm RMS)
- ZTD + Gradients from FIN_*.TRO files
- Ambiguity resolution from AMB_*.SUM files

Usage:
    # Ingest single day
    python3 -m frontend.ingest_all 296

    # Ingest range of days
    python3 -m frontend.ingest_all 296 303

    # Ingest specific days
    python3 -m frontend.ingest_all 296 297 300 303
"""

import sys
import os
from pathlib import Path

import re
import glob

from .db_models import (QualityDatabase, AmbiguityResolution, ZTDHourly, PPPSolution,
                        SatelliteTracking, ProcessingStats, SatelliteAmbiguityPRN,
                        StationResidual, DataAvailability)
from .bernese_parser import parse_fin_out_all, parse_edl_sum, parse_chk_sum, parse_qc_data, parse_clock_data, find_ig_campaign_dir
from .config import CAMPAIGN_PATH, DB_PATH

from pygnss_rt.utils.dates import doy_to_mjd


def prn_to_constellation(prn: int) -> str:
    """Convert PRN number to constellation name"""
    if 1 <= prn <= 32:
        return 'GPS'
    elif 101 <= prn <= 128:
        return 'GLONASS'
    elif 201 <= prn <= 250:
        return 'GALILEO'
    return 'UNKNOWN'


def parse_processing_stats(out_dir: str, year: int, doy: int, db: QualityDatabase) -> int:
    """Parse FIN_*.OUT files for processing statistics.

    Extracts from GPSEST output:
    - A posteriori RMS of unit weight
    - Chi**2/DOF
    - Total number of observations
    - Degree of freedom (DOF)

    Returns count of records saved.
    """
    count = 0
    mjd = doy_to_mjd(year, doy)

    # Find all FIN_*.OUT files for individual stations
    pattern = os.path.join(out_dir, f"FIN_{year}{doy:03d}0_*.OUT")
    files = glob.glob(pattern)

    for filepath in files:
        # Extract station name from filename: FIN_YYYYDDD0_STATION.OUT
        filename = os.path.basename(filepath)
        match = re.match(r'FIN_\d{7}0_(\w+)\.OUT', filename)
        if not match:
            continue

        station_id = match.group(1)

        try:
            with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read()

            stats = {}

            # Parse A posteriori RMS of unit weight
            rms_match = re.search(r'A posteriori RMS of unit weight\s+([\d.]+)\s*m', content)
            if rms_match:
                stats['rms_unit_weight'] = float(rms_match.group(1))

            # Parse Chi**2/DOF
            chi2_match = re.search(r'Chi\*\*2/DOF\s+([\d.]+)', content)
            if chi2_match:
                stats['chi2_dof'] = float(chi2_match.group(1))

            # Parse Total number of observations
            obs_match = re.search(r'Total number of observations\s+(\d+)', content)
            if obs_match:
                stats['num_observations'] = int(obs_match.group(1))

            # Parse Total number of adjusted parameters
            params_match = re.search(r'Total number of adjusted parameters\s+(\d+)', content)
            if params_match:
                stats['num_parameters'] = int(params_match.group(1))

            # Parse Degree of freedom (DOF)
            dof_match = re.search(r'Degree of freedom \(DOF\)\s+(\d+)', content)
            if dof_match:
                stats['dof'] = int(dof_match.group(1))

            # Only save if we have essential statistics
            if stats.get('rms_unit_weight') and stats.get('num_observations'):
                proc_stats = ProcessingStats(
                    station_id=station_id,
                    year=year,
                    doy=doy,
                    mjd=mjd,
                    rms_unit_weight=stats['rms_unit_weight'],
                    chi2_dof=stats.get('chi2_dof'),
                    num_observations=stats['num_observations'],
                    num_parameters=stats.get('num_parameters'),
                    dof=stats.get('dof')
                )
                db.insert_stats(proc_stats)
                count += 1

        except Exception as e:
            continue

    return count


def parse_chk_file(filepath: str, year: int, doy: int, db: QualityDatabase) -> int:
    """Parse CHK_*.SUM file and save satellite tracking stats to database.

    CHK format:
    PRN | % Observations   Difference   | # Observations   Difference   | RMS
        | before   after   abs      rel | before    after  abs      rel | bef    aft
    ----+-------------------------------+-------------------------------+-----------
      1 |   0.99    0.99    0.00   0.00 |   1600    1600       0   0.00 |  2.4   2.4

    Returns count of records saved.
    """
    count = 0

    with open(filepath, 'r') as f:
        content = f.read()

    for line in content.split('\n'):
        line = line.strip()
        # Skip header lines, separators, and TOT line
        if not line or line.startswith('-') or line.startswith('PRN') or 'TOT' in line:
            continue

        # Parse data line: PRN | %obs_bef %obs_aft ... | #obs_bef #obs_aft ... | rms_bef rms_aft
        parts = line.replace('|', ' ').split()
        if len(parts) >= 11:
            try:
                prn = int(parts[0])
                obs_percent = float(parts[2])  # after value
                obs_count = int(parts[6])  # after value
                rms = float(parts[10])  # after value

                constellation = prn_to_constellation(prn)
                if constellation == 'UNKNOWN':
                    continue

                sat = SatelliteTracking(
                    year=year,
                    doy=doy,
                    prn=prn,
                    constellation=constellation,
                    obs_percent=obs_percent,
                    obs_count=obs_count,
                    rms=rms
                )
                db.insert_satellite_tracking(sat)
                count += 1
            except (ValueError, IndexError):
                continue

    return count


def parse_tro_file(filepath: str, db: QualityDatabase) -> int:
    """Parse TRO file and save to database. Returns count of records saved.

    TRO format: STATION EPOCH TROTOT STDDEV TGNTOT STDDEV TGETOT STDDEV
    Supports both plain text and gzipped (.gz) files.
    """
    import gzip
    count = 0

    # Handle gzipped files
    if filepath.endswith('.gz'):
        with gzip.open(filepath, 'rt') as f:
            content = f.read()
    else:
        with open(filepath, 'r') as f:
            content = f.read()

    in_solution = False
    for line in content.split('\n'):
        if '+TROP/SOLUTION' in line:
            in_solution = True
            continue
        if '-TROP/SOLUTION' in line:
            break
        if not in_solution or line.startswith('*') or not line.strip():
            continue

        parts = line.split()
        if len(parts) >= 8:
            station_id = parts[0]
            epoch = parts[1]
            epoch_parts = epoch.split(':')

            if len(epoch_parts) == 3:
                year = int(epoch_parts[0])
                doy = int(epoch_parts[1])
                seconds = int(epoch_parts[2])
                hour = seconds // 3600

                # Parse all values: TROTOT STDDEV TGNTOT STDDEV TGETOT STDDEV
                # Skip lines with invalid values (e.g., '******')
                try:
                    trotot = float(parts[2]) / 1000.0  # mm to m
                    trotot_std = float(parts[3]) / 1000.0  # TROTOT STDDEV
                    tgn = float(parts[4]) / 1000.0  # TGNTOT
                    tgn_std = float(parts[5]) / 1000.0  # TGNTOT STDDEV
                    tge = float(parts[6]) / 1000.0  # TGETOT
                    tge_std = float(parts[7]) / 1000.0  # TGETOT STDDEV
                except ValueError:
                    # Skip lines with invalid/missing values
                    continue

                mjd = doy_to_mjd(year, doy) + hour / 24.0

                ztd = ZTDHourly(
                    station_id=station_id,
                    year=year,
                    doy=doy,
                    hour=hour,
                    mjd=mjd,
                    ztd=trotot,
                    ztd_rms=trotot_std,
                    grad_n=tgn,
                    grad_n_rms=tgn_std,
                    grad_e=tge,
                    grad_e_rms=tge_std
                )
                db.insert_ztd(ztd)
                count += 1

    return count


def parse_amb_file(filepath: str, year: int, doy: int, db: QualityDatabase) -> int:
    """Parse AMB file and save to database. Returns count of records saved."""
    count = 0

    with open(filepath) as f:
        content = f.read()

    wl_lines = [l for l in content.split("\n") if "#AR_WL" in l and "Tot:" not in l]
    nl_lines = [l for l in content.split("\n") if "#AR_NL" in l and "Tot:" not in l]

    stations = {}
    for line in wl_lines:
        parts = line.split()
        if len(parts) >= 8:
            sta = parts[1]
            rate = parts[6]
            sys_type = parts[7]
            sys2 = parts[8] if len(parts) > 8 else ""

            if sta not in stations:
                stations[sta] = {"WL_G": None, "WL_E": None, "WL_GE": None,
                                "NL_G": None, "NL_E": None, "NL_GE": None, "receiver": ""}

            if sys_type == "G" and sys2 != "E":
                stations[sta]["WL_G"] = float(rate)
            elif sys_type == "E":
                stations[sta]["WL_E"] = float(rate)
            elif sys_type == "G" and sys2 == "E":
                stations[sta]["WL_GE"] = float(rate)
                for i, p in enumerate(parts):
                    if p in ["LEICA", "TRIMBLE", "SEPT", "JAVAD"]:
                        stations[sta]["receiver"] = " ".join(parts[i:i+2])
                        break

    for line in nl_lines:
        parts = line.split()
        if len(parts) >= 8:
            sta = parts[1]
            rate = parts[6]
            sys_type = parts[7]
            sys2 = parts[8] if len(parts) > 8 else ""

            if sta in stations:
                if sys_type == "G" and sys2 != "E":
                    stations[sta]["NL_G"] = float(rate)
                elif sys_type == "E":
                    stations[sta]["NL_E"] = float(rate)
                elif sys_type == "G" and sys2 == "E":
                    stations[sta]["NL_GE"] = float(rate)

    for sta, s in stations.items():
        amb = AmbiguityResolution(
            station_id=sta,
            year=year,
            doy=doy,
            receiver=s['receiver'] if s['receiver'] else None,
            wl_gps=s['WL_G'],
            wl_gal=s['WL_E'],
            wl_combined=s['WL_GE'],
            nl_gps=s['NL_G'],
            nl_gal=s['NL_E'],
            nl_combined=s['NL_GE'],
        )
        db.insert_ambiguity(amb)
        count += 1

    return count


def parse_satellite_ambiguity_prn(filepath: str, year: int, doy: int, db: QualityDatabase) -> int:
    """Parse satellite-wise ambiguity resolution statistics from AMB_*.SUM file.

    Parses the section:
    PRN |   Amb   |  L1 & L2 ambiguities  |     L5 ambiguities
        |  total  |  solved  #/clu    rel |  solved  #/clu    rel
    ----+---------+-----------------------+----------------------
      1 |      62 |      46   11.9   68.0 |      62   48.1   97.9
    ...
    GPS |    2411 |    2202   41.5   89.1 |    2411   50.2   98.0
    GAL |    1660 |    1539   21.4   88.4 |    1660   23.7   95.8

    Returns count of records saved.
    """
    count = 0

    with open(filepath) as f:
        content = f.read()

    # Find the satellite-wise section
    in_sat_section = False
    for line in content.split('\n'):
        # Detect start of satellite-wise section
        if 'Satellite-wise Ambiguity Resolution Statistics' in line:
            in_sat_section = True
            continue

        if not in_sat_section:
            continue

        # Skip header and separator lines
        line = line.strip()
        if not line or line.startswith('PRN') or line.startswith('---') or '|' not in line:
            continue

        # Stop at summary lines (GPS, GAL) - we want individual PRNs
        if line.startswith('GPS') or line.startswith('GAL'):
            continue

        # Parse data line: PRN | amb_total | l1l2_solved #/clu rel | l5_solved #/clu rel
        # Example:   1 |      62 |      46   11.9   68.0 |      62   48.1   97.9
        parts = line.replace('|', ' ').split()
        if len(parts) >= 7:
            try:
                prn = int(parts[0])
                amb_total = int(parts[1])
                l1l2_solved = int(parts[2])
                l1l2_cluster = float(parts[3])
                l1l2_rel = float(parts[4])
                l5_solved = int(parts[5])
                l5_cluster = float(parts[6])
                l5_rel = float(parts[7]) if len(parts) > 7 else 0.0

                # Determine constellation from PRN
                if 1 <= prn <= 32:
                    constellation = 'GPS'
                elif 201 <= prn <= 250:
                    constellation = 'GAL'
                else:
                    continue  # Skip unknown constellations

                sat_amb = SatelliteAmbiguityPRN(
                    year=year,
                    doy=doy,
                    prn=prn,
                    constellation=constellation,
                    amb_total=amb_total,
                    l1l2_solved=l1l2_solved,
                    l1l2_cluster=l1l2_cluster,
                    l1l2_rel=l1l2_rel,
                    l5_solved=l5_solved,
                    l5_cluster=l5_cluster,
                    l5_rel=l5_rel
                )
                db.insert_satellite_ambiguity_prn(sat_amb)
                count += 1
            except (ValueError, IndexError):
                continue

    return count


def ingest_day(campaign_path: str, year: int, doy: int, db: QualityDatabase) -> dict:
    """Ingest all data for a single day. Returns counts."""
    session_path = find_ig_campaign_dir(campaign_path, year, doy)

    results = {
        'doy': doy,
        'coordinates': 0,
        'proc_stats': 0,
        'ztd': 0,
        'ambiguity': 0,
        'satellites': 0,
        'sat_ambiguity_prn': 0,
        'station_residuals': 0,
        'data_availability': 0,
        'receiver_clocks': 0,
        'errors': []
    }

    if not session_path or not os.path.exists(session_path):
        results['errors'].append(f"Session directory not found for DOY {doy}")
        return results

    # 1. Parse coordinates from FIN_*.OUT files (Estimated value with sub-mm RMS)
    out_dir = os.path.join(session_path, "OUT")
    if os.path.exists(out_dir):
        try:
            coord_results = parse_fin_out_all(out_dir, year, doy)
            mjd = doy_to_mjd(year, doy)
            for coord in coord_results:
                solution = PPPSolution(
                    station_id=coord['station_id'],
                    year=year,
                    doy=doy,
                    mjd=mjd,
                    x=coord['x'],
                    y=coord['y'],
                    z=coord['z'],
                    x_rms=coord['x_rms'],
                    y_rms=coord['y_rms'],
                    z_rms=coord['z_rms']
                )
                db.insert_solution(solution)
            results['coordinates'] = len(coord_results)
        except Exception as e:
            results['errors'].append(f"Coordinates: {e}")

        # 1b. Parse processing statistics from FIN_*.OUT files
        try:
            results['proc_stats'] = parse_processing_stats(out_dir, year, doy, db)
        except Exception as e:
            results['errors'].append(f"Processing stats: {e}")
    else:
        results['errors'].append(f"OUT directory not found: {out_dir}")

    # 2. Parse ZTD from TRO file (try multiple patterns)
    tro_patterns = [
        os.path.join(session_path, "ATM", f"FIN_{year}{doy:03d}0.TRO"),
        os.path.join(session_path, "ATM", f"FIN_{year}{doy:03d}0.TRO.gz"),
        os.path.join(session_path, "ATM", f"F10_{year}{doy:03d}0.TRO"),
        os.path.join(session_path, "ATM", f"F10_{year}{doy:03d}0.TRO.gz"),
    ]
    tro_file = None
    for pattern in tro_patterns:
        if os.path.exists(pattern):
            tro_file = pattern
            break

    if tro_file:
        try:
            results['ztd'] = parse_tro_file(tro_file, db)
        except Exception as e:
            results['errors'].append(f"ZTD: {e}")
    else:
        results['errors'].append(f"TRO file not found")

    # 3. Parse ambiguity from AMB file
    amb_file = os.path.join(session_path, "OUT", f"AMB_{year}{doy:03d}0.SUM")
    if os.path.exists(amb_file):
        try:
            results['ambiguity'] = parse_amb_file(amb_file, year, doy, db)
        except Exception as e:
            results['errors'].append(f"Ambiguity: {e}")

        # 3b. Parse satellite-wise ambiguity resolution from same AMB file
        try:
            results['sat_ambiguity_prn'] = parse_satellite_ambiguity_prn(amb_file, year, doy, db)
        except Exception as e:
            results['errors'].append(f"Sat Ambiguity PRN: {e}")
    else:
        results['errors'].append(f"AMB file not found")

    # 4. Parse satellite tracking from CHK file
    chk_file = os.path.join(session_path, "OUT", f"CHK_{year}{doy:03d}0.SUM")
    if os.path.exists(chk_file):
        try:
            results['satellites'] = parse_chk_file(chk_file, year, doy, db)
        except Exception as e:
            results['errors'].append(f"Satellites: {e}")
    else:
        results['errors'].append(f"CHK file not found")

    # 5. Parse QC data (station residuals and data availability)
    try:
        qc_counts = parse_qc_data(campaign_path, year, doy, db)
        results['station_residuals'] = qc_counts.get('station_residuals', 0)
        results['data_availability'] = qc_counts.get('data_availability', 0)
    except Exception as e:
        results['errors'].append(f"QC Data: {e}")

    # 6. Parse receiver clock data from RINEX clock files
    try:
        clk_counts = parse_clock_data(campaign_path, year, doy, db)
        results['receiver_clocks'] = clk_counts.get('receiver_clocks', 0)
    except Exception as e:
        results['errors'].append(f"Clock Data: {e}")

    return results


def reingest_ztd_from_pppar(campaign_path: str, year: int, doys: list, db: QualityDatabase) -> int:
    """
    Re-ingest ZTD data from PPP-AR TRO files in CAMPAIGN54.

    Directory format: {campaign_path}/{yy}{doy}IG/ATM/FIN_{year}{doy}0_{station}.TRO

    PPP-AR produces per-station TRO files, unlike DD which has all stations in one file.
    """
    import glob
    total_count = 0

    # Clear existing ZTD data for these DOYs
    conn = db.connect()
    for doy in doys:
        conn.execute("DELETE FROM ztd_hourly WHERE year = ? AND doy = ?", [year, doy])
    print(f"Cleared existing ZTD data for DOYs {min(doys)}-{max(doys)}")

    for doy in doys:
        session_path = find_ig_campaign_dir(campaign_path, year, doy)
        if not session_path:
            print(f"  DOY {doy}: Session directory not found")
            continue
        atm_dir = os.path.join(session_path, "ATM")

        if not os.path.exists(atm_dir):
            print(f"  DOY {doy}: ATM directory not found")
            continue

        # Find all per-station TRO files: FIN_YYYYDDD0_*.TRO
        pattern = os.path.join(atm_dir, f"FIN_{year}{doy:03d}0_*.TRO")
        tro_files = glob.glob(pattern)

        if not tro_files:
            print(f"  DOY {doy}: No PPP-AR TRO files found")
            continue

        day_count = 0
        for tro_file in tro_files:
            count = parse_tro_file(tro_file, db)
            day_count += count

        total_count += day_count
        print(f"  DOY {doy}: {day_count} ZTD records from {len(tro_files)} station files")

    return total_count


def reingest_ztd_from_mgx(base_dir: str, year: int, doys: list, db: QualityDatabase) -> int:
    """
    Re-ingest ZTD data from MGX directory structure.

    Directory format: {base_dir}/{year}/{doy}/ATM/F10_{year}{doy}0.TRO.gz

    This function first clears existing ZTD data for the specified DOYs,
    then re-ingests from the correct TRO files.
    """
    import gzip
    total_count = 0

    # Clear existing ZTD data for these DOYs
    conn = db.connect()
    for doy in doys:
        conn.execute("DELETE FROM ztd_hourly WHERE year = ? AND doy = ?", [year, doy])
    print(f"Cleared existing ZTD data for DOYs {min(doys)}-{max(doys)}")

    for doy in doys:
        # Try different file patterns
        tro_patterns = [
            f"{base_dir}/{year}/{doy:03d}/ATM/F10_{year}{doy:03d}0.TRO.gz",
            f"{base_dir}/{year}/{doy}/ATM/F10_{year}{doy:03d}0.TRO.gz",
            f"{base_dir}/{year}/{doy:03d}/ATM/F10_{year}{doy:03d}0.TRO",
        ]

        tro_file = None
        for pattern in tro_patterns:
            if os.path.exists(pattern):
                tro_file = pattern
                break

        if not tro_file:
            print(f"  DOY {doy}: TRO file not found")
            continue

        count = parse_tro_file(tro_file, db)
        total_count += count
        print(f"  DOY {doy}: {count} ZTD records ingested from {os.path.basename(tro_file)}")

    return total_count


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        print("\nAvailable sessions:")
        for d in sorted(os.listdir(CAMPAIGN_PATH)):
            if d.endswith("IG") and len(d) == 7:
                print(f"  {d}")
        sys.exit(1)

    # Parse DOY arguments
    args = sys.argv[1:]

    # Check if it's a range (2 args) or list of DOYs
    if len(args) == 2 and args[0].isdigit() and args[1].isdigit():
        start_doy = int(args[0])
        end_doy = int(args[1])
        if end_doy > start_doy:
            # It's a range
            doys = list(range(start_doy, end_doy + 1))
        else:
            # Just two DOYs
            doys = [start_doy, end_doy]
    else:
        doys = [int(d) for d in args]

    year = 2025  # Default year

    print("=" * 70)
    print("UNIFIED DATA INGESTION")
    print("=" * 70)
    print(f"Campaign: {CAMPAIGN_PATH}")
    print(f"Database: {DB_PATH}")
    print(f"DOYs to process: {doys}")
    print("=" * 70)

    # Initialize database
    db = QualityDatabase(DB_PATH)
    db.connect()
    db.create_tables()

    totals = {'coordinates': 0, 'proc_stats': 0, 'ztd': 0, 'ambiguity': 0, 'satellites': 0,
              'sat_ambiguity_prn': 0, 'station_residuals': 0, 'data_availability': 0, 'receiver_clocks': 0}

    for doy in doys:
        print(f"\n--- DOY {doy} ---")
        result = ingest_day(CAMPAIGN_PATH, year, doy, db)

        print(f"  Coordinates: {result['coordinates']} stations")
        print(f"  Proc Stats:  {result['proc_stats']} stations")
        print(f"  ZTD records: {result['ztd']}")
        print(f"  Ambiguity:   {result['ambiguity']} stations")
        print(f"  Satellites:  {result['satellites']} PRNs")
        print(f"  Sat Amb PRN: {result['sat_ambiguity_prn']} PRNs")
        print(f"  Sta Resid:   {result['station_residuals']} records")
        print(f"  Data Avail:  {result['data_availability']} records")
        print(f"  Rcvr Clocks: {result['receiver_clocks']} records")

        if result['errors']:
            for err in result['errors']:
                print(f"  Warning: {err}")

        totals['coordinates'] += result['coordinates']
        totals['proc_stats'] += result['proc_stats']
        totals['ztd'] += result['ztd']
        totals['ambiguity'] += result['ambiguity']
        totals['satellites'] += result['satellites']
        totals['sat_ambiguity_prn'] += result['sat_ambiguity_prn']
        totals['station_residuals'] += result['station_residuals']
        totals['data_availability'] += result['data_availability']
        totals['receiver_clocks'] += result['receiver_clocks']

    db.close()

    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"Total coordinates: {totals['coordinates']}")
    print(f"Total proc stats:  {totals['proc_stats']}")
    print(f"Total ZTD records: {totals['ztd']}")
    print(f"Total ambiguity:   {totals['ambiguity']}")
    print(f"Total satellites:  {totals['satellites']}")
    print(f"Total sat amb PRN: {totals['sat_ambiguity_prn']}")
    print(f"Total sta resid:   {totals['station_residuals']}")
    print(f"Total data avail:  {totals['data_availability']}")
    print(f"Total rcvr clocks: {totals['receiver_clocks']}")
    print("=" * 70)


if __name__ == "__main__":
    main()
