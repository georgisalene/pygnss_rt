#!/usr/bin/env python3
"""
Troposphere (TRO) File Parser

Parses Bernese FIN_*.TRO files to extract hourly ZTD and gradient estimates.

Usage:
    # View report only
    python -m frontend.tro_parser 296 297

    # Save to database
    python -m frontend.tro_parser 296 297 --save

    # Query from database
    python -m frontend.tro_parser --query 2025 296 297
"""

import sys
import os
import re
from pathlib import Path
from dataclasses import dataclass
from typing import Optional

from .db_models import QualityDatabase, ZTDHourly
from .bernese_parser import find_ig_campaign_dir
from .config import CAMPAIGN_PATH, DB_PATH

from pygnss_rt.utils.dates import doy_to_mjd


def parse_tro_file(filepath: str, save_to_db: bool = False, db: Optional[QualityDatabase] = None) -> list[dict]:
    """
    Parse a TRO file and extract ZTD + gradient data.

    Returns list of dicts with:
    - station_id, year, doy, hour, epoch_seconds
    - trotot (ZTD in mm), trotot_std
    - tgn (north gradient), tgn_std
    - tge (east gradient), tge_std
    """
    results = []

    with open(filepath, 'r') as f:
        content = f.read()

    # Find the TROP/SOLUTION section
    in_solution = False

    for line in content.split('\n'):
        if '+TROP/SOLUTION' in line:
            in_solution = True
            continue
        if '-TROP/SOLUTION' in line:
            break
        if not in_solution:
            continue
        if line.startswith('*') or not line.strip():
            continue

        # Parse data line
        # Format: STATION__ YYYY:DOY:SSSSS TROTOT STDDEV  TGNTOT STDDEV  TGETOT STDDEV
        # Example: 0531      2025:298:03600 2215.4    0.6  -0.192  0.041  -0.217  0.041
        parts = line.split()
        if len(parts) >= 8:
            station_id = parts[0]
            epoch = parts[1]  # YYYY:DOY:SSSSS

            # Parse epoch
            epoch_parts = epoch.split(':')
            if len(epoch_parts) == 3:
                year = int(epoch_parts[0])
                doy = int(epoch_parts[1])
                seconds = int(epoch_parts[2])
                hour = seconds // 3600

                # Parse values (in mm for ZTD, mm for gradients)
                trotot = float(parts[2])  # ZTD in mm
                trotot_std = float(parts[3])
                tgn = float(parts[4])  # North gradient
                tgn_std = float(parts[5])
                tge = float(parts[6])  # East gradient
                tge_std = float(parts[7])

                record = {
                    'station_id': station_id,
                    'year': year,
                    'doy': doy,
                    'hour': hour,
                    'epoch_seconds': seconds,
                    'trotot': trotot,
                    'trotot_std': trotot_std,
                    'tgn': tgn,
                    'tgn_std': tgn_std,
                    'tge': tge,
                    'tge_std': tge_std
                }
                results.append(record)

                # Save to database
                if save_to_db and db:
                    # Convert mm to meters for database storage
                    mjd = doy_to_mjd(year, doy) + hour / 24.0
                    ztd = ZTDHourly(
                        station_id=station_id,
                        year=year,
                        doy=doy,
                        hour=hour,
                        mjd=mjd,
                        ztd=trotot / 1000.0,  # Convert mm to m
                        ztd_rms=trotot_std / 1000.0,
                        grad_n=tgn / 1000.0,
                        grad_e=tge / 1000.0
                    )
                    db.insert_ztd(ztd)

    return results


def print_tro_summary(results: list[dict], doy: int):
    """Print summary of TRO data"""
    print("=" * 100)
    print(f"TROPOSPHERE (ZTD) REPORT - DOY {doy}")
    print("=" * 100)
    print()

    # Group by station
    stations = {}
    for r in results:
        sta = r['station_id']
        if sta not in stations:
            stations[sta] = []
        stations[sta].append(r)

    print(f"{'Station':<8} {'Hours':>6} {'ZTD Mean':>10} {'ZTD Std':>10} {'ZTD Range':>10} {'GradN Mean':>10} {'GradE Mean':>10}")
    print("-" * 80)

    for sta in sorted(stations.keys()):
        data = stations[sta]
        ztds = [d['trotot'] for d in data]
        gn = [d['tgn'] for d in data]
        ge = [d['tge'] for d in data]

        mean_ztd = sum(ztds) / len(ztds)
        std_ztd = (sum((x - mean_ztd)**2 for x in ztds) / len(ztds))**0.5
        range_ztd = max(ztds) - min(ztds)
        mean_gn = sum(gn) / len(gn)
        mean_ge = sum(ge) / len(ge)

        print(f"{sta:<8} {len(data):>6} {mean_ztd:>10.1f} {std_ztd:>10.2f} {range_ztd:>10.1f} {mean_gn:>10.3f} {mean_ge:>10.3f}")

    print("-" * 80)
    print(f"Total: {len(stations)} stations, {len(results)} hourly records")


def query_tro_data(year: int, start_doy: int, end_doy: int, station: Optional[str] = None):
    """Query and display ZTD data from database"""
    db = QualityDatabase(DB_PATH)
    db.connect()

    for doy in range(start_doy, end_doy + 1):
        if station:
            data = db.get_ztd(station, year, doy)
        else:
            # Get all stations for this DOY
            conn = db.connect()
            result = conn.execute("""
                SELECT * FROM ztd_hourly
                WHERE year = ? AND doy = ?
                ORDER BY station_id, hour
            """, [year, doy]).fetchdf()
            data = result.to_dict('records')

        if data:
            print(f"\n{'='*80}")
            print(f"ZTD DATA FROM DATABASE - {year} DOY {doy}")
            print(f"{'='*80}")
            print(f"{'Station':<8} {'Hour':>4} {'ZTD(mm)':>10} {'RMS':>8} {'GradN':>8} {'GradE':>8}")
            print("-" * 60)

            for row in data:
                ztd_mm = row['ztd'] * 1000
                rms_mm = row['ztd_rms'] * 1000
                gn = row.get('grad_n', 0) or 0
                ge = row.get('grad_e', 0) or 0
                gn_mm = gn * 1000
                ge_mm = ge * 1000
                print(f"{row['station_id']:<8} {row['hour']:>4} {ztd_mm:>10.1f} {rms_mm:>8.2f} {gn_mm:>8.3f} {ge_mm:>8.3f}")

            print(f"\nTotal: {len(data)} records")

    db.close()


def main():
    campaign_path = CAMPAIGN_PATH

    # Check for query mode
    if len(sys.argv) > 1 and sys.argv[1] == "--query":
        if len(sys.argv) < 5:
            print("Usage: python -m frontend.tro_parser --query <year> <start_doy> <end_doy> [station]")
            sys.exit(1)
        year = int(sys.argv[2])
        start_doy = int(sys.argv[3])
        end_doy = int(sys.argv[4])
        station = sys.argv[5] if len(sys.argv) > 5 else None
        query_tro_data(year, start_doy, end_doy, station)
        return

    # Check for --save flag
    save_to_db = "--save" in sys.argv
    args = [a for a in sys.argv[1:] if a != "--save"]

    # Initialize database if saving
    db = None
    if save_to_db:
        db = QualityDatabase(DB_PATH)
        db.connect()
        db.create_tables()

    # Get DOYs from command line or auto-detect
    if args:
        doys = [int(d) for d in args]
    else:
        doys = []
        for d in os.listdir(campaign_path):
            # Match IG, IG_G, IG_GE, IG_GRE, etc.
            if "IG" in d and len(d) >= 7:
                try:
                    doy = int(d[2:5])
                    doys.append(doy)
                except ValueError:
                    pass
        doys = sorted(set(doys))

    total_saved = 0
    for doy in doys:
        session_path = find_ig_campaign_dir(campaign_path, 2025, doy)
        if not session_path:
            print(f"TRO session dir not found for DOY {doy}")
            continue
        tro_file = os.path.join(session_path, "ATM", f"FIN_2025{doy:03d}0.TRO")

        if os.path.exists(tro_file):
            results = parse_tro_file(tro_file, save_to_db=save_to_db, db=db)
            print_tro_summary(results, doy)

            if save_to_db:
                print(f"\n>> Saved {len(results)} ZTD records to database")
                total_saved += len(results)
            print()
        else:
            print(f"TRO file not found for DOY {doy}: {tro_file}")

    if db:
        db.close()
        if total_saved > 0:
            print(f"\nTotal saved: {total_saved} records")


if __name__ == "__main__":
    main()
