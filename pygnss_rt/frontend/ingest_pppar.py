#!/usr/bin/env python3
"""
Ingest PPP-AR Coordinates from Bernese CRD Files into DuckDB

This script reads PPP-AR final coordinate files from:
  /home/ahunegnaw/GPSDATA/CAMPAIGN54/25{doy}IG/STA/FIN_2025{doy}0.CRD

And stores them in the DuckDB database for dashboard visualization.

Usage:
    python ingest_pppar.py                    # Ingest all available DOYs
    python ingest_pppar.py 296 310            # Ingest DOY range 296-310
    python ingest_pppar.py --list             # List available DOYs
"""

import os
import sys
import glob
import argparse
from datetime import datetime
from pathlib import Path

# Import from package modules
from pygnss_rt.frontend.config import DB_PATH, CAMPAIGN_PATH
from pygnss_rt.frontend.db_models import QualityDatabase, PPPSolution
from pygnss_rt.frontend.bernese_parser import find_ig_campaign_dir
from pygnss_rt.stations.coordinates import xyz_to_llh
from pygnss_rt.utils.dates import doy_to_mjd


def parse_crd_file(crd_path: str, year: int, doy: int) -> list:
    """
    Parse a Bernese CRD coordinate file.

    Args:
        crd_path: Path to the CRD file
        year: Year of the solution
        doy: Day of year

    Returns:
        List of PPPSolution objects
    """
    if not os.path.exists(crd_path):
        return []

    solutions = []
    mjd = doy_to_mjd(year, doy)

    with open(crd_path, 'r') as f:
        in_data = False
        for line in f:
            # Skip until we find the header
            if 'NUM  STATION NAME' in line:
                in_data = True
                continue

            if not in_data:
                continue

            # Skip empty lines
            if not line.strip():
                continue

            # Parse data line: NUM  STATION  X  Y  Z  FLAG  SYSTEM
            parts = line.split()
            if len(parts) >= 5:
                try:
                    station_id = parts[1]
                    x = float(parts[2])
                    y = float(parts[3])
                    z = float(parts[4])

                    # Convert to lat/lon/height
                    lat, lon, height = xyz_to_llh(x, y, z)

                    # Create PPPSolution
                    solutions.append(PPPSolution(
                        station_id=station_id,
                        year=year,
                        doy=doy,
                        mjd=mjd,
                        x=x,
                        y=y,
                        z=z,
                        x_rms=0.001,  # Default 1mm RMS (not available in CRD)
                        y_rms=0.001,
                        z_rms=0.001,
                        lat=lat,
                        lon=lon,
                        height=height,
                        created_at=datetime.now()
                    ))
                except (ValueError, IndexError):
                    continue

    return solutions


def get_available_doys(year: int = 2025) -> list:
    """Get list of DOYs with PPP-AR CRD files"""
    yy = year % 100
    pattern = f"{CAMPAIGN_PATH}/{yy}[0-9][0-9][0-9]IG*/STA/FIN_{year}*0.CRD"

    files = glob.glob(pattern)
    doys = set()

    for f in files:
        # Extract DOY from filename: FIN_20253020.CRD -> 302
        basename = os.path.basename(f)
        if basename.startswith('FIN_') and len(basename) >= 15:
            try:
                doy_str = basename[8:11]
                doys.add(int(doy_str))
            except (ValueError, IndexError):
                continue

    return sorted(list(doys))


def ingest_doy(db: QualityDatabase, year: int, doy: int, verbose: bool = True) -> int:
    """
    Ingest PPP-AR solutions for a single DOY.

    Returns:
        Number of solutions ingested
    """
    session_path = find_ig_campaign_dir(CAMPAIGN_PATH, year, doy)
    if not session_path:
        if verbose:
            print(f"  DOY {doy}: Campaign directory not found")
        return 0
    crd_path = f"{session_path}/STA/FIN_{year}{doy:03d}0.CRD"

    if not os.path.exists(crd_path):
        if verbose:
            print(f"  DOY {doy}: CRD file not found")
        return 0

    solutions = parse_crd_file(crd_path, year, doy)

    if not solutions:
        if verbose:
            print(f"  DOY {doy}: No solutions parsed")
        return 0

    # Insert into database
    count = 0
    for sol in solutions:
        try:
            db.insert_solution(sol)
            count += 1
        except Exception as e:
            if verbose:
                print(f"  Warning: Failed to add {sol.station_id} DOY {doy}: {e}")

    if verbose:
        print(f"  DOY {doy}: Ingested {count} stations")

    return count


def main():
    parser = argparse.ArgumentParser(description='Ingest PPP-AR coordinates into DuckDB')
    parser.add_argument('start_doy', type=int, nargs='?', help='Start DOY')
    parser.add_argument('end_doy', type=int, nargs='?', help='End DOY')
    parser.add_argument('--year', type=int, default=2025, help='Year (default: 2025)')
    parser.add_argument('--list', action='store_true', help='List available DOYs')
    parser.add_argument('--clear', action='store_true', help='Clear existing solutions before ingesting')
    args = parser.parse_args()

    # List available DOYs
    if args.list:
        doys = get_available_doys(args.year)
        print(f"Available DOYs for {args.year}: {doys}")
        print(f"Total: {len(doys)} days")
        return

    # Determine DOY range
    available_doys = get_available_doys(args.year)

    if args.start_doy and args.end_doy:
        doys = [d for d in range(args.start_doy, args.end_doy + 1) if d in available_doys]
    elif args.start_doy:
        doys = [args.start_doy] if args.start_doy in available_doys else []
    else:
        doys = available_doys

    if not doys:
        print("No DOYs to process")
        return

    print(f"PPP-AR Ingestion to DuckDB")
    print(f"=" * 50)
    print(f"Database: {DB_PATH}")
    print(f"Campaign: {CAMPAIGN_PATH}")
    print(f"Year: {args.year}")
    print(f"DOYs to process: {min(doys)}-{max(doys)} ({len(doys)} days)")
    print()

    # Connect to database
    db = QualityDatabase(DB_PATH)
    db.connect()
    db.create_tables()

    # Clear if requested
    if args.clear:
        print("Clearing existing solutions...")
        conn = db.connect()
        conn.execute("DELETE FROM ppp_solutions")
        print("  Done")

    # Ingest each DOY
    print(f"\nIngesting PPP-AR solutions...")
    total = 0
    for doy in doys:
        count = ingest_doy(db, args.year, doy)
        total += count

    print()
    print(f"=" * 50)
    print(f"Total: {total} solutions ingested")

    # Verify
    conn = db.connect()
    result = conn.execute("SELECT COUNT(*) as cnt FROM ppp_solutions").fetchone()
    print(f"Database now has {result[0]} total solutions")

    # Show stations
    result = conn.execute("SELECT DISTINCT station_id FROM ppp_solutions ORDER BY station_id").fetchdf()
    print(f"Stations: {', '.join(result['station_id'].tolist())}")

    db.close()


if __name__ == '__main__':
    main()
