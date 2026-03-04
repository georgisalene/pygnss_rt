#!/usr/bin/env python3
"""
Ingest PPP-AR Coordinates and Formal Errors from SINEX Files into DuckDB

This script reads PPP-AR SINEX files from:
  /home/ahunegnaw/GPSDATA/CAMPAIGN54/25{doy}IG/SOL/RED_{year}{doy}0.SNX

And stores coordinates WITH formal errors in the DuckDB database.

Unlike ingest_pppar.py (CRD files), this script extracts:
  - XYZ coordinates
  - Formal errors (σx, σy, σz) from SOLUTION/ESTIMATE block

Usage:
    python ingest_pppar_snx.py                    # Ingest all available DOYs
    python ingest_pppar_snx.py 296 310            # Ingest DOY range 296-310
    python ingest_pppar_snx.py --list             # List available DOYs
    python ingest_pppar_snx.py --clear            # Clear existing data first

Author: Addisu Hunegnaw
Date: January 2026
"""

import os
import sys
import glob
import math
import argparse
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, List
from dataclasses import dataclass

# Import from package modules
from pygnss_rt.frontend.config import DB_PATH, CAMPAIGN_PATH

from pygnss_rt.stations.coordinates import xyz_to_llh
from pygnss_rt.utils.dates import doy_to_mjd


@dataclass
class SINEXSolution:
    """Single station solution from SINEX file"""
    station_id: str
    year: int
    doy: int
    mjd: float
    x: float
    y: float
    z: float
    sigma_x: float  # Formal error in meters
    sigma_y: float
    sigma_z: float
    lat: float
    lon: float
    height: float
    created_at: datetime


def parse_sinex_file(filepath: str, year: int, doy: int) -> List[SINEXSolution]:
    """
    Parse a SINEX file and extract coordinates with formal errors.

    Args:
        filepath: Path to the SINEX file
        year: Year of the solution
        doy: Day of year

    Returns:
        List of SINEXSolution objects
    """
    solutions = []

    if not os.path.exists(filepath):
        return solutions

    mjd = doy_to_mjd(year, doy)

    with open(filepath, 'r') as f:
        content = f.read()

    # Find SOLUTION/ESTIMATE block
    start_marker = '+SOLUTION/ESTIMATE'
    end_marker = '-SOLUTION/ESTIMATE'

    start_idx = content.find(start_marker)
    end_idx = content.find(end_marker)

    if start_idx == -1 or end_idx == -1:
        return solutions

    estimate_block = content[start_idx:end_idx]

    # Parse station coordinates and errors
    station_data: Dict[str, Dict] = {}

    for line in estimate_block.split('\n'):
        if line.startswith('*') or line.startswith('+'):
            continue

        parts = line.split()
        if len(parts) < 10:
            continue

        try:
            param_type = parts[1]  # STAX, STAY, STAZ
            station = parts[2]
            value = float(parts[8])
            sigma = float(parts[9])

            if station not in station_data:
                station_data[station] = {}

            if param_type == 'STAX':
                station_data[station]['x'] = value
                station_data[station]['sigma_x'] = sigma
            elif param_type == 'STAY':
                station_data[station]['y'] = value
                station_data[station]['sigma_y'] = sigma
            elif param_type == 'STAZ':
                station_data[station]['z'] = value
                station_data[station]['sigma_z'] = sigma

        except (ValueError, IndexError):
            continue

    # Create solutions for complete stations
    for station, data in station_data.items():
        required_keys = ['x', 'y', 'z', 'sigma_x', 'sigma_y', 'sigma_z']
        if all(k in data for k in required_keys):
            lat, lon, height = xyz_to_llh(data['x'], data['y'], data['z'])

            solutions.append(SINEXSolution(
                station_id=station,
                year=year,
                doy=doy,
                mjd=mjd,
                x=data['x'],
                y=data['y'],
                z=data['z'],
                sigma_x=data['sigma_x'],
                sigma_y=data['sigma_y'],
                sigma_z=data['sigma_z'],
                lat=lat,
                lon=lon,
                height=height,
                created_at=datetime.now()
            ))

    return solutions


def get_available_snx_files(year: int = 2025) -> Dict[int, str]:
    """Get dictionary of DOY -> SINEX file path"""
    yy = year % 100
    snx_files = {}

    # Search for SINEX files (match IG and IG_GRE, IG_GE, etc.)
    pattern = f"{CAMPAIGN_PATH}/{yy}[0-9][0-9][0-9]IG*/SOL/RED_{year}*0.SNX"
    files = glob.glob(pattern)

    for f in files:
        basename = os.path.basename(f)
        # Extract DOY from filename: RED_20253020.SNX -> 302
        if basename.startswith('RED_') and len(basename) >= 15:
            try:
                doy_str = basename[8:11]
                doy = int(doy_str)
                # Only keep one file per DOY (prefer non-suffixed paths)
                if doy not in snx_files or '_1' not in f:
                    snx_files[doy] = f
            except (ValueError, IndexError):
                continue

    return snx_files


def ingest_doy(conn, year: int, doy: int, snx_path: str, verbose: bool = True) -> int:
    """
    Ingest PPP-AR solutions from SINEX for a single DOY.

    Returns:
        Number of solutions ingested
    """
    solutions = parse_sinex_file(snx_path, year, doy)

    if not solutions:
        if verbose:
            print(f"  DOY {doy}: No solutions parsed from SINEX")
        return 0

    # Delete existing data for this DOY
    conn.execute("""
        DELETE FROM ppp_solutions
        WHERE year = ? AND doy = ?
    """, [year, doy])

    # Insert new data
    count = 0
    for sol in solutions:
        try:
            conn.execute("""
                INSERT INTO ppp_solutions
                (station_id, year, doy, mjd, x, y, z, x_rms, y_rms, z_rms, lat, lon, height, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, [
                sol.station_id, sol.year, sol.doy, sol.mjd,
                sol.x, sol.y, sol.z,
                sol.sigma_x, sol.sigma_y, sol.sigma_z,
                sol.lat, sol.lon, sol.height,
                sol.created_at
            ])
            count += 1
        except Exception as e:
            if verbose:
                print(f"  Warning: Failed to insert {sol.station_id} DOY {doy}: {e}")

    if verbose:
        # Show sample sigma values
        if solutions:
            avg_sigma_x = sum(s.sigma_x for s in solutions) / len(solutions) * 1000
            avg_sigma_z = sum(s.sigma_z for s in solutions) / len(solutions) * 1000
            print(f"  DOY {doy}: Ingested {count} stations (mean σX={avg_sigma_x:.2f}mm, σZ={avg_sigma_z:.2f}mm)")

    return count


def main():
    parser = argparse.ArgumentParser(
        description='Ingest PPP-AR coordinates with formal errors from SINEX into DuckDB',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s                    # Ingest all available DOYs
  %(prog)s 296 310            # Ingest DOY range 296-310
  %(prog)s --list             # List available SINEX files
  %(prog)s --clear            # Clear existing data first
        """
    )
    parser.add_argument('start_doy', type=int, nargs='?', help='Start DOY')
    parser.add_argument('end_doy', type=int, nargs='?', help='End DOY')
    parser.add_argument('--year', type=int, default=2025, help='Year (default: 2025)')
    parser.add_argument('--list', action='store_true', help='List available SINEX files')
    parser.add_argument('--clear', action='store_true', help='Clear existing solutions before ingesting')
    parser.add_argument('--db', type=str, default=DB_PATH, help='Database path')
    args = parser.parse_args()

    # Get available SINEX files
    snx_files = get_available_snx_files(args.year)

    # List mode
    if args.list:
        print(f"Available SINEX files for {args.year}:")
        print(f"{'DOY':<6} {'Path'}")
        print("-" * 80)
        for doy in sorted(snx_files.keys()):
            print(f"{doy:<6} {snx_files[doy]}")
        print(f"\nTotal: {len(snx_files)} files")
        return

    if not snx_files:
        print(f"Error: No SINEX files found for year {args.year}")
        sys.exit(1)

    # Determine DOY range
    if args.start_doy and args.end_doy:
        doys_to_process = {d: snx_files[d] for d in range(args.start_doy, args.end_doy + 1) if d in snx_files}
    elif args.start_doy:
        doys_to_process = {args.start_doy: snx_files[args.start_doy]} if args.start_doy in snx_files else {}
    else:
        doys_to_process = snx_files

    if not doys_to_process:
        print("No DOYs to process")
        return

    print("=" * 70)
    print("PPP-AR SINEX Ingestion to DuckDB")
    print("=" * 70)
    print(f"Database:  {args.db}")
    print(f"Campaign:  {CAMPAIGN_PATH}")
    print(f"Year:      {args.year}")
    print(f"DOYs:      {min(doys_to_process.keys())}-{max(doys_to_process.keys())} ({len(doys_to_process)} files)")
    print(f"Source:    SINEX files (RED_*.SNX) with formal errors")
    print("=" * 70)

    # Connect to database
    import duckdb
    conn = duckdb.connect(args.db)

    # Ensure table exists with correct schema
    conn.execute("""
        CREATE TABLE IF NOT EXISTS ppp_solutions (
            station_id VARCHAR,
            year INTEGER,
            doy INTEGER,
            mjd DOUBLE,
            x DOUBLE,
            y DOUBLE,
            z DOUBLE,
            x_rms DOUBLE,
            y_rms DOUBLE,
            z_rms DOUBLE,
            lat DOUBLE,
            lon DOUBLE,
            height DOUBLE,
            created_at TIMESTAMP
        )
    """)

    # Clear if requested
    if args.clear:
        print("\nClearing existing solutions...")
        conn.execute("DELETE FROM ppp_solutions")
        print("  Done")

    # Ingest each DOY
    print(f"\nIngesting PPP-AR solutions from SINEX...")
    total = 0
    for doy in sorted(doys_to_process.keys()):
        count = ingest_doy(conn, args.year, doy, doys_to_process[doy])
        total += count

    print()
    print("=" * 70)
    print(f"Total: {total} solutions ingested from {len(doys_to_process)} SINEX files")

    # Verify
    result = conn.execute("SELECT COUNT(*) as cnt FROM ppp_solutions").fetchone()
    print(f"Database now has {result[0]} total solutions")

    # Show sample with RMS values
    print("\nSample data (showing formal errors are now real values):")
    sample = conn.execute("""
        SELECT station_id, doy,
               ROUND(x_rms * 1000, 3) as sigma_x_mm,
               ROUND(y_rms * 1000, 3) as sigma_y_mm,
               ROUND(z_rms * 1000, 3) as sigma_z_mm
        FROM ppp_solutions
        ORDER BY station_id, doy
        LIMIT 5
    """).fetchdf()
    print(sample.to_string(index=False))

    # Show stations
    result = conn.execute("SELECT DISTINCT station_id FROM ppp_solutions ORDER BY station_id").fetchdf()
    print(f"\nStations: {', '.join(result['station_id'].tolist())}")

    conn.close()
    print("\nDone!")


if __name__ == '__main__':
    main()
