#!/usr/bin/env python3
"""
Calculate PPP-AR Coordinate Repeatability from CRD Files

This script calculates station repeatability by computing the standard deviation
of daily coordinate solutions transformed to local ENU (East-North-Up) frame.

Source: Bernese CRD files from PPP-AR processing
Method: Transform XYZ to ENU, then compute std deviation across days

Usage:
    python calc_repeatability_crd.py                          # All available DOYs
    python calc_repeatability_crd.py --start 296 --end 310    # DOY range
    python calc_repeatability_crd.py --year 2025 --csv        # Output as CSV
    python calc_repeatability_crd.py --help                   # Show help

Author: Addisu Hunegnaw
Date: January 2026
"""

import os
import sys
import glob
import math
import argparse
from dataclasses import dataclass
from typing import Dict, List, Tuple, Optional

from pygnss_rt.frontend.bernese_parser import find_ig_campaign_dir


@dataclass
class StationCoord:
    """Single station coordinate"""
    station: str
    doy: int
    x: float
    y: float
    z: float


@dataclass
class StationRepeatability:
    """Repeatability results for a station"""
    station: str
    n_days: int
    e_mm: float
    n_mm: float
    u_mm: float

    @property
    def rms_3d(self) -> float:
        return math.sqrt(self.e_mm**2 + self.n_mm**2 + self.u_mm**2)


def parse_crd_file(filepath: str, doy: int) -> List[StationCoord]:
    """Parse a Bernese CRD file and extract XYZ coordinates"""
    coords = []

    if not os.path.exists(filepath):
        return coords

    with open(filepath, 'r') as f:
        in_data = False
        for line in f:
            if 'NUM  STATION NAME' in line:
                in_data = True
                continue

            if not in_data:
                continue

            if not line.strip():
                continue

            parts = line.split()
            if len(parts) >= 5:
                try:
                    station = parts[1]
                    x = float(parts[2])
                    y = float(parts[3])
                    z = float(parts[4])

                    coords.append(StationCoord(
                        station=station,
                        doy=doy,
                        x=x, y=y, z=z
                    ))
                except (ValueError, IndexError):
                    continue

    return coords


def xyz_to_enu(x: float, y: float, z: float,
               x0: float, y0: float, z0: float) -> Tuple[float, float, float]:
    """
    Transform XYZ coordinates to local ENU relative to reference point.

    Args:
        x, y, z: ECEF coordinates to transform
        x0, y0, z0: Reference point (typically mean position)

    Returns:
        (dE, dN, dU): Differences in local ENU frame (meters)
    """
    # WGS84 parameters
    a = 6378137.0
    f = 1/298.257223563
    e2 = 2*f - f*f

    # Calculate geodetic coordinates of reference point
    lon = math.atan2(y0, x0)
    p = math.sqrt(x0**2 + y0**2)
    lat = math.atan2(z0, p * (1 - e2))

    # Iterative latitude calculation
    for _ in range(10):
        N = a / math.sqrt(1 - e2 * math.sin(lat)**2)
        lat = math.atan2(z0 + e2 * N * math.sin(lat), p)

    sin_lat, cos_lat = math.sin(lat), math.cos(lat)
    sin_lon, cos_lon = math.sin(lon), math.cos(lon)

    # Coordinate differences
    dx, dy, dz = x - x0, y - y0, z - z0

    # Rotation to ENU
    dE = -sin_lon * dx + cos_lon * dy
    dN = -sin_lat * cos_lon * dx - sin_lat * sin_lon * dy + cos_lat * dz
    dU = cos_lat * cos_lon * dx + cos_lat * sin_lon * dy + sin_lat * dz

    return dE, dN, dU


def calculate_repeatability(coords_by_station: Dict[str, List[StationCoord]]) -> List[StationRepeatability]:
    """
    Calculate repeatability for all stations.

    Repeatability = standard deviation of daily positions in ENU frame
    """
    results = []

    for station, coords in sorted(coords_by_station.items()):
        if len(coords) < 2:
            continue

        # Calculate mean position as reference
        x0 = sum(c.x for c in coords) / len(coords)
        y0 = sum(c.y for c in coords) / len(coords)
        z0 = sum(c.z for c in coords) / len(coords)

        # Transform each day to ENU
        e_vals, n_vals, u_vals = [], [], []
        for c in coords:
            dE, dN, dU = xyz_to_enu(c.x, c.y, c.z, x0, y0, z0)
            e_vals.append(dE)
            n_vals.append(dN)
            u_vals.append(dU)

        # Calculate standard deviation (in mm)
        n_days = len(coords)

        def std_dev(vals):
            mean = sum(vals) / len(vals)
            variance = sum((v - mean)**2 for v in vals) / (len(vals) - 1)
            return math.sqrt(variance) * 1000  # Convert to mm

        results.append(StationRepeatability(
            station=station,
            n_days=n_days,
            e_mm=std_dev(e_vals),
            n_mm=std_dev(n_vals),
            u_mm=std_dev(u_vals)
        ))

    return results


def find_crd_files(base_path: str, year: int, start_doy: int, end_doy: int) -> Dict[int, str]:
    """Find CRD files for the specified DOY range"""
    yy = year % 100
    crd_files = {}

    for doy in range(start_doy, end_doy + 1):
        # Try different path patterns (supports IG_GRE, IG_GE, etc.)
        session_path = find_ig_campaign_dir(base_path, year, doy)
        patterns = []
        if session_path:
            patterns.extend([
                f"{session_path}/STA/FIN_{year}{doy:03d}0.CRD",
                f"{session_path}/STA/FIN_{year}{doy:03d}.CRD",
            ])
        patterns.extend([
            f"{base_path}/{yy}{doy:03d}IG/STA/FIN_{year}{doy:03d}0.CRD",
            f"{base_path}/{yy}{doy:03d}IG/STA/FIN_{year}{doy:03d}.CRD",
        ])

        for pattern in patterns:
            if os.path.exists(pattern):
                crd_files[doy] = pattern
                break

    return crd_files


def main():
    parser = argparse.ArgumentParser(
        description='Calculate PPP-AR repeatability from CRD files',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s                              # All available DOYs
  %(prog)s --start 296 --end 310        # DOY range 296-310
  %(prog)s --year 2025 --csv            # Output as CSV
  %(prog)s --campaign /path/to/data     # Custom campaign path
        """
    )
    parser.add_argument('--campaign', type=str,
                       default='/home/ahunegnaw/GPSDATA/CAMPAIGN54',
                       help='Campaign base path')
    parser.add_argument('--year', type=int, default=2025, help='Year')
    parser.add_argument('--start', type=int, default=296, help='Start DOY')
    parser.add_argument('--end', type=int, default=310, help='End DOY')
    parser.add_argument('--csv', action='store_true', help='Output as CSV')
    parser.add_argument('--station', type=str, help='Filter single station')
    args = parser.parse_args()

    # Find CRD files
    crd_files = find_crd_files(args.campaign, args.year, args.start, args.end)

    if not crd_files:
        print(f"Error: No CRD files found in {args.campaign} for DOY {args.start}-{args.end}")
        sys.exit(1)

    # Parse all CRD files
    coords_by_station: Dict[str, List[StationCoord]] = {}

    for doy, filepath in sorted(crd_files.items()):
        coords = parse_crd_file(filepath, doy)
        for c in coords:
            if args.station and c.station != args.station.upper():
                continue
            if c.station not in coords_by_station:
                coords_by_station[c.station] = []
            coords_by_station[c.station].append(c)

    # Calculate repeatability
    results = calculate_repeatability(coords_by_station)

    if not results:
        print("Error: No stations with multiple days found")
        sys.exit(1)

    # Output results
    if args.csv:
        # Output as two-column markdown table with overall mean
        n_stations = len(results)
        half = (n_stations + 1) // 2  # First column gets extra if odd

        # Header
        print("| Station | Days | E(mm) | N(mm) | U(mm) |     | Station | Days | E(mm) | N(mm) | U(mm) |")
        print("|---------|------|-------|-------|-------|-----|---------|------|-------|-------|-------|")

        # Data rows (two columns)
        for i in range(half):
            # Left column
            r1 = results[i]
            left = f"| {r1.station:<7} | {r1.n_days:>4} | {r1.e_mm:>5.2f} | {r1.n_mm:>5.2f} | {r1.u_mm:>5.2f} |"

            # Right column
            j = i + half
            if j < n_stations:
                r2 = results[j]
                right = f"| {r2.station:<7} | {r2.n_days:>4} | {r2.e_mm:>5.2f} | {r2.n_mm:>5.2f} | {r2.u_mm:>5.2f} |"
            else:
                right = "|         |      |       |       |       |"

            print(f"{left}     {right}")

        # Overall mean
        mean_e = sum(r.e_mm for r in results) / len(results)
        mean_n = sum(r.n_mm for r in results) / len(results)
        mean_u = sum(r.u_mm for r in results) / len(results)

        print()
        print(f"**Overall Mean ({len(results)} stations):** E: {mean_e:.2f} mm | N: {mean_n:.2f} mm | U: {mean_u:.2f} mm")
    else:
        print("=" * 75)
        print(f"PPP-AR Coordinate Repeatability from CRD Files")
        print(f"Campaign: {args.campaign}")
        print(f"Year: {args.year}, DOY: {args.start}-{args.end}")
        print(f"Method: Standard deviation of daily positions in ENU frame")
        print("=" * 75)
        print()
        print(f"{'Station':<10} {'Days':>5} {'E(mm)':>8} {'N(mm)':>8} {'U(mm)':>8} {'3D(mm)':>8}")
        print("-" * 75)

        for r in results:
            print(f"{r.station:<10} {r.n_days:>5} {r.e_mm:>8.2f} {r.n_mm:>8.2f} {r.u_mm:>8.2f} {r.rms_3d:>8.2f}")

        print("-" * 75)

        # Network mean
        mean_e = sum(r.e_mm for r in results) / len(results)
        mean_n = sum(r.n_mm for r in results) / len(results)
        mean_u = sum(r.u_mm for r in results) / len(results)
        mean_3d = math.sqrt(mean_e**2 + mean_n**2 + mean_u**2)

        print(f"{'MEAN':.<10} {len(results):>5} {mean_e:>8.2f} {mean_n:>8.2f} {mean_u:>8.2f} {mean_3d:>8.2f}")
        print()
        print(f"Total: {len(results)} stations, {len(crd_files)} days")


if __name__ == '__main__':
    main()
