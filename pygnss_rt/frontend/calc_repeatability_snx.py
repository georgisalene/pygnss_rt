#!/usr/bin/env python3
"""
Calculate PPP-AR Coordinate Repeatability from SINEX Files

This script calculates station repeatability using two methods:
1. Formal Errors: Standard deviations from SINEX SOLUTION/ESTIMATE block
2. Position Scatter: Standard deviation of daily positions (same as CRD method)

Source: Bernese SINEX files from PPP-AR processing
Formal errors are transformed from XYZ to ENU using error propagation.

Usage:
    python calc_repeatability_snx.py                          # All available DOYs
    python calc_repeatability_snx.py --start 296 --end 310    # DOY range
    python calc_repeatability_snx.py --formal-only            # Only formal errors
    python calc_repeatability_snx.py --scatter-only           # Only position scatter
    python calc_repeatability_snx.py --csv                    # Output as CSV
    python calc_repeatability_snx.py --help                   # Show help

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
class StationSolution:
    """Single station solution from SINEX"""
    station: str
    doy: int
    x: float
    y: float
    z: float
    sigma_x: float  # Formal error in X (meters)
    sigma_y: float  # Formal error in Y (meters)
    sigma_z: float  # Formal error in Z (meters)


@dataclass
class StationRepeatability:
    """Repeatability results for a station"""
    station: str
    n_days: int
    # Formal errors (mean of daily formal errors, transformed to ENU)
    formal_e_mm: float
    formal_n_mm: float
    formal_u_mm: float
    # Position scatter (std dev of daily positions)
    scatter_e_mm: float
    scatter_n_mm: float
    scatter_u_mm: float

    @property
    def formal_3d(self) -> float:
        return math.sqrt(self.formal_e_mm**2 + self.formal_n_mm**2 + self.formal_u_mm**2)

    @property
    def scatter_3d(self) -> float:
        return math.sqrt(self.scatter_e_mm**2 + self.scatter_n_mm**2 + self.scatter_u_mm**2)


def parse_sinex_file(filepath: str, doy: int) -> List[StationSolution]:
    """
    Parse a SINEX file and extract coordinates with formal errors.

    Reads the SOLUTION/ESTIMATE block for STAX, STAY, STAZ parameters.
    """
    solutions = []

    if not os.path.exists(filepath):
        return solutions

    # Read file
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

    # Parse station coordinates
    station_data: Dict[str, Dict] = {}

    for line in estimate_block.split('\n'):
        if line.startswith('*') or line.startswith('+'):
            continue

        parts = line.split()
        if len(parts) < 9:
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
        if all(k in data for k in ['x', 'y', 'z', 'sigma_x', 'sigma_y', 'sigma_z']):
            solutions.append(StationSolution(
                station=station,
                doy=doy,
                x=data['x'],
                y=data['y'],
                z=data['z'],
                sigma_x=data['sigma_x'],
                sigma_y=data['sigma_y'],
                sigma_z=data['sigma_z']
            ))

    return solutions


def xyz_to_enu(x: float, y: float, z: float,
               x0: float, y0: float, z0: float) -> Tuple[float, float, float]:
    """Transform XYZ coordinates to local ENU"""
    # WGS84 parameters
    a = 6378137.0
    f = 1/298.257223563
    e2 = 2*f - f*f

    lon = math.atan2(y0, x0)
    p = math.sqrt(x0**2 + y0**2)
    lat = math.atan2(z0, p * (1 - e2))

    for _ in range(10):
        N = a / math.sqrt(1 - e2 * math.sin(lat)**2)
        lat = math.atan2(z0 + e2 * N * math.sin(lat), p)

    sin_lat, cos_lat = math.sin(lat), math.cos(lat)
    sin_lon, cos_lon = math.sin(lon), math.cos(lon)

    dx, dy, dz = x - x0, y - y0, z - z0

    dE = -sin_lon * dx + cos_lon * dy
    dN = -sin_lat * cos_lon * dx - sin_lat * sin_lon * dy + cos_lat * dz
    dU = cos_lat * cos_lon * dx + cos_lat * sin_lon * dy + sin_lat * dz

    return dE, dN, dU


def transform_sigma_to_enu(sigma_x: float, sigma_y: float, sigma_z: float,
                           x0: float, y0: float, z0: float) -> Tuple[float, float, float]:
    """
    Transform XYZ formal errors to ENU using error propagation.

    For diagonal covariance (assuming uncorrelated XYZ errors):
    Σ_ENU = R × Σ_XYZ × R^T

    Where R is the rotation matrix from XYZ to ENU.
    """
    # WGS84 parameters
    a = 6378137.0
    f = 1/298.257223563
    e2 = 2*f - f*f

    lon = math.atan2(y0, x0)
    p = math.sqrt(x0**2 + y0**2)
    lat = math.atan2(z0, p * (1 - e2))

    for _ in range(10):
        N = a / math.sqrt(1 - e2 * math.sin(lat)**2)
        lat = math.atan2(z0 + e2 * N * math.sin(lat), p)

    sin_lat, cos_lat = math.sin(lat), math.cos(lat)
    sin_lon, cos_lon = math.sin(lon), math.cos(lon)

    # Rotation matrix R (XYZ to ENU)
    # R = [[-sin_lon,        cos_lon,       0      ],
    #      [-sin_lat*cos_lon, -sin_lat*sin_lon, cos_lat],
    #      [cos_lat*cos_lon,  cos_lat*sin_lon, sin_lat]]

    # For diagonal covariance matrix, σ²_E = Σ(R_Ei² × σ²_i)
    var_x = sigma_x**2
    var_y = sigma_y**2
    var_z = sigma_z**2

    # East variance
    var_e = (sin_lon**2 * var_x +
             cos_lon**2 * var_y +
             0 * var_z)

    # North variance
    var_n = ((sin_lat * cos_lon)**2 * var_x +
             (sin_lat * sin_lon)**2 * var_y +
             cos_lat**2 * var_z)

    # Up variance
    var_u = ((cos_lat * cos_lon)**2 * var_x +
             (cos_lat * sin_lon)**2 * var_y +
             sin_lat**2 * var_z)

    return math.sqrt(var_e), math.sqrt(var_n), math.sqrt(var_u)


def calculate_repeatability(solutions_by_station: Dict[str, List[StationSolution]]) -> List[StationRepeatability]:
    """Calculate repeatability for all stations"""
    results = []

    for station, solutions in sorted(solutions_by_station.items()):
        if len(solutions) < 2:
            continue

        # Mean position as reference
        x0 = sum(s.x for s in solutions) / len(solutions)
        y0 = sum(s.y for s in solutions) / len(solutions)
        z0 = sum(s.z for s in solutions) / len(solutions)

        # Transform each day's coordinates and errors to ENU
        e_vals, n_vals, u_vals = [], [], []
        formal_e_list, formal_n_list, formal_u_list = [], [], []

        for s in solutions:
            # Position transformation
            dE, dN, dU = xyz_to_enu(s.x, s.y, s.z, x0, y0, z0)
            e_vals.append(dE)
            n_vals.append(dN)
            u_vals.append(dU)

            # Error transformation
            sigma_e, sigma_n, sigma_u = transform_sigma_to_enu(
                s.sigma_x, s.sigma_y, s.sigma_z, x0, y0, z0
            )
            formal_e_list.append(sigma_e)
            formal_n_list.append(sigma_n)
            formal_u_list.append(sigma_u)

        n_days = len(solutions)

        # Position scatter (std dev)
        def std_dev(vals):
            mean = sum(vals) / len(vals)
            variance = sum((v - mean)**2 for v in vals) / (len(vals) - 1)
            return math.sqrt(variance) * 1000  # mm

        # Mean formal errors (convert to mm)
        mean_formal_e = sum(formal_e_list) / len(formal_e_list) * 1000
        mean_formal_n = sum(formal_n_list) / len(formal_n_list) * 1000
        mean_formal_u = sum(formal_u_list) / len(formal_u_list) * 1000

        results.append(StationRepeatability(
            station=station,
            n_days=n_days,
            formal_e_mm=mean_formal_e,
            formal_n_mm=mean_formal_n,
            formal_u_mm=mean_formal_u,
            scatter_e_mm=std_dev(e_vals),
            scatter_n_mm=std_dev(n_vals),
            scatter_u_mm=std_dev(u_vals)
        ))

    return results


def find_sinex_files(base_path: str, year: int, start_doy: int, end_doy: int) -> Dict[int, str]:
    """Find SINEX files for the specified DOY range"""
    yy = year % 100
    snx_files = {}

    for doy in range(start_doy, end_doy + 1):
        # Try different path patterns (supports IG_GRE, IG_GE, etc.)
        session_path = find_ig_campaign_dir(base_path, year, doy)
        patterns = []
        if session_path:
            patterns.extend([
                f"{session_path}/SOL/RED_{year}{doy:03d}0.SNX",
                f"{session_path}/SOL/FIN_{year}{doy:03d}0.SNX",
            ])
        patterns.extend([
            f"{base_path}/{yy}{doy:03d}IG/SOL/RED_{year}{doy:03d}0.SNX",
            f"{base_path}/{yy}{doy:03d}IG/SOL/FIN_{year}{doy:03d}0.SNX",
        ])

        for pattern in patterns:
            if os.path.exists(pattern):
                snx_files[doy] = pattern
                break

    return snx_files


def main():
    parser = argparse.ArgumentParser(
        description='Calculate PPP-AR repeatability from SINEX files (with formal errors)',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Methods:
  Formal Errors:   Mean of daily formal errors from SINEX, transformed to ENU
  Position Scatter: Standard deviation of daily positions in ENU (same as CRD)

Examples:
  %(prog)s                              # All available DOYs, both methods
  %(prog)s --start 296 --end 310        # DOY range
  %(prog)s --formal-only                # Only formal errors
  %(prog)s --scatter-only               # Only position scatter
  %(prog)s --csv                        # Output as CSV
        """
    )
    parser.add_argument('--campaign', type=str,
                       default='/home/ahunegnaw/GPSDATA/CAMPAIGN54',
                       help='Campaign base path')
    parser.add_argument('--year', type=int, default=2025, help='Year')
    parser.add_argument('--start', type=int, default=296, help='Start DOY')
    parser.add_argument('--end', type=int, default=310, help='End DOY')
    parser.add_argument('--formal-only', action='store_true',
                       help='Show only formal errors')
    parser.add_argument('--scatter-only', action='store_true',
                       help='Show only position scatter')
    parser.add_argument('--csv', action='store_true', help='Output as CSV')
    parser.add_argument('--station', type=str, help='Filter single station')
    args = parser.parse_args()

    # Find SINEX files
    snx_files = find_sinex_files(args.campaign, args.year, args.start, args.end)

    if not snx_files:
        print(f"Error: No SINEX files found in {args.campaign} for DOY {args.start}-{args.end}")
        sys.exit(1)

    # Parse all SINEX files
    solutions_by_station: Dict[str, List[StationSolution]] = {}

    print(f"Parsing {len(snx_files)} SINEX files...", file=sys.stderr)

    for doy, filepath in sorted(snx_files.items()):
        solutions = parse_sinex_file(filepath, doy)
        for s in solutions:
            if args.station and s.station != args.station.upper():
                continue
            if s.station not in solutions_by_station:
                solutions_by_station[s.station] = []
            solutions_by_station[s.station].append(s)

    # Calculate repeatability
    results = calculate_repeatability(solutions_by_station)

    if not results:
        print("Error: No stations with multiple days found")
        sys.exit(1)

    # Determine output mode
    show_formal = not args.scatter_only
    show_scatter = not args.formal_only

    # Output results
    if args.csv:
        # Output as two-column markdown table with overall mean
        # Split stations into two columns
        n_stations = len(results)
        half = (n_stations + 1) // 2  # First column gets the extra station if odd

        # Determine which values to show based on mode
        def get_vals(r):
            if show_scatter and not show_formal:
                return r.scatter_e_mm, r.scatter_n_mm, r.scatter_u_mm
            else:
                # Default to scatter (position repeatability) for the table
                return r.scatter_e_mm, r.scatter_n_mm, r.scatter_u_mm

        # Header
        print("| Station | Days | E(mm) | N(mm) | U(mm) |     | Station | Days | E(mm) | N(mm) | U(mm) |")
        print("|---------|------|-------|-------|-------|-----|---------|------|-------|-------|-------|")

        # Data rows (two columns)
        for i in range(half):
            # Left column
            r1 = results[i]
            e1, n1, u1 = get_vals(r1)
            left = f"| {r1.station:<7} | {r1.n_days:>4} | {e1:>5.2f} | {n1:>5.2f} | {u1:>5.2f} |"

            # Right column
            j = i + half
            if j < n_stations:
                r2 = results[j]
                e2, n2, u2 = get_vals(r2)
                right = f"| {r2.station:<7} | {r2.n_days:>4} | {e2:>5.2f} | {n2:>5.2f} | {u2:>5.2f} |"
            else:
                right = "|         |      |       |       |       |"

            print(f"{left}     {right}")

        # Overall mean
        mean_e = sum(r.scatter_e_mm for r in results) / len(results)
        mean_n = sum(r.scatter_n_mm for r in results) / len(results)
        mean_u = sum(r.scatter_u_mm for r in results) / len(results)

        print()
        print(f"**Overall Mean ({len(results)} stations):** E: {mean_e:.2f} mm | N: {mean_n:.2f} mm | U: {mean_u:.2f} mm")
    else:
        print("=" * 95)
        print(f"PPP-AR Coordinate Repeatability from SINEX Files")
        print(f"Campaign: {args.campaign}")
        print(f"Year: {args.year}, DOY: {args.start}-{args.end}")
        print("=" * 95)

        if show_formal and show_scatter:
            print()
            print("                    -------- Formal Errors --------   ------ Position Scatter ------")
            print(f"{'Station':<10} {'Days':>5} {'E(mm)':>8} {'N(mm)':>8} {'U(mm)':>8}   "
                  f"{'E(mm)':>8} {'N(mm)':>8} {'U(mm)':>8}")
            print("-" * 95)

            for r in results:
                print(f"{r.station:<10} {r.n_days:>5} {r.formal_e_mm:>8.3f} {r.formal_n_mm:>8.3f} "
                      f"{r.formal_u_mm:>8.3f}   {r.scatter_e_mm:>8.2f} {r.scatter_n_mm:>8.2f} "
                      f"{r.scatter_u_mm:>8.2f}")

            print("-" * 95)

            # Network means
            mean_fe = sum(r.formal_e_mm for r in results) / len(results)
            mean_fn = sum(r.formal_n_mm for r in results) / len(results)
            mean_fu = sum(r.formal_u_mm for r in results) / len(results)
            mean_se = sum(r.scatter_e_mm for r in results) / len(results)
            mean_sn = sum(r.scatter_n_mm for r in results) / len(results)
            mean_su = sum(r.scatter_u_mm for r in results) / len(results)

            print(f"{'MEAN':.<10} {len(results):>5} {mean_fe:>8.3f} {mean_fn:>8.3f} {mean_fu:>8.3f}   "
                  f"{mean_se:>8.2f} {mean_sn:>8.2f} {mean_su:>8.2f}")

        elif show_formal:
            print("\nFormal Errors from SINEX (mean of daily sigmas, transformed to ENU)")
            print(f"{'Station':<10} {'Days':>5} {'E(mm)':>10} {'N(mm)':>10} {'U(mm)':>10} {'3D(mm)':>10}")
            print("-" * 70)

            for r in results:
                print(f"{r.station:<10} {r.n_days:>5} {r.formal_e_mm:>10.3f} {r.formal_n_mm:>10.3f} "
                      f"{r.formal_u_mm:>10.3f} {r.formal_3d:>10.3f}")

            print("-" * 70)
            mean_e = sum(r.formal_e_mm for r in results) / len(results)
            mean_n = sum(r.formal_n_mm for r in results) / len(results)
            mean_u = sum(r.formal_u_mm for r in results) / len(results)
            mean_3d = math.sqrt(mean_e**2 + mean_n**2 + mean_u**2)
            print(f"{'MEAN':.<10} {len(results):>5} {mean_e:>10.3f} {mean_n:>10.3f} {mean_u:>10.3f} {mean_3d:>10.3f}")

        else:
            print("\nPosition Scatter (std dev of daily positions in ENU)")
            print(f"{'Station':<10} {'Days':>5} {'E(mm)':>10} {'N(mm)':>10} {'U(mm)':>10} {'3D(mm)':>10}")
            print("-" * 70)

            for r in results:
                print(f"{r.station:<10} {r.n_days:>5} {r.scatter_e_mm:>10.2f} {r.scatter_n_mm:>10.2f} "
                      f"{r.scatter_u_mm:>10.2f} {r.scatter_3d:>10.2f}")

            print("-" * 70)
            mean_e = sum(r.scatter_e_mm for r in results) / len(results)
            mean_n = sum(r.scatter_n_mm for r in results) / len(results)
            mean_u = sum(r.scatter_u_mm for r in results) / len(results)
            mean_3d = math.sqrt(mean_e**2 + mean_n**2 + mean_u**2)
            print(f"{'MEAN':.<10} {len(results):>5} {mean_e:>10.2f} {mean_n:>10.2f} {mean_u:>10.2f} {mean_3d:>10.2f}")

        print()
        print(f"Total: {len(results)} stations, {len(snx_files)} days")
        print()
        print("Notes:")
        print("  - Formal Errors: From SINEX SOLUTION/ESTIMATE sigmas, transformed XYZ→ENU")
        print("  - Position Scatter: Standard deviation of daily positions (same as CRD method)")
        print("  - Scatter is typically larger than formal errors (includes unmodeled effects)")


if __name__ == '__main__':
    main()
