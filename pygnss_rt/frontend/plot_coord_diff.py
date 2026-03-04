#!/usr/bin/env python3
"""
Plot Coordinate Differences (ENU): DD vs PPP-AR GRE

Compares daily station coordinates between:
  - DD processing:    F10_2025???0.CRD.gz  (from GPSRESULTS)
  - PPP-AR GRE:       FIN_2025???0.CRD     (from CAMPAIGN54)

Transforms XYZ differences to local ENU frame and plots time series
with formal error bars from SINEX files.

Usage:
    python -m frontend.plot_coord_diff                          # Default: WALF
    python -m frontend.plot_coord_diff --station ONSA
    python -m frontend.plot_coord_diff --start 297 --end 310 --save

Author: Addisu Hunegnaw
Date: March 2026
"""

import os
import sys
import gzip
import math
import argparse
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime, timedelta


@dataclass
class StationXYZ:
    """Daily XYZ coordinate for a station with optional formal errors"""
    station: str
    doy: int
    x: float
    y: float
    z: float
    flag: str
    sx: float = 0.0  # formal error X (m)
    sy: float = 0.0  # formal error Y (m)
    sz: float = 0.0  # formal error Z (m)


def parse_crd_file(filepath: str, station_filter: Optional[str] = None) -> Dict[str, StationXYZ]:
    """Parse a Bernese CRD file (plain text or gzipped)."""
    stations = {}

    if filepath.endswith('.gz'):
        f = gzip.open(filepath, 'rt')
    else:
        f = open(filepath, 'r')

    try:
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
                    if station_filter and station != station_filter:
                        continue
                    x = float(parts[2])
                    y = float(parts[3])
                    z = float(parts[4])
                    flag = parts[5] if len(parts) > 5 else ""

                    stations[station] = StationXYZ(
                        station=station, doy=0,
                        x=x, y=y, z=z, flag=flag
                    )
                except (ValueError, IndexError):
                    continue
    finally:
        f.close()

    return stations


def parse_sinex_sigmas(filepath: str, station_filter: Optional[str] = None) -> Dict[str, Tuple[float, float, float]]:
    """
    Parse SINEX SOLUTION/ESTIMATE to extract formal errors (std dev) for XYZ.

    Returns dict of station_name -> (sx, sy, sz) in meters.
    """
    sigmas = {}

    if filepath.endswith('.gz'):
        f = gzip.open(filepath, 'rt')
    else:
        f = open(filepath, 'r')

    try:
        in_estimate = False
        temp = {}  # station -> {STAX: val, STAY: val, STAZ: val}

        for line in f:
            if '+SOLUTION/ESTIMATE' in line:
                in_estimate = True
                continue
            if '-SOLUTION/ESTIMATE' in line:
                break
            if not in_estimate:
                continue
            if line.startswith('*') or not line.strip():
                continue

            # SINEX ESTIMATE format (fixed-width):
            # INDEX TYPE__ CODE PT SOLN _REF_EPOCH__ UNIT S __ESTIMATED_VALUE____ _STD_DEV___
            #     1 STAX   0531  A    1 25:303:43185 m    2 0.408490254041711E+07 .375993E-03
            parts = line.split()
            if len(parts) >= 9:
                param_type = parts[1]  # STAX, STAY, STAZ
                sta = parts[2]

                if param_type not in ('STAX', 'STAY', 'STAZ'):
                    continue
                if station_filter and sta != station_filter:
                    continue

                try:
                    std_dev = float(parts[-1])
                except ValueError:
                    continue

                if sta not in temp:
                    temp[sta] = {}
                temp[sta][param_type] = std_dev

        # Build output
        for sta, vals in temp.items():
            if 'STAX' in vals and 'STAY' in vals and 'STAZ' in vals:
                sigmas[sta] = (vals['STAX'], vals['STAY'], vals['STAZ'])

    finally:
        f.close()

    return sigmas


def xyz_sigmas_to_enu(sx: float, sy: float, sz: float,
                      x0: float, y0: float, z0: float) -> Tuple[float, float, float]:
    """
    Propagate XYZ formal errors to ENU using rotation matrix.

    Assumes uncorrelated XYZ errors (diagonal covariance).
    Returns (sE, sN, sU) in same units as input.
    """
    # WGS84
    a = 6378137.0
    f_ell = 1 / 298.257223563
    e2 = 2 * f_ell - f_ell * f_ell

    lon = math.atan2(y0, x0)
    p = math.sqrt(x0**2 + y0**2)
    lat = math.atan2(z0, p * (1 - e2))
    for _ in range(10):
        N = a / math.sqrt(1 - e2 * math.sin(lat)**2)
        lat = math.atan2(z0 + e2 * N * math.sin(lat), p)

    sl, cl = math.sin(lat), math.cos(lat)
    slo, clo = math.sin(lon), math.cos(lon)

    # R = rotation matrix XYZ->ENU (rows)
    # E: [-slo,  clo,   0 ]
    # N: [-sl*clo, -sl*slo, cl]
    # U: [ cl*clo,  cl*slo, sl]
    # sigma_ENU^2 = R * diag(sx^2, sy^2, sz^2) * R^T  (diagonal terms)
    sE = math.sqrt(slo**2 * sx**2 + clo**2 * sy**2)
    sN = math.sqrt((sl * clo)**2 * sx**2 + (sl * slo)**2 * sy**2 + cl**2 * sz**2)
    sU = math.sqrt((cl * clo)**2 * sx**2 + (cl * slo)**2 * sy**2 + sl**2 * sz**2)

    return sE, sN, sU


def xyz_to_enu(x: float, y: float, z: float,
               x0: float, y0: float, z0: float) -> Tuple[float, float, float]:
    """Transform XYZ coordinates to local ENU relative to reference point."""
    a = 6378137.0
    f = 1 / 298.257223563
    e2 = 2 * f - f * f

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


def doy_to_datetime(year: int, doy: int) -> datetime:
    return datetime(year, 1, 1) + timedelta(days=doy - 1)


@dataclass
class DayData:
    """All coordinate data for one day"""
    doy: int
    dd_x: float
    dd_y: float
    dd_z: float
    pp_x: float
    pp_y: float
    pp_z: float
    dd_sx: float = 0.0
    dd_sy: float = 0.0
    dd_sz: float = 0.0
    pp_sx: float = 0.0
    pp_sy: float = 0.0
    pp_sz: float = 0.0


def collect_coord_data(
    station: str,
    year: int,
    start_doy: int,
    end_doy: int,
    dd_base: str,
    pppar_base: str,
) -> List[DayData]:
    """
    Collect XYZ coordinates and formal errors from CRD + SINEX files.
    """
    matched = []
    yy = year % 100

    for doy in range(start_doy, end_doy + 1):
        # DD files
        dd_crd = os.path.join(dd_base, str(doy), "STA", f"F10_{year}{doy:03d}0.CRD.gz")
        dd_snx = os.path.join(dd_base, str(doy), "SOL", f"F10_{year}{doy:03d}0.SNX.gz")

        # PPP-AR files
        pppar_crd = os.path.join(pppar_base, f"{yy}{doy:03d}IG_GRE", "STA", f"FIN_{year}{doy:03d}0.CRD")
        pppar_snx = os.path.join(pppar_base, f"{yy}{doy:03d}IG_GRE", "SOL", f"RED_{year}{doy:03d}0.SNX")

        if not os.path.exists(dd_crd) or not os.path.exists(pppar_crd):
            continue

        dd_coords = parse_crd_file(dd_crd, station_filter=station)
        pppar_coords = parse_crd_file(pppar_crd, station_filter=station)

        if station not in dd_coords or station not in pppar_coords:
            continue

        dd = dd_coords[station]
        pp = pppar_coords[station]

        day = DayData(
            doy=doy,
            dd_x=dd.x, dd_y=dd.y, dd_z=dd.z,
            pp_x=pp.x, pp_y=pp.y, pp_z=pp.z,
        )

        # Parse formal errors from SINEX
        if os.path.exists(dd_snx):
            dd_sigs = parse_sinex_sigmas(dd_snx, station_filter=station)
            if station in dd_sigs:
                day.dd_sx, day.dd_sy, day.dd_sz = dd_sigs[station]

        if os.path.exists(pppar_snx):
            pp_sigs = parse_sinex_sigmas(pppar_snx, station_filter=station)
            if station in pp_sigs:
                day.pp_sx, day.pp_sy, day.pp_sz = pp_sigs[station]

        matched.append(day)

    return matched


def plot_coord_differences(
    station: str,
    year: int,
    data: List[DayData],
    save_path: Optional[str] = None,
):
    """Create 3-panel ENU difference plot with error bars."""
    import statistics

    # Mean DD position as ENU reference
    mean_x = sum(d.dd_x for d in data) / len(data)
    mean_y = sum(d.dd_y for d in data) / len(data)
    mean_z = sum(d.dd_z for d in data) / len(data)

    dates = []
    doys = []
    dd_e, dd_n, dd_u = [], [], []
    pp_e, pp_n, pp_u = [], [], []
    de_vals, dn_vals, du_vals = [], [], []
    # Error bars in ENU (mm)
    dd_se, dd_sn, dd_su = [], [], []
    pp_se, pp_sn, pp_su = [], [], []

    for d in data:
        dt = doy_to_datetime(year, d.doy)
        dates.append(dt)
        doys.append(d.doy)

        # DD ENU
        e1, n1, u1 = xyz_to_enu(d.dd_x, d.dd_y, d.dd_z, mean_x, mean_y, mean_z)
        dd_e.append(e1 * 1000)
        dd_n.append(n1 * 1000)
        dd_u.append(u1 * 1000)

        # PPP-AR ENU
        e2, n2, u2 = xyz_to_enu(d.pp_x, d.pp_y, d.pp_z, mean_x, mean_y, mean_z)
        pp_e.append(e2 * 1000)
        pp_n.append(n2 * 1000)
        pp_u.append(u2 * 1000)

        # Differences
        de_vals.append((e2 - e1) * 1000)
        dn_vals.append((n2 - n1) * 1000)
        du_vals.append((u2 - u1) * 1000)

        # Propagate XYZ sigmas to ENU (convert to mm)
        if d.dd_sx > 0:
            se1, sn1, su1 = xyz_sigmas_to_enu(d.dd_sx, d.dd_sy, d.dd_sz, mean_x, mean_y, mean_z)
            dd_se.append(se1 * 1000)
            dd_sn.append(sn1 * 1000)
            dd_su.append(su1 * 1000)
        else:
            dd_se.append(0)
            dd_sn.append(0)
            dd_su.append(0)

        if d.pp_sx > 0:
            se2, sn2, su2 = xyz_sigmas_to_enu(d.pp_sx, d.pp_sy, d.pp_sz, mean_x, mean_y, mean_z)
            pp_se.append(se2 * 1000)
            pp_sn.append(sn2 * 1000)
            pp_su.append(su2 * 1000)
        else:
            pp_se.append(0)
            pp_sn.append(0)
            pp_su.append(0)

    # Statistics
    def stats(vals):
        mean = statistics.mean(vals)
        std = statistics.stdev(vals) if len(vals) > 1 else 0
        rms = (sum(v**2 for v in vals) / len(vals)) ** 0.5
        return mean, std, rms

    e_mean, e_std, e_rms = stats(de_vals)
    n_mean, n_std, n_rms = stats(dn_vals)
    u_mean, u_std, u_rms = stats(du_vals)

    # Create figure
    fig, axes = plt.subplots(3, 1, figsize=(14, 11), sharex=True)
    fig.suptitle(f'Coordinate Differences (ENU): DD vs PPP-AR GRE — Station {station}\n'
                 f'Year {year}, DOY {doys[0]}–{doys[-1]} ({len(data)} days)',
                 fontsize=13, fontweight='bold')

    colors = {'E': '#1f77b4', 'N': '#2ca02c', 'U': '#d62728'}
    components = [
        ('East', de_vals, dd_e, pp_e, dd_se, pp_se, e_mean, e_std, e_rms, colors['E']),
        ('North', dn_vals, dd_n, pp_n, dd_sn, pp_sn, n_mean, n_std, n_rms, colors['N']),
        ('Up', du_vals, dd_u, pp_u, dd_su, pp_su, u_mean, u_std, u_rms, colors['U']),
    ]

    for i, (comp_name, diffs, dd_comp, pp_comp, dd_sig, pp_sig, mean, std, rms, color) in enumerate(components):
        ax = axes[i]

        # DD with error bars
        has_dd_err = any(s > 0 for s in dd_sig)
        has_pp_err = any(s > 0 for s in pp_sig)

        ax.errorbar(dates, dd_comp,
                     yerr=dd_sig if has_dd_err else None,
                     fmt='o-', color='blue', markersize=5, linewidth=1,
                     capsize=3, capthick=0.8, elinewidth=0.8, ecolor='blue',
                     alpha=0.7, label='DD (F10)')

        # PPP-AR with error bars
        ax.errorbar(dates, pp_comp,
                     yerr=pp_sig if has_pp_err else None,
                     fmt='s--', color='red', markersize=5, linewidth=1,
                     capsize=3, capthick=0.8, elinewidth=0.8, ecolor='red',
                     alpha=0.7, label='PPP-AR GRE (FIN)')

        # Twin axis for differences
        ax2 = ax.twinx()
        ax2.bar(dates, diffs, width=0.6, color=color, alpha=0.25, label='Diff')
        ax2.set_ylabel(f'Diff [mm]', color=color)
        ax2.tick_params(axis='y', labelcolor=color)

        ax2.axhline(y=0, color='gray', linestyle=':', linewidth=0.5)
        ax2.axhline(y=mean, color=color, linestyle='--', linewidth=0.8, alpha=0.6)

        # Stats text
        mean_dd_sig = statistics.mean(dd_sig) if has_dd_err else 0
        mean_pp_sig = statistics.mean(pp_sig) if has_pp_err else 0
        stats_text = (f'Mean: {mean:+.1f} mm\n'
                      f'Std:   {std:.1f} mm\n'
                      f'RMS:  {rms:.1f} mm')
        if has_dd_err or has_pp_err:
            stats_text += (f'\nDD  \u03c3: {mean_dd_sig:.2f} mm'
                           f'\nPPP \u03c3: {mean_pp_sig:.2f} mm')

        ax.text(0.02, 0.95, stats_text, transform=ax.transAxes,
                fontsize=8, verticalalignment='top', fontfamily='monospace',
                bbox=dict(boxstyle='round', facecolor='lightyellow', alpha=0.8))

        ax.set_ylabel(f'{comp_name} [mm from mean]')
        ax.grid(True, alpha=0.3)
        ax.legend(loc='upper right', fontsize=8)
        ax.set_title(f'{comp_name} Component', fontsize=11)

    # Format x-axis
    axes[2].xaxis.set_major_formatter(mdates.DateFormatter('%b %d\nDOY %j'))
    axes[2].xaxis.set_major_locator(mdates.DayLocator())
    plt.setp(axes[2].xaxis.get_majorticklabels(), rotation=0, ha='center', fontsize=8)
    axes[2].set_xlabel(f'Date ({year})')

    plt.tight_layout()

    # Print summary table
    print(f"\n{'='*75}")
    print(f"  Coordinate Differences: PPP-AR GRE minus DD  —  Station {station}")
    print(f"  Year {year}, DOY {doys[0]}–{doys[-1]} ({len(data)} days)")
    print(f"{'='*75}")
    print(f"  {'Component':<10} {'Mean [mm]':>10} {'Std [mm]':>10} {'RMS [mm]':>10} {'DD σ [mm]':>10} {'PPP σ [mm]':>11}")
    print(f"  {'-'*62}")

    dd_sig_lists = [dd_se, dd_sn, dd_su]
    pp_sig_lists = [pp_se, pp_sn, pp_su]
    comp_names = ['East', 'North', 'Up']
    comp_stats = [(e_mean, e_std, e_rms), (n_mean, n_std, n_rms), (u_mean, u_std, u_rms)]

    for j, name in enumerate(comp_names):
        m, s, r = comp_stats[j]
        dd_s = statistics.mean(dd_sig_lists[j]) if any(v > 0 for v in dd_sig_lists[j]) else 0
        pp_s = statistics.mean(pp_sig_lists[j]) if any(v > 0 for v in pp_sig_lists[j]) else 0
        print(f"  {name:<10} {m:>+10.2f} {s:>10.2f} {r:>10.2f} {dd_s:>10.2f} {pp_s:>11.2f}")

    print(f"  {'-'*62}")
    rms_3d = math.sqrt(e_rms**2 + n_rms**2 + u_rms**2)
    print(f"  {'3D RMS':<10} {'':>10} {'':>10} {rms_3d:>10.2f}")
    print()

    # Per-DOY table
    print(f"  {'DOY':>5} {'Date':>10} {'dE [mm]':>10} {'dN [mm]':>10} {'dU [mm]':>10}  {'DD σE':>5} {'DD σN':>5} {'DD σU':>5}  {'PP σE':>5} {'PP σN':>5} {'PP σU':>5}")
    print(f"  {'-'*95}")
    for j, d in enumerate(data):
        dt = dates[j]
        print(f"  {d.doy:>5} {dt.strftime('%b %d'):>10} {de_vals[j]:>+10.2f} {dn_vals[j]:>+10.2f} {du_vals[j]:>+10.2f}"
              f"  {dd_se[j]:>5.2f} {dd_sn[j]:>5.2f} {dd_su[j]:>5.2f}"
              f"  {pp_se[j]:>5.2f} {pp_sn[j]:>5.2f} {pp_su[j]:>5.2f}")
    print()

    if save_path:
        fig.savefig(save_path, dpi=150, bbox_inches='tight')
        print(f"  Plot saved to: {save_path}")

    plt.show()


def main():
    parser = argparse.ArgumentParser(
        description='Plot coordinate differences (ENU) between DD and PPP-AR GRE',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s                                      # WALF, DOY 297-310
  %(prog)s --station ONSA --start 297 --end 310
  %(prog)s --station WALF --save
        """
    )
    parser.add_argument('--station', type=str, default='WALF',
                       help='Station name (default: WALF)')
    parser.add_argument('--year', type=int, default=2025, help='Year (default: 2025)')
    parser.add_argument('--start', type=int, default=297, help='Start DOY (default: 297)')
    parser.add_argument('--end', type=int, default=310, help='End DOY (default: 310)')
    parser.add_argument('--dd-path', type=str,
                       default='/home/ahunegnaw/GPSRESULTS/ahunegnaw/OCT2025_MGX/2025',
                       help='DD results base path')
    parser.add_argument('--pppar-path', type=str,
                       default='/home/ahunegnaw/GPSDATA/CAMPAIGN54',
                       help='PPP-AR campaign base path')
    parser.add_argument('--save', action='store_true',
                       help='Save plot as PNG')
    parser.add_argument('--output', type=str,
                       help='Output PNG path (default: auto-generated)')
    args = parser.parse_args()

    station = args.station.upper()
    print(f"Collecting coordinate data for station {station}...")
    print(f"  DD path:     {args.dd_path}")
    print(f"  PPP-AR path: {args.pppar_path}")
    print(f"  DOY range:   {args.start}-{args.end}")

    # Collect matched coordinate data with formal errors
    data = collect_coord_data(
        station=station,
        year=args.year,
        start_doy=args.start,
        end_doy=args.end,
        dd_base=args.dd_path,
        pppar_base=args.pppar_path,
    )

    if not data:
        print(f"\nError: No matched coordinate data found for {station}")
        sys.exit(1)

    print(f"  Matched days: {len(data)}")

    # Plot
    save_path = None
    if args.save:
        save_path = args.output or f"coord_diff_{station}_{args.year}_DOY{args.start}-{args.end}.png"

    plot_coord_differences(station, args.year, data, save_path=save_path)


if __name__ == '__main__':
    main()
