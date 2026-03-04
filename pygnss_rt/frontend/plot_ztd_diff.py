#!/usr/bin/env python3
"""
Plot ZTD Differences: DD vs PPP-AR GRE

Compares TROTOT (Zenith Total Delay) between:
  - DD processing:    F10_2025???0.TRO.gz  (from GPSRESULTS)
  - PPP-AR GRE:       FIN_2025???0.TRO     (from CAMPAIGN54)

Usage:
    python -m frontend.plot_ztd_diff                    # Default: WALF, DOY 296-310
    python -m frontend.plot_ztd_diff --station ONSA
    python -m frontend.plot_ztd_diff --start 297 --end 305
    python -m frontend.plot_ztd_diff --station WALF --save  # Save PNG

Author: Addisu Hunegnaw
Date: March 2026
"""

import os
import sys
import gzip
import argparse
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime, timedelta


@dataclass
class TroRecord:
    """Single troposphere estimate"""
    station: str
    year: int
    doy: int
    seconds: int
    hour: float
    trotot: float      # ZTD in mm
    trotot_std: float
    tgn: float
    tge: float


def parse_sinex_tro(filepath: str, station_filter: Optional[str] = None) -> List[TroRecord]:
    """
    Parse a SINEX-TRO file (plain text or gzipped).

    Handles both:
      - Plain text .TRO files
      - Gzipped .TRO.gz files

    Returns list of TroRecord for the specified station (or all if None).
    """
    records = []

    # Open file (handle gzip)
    if filepath.endswith('.gz'):
        f = gzip.open(filepath, 'rt')
    else:
        f = open(filepath, 'r')

    try:
        in_solution = False
        for line in f:
            if '+TROP/SOLUTION' in line:
                in_solution = True
                continue
            if '-TROP/SOLUTION' in line:
                break
            if not in_solution:
                continue
            if line.startswith('*') or not line.strip():
                continue

            # Parse: STATION__ YYYY:DOY:SSSSS TROTOT STDDEV TGNTOT STDDEV TGETOT STDDEV
            parts = line.split()
            if len(parts) >= 8:
                sta = parts[0]

                # Filter by station if requested
                if station_filter and sta != station_filter:
                    continue

                epoch = parts[1]  # YYYY:DOY:SSSSS
                epoch_parts = epoch.split(':')
                if len(epoch_parts) == 3:
                    year = int(epoch_parts[0])
                    doy = int(epoch_parts[1])
                    seconds = int(epoch_parts[2])
                    hour = seconds / 3600.0

                    records.append(TroRecord(
                        station=sta,
                        year=year,
                        doy=doy,
                        seconds=seconds,
                        hour=hour,
                        trotot=float(parts[2]),
                        trotot_std=float(parts[3]),
                        tgn=float(parts[4]),
                        tge=float(parts[6]),
                    ))
    finally:
        f.close()

    return records


def doy_to_datetime(year: int, doy: int, hour: float = 0.0) -> datetime:
    """Convert year + DOY + fractional hour to datetime"""
    return datetime(year, 1, 1) + timedelta(days=doy - 1, hours=hour)


def collect_ztd_data(
    station: str,
    year: int,
    start_doy: int,
    end_doy: int,
    dd_base: str,
    pppar_base: str,
) -> Tuple[List[Tuple[datetime, float, float]], List[Tuple[datetime, float, float]]]:
    """
    Collect ZTD data from DD and PPP-AR TRO files.

    Returns:
        dd_data: list of (datetime, trotot_mm, stddev_mm)
        pppar_data: list of (datetime, trotot_mm, stddev_mm)
    """
    dd_data = []
    pppar_data = []

    for doy in range(start_doy, end_doy + 1):
        # DD file: /GPSRESULTS/.../2025/{doy}/ATM/F10_2025{doy}0.TRO.gz
        dd_file = os.path.join(dd_base, str(doy), "ATM", f"F10_{year}{doy:03d}0.TRO.gz")

        # PPP-AR file: /CAMPAIGN54/25{doy}IG_GRE/ATM/FIN_2025{doy}0.TRO
        yy = year % 100
        pppar_file = os.path.join(pppar_base, f"{yy}{doy:03d}IG_GRE", "ATM", f"FIN_{year}{doy:03d}0.TRO")

        # Parse DD
        if os.path.exists(dd_file):
            recs = parse_sinex_tro(dd_file, station_filter=station)
            for r in recs:
                dt = doy_to_datetime(r.year, r.doy, r.hour)
                dd_data.append((dt, r.trotot, r.trotot_std))

        # Parse PPP-AR
        if os.path.exists(pppar_file):
            recs = parse_sinex_tro(pppar_file, station_filter=station)
            for r in recs:
                dt = doy_to_datetime(r.year, r.doy, r.hour)
                pppar_data.append((dt, r.trotot, r.trotot_std))

    return dd_data, pppar_data


def compute_differences(
    dd_data: List[Tuple[datetime, float, float]],
    pppar_data: List[Tuple[datetime, float, float]],
) -> List[Tuple[datetime, float]]:
    """
    Compute PPP-AR minus DD differences at matching epochs.

    Returns list of (datetime, diff_mm)
    """
    # Build lookup for DD data by datetime
    dd_lookup: Dict[datetime, float] = {}
    for dt, ztd, _ in dd_data:
        dd_lookup[dt] = ztd

    diffs = []
    for dt, ztd, _ in pppar_data:
        if dt in dd_lookup:
            diff = ztd - dd_lookup[dt]
            diffs.append((dt, diff))

    return diffs


def plot_ztd_comparison(
    station: str,
    dd_data: List[Tuple[datetime, float, float]],
    pppar_data: List[Tuple[datetime, float, float]],
    diffs: List[Tuple[datetime, float]],
    save_path: Optional[str] = None,
):
    """Create a 3-panel plot: DD ZTD, PPP-AR ZTD, and differences"""

    fig, axes = plt.subplots(3, 1, figsize=(14, 10), sharex=True,
                              gridspec_kw={'height_ratios': [2, 2, 1.5]})
    fig.suptitle(f'ZTD Comparison: DD vs PPP-AR GRE — Station {station}',
                 fontsize=14, fontweight='bold')

    # Panel 1: Both ZTD time series
    ax1 = axes[0]
    if dd_data:
        dd_times = [d[0] for d in dd_data]
        dd_ztd = [d[1] for d in dd_data]
        ax1.plot(dd_times, dd_ztd, 'b-', linewidth=0.8, alpha=0.8, label='DD (F10)')
    if pppar_data:
        pp_times = [d[0] for d in pppar_data]
        pp_ztd = [d[1] for d in pppar_data]
        ax1.plot(pp_times, pp_ztd, 'r-', linewidth=0.8, alpha=0.8, label='PPP-AR GRE (FIN)')

    ax1.set_ylabel('ZTD [mm]')
    ax1.legend(loc='upper right')
    ax1.grid(True, alpha=0.3)
    ax1.set_title('Zenith Total Delay')

    # Panel 2: Zoomed overlay (to see differences)
    ax2 = axes[1]
    if dd_data:
        ax2.plot(dd_times, dd_ztd, 'b-', linewidth=1.0, alpha=0.7, label='DD')
    if pppar_data:
        ax2.plot(pp_times, pp_ztd, 'r--', linewidth=1.0, alpha=0.7, label='PPP-AR GRE')
    ax2.set_ylabel('ZTD [mm]')
    ax2.legend(loc='upper right')
    ax2.grid(True, alpha=0.3)
    ax2.set_title('ZTD Overlay (zoomed)')

    # Auto-zoom to show differences
    if dd_data and pppar_data:
        all_ztd = dd_ztd + pp_ztd
        mid = (max(all_ztd) + min(all_ztd)) / 2
        span = max(all_ztd) - min(all_ztd)
        margin = max(span * 0.1, 5)  # At least 5mm margin
        ax2.set_ylim(min(all_ztd) - margin, max(all_ztd) + margin)

    # Panel 3: Differences
    ax3 = axes[2]
    if diffs:
        diff_times = [d[0] for d in diffs]
        diff_vals = [d[1] for d in diffs]
        ax3.plot(diff_times, diff_vals, 'k.-', linewidth=0.6, markersize=2, alpha=0.8)
        ax3.axhline(y=0, color='gray', linestyle='--', linewidth=0.5)

        # Statistics
        import statistics
        mean_diff = statistics.mean(diff_vals)
        std_diff = statistics.stdev(diff_vals) if len(diff_vals) > 1 else 0
        rms_diff = (sum(v**2 for v in diff_vals) / len(diff_vals)) ** 0.5

        ax3.axhline(y=mean_diff, color='red', linestyle=':', linewidth=0.8,
                     label=f'Mean: {mean_diff:.2f} mm')

        stats_text = (f'Mean: {mean_diff:.2f} mm\n'
                      f'Std:  {std_diff:.2f} mm\n'
                      f'RMS:  {rms_diff:.2f} mm\n'
                      f'N:    {len(diff_vals)} epochs')
        ax3.text(0.02, 0.95, stats_text, transform=ax3.transAxes,
                 fontsize=9, verticalalignment='top', fontfamily='monospace',
                 bbox=dict(boxstyle='round', facecolor='lightyellow', alpha=0.8))

    ax3.set_ylabel('PPP-AR − DD [mm]')
    ax3.set_xlabel('Date (2025)')
    ax3.grid(True, alpha=0.3)
    ax3.set_title('ZTD Difference (PPP-AR GRE minus DD)')

    # Format x-axis
    ax3.xaxis.set_major_formatter(mdates.DateFormatter('%b %d'))
    ax3.xaxis.set_major_locator(mdates.DayLocator())
    plt.setp(ax3.xaxis.get_majorticklabels(), rotation=45, ha='right')

    plt.tight_layout()

    if save_path:
        plt.savefig(save_path, dpi=150, bbox_inches='tight')
        print(f"Plot saved to: {save_path}")

    plt.show()


def main():
    parser = argparse.ArgumentParser(
        description='Plot ZTD differences between DD and PPP-AR GRE solutions',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s                                  # WALF, DOY 296-310
  %(prog)s --station ONSA --start 297       # ONSA from DOY 297
  %(prog)s --station WALF --save            # Save PNG
        """
    )
    parser.add_argument('--station', type=str, default='WALF',
                       help='Station name (default: WALF)')
    parser.add_argument('--year', type=int, default=2025, help='Year (default: 2025)')
    parser.add_argument('--start', type=int, default=296, help='Start DOY (default: 296)')
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
    print(f"Collecting ZTD data for station {station}...")
    print(f"  DD path:     {args.dd_path}")
    print(f"  PPP-AR path: {args.pppar_path}")
    print(f"  DOY range:   {args.start}-{args.end}")
    print()

    # Collect data
    dd_data, pppar_data = collect_ztd_data(
        station=station,
        year=args.year,
        start_doy=args.start,
        end_doy=args.end,
        dd_base=args.dd_path,
        pppar_base=args.pppar_path,
    )

    print(f"  DD epochs:     {len(dd_data)}")
    print(f"  PPP-AR epochs: {len(pppar_data)}")

    if not dd_data:
        print(f"Error: No DD data found for {station}")
        sys.exit(1)
    if not pppar_data:
        print(f"Error: No PPP-AR data found for {station}")
        sys.exit(1)

    # Compute differences
    diffs = compute_differences(dd_data, pppar_data)
    print(f"  Matched epochs: {len(diffs)}")

    if not diffs:
        print("Error: No matching epochs between DD and PPP-AR")
        sys.exit(1)

    # Statistics summary
    import statistics
    diff_vals = [d[1] for d in diffs]
    mean_diff = statistics.mean(diff_vals)
    std_diff = statistics.stdev(diff_vals) if len(diff_vals) > 1 else 0
    rms_diff = (sum(v**2 for v in diff_vals) / len(diff_vals)) ** 0.5
    print(f"\n  ZTD Difference Statistics (PPP-AR GRE − DD):")
    print(f"    Mean:  {mean_diff:+.2f} mm")
    print(f"    Std:    {std_diff:.2f} mm")
    print(f"    RMS:    {rms_diff:.2f} mm")
    print(f"    Min:   {min(diff_vals):+.2f} mm")
    print(f"    Max:   {max(diff_vals):+.2f} mm")

    # Plot
    save_path = None
    if args.save:
        save_path = args.output or f"ztd_diff_{station}_{args.year}_DOY{args.start}-{args.end}.png"

    plot_ztd_comparison(station, dd_data, pppar_data, diffs, save_path=save_path)


if __name__ == '__main__':
    main()
