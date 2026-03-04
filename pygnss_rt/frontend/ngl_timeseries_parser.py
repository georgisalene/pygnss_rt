"""
NGL (Nevada Geodetic Laboratory) Time Series Parser

Parses coordinate time series from the NGL GPS Solutions:
- txyz2 format: ECEF XYZ coordinates
- tenv3 format: Topocentric ENU coordinates (recommended for visualization)

Data source: https://geodesy.unr.edu/gps_timeseries/

Author: Addisu Hunegnaw
Date: January 2026
"""

import os
import math
import requests
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from typing import Optional
import numpy as np


# NGL base URLs
NGL_BASE_URL = "https://geodesy.unr.edu/gps_timeseries"
NGL_TXYZ2_URL = f"{NGL_BASE_URL}/IGS20/txyz"
NGL_TENV3_URL = f"{NGL_BASE_URL}/IGS20/tenv3"


@dataclass
class NGLCoordinatePoint:
    """Single coordinate observation from NGL time series"""
    station: str
    date: datetime
    decimal_year: float
    # For txyz2 (ECEF)
    x: Optional[float] = None
    y: Optional[float] = None
    z: Optional[float] = None
    x_sigma: Optional[float] = None
    y_sigma: Optional[float] = None
    z_sigma: Optional[float] = None
    # For tenv3 (Topocentric)
    east: Optional[float] = None  # meters from reference
    north: Optional[float] = None  # meters from reference
    up: Optional[float] = None  # meters from reference
    east_sigma: Optional[float] = None
    north_sigma: Optional[float] = None
    up_sigma: Optional[float] = None
    # Antenna height
    antenna_height: Optional[float] = None


@dataclass
class NGLTimeSeries:
    """Complete time series for a station"""
    station: str
    format_type: str  # 'txyz2' or 'tenv3'
    reference_frame: str = "IGS20"
    points: list[NGLCoordinatePoint] = field(default_factory=list)
    # Reference coordinates (for tenv3)
    ref_lat: Optional[float] = None
    ref_lon: Optional[float] = None
    ref_height: Optional[float] = None

    @property
    def dates(self) -> list[datetime]:
        return [p.date for p in self.points]

    @property
    def decimal_years(self) -> list[float]:
        return [p.decimal_year for p in self.points]

    @property
    def east_mm(self) -> list[float]:
        """East component in mm (relative to mean)"""
        if self.format_type == 'tenv3':
            vals = [p.east * 1000 for p in self.points if p.east is not None]
            if vals:
                mean_val = np.mean(vals)
                return [v - mean_val for v in vals]
        return []

    @property
    def north_mm(self) -> list[float]:
        """North component in mm (relative to mean)"""
        if self.format_type == 'tenv3':
            vals = [p.north * 1000 for p in self.points if p.north is not None]
            if vals:
                mean_val = np.mean(vals)
                return [v - mean_val for v in vals]
        return []

    @property
    def up_mm(self) -> list[float]:
        """Up component in mm (relative to mean)"""
        if self.format_type == 'tenv3':
            vals = [p.up * 1000 for p in self.points if p.up is not None]
            if vals:
                mean_val = np.mean(vals)
                return [v - mean_val for v in vals]
        return []

    def get_statistics(self) -> dict:
        """Calculate time series statistics"""
        stats = {
            'station': self.station,
            'n_points': len(self.points),
            'start_date': min(self.dates) if self.points else None,
            'end_date': max(self.dates) if self.points else None,
        }

        if self.format_type == 'tenv3' and self.points:
            east_vals = [p.east * 1000 for p in self.points if p.east is not None]
            north_vals = [p.north * 1000 for p in self.points if p.north is not None]
            up_vals = [p.up * 1000 for p in self.points if p.up is not None]

            if east_vals:
                stats['east_std_mm'] = np.std(east_vals)
                stats['east_range_mm'] = max(east_vals) - min(east_vals)
            if north_vals:
                stats['north_std_mm'] = np.std(north_vals)
                stats['north_range_mm'] = max(north_vals) - min(north_vals)
            if up_vals:
                stats['up_std_mm'] = np.std(up_vals)
                stats['up_range_mm'] = max(up_vals) - min(up_vals)

        return stats


def parse_ngl_date(date_str: str) -> Optional[datetime]:
    """
    Parse NGL date formats.

    Args:
        date_str: Date string in YYMONDD format (e.g., "11NOV17", "23JAN05")

    Returns:
        datetime object or None
    """
    try:
        # Format: YYMONDD (e.g., 11NOV17)
        month_map = {
            'JAN': 1, 'FEB': 2, 'MAR': 3, 'APR': 4, 'MAY': 5, 'JUN': 6,
            'JUL': 7, 'AUG': 8, 'SEP': 9, 'OCT': 10, 'NOV': 11, 'DEC': 12
        }

        year_2digit = int(date_str[:2])
        month_str = date_str[2:5].upper()
        day = int(date_str[5:7])

        # Convert 2-digit year to 4-digit
        year = 2000 + year_2digit if year_2digit < 80 else 1900 + year_2digit
        month = month_map.get(month_str, 1)

        return datetime(year, month, day)
    except (ValueError, KeyError, IndexError):
        return None


def parse_txyz2_line(line: str) -> Optional[NGLCoordinatePoint]:
    """
    Parse a single line from txyz2 format.

    Format: station date decimal_year X Y Z X_sig Y_sig Z_sig XY_corr YZ_corr XZ_corr ant_height
    """
    parts = line.strip().split()
    if len(parts) < 13:
        return None

    try:
        station = parts[0]
        date = parse_ngl_date(parts[1])
        if date is None:
            return None

        decimal_year = float(parts[2])
        x = float(parts[3])
        y = float(parts[4])
        z = float(parts[5])
        x_sigma = float(parts[6])
        y_sigma = float(parts[7])
        z_sigma = float(parts[8])
        antenna_height = float(parts[12]) if len(parts) > 12 else None

        return NGLCoordinatePoint(
            station=station,
            date=date,
            decimal_year=decimal_year,
            x=x, y=y, z=z,
            x_sigma=x_sigma, y_sigma=y_sigma, z_sigma=z_sigma,
            antenna_height=antenna_height
        )
    except (ValueError, IndexError):
        return None


def parse_tenv3_line(line: str) -> Optional[NGLCoordinatePoint]:
    """
    Parse a single line from tenv3 format.

    Format: station date decimal_year mjd gps_week gps_day ref_meridian
            east_int east_frac north_int north_frac vert_int vert_frac
            ant_height east_sig north_sig vert_sig en_corr ev_corr nv_corr
            nom_lat nom_lon nom_height
    """
    parts = line.strip().split()
    if len(parts) < 23:
        return None

    try:
        station = parts[0]
        date = parse_ngl_date(parts[1])
        if date is None:
            return None

        decimal_year = float(parts[2])

        # Combine integer and fractional parts
        east = float(parts[7]) + float(parts[8])
        north = float(parts[9]) + float(parts[10])
        up = float(parts[11]) + float(parts[12])

        antenna_height = float(parts[13])
        east_sigma = float(parts[14])
        north_sigma = float(parts[15])
        up_sigma = float(parts[16])

        return NGLCoordinatePoint(
            station=station,
            date=date,
            decimal_year=decimal_year,
            east=east, north=north, up=up,
            east_sigma=east_sigma, north_sigma=north_sigma, up_sigma=up_sigma,
            antenna_height=antenna_height
        )
    except (ValueError, IndexError):
        return None


# NGL region codes for tenv3 files
NGL_TENV3_REGIONS = [
    'EU',  # Europe
    'NA',  # North America
    'SA',  # South America
    'AF',  # Africa
    'AU',  # Australia
    'PA',  # Pacific
    'AN',  # Antarctica
    'AR',  # Arctic
    'IN',  # Indian Ocean
    'SO',  # Southern Ocean
    'IGS20',  # Global IGS stations
    'CA',  # Canada
    'NZ',  # New Zealand
]


def fetch_ngl_timeseries(station: str, format_type: str = 'tenv3',
                         reference_frame: str = 'IGS20',
                         region: Optional[str] = None,
                         timeout: int = 30) -> Optional[NGLTimeSeries]:
    """
    Fetch NGL time series from the web.

    Args:
        station: 4-character station code (e.g., "BRUX", "ONSA")
        format_type: 'txyz2' for ECEF or 'tenv3' for topocentric
        reference_frame: Reference frame (default: IGS20)
        region: For tenv3, the region code (EU, NA, etc.). If None, searches common regions.
        timeout: Request timeout in seconds

    Returns:
        NGLTimeSeries object or None if fetch fails
    """
    station = station.upper()

    if format_type == 'txyz2':
        # txyz2 files are in a flat directory
        url = f"{NGL_BASE_URL}/{reference_frame}/txyz/{station}.txyz2"
        parser = parse_txyz2_line
        urls_to_try = [url]
    elif format_type == 'tenv3':
        parser = parse_tenv3_line
        # tenv3 files are organized by region: REGION/STATION.REGION.tenv3
        if region:
            urls_to_try = [f"{NGL_BASE_URL}/{reference_frame}/tenv3/{region}/{station}.{region}.tenv3"]
        else:
            # Search through common regions
            urls_to_try = [
                f"{NGL_BASE_URL}/{reference_frame}/tenv3/{r}/{station}.{r}.tenv3"
                for r in NGL_TENV3_REGIONS
            ]
    else:
        return None

    response = None
    for url in urls_to_try:
        try:
            response = requests.get(url, timeout=timeout)
            if response.status_code == 200:
                break
            response = None
        except requests.RequestException:
            continue

    if response is None:
        print(f"Could not find NGL {format_type} data for {station}")
        return None

    ts = NGLTimeSeries(
        station=station,
        format_type=format_type,
        reference_frame=reference_frame
    )

    for line in response.text.strip().split('\n'):
        if not line.strip():
            continue
        point = parser(line)
        if point:
            ts.points.append(point)

    # Extract reference coordinates from first tenv3 point
    if format_type == 'tenv3' and ts.points:
        first_line = response.text.strip().split('\n')[0]
        parts = first_line.split()
        if len(parts) >= 23:
            try:
                ts.ref_lat = float(parts[20])
                ts.ref_lon = float(parts[21])
                ts.ref_height = float(parts[22])
            except (ValueError, IndexError):
                pass

    return ts if ts.points else None


def parse_local_txyz2_file(filepath: str) -> Optional[NGLTimeSeries]:
    """Parse a local txyz2 file."""
    if not os.path.exists(filepath):
        return None

    station = os.path.basename(filepath).split('.')[0].upper()
    ts = NGLTimeSeries(station=station, format_type='txyz2')

    with open(filepath, 'r') as f:
        for line in f:
            if not line.strip():
                continue
            point = parse_txyz2_line(line)
            if point:
                ts.points.append(point)

    return ts if ts.points else None


def parse_local_tenv3_file(filepath: str) -> Optional[NGLTimeSeries]:
    """Parse a local tenv3 file."""
    if not os.path.exists(filepath):
        return None

    station = os.path.basename(filepath).split('.')[0].upper()
    ts = NGLTimeSeries(station=station, format_type='tenv3')

    with open(filepath, 'r') as f:
        lines = f.readlines()

        for line in lines:
            if not line.strip():
                continue
            point = parse_tenv3_line(line)
            if point:
                ts.points.append(point)

        # Extract reference coordinates from first line
        if lines:
            parts = lines[0].split()
            if len(parts) >= 23:
                try:
                    ts.ref_lat = float(parts[20])
                    ts.ref_lon = float(parts[21])
                    ts.ref_height = float(parts[22])
                except (ValueError, IndexError):
                    pass

    return ts if ts.points else None


def get_available_ngl_stations(reference_frame: str = 'IGS20') -> list[str]:
    """
    Get list of available stations from NGL.
    Note: This is a heavy operation - use sparingly.
    """
    # For now, return common European/global stations
    # A full implementation would scrape the directory listing
    return [
        "BRUX", "ONSA", "MATE", "GRAS", "WTZR", "POTS", "HERS",
        "WARN", "WSRT", "TLSE", "GRAZ", "BOR1", "JOZE", "LAMA",
        "MAR6", "ZIMM", "REYK", "HOFN", "KIRU", "TRO1", "NYA1",
        "NYAL", "STAS", "TRDS", "ALES", "ISTA", "ANKR", "SOFI"
    ]


def filter_timeseries_by_date(ts: NGLTimeSeries,
                               start_date: Optional[datetime] = None,
                               end_date: Optional[datetime] = None) -> NGLTimeSeries:
    """Filter time series to a specific date range."""
    filtered = NGLTimeSeries(
        station=ts.station,
        format_type=ts.format_type,
        reference_frame=ts.reference_frame,
        ref_lat=ts.ref_lat,
        ref_lon=ts.ref_lon,
        ref_height=ts.ref_height
    )

    for point in ts.points:
        if start_date and point.date < start_date:
            continue
        if end_date and point.date > end_date:
            continue
        filtered.points.append(point)

    return filtered


def compute_velocity(ts: NGLTimeSeries) -> dict:
    """
    Compute linear velocity from time series using least squares.

    Returns:
        Dictionary with velocities in mm/year for E, N, U components
    """
    if len(ts.points) < 10:
        return {}

    years = np.array(ts.decimal_years)

    result = {}

    if ts.format_type == 'tenv3':
        for comp, attr in [('east', 'east'), ('north', 'north'), ('up', 'up')]:
            vals = np.array([getattr(p, attr) * 1000 for p in ts.points])  # to mm
            if len(vals) > 0:
                # Simple linear regression
                A = np.vstack([years, np.ones(len(years))]).T
                slope, intercept = np.linalg.lstsq(A, vals, rcond=None)[0]
                result[f'{comp}_velocity_mm_yr'] = slope

    return result


if __name__ == '__main__':
    import sys

    if len(sys.argv) > 1:
        station = sys.argv[1].upper()
    else:
        station = "BRUX"

    print(f"Fetching NGL time series for {station}...")
    print()

    ts = fetch_ngl_timeseries(station, format_type='tenv3')

    if ts:
        stats = ts.get_statistics()
        print(f"Station: {ts.station}")
        print(f"Reference Frame: {ts.reference_frame}")
        print(f"Points: {stats['n_points']}")
        print(f"Date range: {stats['start_date']} to {stats['end_date']}")
        print()

        if 'east_std_mm' in stats:
            print(f"Standard deviations:")
            print(f"  East:  {stats['east_std_mm']:.2f} mm")
            print(f"  North: {stats['north_std_mm']:.2f} mm")
            print(f"  Up:    {stats['up_std_mm']:.2f} mm")

        print()
        print("Computing velocities...")
        velocities = compute_velocity(ts)
        if velocities:
            print(f"  Ve: {velocities.get('east_velocity_mm_yr', 0):.2f} mm/yr")
            print(f"  Vn: {velocities.get('north_velocity_mm_yr', 0):.2f} mm/yr")
            print(f"  Vu: {velocities.get('up_velocity_mm_yr', 0):.2f} mm/yr")

        print()
        print("Last 5 points:")
        for p in ts.points[-5:]:
            print(f"  {p.date.strftime('%Y-%m-%d')}: E={p.east*1000:.1f}mm N={p.north*1000:.1f}mm U={p.up*1000:.1f}mm")
    else:
        print(f"Failed to fetch data for {station}")
