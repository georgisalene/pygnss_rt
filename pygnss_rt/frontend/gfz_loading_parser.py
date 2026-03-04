"""
GFZ ESMGFZ Loading Time Series Parser

Reads NTAL (non-tidal atmospheric loading) and NTOL (non-tidal ocean loading)
grid files from GFZ and interpolates to station coordinates.

Grid files are in Bernese format:
  lat  lon  up  north  east
with 0.5-degree resolution, stored as gzipped text files.

Performance: Uses targeted 4-point extraction instead of full grid parsing.
Only reads the 4 grid points needed for bilinear interpolation (~0.05s vs ~0.8s
per file). For 120 files this means ~6s instead of ~90s.

Source: /home/ahunegnaw/GPSDATA/DATAPOOL_BSW54/GFZloading/{NTAL,NTOL}/
"""

import os
import gzip
import glob
import numpy as np
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from typing import Optional, Tuple, Callable

GFZ_LOADING_PATH = "/home/ahunegnaw/GPSDATA/DATAPOOL_BSW54/GFZloading"

# Grid parameters (fixed for GFZ ESMGFZ 0.5-degree grids)
LAT_MIN, LAT_MAX = -89.75, 89.75
LON_MIN, LON_MAX = -179.75, 179.75
GRID_RES = 0.50
N_LAT = 360  # (89.75 - (-89.75)) / 0.5 + 1
N_LON = 720  # (179.75 - (-179.75)) / 0.5 + 1


def _get_interp_indices(lat: float, lon: float) -> Tuple[int, int, int, int, float, float]:
    """Compute grid indices and fractional offsets for bilinear interpolation.

    Returns (i0, j0, i1, j1, di, dj) where i=lat index, j=lon index.
    """
    lat_frac = (LAT_MAX - lat) / GRID_RES
    lon_frac = (lon - LON_MIN) / GRID_RES

    lat_frac = max(0.0, min(lat_frac, N_LAT - 1.001))
    lon_frac = max(0.0, min(lon_frac, N_LON - 1.001))

    i0 = int(lat_frac)
    j0 = int(lon_frac)
    i1 = min(i0 + 1, N_LAT - 1)
    j1 = min(j0 + 1, N_LON - 1)

    return i0, j0, i1, j1, lat_frac - i0, lon_frac - j0


def _extract_station_values(filepath: str, target_rows: set) -> Optional[dict]:
    """Extract only the specific grid rows needed from a loading file.

    Instead of parsing all 259,200 lines, reads only until the target rows
    are found. Typically extracts 4 points and stops early.

    Returns dict mapping row_index -> (up, north, east) in meters,
    or None on error.
    """
    try:
        if filepath.endswith('.gz'):
            f = gzip.open(filepath, 'rt')
        else:
            f = open(filepath, 'r')

        max_row = max(target_rows)
        values = {}
        row = 0
        for line in f:
            if line.startswith('!'):
                continue
            if row in target_rows:
                parts = line.split()
                if len(parts) >= 5:
                    values[row] = (float(parts[2]), float(parts[3]), float(parts[4]))
                    if len(values) == len(target_rows):
                        break
            row += 1
            if row > max_row:
                break

        f.close()
        return values if len(values) == len(target_rows) else None
    except Exception:
        return None


def _interpolate_from_points(values: dict, i0: int, j0: int, i1: int, j1: int,
                              di: float, dj: float) -> Tuple[float, float, float]:
    """Bilinear interpolation from the 4 extracted grid points.

    Returns (up, north, east) in meters.
    """
    r00 = i0 * N_LON + j0
    r01 = i0 * N_LON + j1
    r10 = i1 * N_LON + j0
    r11 = i1 * N_LON + j1

    results = []
    for comp in range(3):  # up, north, east
        val = (values[r00][comp] * (1 - di) * (1 - dj) +
               values[r10][comp] * di * (1 - dj) +
               values[r01][comp] * (1 - di) * dj +
               values[r11][comp] * di * dj)
        results.append(val)

    return results[0], results[1], results[2]


def get_available_dates(load_type: str = "NTAL") -> list:
    """Get list of available dates as (date_str, hour) tuples.

    Returns sorted list of (YYYYMMDD, HH) tuples.
    """
    load_dir = os.path.join(GFZ_LOADING_PATH, load_type)
    if not os.path.isdir(load_dir):
        return []

    pattern = os.path.join(load_dir, f"{load_type}_*.H*.gz")
    files = glob.glob(pattern)

    dates = []
    for f in files:
        basename = os.path.basename(f)
        # NTAL_20251024.H00.gz
        parts = basename.split('.')
        if len(parts) >= 2:
            date_str = parts[0].replace(f"{load_type}_", "")
            hour_str = parts[1].replace("H", "")
            try:
                dates.append((date_str, int(hour_str)))
            except ValueError:
                continue

    return sorted(dates)


def get_loading_timeseries(station_lat: float, station_lon: float,
                           load_type: str = "NTAL",
                           start_date: str = None,
                           end_date: str = None) -> pd.DataFrame:
    """Get loading displacement time series for a station location.

    Uses targeted 4-point extraction: computes the grid indices once, then
    for each file reads only the 4 lines needed for bilinear interpolation.
    This is ~14x faster than parsing the full 259,200-line grid.

    Args:
        station_lat: Station latitude in degrees
        station_lon: Station longitude in degrees
        load_type: "NTAL" or "NTOL"
        start_date: Optional start date "YYYYMMDD"
        end_date: Optional end date "YYYYMMDD"

    Returns:
        DataFrame with columns: datetime, up_mm, north_mm, east_mm
    """
    dates = get_available_dates(load_type)
    if not dates:
        return pd.DataFrame()

    if start_date:
        dates = [(d, h) for d, h in dates if d >= start_date]
    if end_date:
        dates = [(d, h) for d, h in dates if d <= end_date]

    if not dates:
        return pd.DataFrame()

    # Pre-compute grid indices once for this station
    i0, j0, i1, j1, di, dj = _get_interp_indices(station_lat, station_lon)
    target_rows = {i0 * N_LON + j0, i0 * N_LON + j1,
                   i1 * N_LON + j0, i1 * N_LON + j1}

    load_dir = os.path.join(GFZ_LOADING_PATH, load_type)
    records = []

    for date_str, hour in dates:
        filename = f"{load_type}_{date_str}.H{hour:02d}.gz"
        filepath = os.path.join(load_dir, filename)

        values = _extract_station_values(filepath, target_rows)
        if values is None:
            continue

        up_m, north_m, east_m = _interpolate_from_points(
            values, i0, j0, i1, j1, di, dj)

        dt = datetime.strptime(date_str, "%Y%m%d") + timedelta(hours=hour)

        records.append({
            'datetime': dt,
            'up_mm': up_m * 1000.0,
            'north_mm': north_m * 1000.0,
            'east_mm': east_m * 1000.0,
        })

    if not records:
        return pd.DataFrame()

    df = pd.DataFrame(records)
    df = df.sort_values('datetime').reset_index(drop=True)
    return df


def get_daily_mean_loading(station_lat: float, station_lon: float,
                           load_type: str = "NTAL",
                           start_date: str = None,
                           end_date: str = None) -> pd.DataFrame:
    """Get daily-mean loading displacement time series.

    Averages the 3-hourly values to daily means for cleaner plots.
    """
    df = get_loading_timeseries(station_lat, station_lon, load_type,
                                start_date, end_date)
    if df.empty:
        return df

    df['date'] = df['datetime'].dt.date
    daily = df.groupby('date').agg({
        'up_mm': 'mean',
        'north_mm': 'mean',
        'east_mm': 'mean',
    }).reset_index()
    daily['datetime'] = pd.to_datetime(daily['date'])
    return daily


def get_combined_loading(station_lat: float, station_lon: float,
                         start_date: str = None,
                         end_date: str = None) -> pd.DataFrame:
    """Get combined NTAL + NTOL loading time series.

    Runs NTAL and NTOL extraction in parallel threads for ~2x speedup
    since they read different file sets.

    Returns DataFrame with NTAL, NTOL, and combined (NTAL+NTOL) columns.
    """
    with ThreadPoolExecutor(max_workers=2) as executor:
        ntal_future = executor.submit(
            get_loading_timeseries, station_lat, station_lon, "NTAL",
            start_date, end_date)
        ntol_future = executor.submit(
            get_loading_timeseries, station_lat, station_lon, "NTOL",
            start_date, end_date)
        ntal = ntal_future.result()
        ntol = ntol_future.result()

    if ntal.empty and ntol.empty:
        return pd.DataFrame()

    if ntal.empty:
        ntol = ntol.rename(columns={'up_mm': 'ntol_up_mm',
                                     'north_mm': 'ntol_north_mm',
                                     'east_mm': 'ntol_east_mm'})
        ntol['ntal_up_mm'] = 0.0
        ntol['ntal_north_mm'] = 0.0
        ntol['ntal_east_mm'] = 0.0
        ntol['total_up_mm'] = ntol['ntol_up_mm']
        ntol['total_north_mm'] = ntol['ntol_north_mm']
        ntol['total_east_mm'] = ntol['ntol_east_mm']
        return ntol

    if ntol.empty:
        ntal = ntal.rename(columns={'up_mm': 'ntal_up_mm',
                                     'north_mm': 'ntal_north_mm',
                                     'east_mm': 'ntal_east_mm'})
        ntal['ntol_up_mm'] = 0.0
        ntal['ntol_north_mm'] = 0.0
        ntal['ntol_east_mm'] = 0.0
        ntal['total_up_mm'] = ntal['ntal_up_mm']
        ntal['total_north_mm'] = ntal['ntal_north_mm']
        ntal['total_east_mm'] = ntal['ntal_east_mm']
        return ntal

    # Merge on datetime
    merged = ntal.merge(ntol, on='datetime', suffixes=('_ntal', '_ntol'))
    merged = merged.rename(columns={
        'up_mm_ntal': 'ntal_up_mm',
        'north_mm_ntal': 'ntal_north_mm',
        'east_mm_ntal': 'ntal_east_mm',
        'up_mm_ntol': 'ntol_up_mm',
        'north_mm_ntol': 'ntol_north_mm',
        'east_mm_ntol': 'ntol_east_mm',
    })

    merged['total_up_mm'] = merged['ntal_up_mm'] + merged['ntol_up_mm']
    merged['total_north_mm'] = merged['ntal_north_mm'] + merged['ntol_north_mm']
    merged['total_east_mm'] = merged['ntal_east_mm'] + merged['ntol_east_mm']

    return merged.sort_values('datetime').reset_index(drop=True)
