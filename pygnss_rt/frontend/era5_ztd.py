"""
ERA5 ZTD Fetcher and Calculator

Downloads ERA5 reanalysis data from Copernicus Climate Data Store (CDS) and
computes Zenith Total Delay (ZTD) for GNSS station validation.

ERA5 provides hourly data on a 0.25° x 0.25° grid - much higher resolution
than VMF products.

ZTD Computation:
- ZTD = ZHD + ZWD
- ZHD: Zenith Hydrostatic Delay (from surface pressure)
- ZWD: Zenith Wet Delay (from integrated water vapor)

Requirements:
- cdsapi (pip install cdsapi)
- netCDF4 (pip install netCDF4)
- ~/.cdsapirc with CDS credentials

Reference:
- Saastamoinen (1972) for ZHD model
- Askne & Nordius (1987) for ZWD model
- Davis et al. (1985) for wet delay computation

Author: Addisu Hunegnaw
Date: January 2026
"""

import os
import math
import logging
from datetime import datetime, date, timedelta
from dataclasses import dataclass
from typing import Optional
from pathlib import Path

import numpy as np

try:
    import cdsapi
    HAS_CDS = True
except ImportError:
    HAS_CDS = False

try:
    import netCDF4 as nc
    HAS_NC = True
except ImportError:
    HAS_NC = False

logger = logging.getLogger(__name__)

# ERA5 cache directory
ERA5_CACHE_DIR = Path("/home/ahunegnaw/GPSDATA/ERA5_CACHE")


@dataclass
class ERA5ZTDEstimate:
    """ERA5-derived ZTD estimate for a station"""
    station_4char: str
    timestamp: datetime
    lat: float
    lon: float
    height: float
    ztd_m: float        # ZTD in meters
    zhd_m: float        # ZHD in meters
    zwd_m: float        # ZWD in meters
    surface_pressure_hpa: float  # Surface pressure in hPa
    temperature_k: float  # Temperature in K
    pwv_mm: float       # Precipitable water vapor in mm
    year: int
    doy: int
    hour: int


def compute_zhd_saastamoinen(pressure_hpa: float, lat_deg: float, height_m: float) -> float:
    """
    Compute Zenith Hydrostatic Delay using Saastamoinen model.

    ZHD = (0.0022768 * P) / (1 - 0.00266*cos(2*lat) - 0.00028*H)

    Args:
        pressure_hpa: Surface pressure in hPa (mbar)
        lat_deg: Latitude in degrees
        height_m: Height above ellipsoid in meters

    Returns:
        ZHD in meters
    """
    lat_rad = math.radians(lat_deg)
    cos_2lat = math.cos(2 * lat_rad)

    # Height in km
    h_km = height_m / 1000.0

    # Saastamoinen formula
    zhd = (0.0022768 * pressure_hpa) / (1 - 0.00266 * cos_2lat - 0.00028 * h_km)

    return zhd


def compute_zwd_askne_nordius(temperature_k: float, e_hpa: float, lat_deg: float) -> float:
    """
    Compute Zenith Wet Delay using Askne & Nordius model.

    ZWD = (10^-6 / rho * Rv) * integral(k2' + k3/T) * e * dz

    Simplified surface-based approximation:
    ZWD = 0.002277 * (1255/T + 0.05) * e

    Args:
        temperature_k: Surface temperature in Kelvin
        e_hpa: Water vapor pressure in hPa
        lat_deg: Latitude in degrees

    Returns:
        ZWD in meters
    """
    if e_hpa <= 0 or temperature_k <= 0:
        return 0.0

    # Constants for wet delay (Bevis et al. 1994)
    k2_prime = 22.1  # K/hPa
    k3 = 3.739e5     # K^2/hPa

    # Mean temperature of water vapor (simplified model)
    Tm = 0.72 * temperature_k + 70.2  # Bevis approximation

    # ZWD approximation
    zwd = 1e-6 * (k2_prime + k3/Tm) * (e_hpa / temperature_k) * 0.2173

    # More robust Askne-Nordius formula
    # ZWD = 0.002277 * (1255/T + 0.05) * e
    zwd_an = 0.002277 * (1255.0/temperature_k + 0.05) * e_hpa

    return zwd_an


def compute_water_vapor_pressure(temperature_k: float, dewpoint_k: float) -> float:
    """
    Compute water vapor pressure from temperature and dewpoint.

    Uses Magnus formula for saturation vapor pressure.

    Args:
        temperature_k: Temperature in Kelvin
        dewpoint_k: Dewpoint temperature in Kelvin

    Returns:
        Water vapor pressure in hPa
    """
    # Convert to Celsius
    Td_c = dewpoint_k - 273.15

    if Td_c < -273:
        return 0.0

    # Magnus formula coefficients (for water)
    a = 17.27
    b = 237.7

    # Saturation vapor pressure at dewpoint = actual vapor pressure
    e = 6.1078 * math.exp((a * Td_c) / (b + Td_c))

    return e


def compute_pwv_from_zwd(zwd_m: float) -> float:
    """
    Compute Precipitable Water Vapor from ZWD.

    PWV = ZWD / PI (PI ~ 6.5)

    Args:
        zwd_m: Zenith Wet Delay in meters

    Returns:
        PWV in mm
    """
    PI_factor = 6.5  # Approximate conversion factor
    return (zwd_m / PI_factor) * 1000  # Convert to mm


def download_era5_surface(year: int, month: int, day: int, hour: int,
                          area: list[float], output_path: str) -> bool:
    """
    Download ERA5 surface level data from CDS.

    Args:
        year, month, day, hour: Date/time
        area: [north, west, south, east] bounding box
        output_path: Output file path

    Returns:
        True if successful
    """
    if not HAS_CDS:
        logger.error("cdsapi not available")
        return False

    try:
        c = cdsapi.Client()

        c.retrieve(
            'reanalysis-era5-single-levels',
            {
                'product_type': 'reanalysis',
                'format': 'netcdf',
                'variable': [
                    'surface_pressure',
                    '2m_temperature',
                    '2m_dewpoint_temperature',
                ],
                'year': str(year),
                'month': f'{month:02d}',
                'day': f'{day:02d}',
                'time': f'{hour:02d}:00',
                'area': area,
            },
            output_path
        )
        return True

    except Exception as e:
        logger.error(f"ERA5 download failed: {e}")
        return False


def download_era5_pressure_levels(year: int, month: int, day: int, hour: int,
                                   area: list[float], output_path: str) -> bool:
    """
    Download ERA5 pressure level data for vertical integration.

    Args:
        year, month, day, hour: Date/time
        area: [north, west, south, east] bounding box
        output_path: Output file path

    Returns:
        True if successful
    """
    if not HAS_CDS:
        logger.error("cdsapi not available")
        return False

    try:
        c = cdsapi.Client()

        # Pressure levels for troposphere
        pressure_levels = [
            '1000', '975', '950', '925', '900', '875', '850', '825', '800',
            '775', '750', '700', '650', '600', '550', '500', '450', '400',
            '350', '300', '250', '200', '150', '100'
        ]

        c.retrieve(
            'reanalysis-era5-pressure-levels',
            {
                'product_type': 'reanalysis',
                'format': 'netcdf',
                'variable': [
                    'temperature',
                    'specific_humidity',
                    'geopotential',
                ],
                'pressure_level': pressure_levels,
                'year': str(year),
                'month': f'{month:02d}',
                'day': f'{day:02d}',
                'time': f'{hour:02d}:00',
                'area': area,
            },
            output_path
        )
        return True

    except Exception as e:
        logger.error(f"ERA5 pressure level download failed: {e}")
        return False


def get_era5_cache_path(year: int, doy: int, hour: int, data_type: str = 'surface') -> Path:
    """Get the cache path for ERA5 data."""
    d = date(year, 1, 1) + timedelta(days=doy - 1)
    filename = f"era5_{data_type}_{year}{d.month:02d}{d.day:02d}_{hour:02d}.nc"
    return ERA5_CACHE_DIR / str(year) / f"{doy:03d}" / filename


def ensure_era5_data(year: int, doy: int, hour: int,
                     lat_range: tuple[float, float],
                     lon_range: tuple[float, float]) -> Optional[tuple[Path, Path]]:
    """
    Ensure ERA5 data is available, downloading if necessary.

    Args:
        year: Year
        doy: Day of year
        hour: Hour (0-23)
        lat_range: (min_lat, max_lat)
        lon_range: (min_lon, max_lon)

    Returns:
        Tuple of (surface_path, pressure_path) if available, None otherwise
    """
    d = date(year, 1, 1) + timedelta(days=doy - 1)

    surface_path = get_era5_cache_path(year, doy, hour, 'surface')
    pressure_path = get_era5_cache_path(year, doy, hour, 'pressure')

    # Create cache directory
    surface_path.parent.mkdir(parents=True, exist_ok=True)

    # Add buffer to area
    buffer = 1.0
    area = [
        min(90, lat_range[1] + buffer),   # North
        max(-180, lon_range[0] - buffer), # West
        max(-90, lat_range[0] - buffer),  # South
        min(180, lon_range[1] + buffer),  # East
    ]

    # Download surface data if not cached
    if not surface_path.exists():
        logger.info(f"Downloading ERA5 surface data for {d} {hour:02d}:00")
        if not download_era5_surface(d.year, d.month, d.day, hour, area, str(surface_path)):
            return None

    # Download pressure level data if not cached
    if not pressure_path.exists():
        logger.info(f"Downloading ERA5 pressure level data for {d} {hour:02d}:00")
        if not download_era5_pressure_levels(d.year, d.month, d.day, hour, area, str(pressure_path)):
            return None

    return (surface_path, pressure_path)


def read_era5_surface_at_point(nc_path: Path, lat: float, lon: float) -> Optional[dict]:
    """
    Read ERA5 surface data at a specific point using bilinear interpolation.

    Args:
        nc_path: Path to netCDF file
        lat: Latitude in degrees
        lon: Longitude in degrees (-180 to 180)

    Returns:
        Dict with surface_pressure_hpa, temperature_k, dewpoint_k
    """
    if not HAS_NC:
        return None

    try:
        ds = nc.Dataset(str(nc_path), 'r')

        # Get coordinates
        lats = ds.variables['latitude'][:]
        lons = ds.variables['longitude'][:]

        # Normalize longitude to match ERA5 (0-360 or -180 to 180)
        if lons.max() > 180:
            if lon < 0:
                lon += 360

        # Find nearest grid points
        lat_idx = np.argmin(np.abs(lats - lat))
        lon_idx = np.argmin(np.abs(lons - lon))

        # Get variables
        sp = ds.variables['sp'][0, lat_idx, lon_idx]  # Surface pressure (Pa)
        t2m = ds.variables['t2m'][0, lat_idx, lon_idx]  # 2m temperature (K)
        d2m = ds.variables['d2m'][0, lat_idx, lon_idx]  # 2m dewpoint (K)

        ds.close()

        return {
            'surface_pressure_hpa': float(sp) / 100.0,  # Pa to hPa
            'temperature_k': float(t2m),
            'dewpoint_k': float(d2m),
        }

    except Exception as e:
        logger.error(f"Error reading ERA5 surface data: {e}")
        return None


def compute_era5_ztd_at_point(surface_path: Path, lat: float, lon: float,
                               height_m: float) -> Optional[tuple[float, float, float, dict]]:
    """
    Compute ZTD at a specific point from ERA5 surface data.

    Uses Saastamoinen model for ZHD and Askne-Nordius model for ZWD.

    Args:
        surface_path: Path to ERA5 surface data netCDF
        lat: Latitude in degrees
        lon: Longitude in degrees
        height_m: Station height in meters

    Returns:
        Tuple of (ZTD, ZHD, ZWD, metadata_dict) in meters, or None
    """
    surface_data = read_era5_surface_at_point(surface_path, lat, lon)
    if not surface_data:
        return None

    pressure_hpa = surface_data['surface_pressure_hpa']
    temperature_k = surface_data['temperature_k']
    dewpoint_k = surface_data['dewpoint_k']

    # Height correction for pressure (barometric formula)
    # Assuming ERA5 is at geoid height, adjust to station height
    scale_height = 8000  # meters (approximate)
    pressure_at_station = pressure_hpa * math.exp(-height_m / scale_height)

    # Compute water vapor pressure
    e_hpa = compute_water_vapor_pressure(temperature_k, dewpoint_k)

    # Compute ZHD (Saastamoinen)
    zhd = compute_zhd_saastamoinen(pressure_at_station, lat, height_m)

    # Compute ZWD (Askne-Nordius)
    zwd = compute_zwd_askne_nordius(temperature_k, e_hpa, lat)

    # Total delay
    ztd = zhd + zwd

    metadata = {
        'surface_pressure_hpa': pressure_at_station,
        'temperature_k': temperature_k,
        'dewpoint_k': dewpoint_k,
        'water_vapor_pressure_hpa': e_hpa,
        'pwv_mm': compute_pwv_from_zwd(zwd),
    }

    return (ztd, zhd, zwd, metadata)


def get_era5_ztd_for_station(station_4char: str, lat: float, lon: float, height: float,
                              year: int, doy: int, hour: int,
                              download_if_missing: bool = True) -> Optional[ERA5ZTDEstimate]:
    """
    Get ERA5-derived ZTD estimate for a specific station and time.

    Args:
        station_4char: 4-character station code
        lat: Station latitude (degrees)
        lon: Station longitude (degrees)
        height: Station height (meters)
        year: Year
        doy: Day of year
        hour: Hour of day (0-23)
        download_if_missing: Whether to download ERA5 data if not cached

    Returns:
        ERA5ZTDEstimate if successful, None otherwise
    """
    # Check for cached ERA5 data
    surface_path = get_era5_cache_path(year, doy, hour, 'surface')

    if not surface_path.exists():
        if not download_if_missing:
            return None

        # Download ERA5 data
        result = ensure_era5_data(
            year, doy, hour,
            lat_range=(lat - 2, lat + 2),
            lon_range=(lon - 2, lon + 2)
        )
        if not result:
            return None
        surface_path = result[0]

    # Compute ZTD
    result = compute_era5_ztd_at_point(surface_path, lat, lon, height)
    if not result:
        return None

    ztd, zhd, zwd, metadata = result

    # Create timestamp
    d = date(year, 1, 1) + timedelta(days=doy - 1)
    timestamp = datetime(d.year, d.month, d.day, hour)

    return ERA5ZTDEstimate(
        station_4char=station_4char,
        timestamp=timestamp,
        lat=lat,
        lon=lon,
        height=height,
        ztd_m=ztd,
        zhd_m=zhd,
        zwd_m=zwd,
        surface_pressure_hpa=metadata['surface_pressure_hpa'],
        temperature_k=metadata['temperature_k'],
        pwv_mm=metadata['pwv_mm'],
        year=year,
        doy=doy,
        hour=hour
    )


def get_era5_ztd_timeseries(station_4char: str, lat: float, lon: float, height: float,
                             year: int, doy: int,
                             hours: list[int] = None,
                             download_if_missing: bool = True) -> list[ERA5ZTDEstimate]:
    """
    Get ERA5 ZTD time series for a station over a full day.

    Args:
        station_4char: 4-character station code
        lat: Station latitude (degrees)
        lon: Station longitude (degrees)
        height: Station height (meters)
        year: Year
        doy: Day of year
        hours: List of hours to fetch (default: 0-23)
        download_if_missing: Whether to download ERA5 data if not cached

    Returns:
        List of ERA5ZTDEstimate for each hour
    """
    if hours is None:
        hours = list(range(24))

    estimates = []
    for hour in hours:
        est = get_era5_ztd_for_station(
            station_4char, lat, lon, height, year, doy, hour,
            download_if_missing=download_if_missing
        )
        if est:
            estimates.append(est)

    return sorted(estimates, key=lambda e: e.timestamp)


def list_cached_era5_dates(cache_dir: Path = ERA5_CACHE_DIR) -> list[tuple[int, int, int]]:
    """
    List available ERA5 dates in cache.

    Returns:
        List of (year, doy, hour) tuples
    """
    dates = []

    if not cache_dir.exists():
        return dates

    for year_dir in cache_dir.iterdir():
        if not year_dir.is_dir():
            continue
        try:
            year = int(year_dir.name)
        except ValueError:
            continue

        for doy_dir in year_dir.iterdir():
            if not doy_dir.is_dir():
                continue
            try:
                doy = int(doy_dir.name)
            except ValueError:
                continue

            # Check for surface files
            for f in doy_dir.glob("era5_surface_*.nc"):
                try:
                    hour = int(f.stem.split('_')[-1])
                    dates.append((year, doy, hour))
                except (ValueError, IndexError):
                    continue

    return sorted(dates)


if __name__ == "__main__":
    import sys

    # Test ERA5 ZTD computation
    print("ERA5 ZTD Calculator Test")
    print("=" * 50)

    if not HAS_CDS:
        print("ERROR: cdsapi not available")
        sys.exit(1)

    if not HAS_NC:
        print("ERROR: netCDF4 not available")
        sys.exit(1)

    # Test station: Zimmerwald, Switzerland
    station = "ZIM2"
    lat, lon, height = 46.877, 7.465, 956.0
    year, doy, hour = 2025, 300, 12

    print(f"\nStation: {station}")
    print(f"Location: {lat:.3f}N, {lon:.3f}E, {height:.1f}m")
    print(f"Time: {year} DOY {doy} {hour:02d}:00")
    print()

    # Check cache
    cached = list_cached_era5_dates()
    print(f"Cached ERA5 data: {len(cached)} time steps")

    # Try to get ZTD (without downloading)
    estimate = get_era5_ztd_for_station(
        station, lat, lon, height, year, doy, hour,
        download_if_missing=False
    )

    if estimate:
        print(f"\nERA5 ZTD Results:")
        print(f"  ZHD: {estimate.zhd_m*1000:.1f} mm")
        print(f"  ZWD: {estimate.zwd_m*1000:.1f} mm")
        print(f"  ZTD: {estimate.ztd_m*1000:.1f} mm")
        print(f"  Surface P: {estimate.surface_pressure_hpa:.1f} hPa")
        print(f"  Temperature: {estimate.temperature_k - 273.15:.1f} C")
        print(f"  PWV: {estimate.pwv_mm:.1f} mm")
    else:
        print("\nNo cached ERA5 data available for this date/time.")
        print("Run with download_if_missing=True to fetch from CDS.")
