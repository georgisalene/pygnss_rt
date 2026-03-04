"""
VMF (Vienna Mapping Function) Grid Parser for ERA5-based ZTD Validation

Parses VMF1/VMF3 grid files from TU Vienna which are derived from ECMWF
atmospheric data (ERA-Interim, ERA5). These files provide:
- ZHD: Zenith Hydrostatic Delay (meters)
- ZWD: Zenith Wet Delay (meters)
- ah, aw: Hydrostatic and wet mapping function coefficients

ZTD = ZHD + ZWD provides an independent atmospheric model-based estimate
for validating GNSS-derived troposphere estimates.

File format: VMFG_YYYYMMDD.Hhh.gz
Grid resolution: 2° lat x 2.5° lon (global)
Temporal resolution: 6-hourly (H00, H06, H12, H18)

Source: http://vmf.geo.tuwien.ac.at/
Reference: Boehm et al. (2006), Global Mapping Function (GMF)
"""

import os
import gzip
import math
from datetime import datetime, timedelta
from dataclasses import dataclass
from typing import Optional
from pathlib import Path


@dataclass
class VMFGridPoint:
    """Single VMF grid point with delay values"""
    lat: float          # Latitude (degrees)
    lon: float          # Longitude (degrees)
    ah: float           # Hydrostatic mapping function coefficient
    aw: float           # Wet mapping function coefficient
    zhd: float          # Zenith Hydrostatic Delay (meters)
    zwd: float          # Zenith Wet Delay (meters)

    @property
    def ztd(self) -> float:
        """Zenith Total Delay (meters)"""
        return self.zhd + self.zwd


@dataclass
class VMFEpoch:
    """VMF data for a single epoch"""
    timestamp: datetime
    grid_points: list[VMFGridPoint]
    lat_range: tuple[float, float]  # (min, max)
    lon_range: tuple[float, float]  # (min, max)
    lat_res: float  # Grid resolution in latitude
    lon_res: float  # Grid resolution in longitude

    def get_ztd_at_position(self, lat: float, lon: float) -> Optional[tuple[float, float, float]]:
        """
        Interpolate ZTD at a specific position using bilinear interpolation.

        Args:
            lat: Latitude in degrees (-90 to 90)
            lon: Longitude in degrees (0 to 360)

        Returns:
            Tuple of (ZTD, ZHD, ZWD) in meters, or None if position is out of range
        """
        # Normalize longitude to 0-360 range
        if lon < 0:
            lon += 360

        # Find surrounding grid points
        lat_idx_low = int((self.lat_range[1] - lat) / self.lat_res)
        lon_idx_low = int(lon / self.lon_res)

        lat_low = self.lat_range[1] - lat_idx_low * self.lat_res
        lat_high = lat_low - self.lat_res
        lon_low = lon_idx_low * self.lon_res
        lon_high = lon_low + self.lon_res

        # Handle longitude wrap-around
        if lon_high >= 360:
            lon_high = 0

        # Find the four corner points
        def find_point(target_lat, target_lon):
            for pt in self.grid_points:
                if abs(pt.lat - target_lat) < 0.1 and abs(pt.lon - target_lon) < 0.1:
                    return pt
            return None

        p00 = find_point(lat_low, lon_low)    # SW
        p01 = find_point(lat_low, lon_high)   # SE
        p10 = find_point(lat_high, lon_low)   # NW
        p11 = find_point(lat_high, lon_high)  # NE

        if not all([p00, p01, p10, p11]):
            # If exact grid points not found, use nearest neighbor
            return self._nearest_neighbor(lat, lon)

        # Bilinear interpolation weights
        t = (lat_low - lat) / self.lat_res if self.lat_res != 0 else 0
        u = (lon - lon_low) / self.lon_res if self.lon_res != 0 else 0

        # Interpolate ZHD
        zhd = ((1-t)*(1-u)*p00.zhd + (1-t)*u*p01.zhd +
               t*(1-u)*p10.zhd + t*u*p11.zhd)

        # Interpolate ZWD
        zwd = ((1-t)*(1-u)*p00.zwd + (1-t)*u*p01.zwd +
               t*(1-u)*p10.zwd + t*u*p11.zwd)

        ztd = zhd + zwd
        return (ztd, zhd, zwd)

    def _nearest_neighbor(self, lat: float, lon: float) -> Optional[tuple[float, float, float]]:
        """Find nearest grid point"""
        if lon < 0:
            lon += 360

        min_dist = float('inf')
        nearest = None

        for pt in self.grid_points:
            # Simple Euclidean distance (sufficient for nearby points)
            dlat = pt.lat - lat
            dlon = min(abs(pt.lon - lon), 360 - abs(pt.lon - lon))
            dist = math.sqrt(dlat**2 + dlon**2)

            if dist < min_dist:
                min_dist = dist
                nearest = pt

        if nearest and min_dist < 5:  # Within 5 degrees
            return (nearest.ztd, nearest.zhd, nearest.zwd)
        return None


@dataclass
class VMFStationEstimate:
    """VMF-derived ZTD estimate for a station"""
    station_4char: str
    timestamp: datetime
    lat: float
    lon: float
    height: float
    ztd_m: float        # ZTD in meters
    zhd_m: float        # ZHD in meters
    zwd_m: float        # ZWD in meters
    source: str = "VMF1"  # VMF1 or VMF3
    year: int = 0
    doy: int = 0


def parse_vmf_grid_file(filepath: str) -> Optional[VMFEpoch]:
    """
    Parse a VMF grid file.

    Args:
        filepath: Path to VMF file (can be .gz compressed)

    Returns:
        VMFEpoch object with all grid points, or None if parsing fails
    """
    grid_points = []
    timestamp = None
    lat_res = 2.0  # Default
    lon_res = 2.5  # Default

    # Determine file open method
    if filepath.endswith('.gz'):
        open_func = lambda f: gzip.open(f, 'rt')
    else:
        open_func = lambda f: open(f, 'r')

    try:
        with open_func(filepath) as f:
            for line in f:
                line = line.strip()

                # Skip empty lines
                if not line:
                    continue

                # Parse header comments
                if line.startswith('!'):
                    # Parse epoch from header
                    if 'Epoch:' in line:
                        # Format: "! Epoch:              2017 08 01 12 00  0.0"
                        parts = line.split('Epoch:')[1].strip().split()
                        if len(parts) >= 5:
                            try:
                                year = int(parts[0])
                                month = int(parts[1])
                                day = int(parts[2])
                                hour = int(parts[3])
                                minute = int(parts[4])
                                timestamp = datetime(year, month, day, hour, minute)
                            except (ValueError, IndexError):
                                pass

                    # Parse resolution
                    if 'Range/resolution:' in line:
                        # Format: "! Range/resolution:   -90 90 0 360 2 2.5"
                        parts = line.split('Range/resolution:')[1].strip().split()
                        if len(parts) >= 6:
                            try:
                                lat_res = float(parts[4])
                                lon_res = float(parts[5])
                            except (ValueError, IndexError):
                                pass
                    continue

                # Parse data lines: lat lon ah aw zhd zwd
                parts = line.split()
                if len(parts) >= 6:
                    try:
                        lat = float(parts[0])
                        lon = float(parts[1])
                        ah = float(parts[2])
                        aw = float(parts[3])
                        zhd = float(parts[4])
                        zwd = float(parts[5])

                        grid_points.append(VMFGridPoint(
                            lat=lat,
                            lon=lon,
                            ah=ah,
                            aw=aw,
                            zhd=zhd,
                            zwd=zwd
                        ))
                    except (ValueError, IndexError):
                        continue

        if not grid_points:
            return None

        # Determine lat/lon ranges
        lats = [p.lat for p in grid_points]
        lons = [p.lon for p in grid_points]

        return VMFEpoch(
            timestamp=timestamp or datetime.now(),
            grid_points=grid_points,
            lat_range=(min(lats), max(lats)),
            lon_range=(min(lons), max(lons)),
            lat_res=lat_res,
            lon_res=lon_res
        )

    except Exception as e:
        print(f"Error parsing VMF file {filepath}: {e}")
        return None


def get_vmf_file_path(base_dir: str, year: int, doy: int, hour: int = 0) -> Optional[str]:
    """
    Get the path to a VMF grid file.

    Args:
        base_dir: Base directory for VMF files (e.g., /home/.../DATAPOOL_52/VMF1)
        year: Year
        doy: Day of year
        hour: Hour (0, 6, 12, 18)

    Returns:
        Path to VMF file if it exists, None otherwise
    """
    from datetime import date, timedelta

    # Convert DOY to date
    d = date(year, 1, 1) + timedelta(days=doy - 1)

    # Snap hour to nearest 6-hour interval
    hour = (hour // 6) * 6

    # VMF filename format: VMFG_YYYYMMDD.Hhh.gz
    filename = f"VMFG_{d.year}{d.month:02d}{d.day:02d}.H{hour:02d}.gz"

    filepath = os.path.join(base_dir, filename)
    if os.path.exists(filepath):
        return filepath

    # Try without .gz
    filepath_nogz = filepath[:-3]
    if os.path.exists(filepath_nogz):
        return filepath_nogz

    return None


def get_vmf_ztd_for_station(station_4char: str, lat: float, lon: float, height: float,
                            year: int, doy: int, hour: int,
                            vmf_base_dir: str = "/home/ahunegnaw/GPSDATA/DATAPOOL_52/VMF1"
                            ) -> Optional[VMFStationEstimate]:
    """
    Get VMF-derived ZTD estimate for a specific station and time.

    Args:
        station_4char: 4-character station code
        lat: Station latitude (degrees)
        lon: Station longitude (degrees, -180 to 180 or 0 to 360)
        height: Station height (meters, for potential correction)
        year: Year
        doy: Day of year
        hour: Hour of day
        vmf_base_dir: Base directory for VMF files

    Returns:
        VMFStationEstimate if data available, None otherwise
    """
    # Get VMF file path
    vmf_path = get_vmf_file_path(vmf_base_dir, year, doy, hour)
    if not vmf_path:
        return None

    # Parse VMF grid
    vmf_epoch = parse_vmf_grid_file(vmf_path)
    if not vmf_epoch:
        return None

    # Interpolate ZTD at station position
    result = vmf_epoch.get_ztd_at_position(lat, lon)
    if not result:
        return None

    ztd, zhd, zwd = result

    return VMFStationEstimate(
        station_4char=station_4char,
        timestamp=vmf_epoch.timestamp,
        lat=lat,
        lon=lon,
        height=height,
        ztd_m=ztd,
        zhd_m=zhd,
        zwd_m=zwd,
        source="VMF1",
        year=year,
        doy=doy
    )


def get_vmf_ztd_timeseries(station_4char: str, lat: float, lon: float, height: float,
                           year: int, doy: int,
                           vmf_base_dir: str = "/home/ahunegnaw/GPSDATA/DATAPOOL_52/VMF1"
                           ) -> list[VMFStationEstimate]:
    """
    Get VMF ZTD time series for a station over a full day (6-hourly).

    Args:
        station_4char: 4-character station code
        lat: Station latitude (degrees)
        lon: Station longitude (degrees)
        height: Station height (meters)
        year: Year
        doy: Day of year
        vmf_base_dir: Base directory for VMF files

    Returns:
        List of VMFStationEstimate for each 6-hour epoch
    """
    estimates = []

    for hour in [0, 6, 12, 18]:
        est = get_vmf_ztd_for_station(
            station_4char, lat, lon, height, year, doy, hour, vmf_base_dir
        )
        if est:
            estimates.append(est)

    return sorted(estimates, key=lambda e: e.timestamp)


def get_station_coordinates_from_crd(crd_file: str) -> dict[str, tuple[float, float, float]]:
    """
    Get station coordinates (lat, lon, height) from a CRD file.

    Args:
        crd_file: Path to Bernese CRD file

    Returns:
        Dictionary mapping station code to (lat, lon, height)
    """
    from .dd_crd_parser import parse_dd_crd_file

    coords = {}

    try:
        # Parse with dummy year/doy (not needed for coordinate extraction)
        dd_coords = parse_dd_crd_file(crd_file, 2025, 1)

        for coord in dd_coords:
            lat, lon, height = coord.lat_lon_height
            coords[coord.station_4char.upper()] = (lat, lon, height)
    except Exception:
        pass

    return coords


def list_available_vmf_dates(vmf_base_dir: str) -> list[tuple[int, int, int]]:
    """
    List available VMF dates in the directory.

    Args:
        vmf_base_dir: Base directory for VMF files

    Returns:
        List of (year, month, day) tuples for available dates
    """
    import re

    dates = set()
    pattern = re.compile(r'VMFG_(\d{4})(\d{2})(\d{2})\.H\d{2}')

    if not os.path.isdir(vmf_base_dir):
        return []

    for filename in os.listdir(vmf_base_dir):
        match = pattern.match(filename)
        if match:
            year = int(match.group(1))
            month = int(match.group(2))
            day = int(match.group(3))
            dates.add((year, month, day))

    return sorted(dates)


if __name__ == "__main__":
    # Test the parser
    import sys

    vmf_dir = "/home/ahunegnaw/GPSDATA/DATAPOOL_52/VMF1"

    # List available dates
    dates = list_available_vmf_dates(vmf_dir)
    print(f"Available VMF dates: {len(dates)}")
    if dates:
        print(f"Date range: {dates[0]} to {dates[-1]}")

    # Test parsing a file
    test_file = f"{vmf_dir}/VMFG_20170801.H12.gz"
    if os.path.exists(test_file):
        print(f"\nParsing: {test_file}")
        epoch = parse_vmf_grid_file(test_file)
        if epoch:
            print(f"Timestamp: {epoch.timestamp}")
            print(f"Grid points: {len(epoch.grid_points)}")
            print(f"Lat range: {epoch.lat_range}")
            print(f"Lon range: {epoch.lon_range}")

            # Test interpolation for Zimmerwald, Switzerland
            lat, lon = 46.877, 7.465
            result = epoch.get_ztd_at_position(lat, lon)
            if result:
                ztd, zhd, zwd = result
                print(f"\nZTD at Zimmerwald ({lat}, {lon}):")
                print(f"  ZHD: {zhd:.4f} m ({zhd*1000:.1f} mm)")
                print(f"  ZWD: {zwd:.4f} m ({zwd*1000:.1f} mm)")
                print(f"  ZTD: {ztd:.4f} m ({ztd*1000:.1f} mm)")
