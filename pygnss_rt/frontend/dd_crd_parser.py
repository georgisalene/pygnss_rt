"""
Double Difference (DD) CRD File Parser

Parses Bernese-style CRD coordinate files from DD processing.
Example file: F10_20253030.CRD.gz

File structure:
- Header lines (title, datum, epoch)
- Station coordinates in XYZ

Data format:
NUM  STATION NAME           X (M)          Y (M)          Z (M)     FLAG     SYSTEM
  1  0531              4084902.65845   443391.87657  4862653.53396    W      GRE
"""

import os
import gzip
import math
from pathlib import Path
from dataclasses import dataclass
from typing import Optional


@dataclass
class DDStationCoord:
    """DD station coordinates from Bernese CRD file"""
    station_4char: str   # 4-char station code (e.g., ZIM2)
    year: int            # Full year (e.g., 2025)
    doy: int             # Day of year
    x: float             # X coordinate in meters
    y: float             # Y coordinate in meters
    z: float             # Z coordinate in meters
    flag: str            # Flag (W = weighted)
    system: str          # Constellation system (GRE, GR, G, etc.)

    @property
    def lat_lon_height(self) -> tuple[float, float, float]:
        """Convert XYZ to geodetic lat/lon/height (WGS84)"""
        # WGS84 parameters
        a = 6378137.0  # semi-major axis
        f = 1/298.257223563  # flattening
        b = a * (1 - f)  # semi-minor axis
        e2 = (a**2 - b**2) / a**2  # first eccentricity squared

        # Calculate longitude
        lon = math.atan2(self.y, self.x)

        # Iterative calculation for latitude and height
        p = math.sqrt(self.x**2 + self.y**2)
        lat = math.atan2(self.z, p * (1 - e2))  # initial guess

        for _ in range(10):  # iterate for convergence
            N = a / math.sqrt(1 - e2 * math.sin(lat)**2)
            h = p / math.cos(lat) - N
            lat = math.atan2(self.z, p * (1 - e2 * N / (N + h)))

        N = a / math.sqrt(1 - e2 * math.sin(lat)**2)
        h = p / math.cos(lat) - N

        return math.degrees(lat), math.degrees(lon), h


def parse_dd_crd_file(filepath: str, year: int, doy: int) -> list[DDStationCoord]:
    """
    Parse a DD CRD coordinate file.

    The file can have mixed formats:
    - Local stations: NUM STATION X Y Z FLAG SYSTEM
    - IGS stations with DOMES: NUM STATION DOMES X Y Z

    Args:
        filepath: Path to the CRD file (can be .gz compressed)
        year: Year of the solution
        doy: Day of year

    Returns:
        List of DDStationCoord with station coordinates
    """
    coords = []

    # Handle gzip compressed files
    if filepath.endswith('.gz'):
        open_func = lambda f: gzip.open(f, 'rt')
    else:
        open_func = lambda f: open(f, 'r')

    with open_func(filepath) as f:
        in_data_section = False
        for line in f:
            line = line.rstrip('\n')

            # Skip empty lines
            if not line.strip():
                continue

            # Look for header line that indicates data section start
            if line.strip().startswith('NUM') and 'STATION NAME' in line:
                in_data_section = True
                continue

            # Parse data lines
            if in_data_section:
                # Handle mixed format - some have DOMES numbers, some have FLAG/SYSTEM
                parts = line.split()
                if len(parts) >= 5:
                    try:
                        # Station name is always the second field (take first 4 chars)
                        station = parts[1][:4].strip()

                        # Find X, Y, Z coordinates by looking for large floats
                        # Coordinates are ~10^6 meters (Earth radius scale)
                        xyz_values = []
                        flag = 'W'
                        system = 'GRE'

                        for i, p in enumerate(parts[2:], start=2):
                            try:
                                val = float(p)
                                # Earth coordinates are millions of meters
                                if abs(val) > 100000:
                                    xyz_values.append(val)
                                    if len(xyz_values) == 3:
                                        # Check if following fields are FLAG/SYSTEM
                                        if len(parts) > i + 1:
                                            next_val = parts[i + 1]
                                            if next_val in ['A', 'W', 'F', 'P']:
                                                flag = next_val
                                                if len(parts) > i + 2:
                                                    system = parts[i + 2]
                                        break
                            except ValueError:
                                continue

                        if len(xyz_values) == 3:
                            coords.append(DDStationCoord(
                                station_4char=station,
                                year=year,
                                doy=doy,
                                x=xyz_values[0],
                                y=xyz_values[1],
                                z=xyz_values[2],
                                flag=flag,
                                system=system
                            ))
                    except (ValueError, IndexError):
                        continue

    return coords


def get_dd_crd_path(base_dir: str, year: int, doy: int,
                    prefix: str = 'F10') -> Optional[str]:
    """
    Get the path to a DD CRD file.

    Args:
        base_dir: Base directory for DD results
        year: Year
        doy: Day of year
        prefix: File prefix (F10 for final, R10 for rapid, P1 for preliminary)

    Returns:
        Path to CRD file if it exists, None otherwise
    """
    # Try different path patterns (both compressed and uncompressed)
    patterns = [
        # Compressed files
        f"{base_dir}/{year}/{doy}/STA/{prefix}_{year}{doy:03d}0.CRD.gz",
        f"{base_dir}/{year}/{doy:03d}/STA/{prefix}_{year}{doy:03d}0.CRD.gz",
        # Uncompressed files
        f"{base_dir}/{year}/{doy}/STA/{prefix}_{year}{doy:03d}0.CRD",
        f"{base_dir}/{year}/{doy:03d}/STA/{prefix}_{year}{doy:03d}0.CRD",
    ]

    for path in patterns:
        if os.path.exists(path):
            return path

    return None


def get_dd_coords_for_station(station_4char: str,
                               dd_coords: list[DDStationCoord]
                               ) -> Optional[DDStationCoord]:
    """
    Find DD coordinates for a specific station.

    Args:
        station_4char: 4-character station code (case-insensitive)
        dd_coords: List of DD coordinates

    Returns:
        DDStationCoord if found, None otherwise
    """
    station_upper = station_4char.upper()
    for coord in dd_coords:
        if coord.station_4char.upper() == station_upper:
            return coord
    return None


def compute_enu_diff_dd(local_x: float, local_y: float, local_z: float,
                        dd_coord: DDStationCoord) -> tuple[float, float, float]:
    """
    Compute ENU differences between local PPP and DD coordinates.

    Args:
        local_x, local_y, local_z: Local PPP coordinates in meters
        dd_coord: DD coordinate

    Returns:
        Tuple of (dE, dN, dU) differences in meters
    """
    # Get reference position for transformation (use DD as reference)
    lat, lon, _ = dd_coord.lat_lon_height
    lat_rad = math.radians(lat)
    lon_rad = math.radians(lon)

    # Compute XYZ differences (Local - DD)
    dx = local_x - dd_coord.x
    dy = local_y - dd_coord.y
    dz = local_z - dd_coord.z

    # Transform to ENU using rotation matrix
    sin_lat = math.sin(lat_rad)
    cos_lat = math.cos(lat_rad)
    sin_lon = math.sin(lon_rad)
    cos_lon = math.cos(lon_rad)

    dE = -sin_lon * dx + cos_lon * dy
    dN = -sin_lat * cos_lon * dx - sin_lat * sin_lon * dy + cos_lat * dz
    dU = cos_lat * cos_lon * dx + cos_lat * sin_lon * dy + sin_lat * dz

    return dE, dN, dU
