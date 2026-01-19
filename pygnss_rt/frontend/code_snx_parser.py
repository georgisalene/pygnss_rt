"""
CODE SINEX Solution File Parser

Parses CODE final coordinate solutions in SINEX format.
Example file: COD0OPSFIN_20252420000_01D_01D_SOL.SNX

File structure:
- +SOLUTION/ESTIMATE: Station coordinates (STAX, STAY, STAZ)

Data format (SOLUTION/ESTIMATE):
*INDEX TYPE__ CODE PT SOLN _REF_EPOCH__ UNIT S __ESTIMATED VALUE____ _STD_DEV___
    1 STAX   ZIM2  A    1 25:242:43200 m    2  4331296.89932        0.00045
    2 STAY   ZIM2  A    1 25:242:43200 m    2   567556.01824        0.00019
    3 STAZ   ZIM2  A    1 25:242:43200 m    2  4633134.04387        0.00045

Coordinates are in ITRF, epoch is YY:DOY:SSSSS (seconds of day).
"""

import os
import gzip
import urllib.request
from pathlib import Path
from dataclasses import dataclass
from typing import Optional
import math


@dataclass
class CODEStationCoord:
    """CODE station coordinates from SINEX solution"""
    station_4char: str   # 4-char station code (e.g., ZIM2)
    year: int            # Full year (e.g., 2025)
    doy: int             # Day of year
    x: float             # X coordinate in meters
    y: float             # Y coordinate in meters
    z: float             # Z coordinate in meters
    x_sigma: float       # X sigma in meters
    y_sigma: float       # Y sigma in meters
    z_sigma: float       # Z sigma in meters

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


def parse_code_snx_file(filepath: str) -> list[CODEStationCoord]:
    """
    Parse a CODE SINEX solution file.

    Args:
        filepath: Path to the CODE SNX file

    Returns:
        List of CODEStationCoord with station coordinates
    """
    # Temporary storage for coordinate components
    # Key: (station, pt, soln), Value: dict with x, y, z, sigmas
    coord_data = {}

    in_estimate_block = False

    with open(filepath, 'r') as f:
        for line in f:
            line = line.rstrip('\n')

            # Track block boundaries
            if line.startswith('+SOLUTION/ESTIMATE'):
                in_estimate_block = True
                continue
            elif line.startswith('-SOLUTION/ESTIMATE'):
                in_estimate_block = False
                continue

            # Skip header lines and comments
            if not in_estimate_block or line.startswith('*') or len(line) < 60:
                continue

            # Parse SOLUTION/ESTIMATE block
            # Format: INDEX TYPE__ CODE PT SOLN _REF_EPOCH__ UNIT S __ESTIMATED VALUE____ _STD_DEV___
            # Example:     1 STAX   ZIM2  A    1 25:242:43200 m    2  4331296.89932        0.00045
            try:
                # Fixed column positions based on SINEX format
                param_type = line[7:13].strip()   # TYPE (STAX, STAY, STAZ)
                station = line[14:18].strip()     # CODE (4-char)
                pt = line[19:21].strip()          # PT
                soln = line[22:26].strip()        # SOLN
                epoch_str = line[27:39].strip()   # REF_EPOCH (YY:DOY:SSSSS)

                # Parse epoch
                epoch_parts = epoch_str.split(':')
                if len(epoch_parts) == 3:
                    yy = int(epoch_parts[0])
                    year = 2000 + yy if yy < 80 else 1900 + yy
                    doy = int(epoch_parts[1])
                else:
                    continue

                # Parse value and sigma (they're at the end of the line)
                # Value starts around column 47, sigma around column 68
                parts = line[46:].split()
                if len(parts) >= 2:
                    value = float(parts[0])
                    sigma = float(parts[1])
                else:
                    continue

                # Only process coordinate parameters
                if param_type not in ('STAX', 'STAY', 'STAZ'):
                    continue

                # Create key for this station/point/solution
                key = (station, pt, soln)

                if key not in coord_data:
                    coord_data[key] = {
                        'station': station,
                        'year': year,
                        'doy': doy,
                        'x': None, 'y': None, 'z': None,
                        'x_sigma': None, 'y_sigma': None, 'z_sigma': None
                    }

                # Store the coordinate component
                if param_type == 'STAX':
                    coord_data[key]['x'] = value
                    coord_data[key]['x_sigma'] = sigma
                elif param_type == 'STAY':
                    coord_data[key]['y'] = value
                    coord_data[key]['y_sigma'] = sigma
                elif param_type == 'STAZ':
                    coord_data[key]['z'] = value
                    coord_data[key]['z_sigma'] = sigma

            except (ValueError, IndexError) as e:
                continue

    # Convert to list of CODEStationCoord objects
    coords = []
    for key, data in coord_data.items():
        # Only include if we have all three coordinates
        if all(data[c] is not None for c in ['x', 'y', 'z']):
            coords.append(CODEStationCoord(
                station_4char=data['station'],
                year=data['year'],
                doy=data['doy'],
                x=data['x'],
                y=data['y'],
                z=data['z'],
                x_sigma=data['x_sigma'] or 0.0,
                y_sigma=data['y_sigma'] or 0.0,
                z_sigma=data['z_sigma'] or 0.0
            ))

    return coords


def download_code_snx(year: int, doy: int,
                      output_dir: str = "/tmp/code_products",
                      product_type: str = "FIN") -> Optional[str]:
    """
    Download CODE SINEX solution file from FTP.

    Args:
        year: Year (e.g., 2025)
        doy: Day of year (1-366)
        output_dir: Directory to save downloaded file
        product_type: Product type - "FIN" (final), "RAP" (rapid)

    Returns:
        Path to downloaded file, or None if download failed

    Example URL:
        http://ftp.aiub.unibe.ch/CODE/2025/COD0OPSFIN_20252420000_01D_01D_SOL.SNX.gz
    """
    os.makedirs(output_dir, exist_ok=True)

    # Construct filename
    # Format: COD0OPS{TYPE}_YYYYDDD0000_01D_01D_SOL.SNX
    filename = f"COD0OPS{product_type}_{year}{doy:03d}0000_01D_01D_SOL.SNX"
    gz_filename = filename + ".gz"

    # Try compressed first, then uncompressed
    base_url = f"http://ftp.aiub.unibe.ch/CODE/{year}/"

    local_path = os.path.join(output_dir, filename)

    # Check if already downloaded
    if os.path.exists(local_path):
        return local_path

    # Try downloading compressed version
    try:
        gz_url = base_url + gz_filename
        gz_local = os.path.join(output_dir, gz_filename)

        print(f"Downloading {gz_url}...")
        urllib.request.urlretrieve(gz_url, gz_local)

        # Decompress
        with gzip.open(gz_local, 'rb') as f_in:
            with open(local_path, 'wb') as f_out:
                f_out.write(f_in.read())

        # Remove compressed file
        os.remove(gz_local)
        print(f"Downloaded and extracted: {local_path}")
        return local_path

    except Exception as e:
        print(f"Failed to download compressed file: {e}")

        # Try uncompressed
        try:
            url = base_url + filename
            print(f"Trying uncompressed: {url}...")
            urllib.request.urlretrieve(url, local_path)
            print(f"Downloaded: {local_path}")
            return local_path
        except Exception as e2:
            print(f"Failed to download: {e2}")
            return None


def get_code_coords_for_station(coords: list[CODEStationCoord],
                                station_4char: str) -> Optional[CODEStationCoord]:
    """
    Get CODE coordinates for a specific station.

    Args:
        coords: List of CODEStationCoord from parsed file
        station_4char: 4-character station code (e.g., "BRUX")

    Returns:
        CODEStationCoord for the station, or None if not found
    """
    station_upper = station_4char.upper()

    for coord in coords:
        if coord.station_4char == station_upper:
            return coord

    return None


def compute_coordinate_diff(local_x: float, local_y: float, local_z: float,
                           code_coord: CODEStationCoord) -> tuple[float, float, float]:
    """
    Compute difference between local and CODE coordinates in mm.

    Returns:
        Tuple of (dX, dY, dZ) in mm
    """
    dx = (local_x - code_coord.x) * 1000  # mm
    dy = (local_y - code_coord.y) * 1000  # mm
    dz = (local_z - code_coord.z) * 1000  # mm
    return dx, dy, dz


def compute_enu_diff(local_x: float, local_y: float, local_z: float,
                     code_coord: CODEStationCoord) -> tuple[float, float, float]:
    """
    Compute difference in local ENU (East, North, Up) coordinates in mm.

    Args:
        local_x, local_y, local_z: Local coordinates in meters
        code_coord: CODE reference coordinates

    Returns:
        Tuple of (dE, dN, dU) in mm
    """
    # Get reference lat/lon from CODE coordinates
    lat, lon, _ = code_coord.lat_lon_height
    lat_rad = math.radians(lat)
    lon_rad = math.radians(lon)

    # XYZ differences in meters
    dx = local_x - code_coord.x
    dy = local_y - code_coord.y
    dz = local_z - code_coord.z

    # Rotation matrix from XYZ to ENU
    # E = -sin(lon)*dX + cos(lon)*dY
    # N = -sin(lat)*cos(lon)*dX - sin(lat)*sin(lon)*dY + cos(lat)*dZ
    # U = cos(lat)*cos(lon)*dX + cos(lat)*sin(lon)*dY + sin(lat)*dZ

    sin_lat = math.sin(lat_rad)
    cos_lat = math.cos(lat_rad)
    sin_lon = math.sin(lon_rad)
    cos_lon = math.cos(lon_rad)

    dE = -sin_lon * dx + cos_lon * dy
    dN = -sin_lat * cos_lon * dx - sin_lat * sin_lon * dy + cos_lat * dz
    dU = cos_lat * cos_lon * dx + cos_lat * sin_lon * dy + sin_lat * dz

    # Convert to mm
    return dE * 1000, dN * 1000, dU * 1000


if __name__ == "__main__":
    # Test with example file
    import sys

    if len(sys.argv) > 1:
        filepath = sys.argv[1]
    else:
        # Try to download
        print("Attempting to download CODE SNX for DOY 242/2025...")
        filepath = download_code_snx(2025, 242)

    if filepath and os.path.exists(filepath):
        print(f"Parsing {filepath}...")
        coords = parse_code_snx_file(filepath)

        print(f"\nFound {len(coords)} stations with coordinates")

        # Show first few stations
        print("\nFirst 10 stations:")
        for coord in coords[:10]:
            lat, lon, h = coord.lat_lon_height
            print(f"  {coord.station_4char}: X={coord.x:.3f} Y={coord.y:.3f} Z={coord.z:.3f}")
            print(f"           Lat={lat:.6f}° Lon={lon:.6f}° H={h:.2f}m")

        # Test station lookup
        zim2 = get_code_coords_for_station(coords, "ZIM2")
        if zim2:
            print(f"\nZIM2 coordinates:")
            print(f"  X={zim2.x:.5f} ± {zim2.x_sigma:.5f} m")
            print(f"  Y={zim2.y:.5f} ± {zim2.y_sigma:.5f} m")
            print(f"  Z={zim2.z:.5f} ± {zim2.z_sigma:.5f} m")
    else:
        print(f"File not found or download failed")
