"""
CODE (Center for Orbit Determination in Europe) Coordinate Parser

Fetches and parses daily coordinate solutions from CODE's FTP server:
ftp://ftp.aiub.unibe.ch/CODE/{year}/

Coordinate files available:
- CODwwwwd.CRD.Z - Daily coordinates in Bernese CRD format
- CODwwwwd.SNX.Z - Daily SINEX files

Author: Addisu Hunegnaw
Date: January 2026
"""

import os
import gzip
import ftplib
import tempfile
from datetime import datetime, timedelta
from dataclasses import dataclass
from typing import Optional, Dict, List
from io import BytesIO
import math


# CODE FTP server
CODE_FTP_HOST = "ftp.aiub.unibe.ch"
CODE_FTP_BASE = "/CODE"


@dataclass
class CODECoordinate:
    """Single station coordinate from CODE solution"""
    station: str
    date: datetime
    gps_week: int
    day_of_week: int
    x: float
    y: float
    z: float
    x_sigma: Optional[float] = None
    y_sigma: Optional[float] = None
    z_sigma: Optional[float] = None
    flag: str = ""


def gps_week_from_date(date: datetime) -> tuple:
    """
    Convert date to GPS week and day of week.
    GPS week 0 started on January 6, 1980.
    """
    gps_epoch = datetime(1980, 1, 6)
    delta = date - gps_epoch
    gps_week = delta.days // 7
    day_of_week = delta.days % 7
    return gps_week, day_of_week


def date_from_gps_week(gps_week: int, day_of_week: int) -> datetime:
    """Convert GPS week and day of week to date."""
    gps_epoch = datetime(1980, 1, 6)
    return gps_epoch + timedelta(days=gps_week * 7 + day_of_week)


def fetch_code_crd_file(year: int, gps_week: int, day_of_week: int,
                         timeout: int = 30) -> Optional[str]:
    """
    Fetch CODE CRD file from FTP server.

    Args:
        year: Year
        gps_week: GPS week number
        day_of_week: Day of week (0=Sunday)
        timeout: Connection timeout

    Returns:
        File content as string or None if failed
    """
    filename = f"COD{gps_week}{day_of_week}.CRD.Z"
    ftp_path = f"{CODE_FTP_BASE}/{year}/{filename}"

    try:
        ftp = ftplib.FTP(CODE_FTP_HOST, timeout=timeout)
        ftp.login()  # Anonymous login

        # Download to memory
        buffer = BytesIO()
        ftp.retrbinary(f"RETR {ftp_path}", buffer.write)
        ftp.quit()

        # Decompress
        buffer.seek(0)
        import zlib
        # .Z files are compress format, try different decompression
        try:
            # Try gzip first
            content = gzip.decompress(buffer.read()).decode('utf-8', errors='replace')
        except:
            # Try raw deflate
            buffer.seek(0)
            try:
                # Unix compress format - use unlzw or similar
                # For now, return None and try SNX format instead
                return None
            except:
                return None

        return content

    except Exception as e:
        print(f"Error fetching CODE CRD {filename}: {e}")
        return None


def fetch_code_snx_file(year: int, gps_week: int, day_of_week: int,
                        timeout: int = 30) -> Optional[str]:
    """
    Fetch CODE SINEX file from FTP server.

    Args:
        year: Year
        gps_week: GPS week number
        day_of_week: Day of week (0=Sunday)
        timeout: Connection timeout

    Returns:
        File content as string or None if failed
    """
    filename = f"COD{gps_week}{day_of_week}.SNX.Z"
    ftp_path = f"{CODE_FTP_BASE}/{year}/{filename}"

    try:
        ftp = ftplib.FTP(CODE_FTP_HOST, timeout=timeout)
        ftp.login()

        buffer = BytesIO()
        ftp.retrbinary(f"RETR {ftp_path}", buffer.write)
        ftp.quit()

        buffer.seek(0)
        compressed_data = buffer.read()

        # Try to decompress - CODE uses Unix compress (.Z)
        # We need to handle this specially
        try:
            import lzw  # If available
            content = lzw.decompress(compressed_data).decode('utf-8', errors='replace')
        except ImportError:
            # Try subprocess with uncompress
            import subprocess
            with tempfile.NamedTemporaryFile(suffix='.Z', delete=False) as tmp:
                tmp.write(compressed_data)
                tmp_path = tmp.name

            try:
                # Use gzip -d or uncompress
                result = subprocess.run(['gzip', '-d', '-f', '-k', tmp_path],
                                       capture_output=True, timeout=10)
                uncompressed_path = tmp_path[:-2]  # Remove .Z
                if os.path.exists(uncompressed_path):
                    with open(uncompressed_path, 'r', errors='replace') as f:
                        content = f.read()
                    os.unlink(uncompressed_path)
                else:
                    content = None
            except:
                content = None
            finally:
                if os.path.exists(tmp_path):
                    os.unlink(tmp_path)

        return content

    except Exception as e:
        print(f"Error fetching CODE SNX {filename}: {e}")
        return None


def parse_sinex_coordinates(content: str, station: str) -> Optional[Dict]:
    """
    Parse SINEX file to extract coordinates for a station.

    Args:
        content: SINEX file content
        station: 4-character station code

    Returns:
        Dictionary with X, Y, Z coordinates or None
    """
    if not content:
        return None

    station = station.upper()

    # Find SOLUTION/ESTIMATE block
    in_estimate = False
    coords = {}

    for line in content.split('\n'):
        if '+SOLUTION/ESTIMATE' in line:
            in_estimate = True
            continue
        if '-SOLUTION/ESTIMATE' in line:
            in_estimate = False
            continue

        if in_estimate and station in line:
            parts = line.split()
            if len(parts) >= 9:
                try:
                    param_type = parts[1]  # STAX, STAY, STAZ
                    sta_name = parts[2][:4].upper()

                    if sta_name == station:
                        value = float(parts[8])

                        if 'STAX' in param_type:
                            coords['X'] = value
                        elif 'STAY' in param_type:
                            coords['Y'] = value
                        elif 'STAZ' in param_type:
                            coords['Z'] = value
                except (ValueError, IndexError):
                    continue

    if len(coords) == 3:
        return coords
    return None


def parse_crd_coordinates(content: str, station: str) -> Optional[Dict]:
    """
    Parse Bernese CRD file to extract coordinates for a station.

    Args:
        content: CRD file content
        station: 4-character station code

    Returns:
        Dictionary with X, Y, Z coordinates or None
    """
    if not content:
        return None

    station = station.upper()

    for line in content.split('\n'):
        if station in line.upper():
            parts = line.split()
            if len(parts) >= 5:
                try:
                    # Find 3 consecutive floats for XYZ
                    floats = []
                    for p in parts:
                        try:
                            val = float(p)
                            # Check if it could be a coordinate (large number)
                            if abs(val) > 1000000:  # Likely ECEF coordinate
                                floats.append(val)
                            elif len(floats) > 0:  # Already started collecting
                                floats.append(val)
                            if len(floats) == 3:
                                break
                        except ValueError:
                            if len(floats) > 0 and len(floats) < 3:
                                floats = []  # Reset if interrupted
                            continue

                    if len(floats) == 3:
                        # Verify it's a valid Earth coordinate
                        magnitude = math.sqrt(sum(v**2 for v in floats))
                        if 6000000 < magnitude < 6800000:
                            return {'X': floats[0], 'Y': floats[1], 'Z': floats[2]}
                except:
                    continue

    return None


def fetch_code_coordinates(station: str, year: int, doy: int,
                          timeout: int = 30) -> Optional[CODECoordinate]:
    """
    Fetch CODE coordinates for a station on a specific day.

    Args:
        station: 4-character station code
        year: Year
        doy: Day of year
        timeout: Connection timeout

    Returns:
        CODECoordinate object or None
    """
    # Convert DOY to date and GPS week
    date = datetime(year, 1, 1) + timedelta(days=doy - 1)
    gps_week, dow = gps_week_from_date(date)

    # Try CRD file first (simpler format)
    content = fetch_code_crd_file(year, gps_week, dow, timeout)
    if content:
        coords = parse_crd_coordinates(content, station)
        if coords:
            return CODECoordinate(
                station=station.upper(),
                date=date,
                gps_week=gps_week,
                day_of_week=dow,
                x=coords['X'],
                y=coords['Y'],
                z=coords['Z']
            )

    # Try SINEX file as fallback
    content = fetch_code_snx_file(year, gps_week, dow, timeout)
    if content:
        coords = parse_sinex_coordinates(content, station)
        if coords:
            return CODECoordinate(
                station=station.upper(),
                date=date,
                gps_week=gps_week,
                day_of_week=dow,
                x=coords['X'],
                y=coords['Y'],
                z=coords['Z']
            )

    return None


def fetch_code_coordinates_range(station: str, year: int,
                                  start_doy: int, end_doy: int,
                                  timeout: int = 30,
                                  progress_callback=None) -> List[CODECoordinate]:
    """
    Fetch CODE coordinates for a range of days.

    Args:
        station: 4-character station code
        year: Year
        start_doy: Starting day of year
        end_doy: Ending day of year
        timeout: Connection timeout per file
        progress_callback: Optional callback function(current, total)

    Returns:
        List of CODECoordinate objects
    """
    results = []
    total = end_doy - start_doy + 1

    for i, doy in enumerate(range(start_doy, end_doy + 1)):
        if progress_callback:
            progress_callback(i + 1, total)

        coord = fetch_code_coordinates(station, year, doy, timeout)
        if coord:
            results.append(coord)

    return results


# Alternative: Use HTTP if FTP is blocked
CODE_HTTP_BASE = "http://ftp.aiub.unibe.ch/CODE"


def fetch_code_coordinates_http(station: str, year: int, doy: int,
                                timeout: int = 30) -> Optional[CODECoordinate]:
    """
    Fetch CODE coordinates via HTTP from AIUB server.

    Uses IGS 3.0 long filename convention:
    COD0OPSFIN_YYYYDDD0000_01D_01D_SOL.SNX.gz

    Args:
        station: 4-character station code
        year: Year
        doy: Day of year
        timeout: Request timeout

    Returns:
        CODECoordinate object or None
    """
    import requests

    date = datetime(year, 1, 1) + timedelta(days=doy - 1)
    gps_week, dow = gps_week_from_date(date)

    # Try different file formats - IGS 3.0 long filename convention first
    filenames = [
        # IGS 3.0 format: COD0OPSFIN_YYYYDDD0000_01D_01D_SOL.SNX.gz
        (f"COD0OPSFIN_{year}{doy:03d}0000_01D_01D_SOL.SNX.gz", 'gz', 'SNX'),
        # Legacy format backup
        (f"COD{gps_week}{dow}.SNX.Z", 'Z', 'SNX'),
        (f"COD{gps_week}{dow}.CRD.Z", 'Z', 'CRD'),
    ]

    for filename, compression, filetype in filenames:
        url = f"{CODE_HTTP_BASE}/{year}/{filename}"

        try:
            response = requests.get(url, timeout=timeout)
            if response.status_code == 200:
                # Decompress based on compression type
                import tempfile

                suffix = '.gz' if compression == 'gz' else '.Z'
                with tempfile.NamedTemporaryFile(suffix=suffix, delete=False) as tmp:
                    tmp.write(response.content)
                    tmp_path = tmp.name

                try:
                    if compression == 'gz':
                        # Handle gzip compression
                        import gzip as gz_module
                        with gz_module.open(tmp_path, 'rt', errors='replace') as f:
                            content = f.read()
                    else:
                        # Handle .Z compression
                        import subprocess
                        subprocess.run(['gzip', '-d', '-f', tmp_path],
                                      capture_output=True, timeout=10)
                        uncompressed_path = tmp_path[:-2]
                        if os.path.exists(uncompressed_path):
                            with open(uncompressed_path, 'r', errors='replace') as f:
                                content = f.read()
                            os.unlink(uncompressed_path)
                        else:
                            continue

                    # Parse based on file type
                    if filetype == 'CRD':
                        coords = parse_crd_coordinates(content, station)
                    else:
                        coords = parse_sinex_coordinates(content, station)

                    if coords:
                        return CODECoordinate(
                            station=station.upper(),
                            date=date,
                            gps_week=gps_week,
                            day_of_week=dow,
                            x=coords['X'],
                            y=coords['Y'],
                            z=coords['Z']
                        )
                except Exception:
                    pass
                finally:
                    for p in [tmp_path, tmp_path[:-2] if not tmp_path.endswith('.gz') else tmp_path[:-3]]:
                        if os.path.exists(p):
                            try:
                                os.unlink(p)
                            except:
                                pass

        except Exception:
            continue

    return None


if __name__ == '__main__':
    import sys

    station = sys.argv[1] if len(sys.argv) > 1 else "BRUX"
    year = int(sys.argv[2]) if len(sys.argv) > 2 else 2025
    doy = int(sys.argv[3]) if len(sys.argv) > 3 else 300

    print(f"Fetching CODE coordinates for {station} on {year}/{doy:03d}...")

    # Try HTTP method
    coord = fetch_code_coordinates_http(station, year, doy)

    if coord:
        print(f"\nStation: {coord.station}")
        print(f"Date: {coord.date.strftime('%Y-%m-%d')}")
        print(f"GPS Week: {coord.gps_week}, DOW: {coord.day_of_week}")
        print(f"X: {coord.x:.5f} m")
        print(f"Y: {coord.y:.5f} m")
        print(f"Z: {coord.z:.5f} m")
    else:
        print("Failed to fetch coordinates")
