"""
CODE Troposphere SINEX File Parser

Parses CODE final troposphere products in SINEX TRO format.
Example file: COD0OPSFIN_20252420000_01D_01H_TRO.TRO

File structure:
- +SITE/ID: Station metadata (ID, coordinates)
- +TROP/SOLUTION: Troposphere estimates (ZTD, gradients)

Data format (TROP/SOLUTION):
STATION__ _____EPOCH____ TROTOT STDDEV  TGNTOT STDDEV  TGETOT STDDEV
ABMF00GLP 2025:242:00000 2579.4    1.4   0.053  0.055  -0.485  0.062

Values are in mm, epoch is YYYY:DOY:SSSSSS (seconds of day).
"""

import re
from pathlib import Path
from dataclasses import dataclass
from typing import Optional
import urllib.request
import gzip
import os
from datetime import datetime


@dataclass
class CODETropoEntry:
    """Single CODE troposphere estimate"""
    station_id: str      # 9-char station ID (e.g., ABMF00GLP)
    station_4char: str   # 4-char station code (e.g., ABMF)
    year: int
    doy: int
    hour: int            # Hour of day (0-23)
    seconds_of_day: int  # Seconds since midnight
    ztd: float           # ZTD in meters
    ztd_sigma: float     # ZTD sigma in meters
    grad_n: float        # North gradient in meters
    grad_n_sigma: float  # North gradient sigma in meters
    grad_e: float        # East gradient in meters
    grad_e_sigma: float  # East gradient sigma in meters


@dataclass
class CODESiteInfo:
    """CODE station metadata"""
    station_id: str      # 9-char station ID
    station_4char: str   # 4-char code
    domes: str           # DOMES number
    longitude: float     # degrees
    latitude: float      # degrees
    height: float        # meters


def parse_code_tro_file(filepath: str) -> tuple[list[CODESiteInfo], list[CODETropoEntry]]:
    """
    Parse a CODE TRO SINEX file.

    Args:
        filepath: Path to the CODE TRO file

    Returns:
        Tuple of (site_info_list, tropo_entries_list)
    """
    sites = []
    entries = []

    in_site_block = False
    in_solution_block = False

    with open(filepath, 'r') as f:
        for line in f:
            line = line.rstrip('\n')

            # Track block boundaries
            if line.startswith('+SITE/ID'):
                in_site_block = True
                continue
            elif line.startswith('-SITE/ID'):
                in_site_block = False
                continue
            elif line.startswith('+TROP/SOLUTION'):
                in_solution_block = True
                continue
            elif line.startswith('-TROP/SOLUTION'):
                in_solution_block = False
                continue

            # Parse SITE/ID block
            # Format: STATION__ PT __DOMES__ T _STATION_DESCRIPTION__ _LONGITUDE _LATITUDE_ _HGT_ELI_ _HGT_MSL_
            # Example: ABMF00GLP  A 97103M001 P                        298.472465  16.262308   -25.572
            if in_site_block and line.startswith(' ') and len(line) > 50:
                try:
                    station_id = line[1:10].strip()
                    domes = line[14:23].strip()
                    # Parse coordinates - they start around position 43
                    parts = line[43:].split()
                    if len(parts) >= 3:
                        lon = float(parts[0])
                        lat = float(parts[1])
                        height = float(parts[2])

                        sites.append(CODESiteInfo(
                            station_id=station_id,
                            station_4char=station_id[:4].upper(),
                            domes=domes,
                            longitude=lon if lon <= 180 else lon - 360,  # Convert to -180/+180
                            latitude=lat,
                            height=height
                        ))
                except (ValueError, IndexError):
                    continue

            # Parse TROP/SOLUTION block
            # Format: STATION__ _____EPOCH____ TROTOT STDDEV  TGNTOT STDDEV  TGETOT STDDEV
            # Example: ABMF00GLP 2025:242:00000 2579.4    1.4   0.053  0.055  -0.485  0.062
            if in_solution_block and line.startswith(' ') and len(line) > 60:
                try:
                    station_id = line[1:10].strip()

                    # Parse epoch: YYYY:DOY:SSSSS
                    epoch_str = line[11:25].strip()
                    epoch_parts = epoch_str.split(':')
                    if len(epoch_parts) == 3:
                        year = int(epoch_parts[0])
                        doy = int(epoch_parts[1])
                        seconds_of_day = int(epoch_parts[2])
                        hour = seconds_of_day // 3600

                        # Parse values - fixed column positions
                        # TROTOT starts around col 26, values are space-separated
                        values_str = line[25:].split()
                        if len(values_str) >= 6:
                            # Values are in mm, convert to meters
                            ztd = float(values_str[0]) / 1000.0
                            ztd_sigma = float(values_str[1]) / 1000.0
                            grad_n = float(values_str[2]) / 1000.0
                            grad_n_sigma = float(values_str[3]) / 1000.0
                            grad_e = float(values_str[4]) / 1000.0
                            grad_e_sigma = float(values_str[5]) / 1000.0

                            entries.append(CODETropoEntry(
                                station_id=station_id,
                                station_4char=station_id[:4].upper(),
                                year=year,
                                doy=doy,
                                hour=hour,
                                seconds_of_day=seconds_of_day,
                                ztd=ztd,
                                ztd_sigma=ztd_sigma,
                                grad_n=grad_n,
                                grad_n_sigma=grad_n_sigma,
                                grad_e=grad_e,
                                grad_e_sigma=grad_e_sigma
                            ))
                except (ValueError, IndexError):
                    continue

    return sites, entries


def download_code_tro(year: int, doy: int,
                      output_dir: str = "/tmp/code_products",
                      product_type: str = "FIN") -> Optional[str]:
    """
    Download CODE troposphere product from FTP.

    Args:
        year: Year (e.g., 2025)
        doy: Day of year (1-366)
        output_dir: Directory to save downloaded file
        product_type: Product type - "FIN" (final), "RAP" (rapid), "ULT" (ultra-rapid)

    Returns:
        Path to downloaded file, or None if download failed

    Example URL:
        http://ftp.aiub.unibe.ch/CODE/2025/COD0OPSFIN_20252420000_01D_01H_TRO.TRO.gz
    """
    os.makedirs(output_dir, exist_ok=True)

    # Construct filename
    # Format: COD0OPS{TYPE}_YYYYDDD0000_01D_01H_TRO.TRO
    filename = f"COD0OPS{product_type}_{year}{doy:03d}0000_01D_01H_TRO.TRO"
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


def get_code_ztd_for_station(entries: list[CODETropoEntry],
                            station_4char: str,
                            year: int = None,
                            doy: int = None) -> list[CODETropoEntry]:
    """
    Filter CODE troposphere entries for a specific station.

    Args:
        entries: List of CODETropoEntry from parsed file
        station_4char: 4-character station code (e.g., "BRUX")
        year: Optional year filter
        doy: Optional DOY filter

    Returns:
        Filtered list of entries
    """
    result = []
    station_upper = station_4char.upper()

    for entry in entries:
        if entry.station_4char == station_upper:
            if year is not None and entry.year != year:
                continue
            if doy is not None and entry.doy != doy:
                continue
            result.append(entry)

    return result


if __name__ == "__main__":
    # Test with example file
    import sys

    if len(sys.argv) > 1:
        filepath = sys.argv[1]
    else:
        filepath = "/home/ahunegnaw/Downloads/COD0OPSFIN_20252420000_01D_01H_TRO.TRO"

    if os.path.exists(filepath):
        print(f"Parsing {filepath}...")
        sites, entries = parse_code_tro_file(filepath)

        print(f"\nFound {len(sites)} stations and {len(entries)} troposphere estimates")

        # Show first few sites
        print("\nFirst 5 stations:")
        for site in sites[:5]:
            print(f"  {site.station_id}: {site.latitude:.4f}°N, {site.longitude:.4f}°E, {site.height:.1f}m")

        # Show first few entries
        print("\nFirst 10 troposphere entries:")
        for entry in entries[:10]:
            print(f"  {entry.station_4char} DOY {entry.doy} Hour {entry.hour:02d}: "
                  f"ZTD={entry.ztd*1000:.1f}mm GN={entry.grad_n*1000:.3f}mm GE={entry.grad_e*1000:.3f}mm")

        # Test station filter
        brux = get_code_ztd_for_station(entries, "BRUX")
        if brux:
            print(f"\nBRUX has {len(brux)} entries")
    else:
        print(f"File not found: {filepath}")
        print("\nTrying to download DOY 242/2025...")
        downloaded = download_code_tro(2025, 242)
        if downloaded:
            print(f"Downloaded to: {downloaded}")
