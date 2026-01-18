"""
Automatic STA File Update from YAML Station Lists.

Reads station lists from YAML files, checks if they exist in the STA file,
and automatically downloads missing station info from gnss-metadata.eu.

Usage:
    from pygnss_rt.stations.sta_auto_update import update_sta_from_yaml

    # Update STA file with missing stations from YAML
    added = update_sta_from_yaml(
        yaml_file="station_data/IGS20gh.yaml",
        sta_file="station_data/IGS20_54.STA"
    )
"""

import logging
import re
from pathlib import Path
from typing import Optional

import yaml

logger = logging.getLogger(__name__)


def get_existing_stations(sta_file: str | Path) -> set[str]:
    """
    Extract existing station IDs from a Bernese STA file.

    Args:
        sta_file: Path to STA file

    Returns:
        Set of 4-character station IDs (uppercase)
    """
    sta_file = Path(sta_file)
    if not sta_file.exists():
        return set()

    try:
        content = sta_file.read_text(encoding='utf-8')
    except UnicodeDecodeError:
        content = sta_file.read_text(encoding='latin-1')

    stations = set()
    # Match TYPE 002 station entries (station info section)
    # Format: XXXX                  001  YYYY MM DD ...
    for line in content.split('\n'):
        match = re.match(r'^([A-Z0-9]{4})\s+001\s+\d{4}', line)
        if match:
            stations.add(match.group(1).upper())

    return stations


def read_yaml_stations(yaml_file: str | Path) -> list[dict]:
    """
    Read station list from YAML file.

    Args:
        yaml_file: Path to YAML station file

    Returns:
        List of station dictionaries with id, domes, iso, etc.
    """
    yaml_file = Path(yaml_file)
    if not yaml_file.exists():
        raise FileNotFoundError(f"YAML file not found: {yaml_file}")

    with open(yaml_file) as f:
        data = yaml.safe_load(f)

    stations = data.get('stations', [])
    return stations


def build_nine_char_id(station: dict) -> str:
    """
    Build 9-character station ID from YAML station entry.

    Format: XXXX00CCC where:
    - XXXX = 4-char station ID
    - 00 = monument number (default)
    - CCC = 3-letter ISO country code

    Args:
        station: Station dictionary with 'id' and 'iso' keys

    Returns:
        9-character station ID (uppercase)
    """
    sta_id = station.get('id', '').upper()[:4]
    iso = station.get('iso', '').upper()[:3]

    # Some stations have specific monument numbers in DOMES
    # Try to extract from DOMES: e.g., "13703M001" -> monument "00" (default)
    domes = station.get('domes', '')
    monument = '00'  # Default

    if len(sta_id) == 4 and len(iso) == 3:
        return f"{sta_id}{monument}{iso}"

    return ""


def find_missing_stations(
    yaml_file: str | Path,
    sta_file: str | Path,
) -> list[dict]:
    """
    Find stations in YAML that are missing from STA file.

    Args:
        yaml_file: Path to YAML station file
        sta_file: Path to STA file

    Returns:
        List of missing station dictionaries
    """
    yaml_stations = read_yaml_stations(yaml_file)
    existing = get_existing_stations(sta_file)

    missing = []
    for station in yaml_stations:
        sta_id = station.get('id', '').upper()[:4]
        if sta_id and sta_id not in existing:
            missing.append(station)

    return missing


def update_sta_from_yaml(
    yaml_file: str | Path,
    sta_file: str | Path,
    output_sta: Optional[str | Path] = None,
    temp_dir: str | Path = "/tmp/m3g_downloads",
    dry_run: bool = False,
) -> int:
    """
    Update STA file with missing stations from YAML list.

    Downloads station info from gnss-metadata.eu for any stations
    in the YAML file that don't exist in the STA file.

    Args:
        yaml_file: Path to YAML station file
        sta_file: Path to existing STA file
        output_sta: Output path (defaults to overwriting sta_file)
        temp_dir: Directory for temporary downloads
        dry_run: If True, only report what would be done

    Returns:
        Number of stations added
    """
    from pygnss_rt.stations.gnss_metadata_eu import GNSSMetadataDownloader
    from pygnss_rt.stations.site_log_parser import parse_site_log
    from pygnss_rt.stations.sta_file_writer import STAFileWriter

    yaml_file = Path(yaml_file)
    sta_file = Path(sta_file)
    output_sta = Path(output_sta) if output_sta else sta_file
    temp_dir = Path(temp_dir)

    # Find missing stations
    missing = find_missing_stations(yaml_file, sta_file)

    if not missing:
        logger.info(f"All stations from {yaml_file.name} exist in STA file")
        return 0

    logger.info(f"Found {len(missing)} missing stations in {yaml_file.name}")

    if dry_run:
        for station in missing:
            nine_char = build_nine_char_id(station)
            print(f"  Would download: {station['id'].upper()} -> {nine_char}")
        return 0

    # Download missing stations from M3G
    temp_dir.mkdir(parents=True, exist_ok=True)
    downloader = GNSSMetadataDownloader()

    downloaded_data = []
    for station in missing:
        nine_char = build_nine_char_id(station)
        if not nine_char:
            logger.warning(f"Cannot build 9-char ID for {station.get('id')}")
            continue

        logger.info(f"Downloading: {nine_char}")
        log_path = downloader.download_site_log(nine_char, temp_dir)

        if log_path and log_path.exists():
            try:
                data = parse_site_log(log_path)
                if data and data.station_id:
                    downloaded_data.append(data)
                    logger.info(f"  Parsed: {data.station_id} - {data.site_identification.site_name}")
            except Exception as e:
                logger.error(f"  Parse error: {e}")
        else:
            logger.warning(f"  Download failed for {nine_char}")

    if not downloaded_data:
        logger.warning("No stations successfully downloaded")
        return 0

    # Read existing STA file
    try:
        existing_content = sta_file.read_text(encoding='utf-8')
    except UnicodeDecodeError:
        existing_content = sta_file.read_text(encoding='latin-1')

    # Build new entries
    writer = STAFileWriter(use_domes=False)
    stations_info = writer._build_station_info(downloaded_data)

    if not stations_info:
        logger.warning("No valid station info generated")
        return 0

    # Find insertion points
    lines = existing_content.split('\n')
    type002_start = -1
    type003_start = -1

    for i, line in enumerate(lines):
        if line.startswith("TYPE 002:"):
            type002_start = i
        if line.startswith("TYPE 003:"):
            type003_start = i

    if type002_start < 0 or type003_start < 0:
        logger.error("Could not find TYPE sections in STA file")
        return 0

    # Build new TYPE 001 entries (insert before TYPE 002)
    new_type001 = []
    for station in stations_info:
        name = station.station_name_no_domes
        old_name = f"{station.station_id.lower()}*"
        new_type001.append(
            f"{name:<16}      001  {'':19}  {'':19}  {old_name.upper():<16}      "
            f"{'M3G auto-update':<24}"
        )

    # Build new TYPE 002 entries (insert before TYPE 003)
    new_type002 = []
    for station in stations_info:
        name = station.station_name_no_domes
        for event in station.events:
            start = event.start_date
            end = event.end_date

            start_str = f"{start.year:4d} {start.month:02d} {start.day:02d} " \
                       f"{start.hour:02d} {start.minute:02d} {start.second:02d}"

            if end.year >= 2099:
                end_str = " " * 19
            else:
                end_str = f"{end.year:4d} {end.month:02d} {end.day:02d} " \
                         f"{end.hour:02d} {end.minute:02d} {end.second:02d}"

            line = f"{name:<16}      001  "
            line += f"{start_str}  "
            line += f"{end_str}  "
            line += f"{event.receiver_type:<20}  "
            line += f"{event.receiver_serial:>20}  "
            line += f"{event.receiver_serial[-6:]:>6}  "
            line += f"{event.antenna_type:<20}  "
            line += f"{event.antenna_serial:>20}  "
            line += f"{event.antenna_serial[-6:]:>6}  "
            line += f"{event.north_ecc:8.4f}  "
            line += f"{event.east_ecc:8.4f}  "
            line += f"{event.up_ecc:8.4f}  "
            line += f"{event.site_name:<22}  "
            line += "M3G auto-update"
            new_type002.append(line)

    # Insert TYPE 001 entries (before TYPE 002 section header)
    lines = lines[:type002_start-1] + new_type001 + lines[type002_start-1:]

    # Recalculate type003_start after TYPE 001 insertion
    type003_start += len(new_type001)
    for i, line in enumerate(lines):
        if line.startswith("TYPE 003:"):
            type003_start = i
            break

    # Insert TYPE 002 entries (before TYPE 003 section header)
    lines = lines[:type003_start-1] + new_type002 + lines[type003_start-1:]

    # Write output
    output_sta.write_text('\n'.join(lines), encoding='utf-8')
    logger.info(f"Added {len(downloaded_data)} stations to {output_sta}")

    return len(downloaded_data)


def update_sta_from_all_yamls(
    yaml_dir: str | Path,
    sta_file: str | Path,
    output_sta: Optional[str | Path] = None,
    dry_run: bool = False,
) -> int:
    """
    Update STA file from all YAML files in a directory.

    Args:
        yaml_dir: Directory containing YAML station files
        sta_file: Path to STA file
        output_sta: Output path (defaults to overwriting sta_file)
        dry_run: If True, only report what would be done

    Returns:
        Total number of stations added
    """
    yaml_dir = Path(yaml_dir)
    total_added = 0

    for yaml_file in sorted(yaml_dir.glob("*.yaml")):
        logger.info(f"Processing: {yaml_file.name}")
        added = update_sta_from_yaml(
            yaml_file=yaml_file,
            sta_file=sta_file,
            output_sta=output_sta,
            dry_run=dry_run,
        )
        total_added += added

    return total_added


# Convenience function for CLI
def check_and_update_sta(
    stations: list[str],
    sta_file: str | Path,
    iso_codes: Optional[dict[str, str]] = None,
) -> int:
    """
    Check if stations exist in STA file and download missing ones.

    Args:
        stations: List of 4-char station IDs
        sta_file: Path to STA file
        iso_codes: Optional dict mapping station ID to ISO country code

    Returns:
        Number of stations added
    """
    from pygnss_rt.stations.gnss_metadata_eu import GNSSMetadataDownloader
    from pygnss_rt.stations.site_log_parser import parse_site_log

    sta_file = Path(sta_file)
    existing = get_existing_stations(sta_file)

    # Default ISO codes for common stations
    DEFAULT_ISO = {
        'BASC': 'LUX', 'ECH2': 'LUX', 'ERPL': 'LUX', 'ROUL': 'LUX', 'TROS': 'LUX',
        'BRUX': 'BEL', 'GOPE': 'CZE', 'GRAZ': 'AUT', 'WTZR': 'DEU', 'ZIMM': 'CHE',
    }

    if iso_codes:
        DEFAULT_ISO.update(iso_codes)

    missing = [s.upper() for s in stations if s.upper() not in existing]

    if not missing:
        logger.info("All stations exist in STA file")
        return 0

    logger.info(f"Missing stations: {missing}")

    # Build station list for download
    to_download = []
    for sta_id in missing:
        iso = DEFAULT_ISO.get(sta_id, '')
        if iso:
            nine_char = f"{sta_id}00{iso}"
            to_download.append({'id': sta_id, 'iso': iso, 'nine_char': nine_char})
        else:
            logger.warning(f"No ISO code for {sta_id}, skipping")

    if not to_download:
        return 0

    # Download and update
    downloader = GNSSMetadataDownloader()
    downloaded_data = []

    for station in to_download:
        log_path = downloader.download_site_log(station['nine_char'], '/tmp/m3g_auto')
        if log_path and log_path.exists():
            try:
                data = parse_site_log(log_path)
                if data and data.station_id:
                    downloaded_data.append(data)
            except Exception as e:
                logger.error(f"Parse error for {station['nine_char']}: {e}")

    if not downloaded_data:
        return 0

    # Merge into STA file (reuse the merge logic)
    from pygnss_rt.stations.sta_file_writer import STAFileWriter

    try:
        existing_content = sta_file.read_text(encoding='utf-8')
    except UnicodeDecodeError:
        existing_content = sta_file.read_text(encoding='latin-1')

    writer = STAFileWriter(use_domes=False)
    stations_info = writer._build_station_info(downloaded_data)

    if not stations_info:
        return 0

    lines = existing_content.split('\n')
    type002_start = type003_start = -1

    for i, line in enumerate(lines):
        if line.startswith("TYPE 002:"):
            type002_start = i
        if line.startswith("TYPE 003:"):
            type003_start = i

    if type002_start < 0 or type003_start < 0:
        logger.error("Invalid STA file format")
        return 0

    # Build new entries
    new_type001 = []
    new_type002 = []

    for station in stations_info:
        name = station.station_name_no_domes
        old_name = f"{station.station_id.lower()}*"
        new_type001.append(
            f"{name:<16}      001  {'':19}  {'':19}  {old_name.upper():<16}      "
            f"{'M3G auto-update':<24}"
        )

        for event in station.events:
            start = event.start_date
            end = event.end_date
            start_str = f"{start.year:4d} {start.month:02d} {start.day:02d} " \
                       f"{start.hour:02d} {start.minute:02d} {start.second:02d}"
            end_str = " " * 19 if end.year >= 2099 else \
                     f"{end.year:4d} {end.month:02d} {end.day:02d} " \
                     f"{end.hour:02d} {end.minute:02d} {end.second:02d}"

            line = f"{name:<16}      001  {start_str}  {end_str}  "
            line += f"{event.receiver_type:<20}  {event.receiver_serial:>20}  "
            line += f"{event.receiver_serial[-6:]:>6}  {event.antenna_type:<20}  "
            line += f"{event.antenna_serial:>20}  {event.antenna_serial[-6:]:>6}  "
            line += f"{event.north_ecc:8.4f}  {event.east_ecc:8.4f}  {event.up_ecc:8.4f}  "
            line += f"{event.site_name:<22}  M3G auto-update"
            new_type002.append(line)

    # Insert entries
    lines = lines[:type002_start-1] + new_type001 + lines[type002_start-1:]
    for i, line in enumerate(lines):
        if line.startswith("TYPE 003:"):
            type003_start = i
            break
    lines = lines[:type003_start-1] + new_type002 + lines[type003_start-1:]

    sta_file.write_text('\n'.join(lines), encoding='utf-8')
    logger.info(f"Added {len(downloaded_data)} stations to STA file")

    return len(downloaded_data)
