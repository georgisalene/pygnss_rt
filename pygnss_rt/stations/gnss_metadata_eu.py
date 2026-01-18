"""
GNSS Metadata EU (M3G) Site Log Downloader and Integration.

Downloads IGS site logs from https://gnss-metadata.eu and integrates
them into Bernese GNSS Software STA files.

Endpoints used:
- /sitelog/exportlog?station=[9-CHAR-ID] - ASCII site log
- /sitelog/exportxml?station=[9-CHAR-ID] - GeodesyML XML

Author: pygnss_rt
"""

import logging
import re
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Optional
from urllib.parse import urljoin

logger = logging.getLogger(__name__)

# Base URL for gnss-metadata.eu
GNSS_METADATA_BASE_URL = "https://gnss-metadata.eu"


@dataclass
class StationMetadata:
    """Basic station metadata from gnss-metadata.eu."""
    nine_char_id: str
    four_char_id: str = ""
    country_code: str = ""
    country_name: str = ""
    status: str = ""  # A=Active, I=Inactive, D=Decommissioned
    satellite_systems: str = ""
    last_update: Optional[datetime] = None
    networks: list[str] = field(default_factory=list)
    license: str = ""

    def __post_init__(self):
        if not self.four_char_id and self.nine_char_id:
            self.four_char_id = self.nine_char_id[:4].upper()


class GNSSMetadataDownloader:
    """
    Download site logs from gnss-metadata.eu.

    Example usage:
        downloader = GNSSMetadataDownloader()

        # Download a single station
        log_path = downloader.download_site_log("BASC00LUX", "/tmp")

        # Download multiple stations
        logs = downloader.download_stations(["BASC00LUX", "ECH200LUX"], "/tmp")
    """

    def __init__(
        self,
        base_url: str = GNSS_METADATA_BASE_URL,
        timeout: int = 30,
    ):
        """
        Initialize the downloader.

        Args:
            base_url: Base URL for gnss-metadata.eu
            timeout: Request timeout in seconds
        """
        self.base_url = base_url.rstrip('/')
        self.timeout = timeout
        self._session = None

    def _get_session(self):
        """Get or create HTTP session."""
        if self._session is None:
            import requests
            self._session = requests.Session()
            self._session.headers.update({
                'User-Agent': 'pygnss_rt/1.0 (GNSS Processing)',
                'Accept': 'text/plain, application/xml, */*',
            })
        return self._session

    def download_site_log(
        self,
        station_id: str,
        output_dir: str | Path,
        format: str = "log",
    ) -> Optional[Path]:
        """
        Download a site log for a station.

        Args:
            station_id: 9-character station ID (e.g., BASC00LUX) or 4-char ID
            output_dir: Directory to save the file
            format: "log" for ASCII or "xml" for GeodesyML

        Returns:
            Path to downloaded file, or None if failed
        """
        import requests

        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        # Normalize station ID to 9-char format if needed
        station_id = self._normalize_station_id(station_id)

        # Build URL
        if format.lower() == "xml":
            endpoint = f"/sitelog/exportxml?station={station_id}"
            ext = "xml"
        else:
            endpoint = f"/sitelog/exportlog?station={station_id}"
            ext = "log"

        url = urljoin(self.base_url, endpoint)
        logger.info(f"Downloading site log: {url}")

        try:
            session = self._get_session()
            response = session.get(url, timeout=self.timeout)
            response.raise_for_status()

            # Check if we got valid content
            content = response.text
            if not content or "Error" in content[:100] or len(content) < 100:
                logger.warning(f"Invalid or empty response for {station_id}")
                return None

            # Generate filename
            today = datetime.now().strftime("%Y%m%d")
            filename = f"{station_id.lower()}_{today}.{ext}"
            output_path = output_dir / filename

            # Write file
            output_path.write_text(content, encoding='utf-8')
            logger.info(f"Downloaded: {output_path}")

            return output_path

        except requests.RequestException as e:
            logger.error(f"Failed to download {station_id}: {e}")
            return None

    def download_stations(
        self,
        station_ids: list[str],
        output_dir: str | Path,
        format: str = "log",
    ) -> dict[str, Optional[Path]]:
        """
        Download site logs for multiple stations.

        Args:
            station_ids: List of station IDs (4-char or 9-char)
            output_dir: Directory to save files
            format: "log" or "xml"

        Returns:
            Dictionary mapping station ID to downloaded file path (or None)
        """
        results = {}
        for station_id in station_ids:
            results[station_id] = self.download_site_log(
                station_id, output_dir, format
            )
        return results

    def _normalize_station_id(self, station_id: str) -> str:
        """
        Normalize station ID to 9-character format.

        Args:
            station_id: 4-char or 9-char station ID

        Returns:
            9-character station ID (uppercase)
        """
        station_id = station_id.strip().upper()

        # Already 9-char format
        if len(station_id) == 9:
            return station_id

        # 4-char format - can't determine country, return as-is
        # User should provide full 9-char ID for best results
        if len(station_id) == 4:
            logger.warning(
                f"4-char station ID '{station_id}' may not work. "
                "Use full 9-char ID (e.g., BASC00LUX) for best results."
            )
            return station_id

        return station_id


def download_and_parse_site_log(
    station_id: str,
    output_dir: str | Path = "/tmp",
) -> Optional['SiteLogData']:
    """
    Download and parse a site log from gnss-metadata.eu.

    Args:
        station_id: Station ID (9-char preferred)
        output_dir: Directory to save downloaded file

    Returns:
        Parsed SiteLogData, or None if failed
    """
    from pygnss_rt.stations.site_log_parser import parse_site_log

    downloader = GNSSMetadataDownloader()
    log_path = downloader.download_site_log(station_id, output_dir)

    if log_path and log_path.exists():
        return parse_site_log(log_path)

    return None


def download_and_add_to_sta(
    station_ids: list[str],
    existing_sta_path: str | Path,
    output_sta_path: Optional[str | Path] = None,
    temp_dir: str | Path = "/tmp/site_logs",
) -> int:
    """
    Download site logs and add stations to an existing STA file.

    Args:
        station_ids: List of station IDs to download
        existing_sta_path: Path to existing STA file
        output_sta_path: Output path (defaults to overwriting existing)
        temp_dir: Directory for temporary site log files

    Returns:
        Number of stations added
    """
    from pygnss_rt.stations.site_log_parser import parse_site_log
    from pygnss_rt.stations.sta_file_writer import STAFileWriter

    existing_sta_path = Path(existing_sta_path)
    output_sta_path = Path(output_sta_path) if output_sta_path else existing_sta_path
    temp_dir = Path(temp_dir)
    temp_dir.mkdir(parents=True, exist_ok=True)

    # Download site logs
    downloader = GNSSMetadataDownloader()
    downloaded = downloader.download_stations(station_ids, temp_dir)

    # Parse downloaded logs
    new_stations = []
    for station_id, log_path in downloaded.items():
        if log_path and log_path.exists():
            try:
                data = parse_site_log(log_path)
                if data and data.station_id:
                    new_stations.append(data)
                    logger.info(f"Parsed station: {data.station_id}")
            except Exception as e:
                logger.error(f"Failed to parse {log_path}: {e}")

    if not new_stations:
        logger.warning("No new stations to add")
        return 0

    # Read existing STA content and parse existing stations
    existing_content = ""
    if existing_sta_path.exists():
        existing_content = existing_sta_path.read_text()

    # Extract existing station IDs to avoid duplicates
    existing_ids = set()
    for line in existing_content.split('\n'):
        # TYPE 002 lines start with station name
        if re.match(r'^[A-Z0-9]{4}\s+001\s+\d{4}', line):
            station_name = line[:4].strip().upper()
            existing_ids.add(station_name)

    # Filter out duplicates
    stations_to_add = [s for s in new_stations if s.station_id.upper() not in existing_ids]

    if not stations_to_add:
        logger.info("All stations already exist in STA file")
        return 0

    # Append new stations to the STA file
    writer = STAFileWriter(use_domes=False)
    stations_info = writer._build_station_info(stations_to_add)

    # Read existing file to find insertion points
    lines = existing_content.split('\n')

    # Find TYPE 001 and TYPE 002 sections
    type001_end = -1
    type002_end = -1

    for i, line in enumerate(lines):
        if line.startswith("TYPE 002:"):
            type001_end = i - 2  # Before blank lines
        if line.startswith("TYPE 003:"):
            type002_end = i - 2

    if type001_end < 0 or type002_end < 0:
        logger.error("Could not find TYPE sections in existing STA file")
        return 0

    # Build new TYPE 001 entries
    new_type001_lines = []
    for station in stations_info:
        name = station.station_name_no_domes
        old_name = f"{station.station_id.lower()}*"
        new_type001_lines.append(
            f"{name:<16}      001  {'':19}  {'':19}  {old_name.upper():<16}      "
            f"{'pygnss_rt generated':<24}"
        )

    # Build new TYPE 002 entries
    new_type002_lines = []
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
            line += "pygnss_rt generated"
            new_type002_lines.append(line)

    # Insert new entries
    # Insert TYPE 001 entries before TYPE 002 section
    lines = lines[:type001_end] + new_type001_lines + lines[type001_end:]

    # Recalculate type002_end after insertion
    type002_end += len(new_type001_lines)

    # Find new TYPE 003 position
    for i, line in enumerate(lines):
        if line.startswith("TYPE 003:"):
            type002_end = i - 2
            break

    # Insert TYPE 002 entries before TYPE 003 section
    lines = lines[:type002_end] + new_type002_lines + lines[type002_end:]

    # Write output
    output_sta_path.write_text('\n'.join(lines))
    logger.info(f"Added {len(stations_to_add)} stations to {output_sta_path}")

    return len(stations_to_add)


# Predefined station lists for common networks
LUXEMBOURG_STATIONS = [
    "BASC00LUX",
    "ECH200LUX",
    "ERPL00LUX",
    "ROUL00LUX",
    "TROS00LUX",
]

# European Reference stations (subset)
EUREF_CORE_STATIONS = [
    "BOR100POL",
    "BRUX00BEL",
    "GOPE00CZE",
    "GRAZ00AUT",
    "KARL00DEU",
    "LEIJ00DEU",
    "ONSA00SWE",
    "POTS00DEU",
    "WTZR00DEU",
    "ZIMM00CHE",
]


def download_country_stations(
    country_code: str,
    output_dir: str | Path,
    station_list: Optional[list[str]] = None,
) -> dict[str, Optional[Path]]:
    """
    Download all site logs for stations in a country.

    Args:
        country_code: 3-letter country code (e.g., LUX, DEU, FRA)
        output_dir: Directory to save files
        station_list: Optional list of specific stations (if None, uses predefined)

    Returns:
        Dictionary mapping station ID to downloaded path
    """
    country_code = country_code.upper()

    # Use predefined list if available
    if station_list is None:
        if country_code == "LUX":
            station_list = LUXEMBOURG_STATIONS
        else:
            logger.warning(
                f"No predefined station list for {country_code}. "
                "Provide station_list parameter."
            )
            return {}

    downloader = GNSSMetadataDownloader()
    return downloader.download_stations(station_list, output_dir)
