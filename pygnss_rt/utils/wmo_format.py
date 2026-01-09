"""
WMO Meteorological Station Format Parser.

Replaces Perl formatWMO.pm module.

Parses WMO (World Meteorological Organization) station data files
and converts them to a standardized format for use in GNSS processing.

WMO station data includes:
- Station ID (WMO number)
- Station name
- Height (meters)
- Latitude (decimal degrees)
- Longitude (decimal degrees)

Usage:
    from pygnss_rt.utils.wmo_format import WMOParser, WMOStation

    parser = WMOParser()
    stations = parser.parse_file("wmo.txt")

    # Write formatted output
    parser.write_formatted("formatted_wmo.txt", stations)
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator

from pygnss_rt.utils.logging import get_logger


logger = get_logger(__name__)


@dataclass
class WMOStation:
    """WMO meteorological station data."""

    station_id: str  # WMO station number
    name: str  # Station name
    height: int  # Height in meters
    latitude: float  # Latitude in decimal degrees
    longitude: float  # Longitude in decimal degrees
    country_code: str = ""  # Country code (derived from station_id prefix)

    @property
    def is_uk_station(self) -> bool:
        """Check if station is in the UK (WMO IDs starting with 03)."""
        return self.station_id.startswith("03")

    @property
    def is_ireland_station(self) -> bool:
        """Check if station is in Ireland (WMO IDs starting with 039)."""
        return self.station_id.startswith("039")

    def to_dict(self) -> dict:
        """Convert to dictionary."""
        return {
            "station_id": self.station_id,
            "name": self.name,
            "height": self.height,
            "latitude": self.latitude,
            "longitude": self.longitude,
            "country_code": self.country_code,
        }

    def __str__(self) -> str:
        """Format as string."""
        return f"{self.station_id:10s} {self.name:40s} {self.height:5d} {self.latitude:8.3f} {self.longitude:8.3f}"


class WMOParser:
    """Parser for WMO station data files.

    Handles various WMO data formats and normalizes the output.

    The parser handles lines with format like:
    STATION_ID STATION_NAME [HEIGHT] metres LATITUDE LONGITUDE

    Where the station name may contain multiple words and the height
    may or may not be present before the "metres" keyword.
    """

    def __init__(self):
        """Initialize WMO parser."""
        self._stations: list[WMOStation] = []

    @staticmethod
    def is_integer(s: str) -> bool:
        """Check if string is an integer.

        Args:
            s: String to check

        Returns:
            True if string is a valid integer
        """
        if not s:
            return False
        try:
            int(s)
            return True
        except ValueError:
            return False

    def parse_line(self, line: str) -> WMOStation | None:
        """Parse a single line of WMO data.

        Args:
            line: Line to parse

        Returns:
            WMOStation object or None if parsing failed
        """
        # Clean up the line
        line = line.strip()
        line = line.replace("\r\n", "").replace("\r", "").replace("\n", "")

        if not line:
            return None

        # Split on whitespace
        parts = line.split()
        if len(parts) < 5:
            return None

        # Find the "metres" keyword to locate the height
        metres_index = -1
        for i, part in enumerate(parts):
            if part.lower() == "metres":
                metres_index = i
                break

        if metres_index == -1:
            # No "metres" keyword found - try alternative parsing
            return self._parse_alternative_format(parts)

        # Extract station ID (first field)
        station_id = parts[0]

        # Extract height (field before "metres")
        height_index = metres_index - 1
        if height_index < 1:
            return None

        # Check if there's a valid height
        if self.is_integer(parts[height_index]):
            height = int(parts[height_index])
            name_end = height_index
        else:
            # Height might be missing, name extends to "metres"
            height = 0
            name_end = metres_index

        # Extract station name (fields between ID and height/metres)
        name_parts = parts[1:name_end]
        name = " ".join(name_parts)

        # Extract latitude and longitude (fields after "metres")
        try:
            latitude = float(parts[metres_index + 1])
            longitude = float(parts[metres_index + 2])
        except (IndexError, ValueError):
            return None

        # Determine country code from station ID prefix
        country_code = self._get_country_code(station_id)

        return WMOStation(
            station_id=station_id,
            name=name,
            height=height,
            latitude=latitude,
            longitude=longitude,
            country_code=country_code,
        )

    def _parse_alternative_format(self, parts: list[str]) -> WMOStation | None:
        """Parse alternative WMO format without 'metres' keyword.

        Format: ID NAME HEIGHT LAT LON

        Args:
            parts: List of line parts

        Returns:
            WMOStation or None
        """
        if len(parts) < 5:
            return None

        station_id = parts[0]

        # Try to find where the numeric fields start from the end
        # Last two should be lat/lon, third from last should be height
        try:
            longitude = float(parts[-1])
            latitude = float(parts[-2])
            height = int(parts[-3])
            name = " ".join(parts[1:-3])
        except (ValueError, IndexError):
            return None

        country_code = self._get_country_code(station_id)

        return WMOStation(
            station_id=station_id,
            name=name,
            height=height,
            latitude=latitude,
            longitude=longitude,
            country_code=country_code,
        )

    @staticmethod
    def _get_country_code(station_id: str) -> str:
        """Get country code from WMO station ID prefix.

        WMO station IDs have country-specific prefixes.

        Args:
            station_id: WMO station ID

        Returns:
            Country code string
        """
        # Common WMO country prefixes
        prefixes = {
            "03": "UK",
            "039": "IE",  # Ireland (within UK block)
            "06": "NL",  # Netherlands
            "07": "FR",  # France
            "08": "ES",  # Spain
            "10": "DE",  # Germany
            "11": "AT",  # Austria
            "12": "CH",  # Switzerland
            "16": "IT",  # Italy
            "01": "NO",  # Norway
            "02": "SE",  # Sweden
            "04": "FI",  # Finland
            "05": "DK",  # Denmark
            "26": "RU",  # Russia
        }

        for prefix, code in prefixes.items():
            if station_id.startswith(prefix):
                return code

        return ""

    def parse_file(self, filepath: str | Path, skip_header: bool = True) -> list[WMOStation]:
        """Parse WMO station data file.

        Args:
            filepath: Path to WMO data file
            skip_header: Skip first line as header (default True)

        Returns:
            List of WMOStation objects
        """
        filepath = Path(filepath)

        if not filepath.exists():
            raise FileNotFoundError(f"WMO file not found: {filepath}")

        stations = []
        errors = 0

        with open(filepath, "r", encoding="utf-8", errors="ignore") as f:
            for line_num, line in enumerate(f, 1):
                # Skip header line
                if skip_header and line_num == 1:
                    continue

                station = self.parse_line(line)
                if station:
                    stations.append(station)
                elif line.strip():  # Non-empty line that failed to parse
                    errors += 1

        logger.info(
            "Parsed WMO file",
            path=str(filepath),
            stations=len(stations),
            errors=errors,
        )

        self._stations = stations
        return stations

    def write_formatted(
        self,
        output_path: str | Path,
        stations: list[WMOStation] | None = None,
    ) -> int:
        """Write formatted WMO station data.

        Args:
            output_path: Output file path
            stations: List of stations (uses parsed stations if None)

        Returns:
            Number of stations written
        """
        output_path = Path(output_path)
        stations = stations or self._stations

        if not stations:
            logger.warning("No stations to write")
            return 0

        output_path.parent.mkdir(parents=True, exist_ok=True)

        with open(output_path, "w", encoding="utf-8") as f:
            for station in stations:
                f.write(
                    f"{station.station_id:10s} {station.name:40s} "
                    f"{station.height:5d} {station.latitude:8.3f} {station.longitude:8.3f}\n"
                )

        logger.info(
            "Wrote formatted WMO file",
            path=str(output_path),
            stations=len(stations),
        )

        return len(stations)

    def filter_by_country(self, country_code: str) -> list[WMOStation]:
        """Filter stations by country code.

        Args:
            country_code: Country code (e.g., "UK", "FR")

        Returns:
            List of stations in that country
        """
        return [s for s in self._stations if s.country_code == country_code]

    def filter_uk_stations(self) -> list[WMOStation]:
        """Get UK stations only.

        Returns:
            List of UK stations
        """
        return [s for s in self._stations if s.is_uk_station]

    def get_station_by_id(self, station_id: str) -> WMOStation | None:
        """Get station by WMO ID.

        Args:
            station_id: WMO station ID

        Returns:
            WMOStation or None
        """
        for station in self._stations:
            if station.station_id == station_id:
                return station
        return None

    def find_nearest_station(
        self,
        latitude: float,
        longitude: float,
        max_distance_km: float | None = None,
    ) -> tuple[WMOStation | None, float]:
        """Find the nearest WMO station to a given location.

        Uses simple Euclidean distance approximation (suitable for small areas).

        Args:
            latitude: Target latitude
            longitude: Target longitude
            max_distance_km: Maximum distance in km (None for no limit)

        Returns:
            Tuple of (nearest station, distance in km)
        """
        if not self._stations:
            return None, float("inf")

        nearest = None
        min_distance = float("inf")

        # Approximate km per degree at mid-latitudes
        km_per_deg_lat = 111.0
        km_per_deg_lon = 111.0 * 0.7  # Rough approximation for European latitudes

        for station in self._stations:
            dlat = (station.latitude - latitude) * km_per_deg_lat
            dlon = (station.longitude - longitude) * km_per_deg_lon
            distance = (dlat**2 + dlon**2) ** 0.5

            if distance < min_distance:
                min_distance = distance
                nearest = station

        if max_distance_km is not None and min_distance > max_distance_km:
            return None, min_distance

        return nearest, min_distance

    def iter_stations(self) -> Iterator[WMOStation]:
        """Iterate over parsed stations.

        Returns:
            Iterator of WMOStation objects
        """
        return iter(self._stations)

    @property
    def stations(self) -> list[WMOStation]:
        """Get list of parsed stations."""
        return self._stations


def format_wmo_file(
    input_path: str | Path,
    output_path: str | Path,
    skip_header: bool = True,
) -> dict:
    """Convenience function to format WMO file.

    Replaces the main functionality of formatWMO.pm.

    Args:
        input_path: Path to input WMO file
        output_path: Path to output formatted file
        skip_header: Skip first line as header

    Returns:
        Dictionary with processing results
    """
    parser = WMOParser()
    stations = parser.parse_file(input_path, skip_header=skip_header)
    written = parser.write_formatted(output_path)

    uk_stations = parser.filter_uk_stations()

    return {
        "total_stations": len(stations),
        "uk_stations": len(uk_stations),
        "written": written,
        "output_path": str(output_path),
    }
