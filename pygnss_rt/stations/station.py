"""
Station management and XML parsing.

Replaces Perl STA.pm module.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from xml.etree import ElementTree

from pygnss_rt.core.exceptions import StationError
from pygnss_rt.utils.logging import get_logger


logger = get_logger(__name__)


@dataclass
class Station:
    """GNSS station information."""

    station_id: str
    name: str | None = None
    network: str | None = None
    latitude: float | None = None
    longitude: float | None = None
    height: float | None = None
    x: float | None = None  # ECEF X coordinate
    y: float | None = None  # ECEF Y coordinate
    z: float | None = None  # ECEF Z coordinate
    receiver_type: str | None = None
    antenna_type: str | None = None
    use_nrt: bool = True
    active: bool = True
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "station_id": self.station_id,
            "name": self.name,
            "network": self.network,
            "latitude": self.latitude,
            "longitude": self.longitude,
            "height": self.height,
            "x": self.x,
            "y": self.y,
            "z": self.z,
            "receiver_type": self.receiver_type,
            "antenna_type": self.antenna_type,
            "use_nrt": self.use_nrt,
            "active": self.active,
        }

    @classmethod
    def from_xml_element(cls, element: ElementTree.Element) -> Station:
        """Create station from XML element.

        Supports both formats:
        - i-GNSS format: <fourCharName>, <approximate_X>, <use_nrt>yes/no</use_nrt>
        - Simple format: <id>, <x>, <use_nrt>1/0</use_nrt>
        """
        # Try i-GNSS format first (fourCharName), then simple format (id)
        station_id = element.findtext("fourCharName", "").lower()
        if not station_id:
            station_id = element.get("id", "").lower()
        if not station_id:
            station_id = element.findtext("id", "").lower()

        # Parse use_nrt - supports "yes"/"no" and "1"/"0"
        use_nrt_text = element.findtext("use_nrt", "yes").lower()
        use_nrt = use_nrt_text in ("yes", "1", "true")

        # Parse active - supports "yes"/"no" and "1"/"0"
        active_text = element.findtext("active", "yes").lower()
        active = active_text in ("yes", "1", "true")

        # Get coordinates - try i-GNSS format (approximate_X) then simple (x)
        x = _parse_float(element.findtext("approximate_X")) or _parse_float(element.findtext("x"))
        y = _parse_float(element.findtext("approximate_Y")) or _parse_float(element.findtext("y"))
        z = _parse_float(element.findtext("approximate_Z")) or _parse_float(element.findtext("z"))

        return cls(
            station_id=station_id,
            name=element.findtext("fullName") or element.findtext("name"),
            network=element.findtext("primaryNet") or element.findtext("network"),
            latitude=_parse_float(element.findtext("latitude")),
            longitude=_parse_float(element.findtext("longitude")),
            height=_parse_float(element.findtext("height")),
            x=x,
            y=y,
            z=z,
            receiver_type=element.findtext("receiver"),
            antenna_type=element.findtext("antenna"),
            use_nrt=use_nrt,
            active=active,
            metadata={
                "domes": element.findtext("DOMES", ""),
                "country": element.findtext("country", ""),
                "iso": element.findtext("ISO", ""),
                "provider": element.findtext("provider", ""),
                "type": element.findtext("type", ""),
            },
        )


class StationManager:
    """Manages station configurations and lookups."""

    def __init__(self):
        """Initialize station manager."""
        self._stations: dict[str, Station] = {}

    def load_xml(self, xml_path: Path | str) -> int:
        """Load stations from XML file.

        Args:
            xml_path: Path to XML file

        Returns:
            Number of stations loaded
        """
        path = Path(xml_path)
        if not path.exists():
            raise StationError("", f"XML file not found: {path}")

        try:
            tree = ElementTree.parse(path)
            root = tree.getroot()

            count = 0
            for station_elem in root.findall(".//station"):
                try:
                    station = Station.from_xml_element(station_elem)
                    if station.station_id:
                        self._stations[station.station_id.lower()] = station
                        count += 1
                except Exception as e:
                    logger.warning(
                        "Failed to parse station",
                        error=str(e),
                    )

            logger.info(
                "Loaded stations from XML",
                path=str(path),
                count=count,
            )
            return count

        except ElementTree.ParseError as e:
            raise StationError("", f"XML parse error: {e}") from e

    def add_station(self, station: Station) -> None:
        """Add a station to the manager."""
        self._stations[station.station_id.lower()] = station

    def get_station(self, station_id: str) -> Station | None:
        """Get station by ID."""
        return self._stations.get(station_id.lower())

    def get_stations(
        self,
        network: str | None = None,
        use_nrt: bool | None = None,
        active: bool | None = None,
    ) -> list[Station]:
        """Get stations with optional filters.

        Args:
            network: Filter by network name
            use_nrt: Filter by NRT status
            active: Filter by active status

        Returns:
            List of matching stations
        """
        result = []
        for station in self._stations.values():
            if network and station.network != network:
                continue
            if use_nrt is not None and station.use_nrt != use_nrt:
                continue
            if active is not None and station.active != active:
                continue
            result.append(station)
        return result

    def get_station_ids(
        self,
        network: str | None = None,
        exclude: list[str] | None = None,
    ) -> list[str]:
        """Get list of station IDs.

        Args:
            network: Filter by network
            exclude: Station IDs to exclude

        Returns:
            List of station IDs
        """
        exclude_set = set(s.lower() for s in (exclude or []))
        stations = self.get_stations(network=network, use_nrt=True, active=True)
        return [
            s.station_id
            for s in stations
            if s.station_id.lower() not in exclude_set
        ]

    def __len__(self) -> int:
        """Return number of stations."""
        return len(self._stations)

    def __iter__(self):
        """Iterate over stations."""
        return iter(self._stations.values())


def _parse_float(value: str | None) -> float | None:
    """Parse float from string, returning None on failure."""
    if value is None:
        return None
    try:
        return float(value)
    except ValueError:
        return None
