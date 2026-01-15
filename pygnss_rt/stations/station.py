"""
Station management and XML/YAML parsing.

Replaces Perl STA.pm module.
Supports both XML (legacy) and YAML (preferred) station files.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from xml.etree import ElementTree

import yaml

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

    @classmethod
    def from_yaml_dict(cls, data: dict) -> Station:
        """Create station from YAML dictionary.

        Args:
            data: Dictionary from YAML file

        Returns:
            Station object
        """
        station_id = data.get("id", "").lower()

        # Parse coordinates if present
        coords = data.get("coordinates", {})
        x = coords.get("x") if coords else None
        y = coords.get("y") if coords else None
        z = coords.get("z") if coords else None

        return cls(
            station_id=station_id,
            name=data.get("name"),
            network=data.get("primary_net"),
            latitude=data.get("latitude"),
            longitude=data.get("longitude"),
            height=data.get("height"),
            x=x,
            y=y,
            z=z,
            receiver_type=data.get("receiver"),
            antenna_type=data.get("antenna"),
            use_nrt=data.get("use_nrt", True),
            active=data.get("active", True),
            metadata={
                "domes": data.get("domes", ""),
                "country": data.get("country", ""),
                "iso": data.get("iso", ""),
                "provider": data.get("provider", ""),
                "type": data.get("type", ""),
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

    def load_yaml(self, yaml_path: Path | str) -> int:
        """Load stations from YAML file.

        Args:
            yaml_path: Path to YAML file

        Returns:
            Number of stations loaded
        """
        path = Path(yaml_path)
        if not path.exists():
            raise StationError("", f"YAML file not found: {path}")

        try:
            with open(path) as f:
                data = yaml.safe_load(f)

            if not data or "stations" not in data:
                raise StationError("", f"Invalid YAML format: missing 'stations' key")

            count = 0
            for station_data in data["stations"]:
                try:
                    station = Station.from_yaml_dict(station_data)
                    if station.station_id:
                        self._stations[station.station_id.lower()] = station
                        count += 1
                except Exception as e:
                    logger.warning(
                        "Failed to parse station",
                        error=str(e),
                    )

            logger.info(
                "Loaded stations from YAML",
                path=str(path),
                count=count,
            )
            return count

        except yaml.YAMLError as e:
            raise StationError("", f"YAML parse error: {e}") from e

    def load(self, file_path: Path | str) -> int:
        """Load stations from file (auto-detect format).

        Supports both XML and YAML formats. YAML is preferred if both exist.

        Args:
            file_path: Path to station file (XML or YAML)

        Returns:
            Number of stations loaded
        """
        path = Path(file_path)

        # If path doesn't exist, try alternative extension
        if not path.exists():
            # Try YAML if XML was specified, or vice versa
            if path.suffix.lower() == ".xml":
                yaml_path = path.with_suffix(".yaml")
                if yaml_path.exists():
                    path = yaml_path
            elif path.suffix.lower() == ".yaml":
                xml_path = path.with_suffix(".xml")
                if xml_path.exists():
                    path = xml_path

        if not path.exists():
            raise StationError("", f"Station file not found: {file_path}")

        # Load based on extension
        if path.suffix.lower() == ".yaml":
            return self.load_yaml(path)
        else:
            return self.load_xml(path)

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
        station_type: str | None = None,
    ) -> list[Station]:
        """Get stations with optional filters.

        Args:
            network: Filter by network name (primaryNet)
            use_nrt: Filter by NRT status
            active: Filter by active status
            station_type: Filter by station type (e.g., "core", "EUREF", "active")

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
            if station_type:
                sta_type = station.metadata.get("type", "")
                if sta_type.lower() != station_type.lower():
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
