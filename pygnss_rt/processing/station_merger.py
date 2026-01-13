"""
All-Network Station Merging for GNSS Processing.

Combines stations from multiple GNSS networks (IGS, EUREF, OS, RGP, etc.)
into a unified station list for processing. Handles duplicate removal
and NRT-capability filtering.

This replaces the Perl station merging logic:
    my @Sta=(@IGSCORE,@EU,@OS,@SC,@IGS,@IR,@IS,@RG,@SS,@CA);

Usage:
    from pygnss_rt.processing.station_merger import StationMerger, NetworkSource

    merger = StationMerger()
    merger.add_source(NetworkSource.IGS_CORE, "/path/to/IGS20gh.xml")
    merger.add_source(NetworkSource.EUREF, "/path/to/eurefgh.xml")

    stations = merger.get_merged_stations(nrt_only=True)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any
import xml.etree.ElementTree as ET

from pygnss_rt.core.paths import get_paths


class NetworkSource(str, Enum):
    """Network source identifiers.

    Matches the Perl network loading from XML files.
    """

    IGS_CORE = "igs_core"      # IGS20gh.xml - core IGS stations
    EUREF = "euref"            # eurefgh.xml - European Reference Frame
    OS_ACTIVE = "os_active"    # stationsgh.xml type=OS active
    SCIENTIFIC = "scientific"  # stationsgh.xml type=scientific
    IGS = "igs"               # stationsgh.xml type=IGS
    IRELAND = "ireland"       # irelandgh.xml
    ICELAND = "iceland"       # icelandgh.xml
    RGP = "rgp"               # RGPgh.xml - Réseau GNSS Permanent (France)
    SUPERSITES = "supersites" # supersitesgh.xml - Netherlands/European supersites
    CANADA = "canada"         # NRCANgh.xml - Natural Resources Canada


@dataclass
class StationInfo:
    """Information about a GNSS station."""

    station_id: str  # 4-character station ID (lowercase)
    name: str = ""
    network: str = ""
    station_type: str = ""
    use_nrt: bool = False
    latitude: float | None = None
    longitude: float | None = None
    height: float | None = None
    receiver: str = ""
    antenna: str = ""
    dome_number: str = ""
    source_file: str = ""

    def __hash__(self) -> int:
        """Hash by station ID for deduplication."""
        return hash(self.station_id.lower())

    def __eq__(self, other: object) -> bool:
        """Compare by station ID (case-insensitive)."""
        if isinstance(other, StationInfo):
            return self.station_id.lower() == other.station_id.lower()
        return False


@dataclass
class MergerConfig:
    """Configuration for station merging."""

    # XML file paths for each network
    xml_paths: dict[NetworkSource, Path] = field(default_factory=dict)

    # Base directory for finding XML files (station_data directory)
    station_data_dir: Path = field(default_factory=lambda: get_paths().station_data_dir)

    # Default XML file names
    default_files: dict[NetworkSource, str] = field(default_factory=lambda: {
        NetworkSource.IGS_CORE: "IGS20gh.xml",
        NetworkSource.EUREF: "eurefgh.xml",
        NetworkSource.OS_ACTIVE: "stationsgh.xml",
        NetworkSource.SCIENTIFIC: "stationsgh.xml",
        NetworkSource.IGS: "stationsgh.xml",
        NetworkSource.IRELAND: "irelandgh.xml",
        NetworkSource.ICELAND: "icelandgh.xml",
        NetworkSource.RGP: "RGPgh.xml",
        NetworkSource.SUPERSITES: "supersitesgh.xml",
        NetworkSource.CANADA: "NRCANgh.xml",
    })

    # Type filters for shared XML files
    type_filters: dict[NetworkSource, str] = field(default_factory=lambda: {
        NetworkSource.IGS_CORE: "core",
        NetworkSource.EUREF: "EUREF",
        NetworkSource.OS_ACTIVE: "OS active",
        NetworkSource.SCIENTIFIC: "scientific",
        NetworkSource.IGS: "IGS",
        NetworkSource.IRELAND: "active",
        NetworkSource.ICELAND: "ICE",
        NetworkSource.RGP: "active",
        NetworkSource.SUPERSITES: "active",
        NetworkSource.CANADA: "active",
    })


class StationMerger:
    """Merges stations from multiple GNSS networks.

    Handles loading stations from XML files, filtering by NRT capability,
    and removing duplicates while preserving priority ordering.
    """

    def __init__(
        self,
        config: MergerConfig | None = None,
        station_data_dir: str | Path | None = None,
        verbose: bool = False,
    ) -> None:
        """Initialize station merger.

        Args:
            config: Full merger configuration
            station_data_dir: Base directory for XML files (shortcut)
            verbose: Enable verbose output
        """
        if config:
            self.config = config
        elif station_data_dir:
            self.config = MergerConfig(station_data_dir=Path(station_data_dir))
        else:
            self.config = MergerConfig()

        self.verbose = verbose
        self._sources: dict[NetworkSource, list[StationInfo]] = {}
        self._xml_cache: dict[Path, ET.Element] = {}

    def add_source(
        self,
        source: NetworkSource,
        xml_path: str | Path | None = None,
    ) -> int:
        """Add a network source.

        Args:
            source: Network source identifier
            xml_path: Path to XML file (uses default if None)

        Returns:
            Number of stations loaded from this source
        """
        # Determine XML path
        if xml_path:
            path = Path(xml_path)
        elif source in self.config.xml_paths:
            path = self.config.xml_paths[source]
        else:
            default_file = self.config.default_files.get(source)
            if not default_file:
                raise ValueError(f"No XML file configured for source: {source}")
            path = self.config.station_data_dir / default_file

        # Get type filter
        type_filter = self.config.type_filters.get(source)

        # Load stations
        stations = self._load_stations_from_xml(path, source, type_filter)
        self._sources[source] = stations

        if self.verbose:
            print(f"  Loaded {len(stations)} stations from {source.value} ({path.name})")

        return len(stations)

    def add_all_sources(self) -> dict[NetworkSource, int]:
        """Add all configured network sources.

        Returns:
            Dictionary of source -> station count
        """
        counts = {}
        for source in NetworkSource:
            try:
                count = self.add_source(source)
                counts[source] = count
            except FileNotFoundError as e:
                if self.verbose:
                    print(f"  Warning: {source.value} - {e}")
                counts[source] = 0
        return counts

    def _load_stations_from_xml(
        self,
        xml_path: Path,
        source: NetworkSource,
        type_filter: str | None = None,
    ) -> list[StationInfo]:
        """Load stations from XML file.

        Args:
            xml_path: Path to XML file
            source: Network source identifier
            type_filter: Optional type filter

        Returns:
            List of StationInfo objects
        """
        if not xml_path.exists():
            raise FileNotFoundError(f"Station XML file not found: {xml_path}")

        # Use cached parse if available
        if xml_path in self._xml_cache:
            root = self._xml_cache[xml_path]
        else:
            tree = ET.parse(xml_path)
            root = tree.getroot()
            self._xml_cache[xml_path] = root

        stations = []

        # Find station elements (may vary by XML format)
        for station_elem in root.iter():
            if station_elem.tag.lower() in ("station", "sta", "site"):
                station = self._parse_station_element(station_elem, source, xml_path)

                # Apply type filter if specified
                if type_filter:
                    if station.station_type.lower() != type_filter.lower():
                        continue

                stations.append(station)

        # Also check for stations in a list/stations container
        for container in root.iter("stations"):
            for station_elem in container:
                station = self._parse_station_element(station_elem, source, xml_path)
                if type_filter and station.station_type.lower() != type_filter.lower():
                    continue
                stations.append(station)

        return stations

    def _parse_station_element(
        self,
        elem: ET.Element,
        source: NetworkSource,
        xml_path: Path,
    ) -> StationInfo:
        """Parse a station element from XML.

        Args:
            elem: XML element
            source: Network source
            xml_path: Source file path

        Returns:
            StationInfo object
        """
        # Get station ID from various possible attributes/children
        station_id = (
            elem.get("id", "")
            or elem.get("name", "")
            or elem.get("sta", "")
            or self._get_child_text(elem, "id")
            or self._get_child_text(elem, "station_id")
            or self._get_child_text(elem, "sta")
            or ""
        ).lower()[:4]  # Normalize to 4-char lowercase

        # Parse NRT capability
        use_nrt_str = (
            elem.get("use_nrt", "")
            or elem.get("nrt", "")
            or self._get_child_text(elem, "use_nrt")
            or self._get_child_text(elem, "nrt")
            or "no"
        ).lower()
        use_nrt = use_nrt_str in ("yes", "true", "1", "y")

        # Parse coordinates
        lat = self._parse_float(
            elem.get("latitude") or self._get_child_text(elem, "latitude")
        )
        lon = self._parse_float(
            elem.get("longitude") or self._get_child_text(elem, "longitude")
        )
        height = self._parse_float(
            elem.get("height") or self._get_child_text(elem, "height")
        )

        return StationInfo(
            station_id=station_id,
            name=elem.get("long_name", "") or self._get_child_text(elem, "name") or "",
            network=source.value,
            station_type=elem.get("type", "") or self._get_child_text(elem, "type") or "",
            use_nrt=use_nrt,
            latitude=lat,
            longitude=lon,
            height=height,
            receiver=elem.get("receiver", "") or self._get_child_text(elem, "receiver") or "",
            antenna=elem.get("antenna", "") or self._get_child_text(elem, "antenna") or "",
            dome_number=elem.get("dome", "") or self._get_child_text(elem, "dome") or "",
            source_file=str(xml_path),
        )

    def _get_child_text(self, elem: ET.Element, tag: str) -> str:
        """Get text content of child element."""
        child = elem.find(tag)
        if child is not None and child.text:
            return child.text.strip()
        # Try case-insensitive
        for child in elem:
            if child.tag.lower() == tag.lower() and child.text:
                return child.text.strip()
        return ""

    def _parse_float(self, value: str | None) -> float | None:
        """Parse float value, returning None on failure."""
        if not value:
            return None
        try:
            return float(value)
        except ValueError:
            return None

    def get_merged_stations(
        self,
        nrt_only: bool = True,
        sources: list[NetworkSource] | None = None,
        priority_order: list[NetworkSource] | None = None,
    ) -> list[StationInfo]:
        """Get merged list of stations from all sources.

        Duplicates are removed, keeping the first occurrence based on
        priority order (earlier sources have higher priority).

        Args:
            nrt_only: Only include NRT-capable stations
            sources: Specific sources to include (all if None)
            priority_order: Order for duplicate resolution (default: enum order)

        Returns:
            Deduplicated list of stations
        """
        if priority_order is None:
            priority_order = list(NetworkSource)

        seen_ids: set[str] = set()
        merged: list[StationInfo] = []

        for source in priority_order:
            if sources and source not in sources:
                continue
            if source not in self._sources:
                continue

            for station in self._sources[source]:
                # Apply NRT filter
                if nrt_only and not station.use_nrt:
                    continue

                # Deduplicate by station ID
                station_key = station.station_id.lower()
                if station_key in seen_ids:
                    continue

                seen_ids.add(station_key)
                merged.append(station)

        if self.verbose:
            print(f"  Merged {len(merged)} unique stations (NRT only: {nrt_only})")

        return merged

    def get_station_ids(
        self,
        nrt_only: bool = True,
        sources: list[NetworkSource] | None = None,
    ) -> list[str]:
        """Get list of merged station IDs.

        Args:
            nrt_only: Only include NRT-capable stations
            sources: Specific sources to include

        Returns:
            List of 4-character station IDs
        """
        stations = self.get_merged_stations(nrt_only=nrt_only, sources=sources)
        return [s.station_id for s in stations]

    def get_statistics(self) -> dict[str, Any]:
        """Get statistics about loaded sources.

        Returns:
            Statistics dictionary
        """
        stats = {
            "sources_loaded": len(self._sources),
            "total_stations": sum(len(s) for s in self._sources.values()),
            "nrt_stations": sum(
                1 for stations in self._sources.values()
                for s in stations if s.use_nrt
            ),
            "by_source": {},
        }

        for source, stations in self._sources.items():
            nrt_count = sum(1 for s in stations if s.use_nrt)
            stats["by_source"][source.value] = {
                "total": len(stations),
                "nrt": nrt_count,
            }

        # Count unique stations
        all_stations = self.get_merged_stations(nrt_only=False)
        nrt_stations = self.get_merged_stations(nrt_only=True)
        stats["unique_total"] = len(all_stations)
        stats["unique_nrt"] = len(nrt_stations)

        return stats


def create_nrddp_merger(
    station_data_dir: str | Path | None = None,
    verbose: bool = False,
) -> StationMerger:
    """Create a station merger configured for NRDDP TRO processing.

    Loads all network sources used in NRDDP TRO:
    - IGS core stations
    - EUREF European network
    - OS active (Great Britain)
    - Scientific stations
    - IGS general
    - Ireland
    - Iceland
    - RGP France
    - Supersites
    - Canada (NRCAN)

    Args:
        station_data_dir: Directory containing station XML files
        verbose: Enable verbose output

    Returns:
        Configured StationMerger with all sources loaded
    """
    merger = StationMerger(station_data_dir=station_data_dir, verbose=verbose)
    merger.add_all_sources()
    return merger


# Default NRDDP TRO station sources (in priority order)
NRDDP_STATION_SOURCES = [
    NetworkSource.IGS_CORE,
    NetworkSource.EUREF,
    NetworkSource.OS_ACTIVE,
    NetworkSource.SCIENTIFIC,
    NetworkSource.IGS,
    NetworkSource.IRELAND,
    NetworkSource.ICELAND,
    NetworkSource.RGP,
    NetworkSource.SUPERSITES,
    NetworkSource.CANADA,
]
