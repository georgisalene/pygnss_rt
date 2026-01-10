"""
Unified Station Information Manager.

Complete Python port of Perl STA.pm - provides interface to station XML files.
Includes all STA.pm methods plus additional functionality for station management.

This module provides:
- StationInfoManager: Main class for loading and querying station XML files
- WMOStationParser: Parser for WMO meteorological station data
- Bernese file writers (CRD, ABB, OTL formats)
- GMT plotting support

Replaces Perl modules:
- STA.pm - Station XML interface
- Parts of station handling in various i-GNSS scripts

Usage:
    from pygnss_rt.stations.station_info import StationInfoManager

    # Load station info from XML
    manager = StationInfoManager("/path/to/IGS20gh.xml")

    # Get station list with filters
    stations = manager.get_list(use_nrt="yes", station_type="core")

    # Get station details
    xyz = manager.get_xyz("ABMF")
    domes = manager.get_domes("ABMF")
    provider = manager.get_provider("ABMF")

    # Write Bernese coordinate file
    manager.write_bernese_coord_file("/path/to/output.CRD", datum="IGS20")
"""

from __future__ import annotations

import math
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator


@dataclass
class StationData:
    """Complete station information from XML.

    Mirrors the Perl station hash structure.
    """

    four_char_name: str  # 4-character station ID (uppercase)
    domes: str = ""  # DOMES number (e.g., "97103M001")
    two_char_name: str = ""  # 2-character abbreviation
    full_name: str = ""  # Full station name
    approximate_x: float = 0.0  # ECEF X coordinate (meters)
    approximate_y: float = 0.0  # ECEF Y coordinate (meters)
    approximate_z: float = 0.0  # ECEF Z coordinate (meters)
    country: str = ""  # Country name
    iso: str = ""  # ISO country code (3-letter)
    primary_net: str = ""  # Primary network (IGS, EUREF, etc.)
    provider: str = ""  # Data provider
    use_nrt: str = "no"  # NRT capability ("yes" or "no")
    station_type: str = ""  # Station type (core, active, etc.)
    receiver: str = ""  # Receiver type
    antenna: str = ""  # Antenna type
    gmt_opt: dict[str, str] = field(default_factory=dict)  # GMT plotting options

    @property
    def is_nrt(self) -> bool:
        """Check if station is NRT capable."""
        return self.use_nrt.lower() == "yes"

    @property
    def long_name(self) -> str:
        """Get long station name with ISO suffix (for Bernese)."""
        return f"{self.four_char_name.lower()}00{self.iso.lower()}"

    def get_xyz(self) -> tuple[float, float, float]:
        """Get ECEF coordinates as tuple."""
        return (self.approximate_x, self.approximate_y, self.approximate_z)

    def get_geodetic(self) -> tuple[float, float, float]:
        """Get geodetic coordinates (lat, lon, height) in degrees/meters."""
        x, y, z = self.approximate_x, self.approximate_y, self.approximate_z

        # WGS84 ellipsoid parameters
        a = 6378137.0  # Semi-major axis
        f = 1 / 298.257223563  # Flattening
        b = a * (1 - f)  # Semi-minor axis
        e2 = (a**2 - b**2) / a**2  # First eccentricity squared

        # Calculate longitude
        lon = math.atan2(y, x)

        # Iterative calculation of latitude
        p = math.sqrt(x**2 + y**2)
        lat = math.atan2(z, p * (1 - e2))  # Initial approximation

        for _ in range(10):  # Iterate for convergence
            n = a / math.sqrt(1 - e2 * math.sin(lat) ** 2)
            lat_new = math.atan2(z + e2 * n * math.sin(lat), p)
            if abs(lat_new - lat) < 1e-12:
                break
            lat = lat_new

        # Calculate height
        n = a / math.sqrt(1 - e2 * math.sin(lat) ** 2)
        height = p / math.cos(lat) - n

        # Convert to degrees
        lat_deg = math.degrees(lat)
        lon_deg = math.degrees(lon)

        return (lat_deg, lon_deg, height)


class StationInfoManager:
    """Manager for station information from XML files.

    Complete port of Perl STA.pm with all methods.

    Usage:
        manager = StationInfoManager("/path/to/stations.xml")

        # Get filtered list
        stations = manager.get_list(use_nrt="yes")

        # Query individual station
        xyz = manager.get_xyz("ABMF")
        domes = manager.get_domes("ABMF")
    """

    def __init__(self, xml_path: str | Path | None = None):
        """Initialize station info manager.

        Args:
            xml_path: Path to station XML file (optional, can load later)
        """
        self._stations: dict[str, StationData] = {}
        self._xml_path: Path | None = None
        self._datum: str = ""
        self._ref_epoch: str = ""

        if xml_path:
            self.load(xml_path)

    def load(self, xml_path: str | Path) -> int:
        """Load stations from XML file.

        Args:
            xml_path: Path to station XML file

        Returns:
            Number of stations loaded

        Raises:
            FileNotFoundError: If XML file doesn't exist
        """
        path = Path(xml_path)
        if not path.exists():
            raise FileNotFoundError(f"Station XML file not found: {path}")

        self._xml_path = path
        self._stations.clear()

        tree = ET.parse(path)
        root = tree.getroot()

        # Get datum and reference epoch if present
        datum_elem = root.find("datum")
        if datum_elem is not None and datum_elem.text:
            self._datum = datum_elem.text.strip()

        ref_ep_elem = root.find("ref_ep")
        if ref_ep_elem is not None and ref_ep_elem.text:
            self._ref_epoch = ref_ep_elem.text.strip()

        # Parse station elements
        for station_elem in root.iter("station"):
            station = self._parse_station_element(station_elem)
            if station.four_char_name:
                self._stations[station.four_char_name.upper()] = station

        return len(self._stations)

    def _parse_station_element(self, elem: ET.Element) -> StationData:
        """Parse a station element from XML."""

        def get_text(tag: str, default: str = "") -> str:
            child = elem.find(tag)
            if child is not None and child.text:
                return child.text.strip()
            return default

        def get_float(tag: str, default: float = 0.0) -> float:
            text = get_text(tag)
            if text:
                try:
                    return float(text)
                except ValueError:
                    pass
            return default

        # Parse GMT options
        gmt_opt = {}
        gmt_elem = elem.find("GMT_opt")
        if gmt_elem is not None:
            for child in gmt_elem:
                if child.text:
                    gmt_opt[child.tag] = child.text.strip()

        return StationData(
            four_char_name=get_text("fourCharName").upper(),
            domes=get_text("DOMES"),
            two_char_name=get_text("twoCharName"),
            full_name=get_text("fullName"),
            approximate_x=get_float("approximate_X"),
            approximate_y=get_float("approximate_Y"),
            approximate_z=get_float("approximate_Z"),
            country=get_text("country"),
            iso=get_text("ISO"),
            primary_net=get_text("primaryNet"),
            provider=get_text("provider"),
            use_nrt=get_text("use_nrt", "no"),
            station_type=get_text("type"),
            receiver=get_text("receiver"),
            antenna=get_text("antenna"),
            gmt_opt=gmt_opt,
        )

    # =========================================================================
    # STA.pm Methods - Direct Ports
    # =========================================================================

    def get_list(
        self,
        use_nrt: str | None = None,
        station_type: str | None = None,
        primary_net: str | None = None,
        include_long_name: bool = True,
    ) -> list[str]:
        """Get filtered list of stations.

        Replaces Perl STA::get_list.

        Args:
            use_nrt: Filter by NRT capability ("yes" or "no")
            station_type: Filter by station type (e.g., "core", "active")
            primary_net: Filter by primary network (e.g., "IGS", "EUREF")
            include_long_name: Return long names (ssss00iso) instead of 4-char

        Returns:
            Sorted list of station identifiers
        """
        result = []

        for sta in self._stations.values():
            # Apply filters
            if use_nrt and sta.use_nrt.lower() != use_nrt.lower():
                continue
            if station_type and sta.station_type.lower() != station_type.lower():
                continue
            if primary_net and sta.primary_net != primary_net:
                continue

            if include_long_name:
                result.append(sta.long_name)
            else:
                result.append(sta.four_char_name.lower())

        return sorted(result)

    def get_xyz(self, station_id: str) -> tuple[float, float, float] | None:
        """Get ECEF coordinates for a station.

        Replaces Perl STA::get_XYZ.

        Args:
            station_id: 4-character station ID

        Returns:
            Tuple of (X, Y, Z) coordinates in meters, or None if not found
        """
        sta = self._stations.get(station_id.upper())
        if sta and sta.approximate_x and sta.approximate_y and sta.approximate_z:
            return sta.get_xyz()
        return None

    def get_full_name(self, station_id: str) -> str:
        """Get full name of a station.

        Replaces Perl STA::get_full_name.

        Args:
            station_id: 4-character station ID

        Returns:
            Full station name or empty string
        """
        sta = self._stations.get(station_id.upper())
        return sta.full_name if sta else ""

    def get_provider(self, station_id: str) -> str:
        """Get data provider for a station.

        Replaces Perl STA::get_provider.

        Args:
            station_id: 4-character station ID

        Returns:
            Provider name or empty string
        """
        sta = self._stations.get(station_id.upper())
        return sta.provider if sta else ""

    def get_country(self, station_id: str) -> str:
        """Get country for a station.

        Replaces Perl STA::get_country.

        Args:
            station_id: 4-character station ID

        Returns:
            Country name or empty string
        """
        sta = self._stations.get(station_id.upper())
        return sta.country if sta else ""

    def get_domes(self, station_id: str) -> str:
        """Get DOMES number for a station.

        Replaces Perl STA::get_DOMES.

        Args:
            station_id: 4-character station ID

        Returns:
            DOMES number or empty string
        """
        sta = self._stations.get(station_id.upper())
        return sta.domes if sta else ""

    def get_iso(self, station_id: str) -> str:
        """Get ISO country code for a station.

        Replaces Perl STA::get_ISO.

        Args:
            station_id: 4-character station ID

        Returns:
            ISO country code or empty string
        """
        sta = self._stations.get(station_id.upper())
        return sta.iso if sta else ""

    def get_gmt_opt(self, station_id: str) -> dict[str, str]:
        """Get GMT plotting options for a station.

        Replaces Perl STA::get_GMT_opt.

        Args:
            station_id: 4-character station ID

        Returns:
            GMT options dictionary
        """
        sta = self._stations.get(station_id.upper())
        return sta.gmt_opt if sta else {}

    def get_list_of_all_sta(self) -> list[str]:
        """Get list of all station IDs.

        Replaces Perl STA::get_list_of_all_sta.

        Returns:
            Sorted list of 4-character station IDs
        """
        return sorted(self._stations.keys())

    def get_list_of_igs_sta(self) -> list[str]:
        """Get list of IGS stations.

        Replaces Perl STA::get_list_of_IGS_sta.

        Returns:
            Sorted list of IGS station IDs
        """
        return sorted(
            sta.four_char_name
            for sta in self._stations.values()
            if sta.primary_net == "IGS"
        )

    def get_list_of_igs_tra_sta(self) -> list[str]:
        """Get list of IGS_TRA stations.

        Replaces Perl STA::get_list_of_IGS_TRA_sta.
        """
        return sorted(
            sta.four_char_name
            for sta in self._stations.values()
            if sta.primary_net == "IGS_TRA"
        )

    def get_list_of_ukcgps_sta(self) -> list[str]:
        """Get list of UKCGPS stations.

        Replaces Perl STA::get_list_of_UKCGPS_sta.
        """
        return sorted(
            sta.four_char_name
            for sta in self._stations.values()
            if sta.primary_net == "UKCGPS"
        )

    def get_list_by_provider(self, provider: str) -> list[str]:
        """Get list of stations by provider.

        Replaces Perl STA::get_list_of_OS_hd_sta, get_list_of_UKCOGR_hd_sta.

        Args:
            provider: Provider name (e.g., "OS", "IESSG", "MO")

        Returns:
            Sorted list of station IDs
        """
        return sorted(
            sta.four_char_name
            for sta in self._stations.values()
            if sta.provider == provider
        )

    def get_part(self, station_list: list[str], session: int, num_sessions: int = 4) -> list[str]:
        """Split station list into parts for parallel processing.

        Replaces Perl STA::get_part.

        Args:
            station_list: Full list of station IDs
            session: Session number (1-based)
            num_sessions: Total number of sessions to split into

        Returns:
            Subset of stations for the specified session
        """
        if session < 1 or session > num_sessions:
            raise ValueError(f"Session must be between 1 and {num_sessions}")

        sorted_list = sorted(station_list)
        total = len(sorted_list)

        # Calculate stations per session
        base_count = total // num_sessions
        remainder = total % num_sessions

        # Calculate start and end indices for this session
        start = 0
        for i in range(1, session):
            start += base_count + (1 if i <= remainder else 0)

        count = base_count + (1 if session <= remainder else 0)

        return sorted_list[start : start + count]

    def get_station(self, station_id: str) -> StationData | None:
        """Get full station data.

        Args:
            station_id: 4-character station ID

        Returns:
            StationData object or None if not found
        """
        return self._stations.get(station_id.upper())

    # =========================================================================
    # File Writers - Bernese Format
    # =========================================================================

    def write_bernese_coord_file(
        self,
        output_path: str | Path,
        datum: str | None = None,
        station_list: list[str] | None = None,
    ) -> int:
        """Write Bernese coordinate file.

        Replaces Perl STA::write_Bernese_coord_file and
        writeBerneseCoordFileForSpecifiedListOfSta.

        Args:
            output_path: Output file path
            datum: Geodetic datum (default: from XML)
            station_list: Specific stations to include (all if None)

        Returns:
            Number of stations written
        """
        now = datetime.now(timezone.utc)
        timestamp = now.strftime("%Y/%m/%d-%H:%M")
        datum = datum or self._datum or "IGS20"

        stations_to_write = []
        if station_list:
            for sta_id in sorted(station_list):
                sta = self._stations.get(sta_id.upper())
                if sta:
                    stations_to_write.append(sta)
        else:
            stations_to_write = sorted(
                self._stations.values(), key=lambda s: s.four_char_name
            )

        with open(output_path, "w") as f:
            # Header
            f.write(f"{'i-GNSS Python Station Manager':<52}{timestamp:>29}\n")
            f.write("-" * 80 + "\n")
            f.write(f"LOCAL GEODETIC DATUM: {datum}\n\n")
            f.write("NUM  STATION NAME           X (M)          Y (M)          Z (M)     FLAG\n\n")

            # Stations
            for num, sta in enumerate(stations_to_write, 1):
                f.write(
                    f"{num:3d}{sta.four_char_name:>6}"
                    f"{sta.approximate_x:27.4f}"
                    f"{sta.approximate_y:15.4f}"
                    f"{sta.approximate_z:15.4f}\n"
                )

        return len(stations_to_write)

    def write_otl_file(self, output_path: str | Path, station_list: list[str] | None = None) -> int:
        """Write ocean tide loading request file.

        Replaces Perl STA::write_otl.
        Output format for http://holt.oso.chalmers.se/loading/

        Args:
            output_path: Output file path
            station_list: Specific stations to include (all if None)

        Returns:
            Number of stations written
        """
        stations_to_write = []
        if station_list:
            for sta_id in sorted(station_list):
                sta = self._stations.get(sta_id.upper())
                if sta:
                    stations_to_write.append(sta)
        else:
            stations_to_write = sorted(
                self._stations.values(), key=lambda s: s.four_char_name
            )

        with open(output_path, "w") as f:
            for sta in stations_to_write:
                f.write(
                    f"{sta.four_char_name:4s}"
                    f"{sta.approximate_x:35.4f}"
                    f"{sta.approximate_y:16.4f}"
                    f"{sta.approximate_z:16.4f}\n"
                )

        return len(stations_to_write)

    def write_abbreviation_table(
        self,
        output_path: str | Path,
        station_list: list[str] | None = None,
    ) -> int:
        """Write Bernese abbreviation table.

        Replaces Perl STA::writeAbbreviationTable.

        Args:
            output_path: Output file path
            station_list: Specific stations to include (all if None)

        Returns:
            Number of stations written
        """
        now = datetime.now(timezone.utc)
        timestamp = now.strftime("%Y/%m/%d-%H:%M")

        stations_to_write = []
        if station_list:
            for sta_id in sorted(station_list):
                sta = self._stations.get(sta_id.upper())
                if sta:
                    stations_to_write.append(sta)
        else:
            stations_to_write = sorted(
                self._stations.values(), key=lambda s: s.four_char_name
            )

        with open(output_path, "w") as f:
            # Header
            f.write(f"{'STATION ABBREVIATION TABLE, BERNESE V5.0 i-GNSS':<52}{timestamp:>29}\n")
            f.write("-" * 80 + "\n\n")
            f.write("Station name             4-ID    2-ID    Remark\n")
            f.write("****************         ****     **     ***************************************\n")

            # Stations
            for sta in stations_to_write:
                f.write(
                    f"{sta.four_char_name:<16}"
                    f"{'':>9}"
                    f"{sta.four_char_name:<4}"
                    f"{'':>5}"
                    f"{sta.two_char_name:2s}"
                    f"{'':>5}"
                    f"{'i-GNSS Python':<39}\n"
                )

        return len(stations_to_write)

    def write_gmt_files(
        self,
        output_dir: str | Path,
        station_list: list[str] | None = None,
    ) -> int:
        """Write GMT plotting files.

        Replaces Perl STA::writeGmtFiles.
        Creates GmtSymbols.txt and GmtLegend.txt.

        Args:
            output_dir: Output directory
            station_list: Specific stations to include (all if None)

        Returns:
            Number of stations written
        """
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        stations_to_write = []
        if station_list:
            for sta_id in sorted(station_list):
                sta = self._stations.get(sta_id.upper())
                if sta:
                    stations_to_write.append(sta)
        else:
            stations_to_write = sorted(
                self._stations.values(), key=lambda s: s.four_char_name
            )

        symbols_file = output_dir / "GmtSymbols.txt"
        legend_file = output_dir / "GmtLegend.txt"

        with open(symbols_file, "w") as f_sym, open(legend_file, "w") as f_leg:
            for sta in stations_to_write:
                lat, lon, _ = sta.get_geodetic()

                f_sym.write(f"{lon:6.3f}, {lat:6.3f}, {sta.four_char_name}\n")
                f_leg.write(f"{lon:6.3f} {lat:6.3f}  8  0  4 LT {sta.four_char_name}\n")

        return len(stations_to_write)

    # =========================================================================
    # Additional Utility Methods
    # =========================================================================

    def __len__(self) -> int:
        """Return number of stations."""
        return len(self._stations)

    def __iter__(self) -> Iterator[StationData]:
        """Iterate over stations."""
        return iter(self._stations.values())

    def __contains__(self, station_id: str) -> bool:
        """Check if station exists."""
        return station_id.upper() in self._stations

    @property
    def datum(self) -> str:
        """Get geodetic datum."""
        return self._datum

    @property
    def reference_epoch(self) -> str:
        """Get reference epoch."""
        return self._ref_epoch

    @property
    def xml_path(self) -> Path | None:
        """Get source XML path."""
        return self._xml_path

    def get_nrt_stations(self) -> list[StationData]:
        """Get all NRT-capable stations."""
        return [sta for sta in self._stations.values() if sta.is_nrt]

    def get_stations_by_network(self, network: str) -> list[StationData]:
        """Get stations by primary network."""
        return [
            sta for sta in self._stations.values() if sta.primary_net == network
        ]

    def search_stations(
        self,
        name_pattern: str | None = None,
        country: str | None = None,
        provider: str | None = None,
    ) -> list[StationData]:
        """Search stations by various criteria.

        Args:
            name_pattern: Substring to match in station name
            country: Country to filter by
            provider: Provider to filter by

        Returns:
            List of matching stations
        """
        results = []
        for sta in self._stations.values():
            if name_pattern and name_pattern.lower() not in sta.full_name.lower():
                continue
            if country and country.lower() not in sta.country.lower():
                continue
            if provider and sta.provider != provider:
                continue
            results.append(sta)
        return results


@dataclass
class WMOStation:
    """WMO meteorological station information."""

    wmo_id: str  # WMO station ID (5 digits)
    name: str = ""
    latitude: float = 0.0
    longitude: float = 0.0
    height: float = 0.0
    country: str = ""
    region: str = ""
    gnss_station: str = ""  # Associated GNSS station if any


class WMOStationParser:
    """Parser for WMO meteorological station data.

    Handles various WMO station data formats including Excel XML exports.
    """

    def __init__(self):
        """Initialize WMO station parser."""
        self._stations: dict[str, WMOStation] = {}

    def load_excel_xml(self, xml_path: str | Path) -> int:
        """Load WMO stations from Excel XML format.

        Args:
            xml_path: Path to Excel XML file

        Returns:
            Number of stations loaded
        """
        path = Path(xml_path)
        if not path.exists():
            raise FileNotFoundError(f"WMO XML file not found: {path}")

        # Parse Excel XML namespace
        namespaces = {
            "ss": "urn:schemas-microsoft-com:office:spreadsheet",
            "o": "urn:schemas-microsoft-com:office:office",
        }

        tree = ET.parse(path)
        root = tree.getroot()

        # Find worksheet data
        for worksheet in root.findall(".//ss:Worksheet", namespaces):
            table = worksheet.find("ss:Table", namespaces)
            if table is None:
                continue

            # Parse rows (skip header)
            rows = table.findall("ss:Row", namespaces)
            for row in rows[1:]:  # Skip header row
                cells = row.findall("ss:Cell", namespaces)
                if len(cells) < 5:
                    continue

                # Extract cell data
                cell_data = []
                for cell in cells:
                    data = cell.find("ss:Data", namespaces)
                    cell_data.append(data.text if data is not None and data.text else "")

                if len(cell_data) >= 5 and cell_data[0]:
                    try:
                        station = WMOStation(
                            wmo_id=cell_data[0],
                            name=cell_data[1] if len(cell_data) > 1 else "",
                            latitude=float(cell_data[2]) if len(cell_data) > 2 and cell_data[2] else 0.0,
                            longitude=float(cell_data[3]) if len(cell_data) > 3 and cell_data[3] else 0.0,
                            height=float(cell_data[4]) if len(cell_data) > 4 and cell_data[4] else 0.0,
                            country=cell_data[5] if len(cell_data) > 5 else "",
                        )
                        self._stations[station.wmo_id] = station
                    except (ValueError, IndexError):
                        continue

        return len(self._stations)

    def load_simple_xml(self, xml_path: str | Path) -> int:
        """Load WMO stations from simple XML format.

        Expected format:
        <stations>
            <station>
                <wmo_id>...</wmo_id>
                <name>...</name>
                <latitude>...</latitude>
                <longitude>...</longitude>
                <height>...</height>
            </station>
        </stations>

        Args:
            xml_path: Path to XML file

        Returns:
            Number of stations loaded
        """
        path = Path(xml_path)
        if not path.exists():
            raise FileNotFoundError(f"WMO XML file not found: {path}")

        tree = ET.parse(path)
        root = tree.getroot()

        for station_elem in root.iter("station"):
            def get_text(tag: str, default: str = "") -> str:
                child = station_elem.find(tag)
                if child is not None and child.text:
                    return child.text.strip()
                return default

            def get_float(tag: str, default: float = 0.0) -> float:
                text = get_text(tag)
                try:
                    return float(text) if text else default
                except ValueError:
                    return default

            wmo_id = get_text("wmo_id") or get_text("id")
            if wmo_id:
                station = WMOStation(
                    wmo_id=wmo_id,
                    name=get_text("name"),
                    latitude=get_float("latitude") or get_float("lat"),
                    longitude=get_float("longitude") or get_float("lon"),
                    height=get_float("height") or get_float("elevation"),
                    country=get_text("country"),
                    region=get_text("region"),
                    gnss_station=get_text("gnss_station") or get_text("gnss"),
                )
                self._stations[wmo_id] = station

        return len(self._stations)

    def get_station(self, wmo_id: str) -> WMOStation | None:
        """Get station by WMO ID."""
        return self._stations.get(wmo_id)

    def get_all_stations(self) -> list[WMOStation]:
        """Get all loaded stations."""
        return list(self._stations.values())

    def find_nearest(
        self,
        latitude: float,
        longitude: float,
        max_distance_km: float = 100.0,
    ) -> list[tuple[WMOStation, float]]:
        """Find WMO stations nearest to a location.

        Args:
            latitude: Target latitude in degrees
            longitude: Target longitude in degrees
            max_distance_km: Maximum distance in kilometers

        Returns:
            List of (station, distance_km) tuples, sorted by distance
        """
        results = []

        for station in self._stations.values():
            dist = self._haversine_distance(
                latitude, longitude, station.latitude, station.longitude
            )
            if dist <= max_distance_km:
                results.append((station, dist))

        return sorted(results, key=lambda x: x[1])

    def find_by_gnss_station(self, gnss_id: str) -> WMOStation | None:
        """Find WMO station associated with a GNSS station.

        Args:
            gnss_id: 4-character GNSS station ID

        Returns:
            Associated WMO station or None
        """
        gnss_id_upper = gnss_id.upper()
        for station in self._stations.values():
            if station.gnss_station.upper() == gnss_id_upper:
                return station
        return None

    @staticmethod
    def _haversine_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        """Calculate distance between two points using Haversine formula.

        Args:
            lat1, lon1: First point (degrees)
            lat2, lon2: Second point (degrees)

        Returns:
            Distance in kilometers
        """
        R = 6371.0  # Earth radius in km

        lat1_rad = math.radians(lat1)
        lat2_rad = math.radians(lat2)
        dlat = math.radians(lat2 - lat1)
        dlon = math.radians(lon2 - lon1)

        a = (
            math.sin(dlat / 2) ** 2
            + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlon / 2) ** 2
        )
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

        return R * c

    def __len__(self) -> int:
        """Return number of stations."""
        return len(self._stations)


# =============================================================================
# Convenience Functions
# =============================================================================


def load_station_info(xml_path: str | Path) -> StationInfoManager:
    """Load station info from XML file.

    Args:
        xml_path: Path to station XML file

    Returns:
        StationInfoManager instance
    """
    return StationInfoManager(xml_path)


def get_nrt_station_list(
    xml_path: str | Path,
    station_type: str | None = None,
    primary_net: str | None = None,
) -> list[str]:
    """Get list of NRT stations from XML file.

    Convenience function for common use case.

    Args:
        xml_path: Path to station XML file
        station_type: Optional type filter
        primary_net: Optional network filter

    Returns:
        List of station long names (ssss00iso format)
    """
    manager = StationInfoManager(xml_path)
    return manager.get_list(use_nrt="yes", station_type=station_type, primary_net=primary_net)


def merge_station_files(
    xml_paths: list[str | Path],
    nrt_only: bool = True,
) -> list[str]:
    """Merge stations from multiple XML files.

    Args:
        xml_paths: List of XML file paths
        nrt_only: Only include NRT-capable stations

    Returns:
        Deduplicated, sorted list of station IDs
    """
    seen = set()
    result = []

    for path in xml_paths:
        try:
            manager = StationInfoManager(path)
            for sta in manager:
                if nrt_only and not sta.is_nrt:
                    continue
                if sta.four_char_name.lower() not in seen:
                    seen.add(sta.four_char_name.lower())
                    result.append(sta.long_name)
        except FileNotFoundError:
            continue

    return sorted(result)
