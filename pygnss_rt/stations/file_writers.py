"""
Bernese station file writers.

Creates station information files in Bernese GNSS Software formats:
- .CRD files: Station coordinate files
- .OTL files: Ocean tide loading files
- .ABB files: Station abbreviation files

Replaces Perl BSWFILES.pm write functions.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

from pygnss_rt.utils.dates import GNSSDate
from pygnss_rt.utils.logging import get_logger


logger = get_logger(__name__)


@dataclass
class StationCoordinate:
    """Station coordinate record for CRD files."""

    station_id: str  # 4-char station code
    flag: str = "A"  # A=active, M=marker, etc.
    x: float = 0.0  # ECEF X (meters)
    y: float = 0.0  # ECEF Y (meters)
    z: float = 0.0  # ECEF Z (meters)
    sigma_x: float = 0.001  # X uncertainty (meters)
    sigma_y: float = 0.001  # Y uncertainty (meters)
    sigma_z: float = 0.001  # Z uncertainty (meters)
    description: str = ""  # Station description (16 char max)

    @property
    def station_name(self) -> str:
        """Get 4-char station name in uppercase."""
        return self.station_id.upper()[:4]


@dataclass
class OceanTideLoading:
    """Ocean tide loading coefficients for OTL files."""

    station_id: str
    # Amplitude and phase for 11 tidal constituents
    # M2, S2, N2, K2, K1, O1, P1, Q1, MF, MM, SSA
    amplitudes_radial: list[float] = field(default_factory=list)
    amplitudes_west: list[float] = field(default_factory=list)
    amplitudes_south: list[float] = field(default_factory=list)
    phases_radial: list[float] = field(default_factory=list)
    phases_west: list[float] = field(default_factory=list)
    phases_south: list[float] = field(default_factory=list)
    model: str = "FES2014b"  # OTL model name
    source: str = ""  # Source/provider


@dataclass
class StationAbbreviation:
    """Station abbreviation entry for ABB files."""

    station_id: str  # 4-char station code
    abbreviation: str = ""  # 2-char abbreviation
    description: str = ""  # Full station name
    country: str = ""  # Country code


class CRDFileWriter:
    """Writer for Bernese .CRD (coordinate) files.

    Format: Fixed-width columns matching BSW 5.4 specification.
    """

    # Column format specification
    HEADER = """CRD: COORDINATES
--------------------------------------------------------------------------------
LOCAL GEODETIC DATUM: {datum}                 EPOCH: {epoch}

NUM  STATION NAME           X (M)          Y (M)          Z (M)     FLAG

"""

    STATION_FORMAT = "{num:4d}  {name:16s}  {x:14.4f}  {y:14.4f}  {z:14.4f}  {flag:s}\n"

    def __init__(
        self,
        datum: str = "IGS20",
        epoch: str = "2015.0",
    ):
        """Initialize CRD file writer.

        Args:
            datum: Reference frame (e.g., IGS20, ITRF2014)
            epoch: Reference epoch
        """
        self.datum = datum
        self.epoch = epoch
        self._stations: list[StationCoordinate] = []

    def add_station(self, station: StationCoordinate) -> None:
        """Add a station to the file."""
        self._stations.append(station)

    def add_stations(self, stations: list[StationCoordinate]) -> None:
        """Add multiple stations."""
        self._stations.extend(stations)

    def clear(self) -> None:
        """Clear all stations."""
        self._stations.clear()

    def write(self, output_path: Path | str) -> int:
        """Write the CRD file.

        Args:
            output_path: Output file path

        Returns:
            Number of stations written
        """
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        with open(output_path, "w", encoding="utf-8") as f:
            # Write header
            f.write(self.HEADER.format(datum=self.datum, epoch=self.epoch))

            # Write stations
            for i, station in enumerate(self._stations, 1):
                f.write(
                    self.STATION_FORMAT.format(
                        num=i,
                        name=station.station_name.ljust(16),
                        x=station.x,
                        y=station.y,
                        z=station.z,
                        flag=station.flag,
                    )
                )

        logger.info(
            "Wrote CRD file",
            path=str(output_path),
            stations=len(self._stations),
        )

        return len(self._stations)

    @classmethod
    def from_dict_list(
        cls,
        stations: list[dict[str, Any]],
        datum: str = "IGS20",
        epoch: str = "2015.0",
    ) -> "CRDFileWriter":
        """Create writer from list of station dictionaries.

        Args:
            stations: List of station dicts with x, y, z, etc.
            datum: Reference frame
            epoch: Reference epoch

        Returns:
            CRDFileWriter instance
        """
        writer = cls(datum=datum, epoch=epoch)

        for s in stations:
            coord = StationCoordinate(
                station_id=s.get("station_id", s.get("name", "")),
                x=s.get("x", 0.0),
                y=s.get("y", 0.0),
                z=s.get("z", 0.0),
                flag=s.get("flag", "A"),
                description=s.get("description", ""),
            )
            writer.add_station(coord)

        return writer


class OTLFileWriter:
    """Writer for Bernese .OTL (ocean tide loading) files.

    Format: BLQ format compatible with IERS conventions.
    """

    HEADER = """$$  OTL MODEL: {model}
$$  GENERATED: {datetime}
$$
$$ COLUMN ORDER:  M2  S2  N2  K2  K1  O1  P1  Q1  MF  MM  SSA
$$

"""

    def __init__(self, model: str = "FES2014b"):
        """Initialize OTL file writer.

        Args:
            model: OTL model name
        """
        self.model = model
        self._stations: list[OceanTideLoading] = []

    def add_station(self, station: OceanTideLoading) -> None:
        """Add a station to the file."""
        self._stations.append(station)

    def add_stations(self, stations: list[OceanTideLoading]) -> None:
        """Add multiple stations."""
        self._stations.extend(stations)

    def clear(self) -> None:
        """Clear all stations."""
        self._stations.clear()

    def write(self, output_path: Path | str) -> int:
        """Write the OTL file.

        Args:
            output_path: Output file path

        Returns:
            Number of stations written
        """
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        with open(output_path, "w", encoding="utf-8") as f:
            # Write header
            f.write(
                self.HEADER.format(
                    model=self.model,
                    datetime=datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                )
            )

            # Write stations
            for station in self._stations:
                f.write(f"  {station.station_id.upper()}\n")

                # Write amplitudes (3 rows: radial, west, south)
                for amplitudes in [
                    station.amplitudes_radial,
                    station.amplitudes_west,
                    station.amplitudes_south,
                ]:
                    values = amplitudes if len(amplitudes) == 11 else [0.0] * 11
                    f.write("  ")
                    f.write("  ".join(f"{v:8.5f}" for v in values))
                    f.write("\n")

                # Write phases (3 rows: radial, west, south)
                for phases in [
                    station.phases_radial,
                    station.phases_west,
                    station.phases_south,
                ]:
                    values = phases if len(phases) == 11 else [0.0] * 11
                    f.write("  ")
                    f.write("  ".join(f"{v:7.1f}" for v in values))
                    f.write("\n")

        logger.info(
            "Wrote OTL file",
            path=str(output_path),
            stations=len(self._stations),
        )

        return len(self._stations)

    @classmethod
    def from_blq_file(cls, blq_path: Path | str) -> "OTLFileWriter":
        """Create writer by parsing an existing BLQ file.

        Args:
            blq_path: Path to BLQ file

        Returns:
            OTLFileWriter instance with parsed stations
        """
        writer = cls()
        blq_path = Path(blq_path)

        if not blq_path.exists():
            raise FileNotFoundError(f"BLQ file not found: {blq_path}")

        with open(blq_path, "r", encoding="utf-8", errors="ignore") as f:
            lines = f.readlines()

        # Parse BLQ format
        i = 0
        while i < len(lines):
            line = lines[i].strip()

            # Skip comments and empty lines
            if line.startswith("$$") or line.startswith("$") or not line:
                i += 1
                continue

            # Station name line
            if len(line) <= 10 and not line[0].isdigit():
                station_id = line.strip()

                # Read next 6 lines for amplitudes/phases
                try:
                    amp_radial = [float(x) for x in lines[i + 1].split()]
                    amp_west = [float(x) for x in lines[i + 2].split()]
                    amp_south = [float(x) for x in lines[i + 3].split()]
                    phase_radial = [float(x) for x in lines[i + 4].split()]
                    phase_west = [float(x) for x in lines[i + 5].split()]
                    phase_south = [float(x) for x in lines[i + 6].split()]

                    otl = OceanTideLoading(
                        station_id=station_id,
                        amplitudes_radial=amp_radial,
                        amplitudes_west=amp_west,
                        amplitudes_south=amp_south,
                        phases_radial=phase_radial,
                        phases_west=phase_west,
                        phases_south=phase_south,
                    )
                    writer.add_station(otl)
                    i += 7
                except (IndexError, ValueError):
                    i += 1
            else:
                i += 1

        return writer


class ABBFileWriter:
    """Writer for Bernese .ABB (abbreviation) files.

    Format: Station abbreviation mapping file.
    """

    HEADER = """ABB: STATION ABBREVIATIONS
--------------------------------------------------------------------------------
STATION NAME  ABBREV  FULL NAME                      COUNTRY
--------------------------------------------------------------------------------

"""

    STATION_FORMAT = "{name:14s}{abbrev:8s}{full:31s}{country:s}\n"

    def __init__(self):
        """Initialize ABB file writer."""
        self._stations: list[StationAbbreviation] = []

    def add_station(self, station: StationAbbreviation) -> None:
        """Add a station to the file."""
        self._stations.append(station)

    def add_stations(self, stations: list[StationAbbreviation]) -> None:
        """Add multiple stations."""
        self._stations.extend(stations)

    def clear(self) -> None:
        """Clear all stations."""
        self._stations.clear()

    def write(self, output_path: Path | str) -> int:
        """Write the ABB file.

        Args:
            output_path: Output file path

        Returns:
            Number of stations written
        """
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        with open(output_path, "w", encoding="utf-8") as f:
            # Write header
            f.write(self.HEADER)

            # Write stations sorted by name
            for station in sorted(self._stations, key=lambda s: s.station_id.upper()):
                abbrev = station.abbreviation or station.station_id[:2].upper()
                f.write(
                    self.STATION_FORMAT.format(
                        name=station.station_id.upper().ljust(14),
                        abbrev=abbrev.ljust(8),
                        full=station.description[:31].ljust(31),
                        country=station.country[:3] if station.country else "",
                    )
                )

        logger.info(
            "Wrote ABB file",
            path=str(output_path),
            stations=len(self._stations),
        )

        return len(self._stations)


class StationListWriter:
    """Writer for station list files (for BSW processing).

    Creates simple list of station IDs for PCF input.
    """

    def __init__(self):
        """Initialize station list writer."""
        self._stations: list[str] = []

    def add_station(self, station_id: str) -> None:
        """Add a station ID."""
        self._stations.append(station_id.upper()[:4])

    def add_stations(self, station_ids: list[str]) -> None:
        """Add multiple station IDs."""
        for sid in station_ids:
            self.add_station(sid)

    def clear(self) -> None:
        """Clear all stations."""
        self._stations.clear()

    def write(self, output_path: Path | str, one_per_line: bool = True) -> int:
        """Write the station list file.

        Args:
            output_path: Output file path
            one_per_line: If True, one station per line; else space-separated

        Returns:
            Number of stations written
        """
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        # Remove duplicates while preserving order
        unique_stations = list(dict.fromkeys(self._stations))

        with open(output_path, "w", encoding="utf-8") as f:
            if one_per_line:
                for station in unique_stations:
                    f.write(f"{station}\n")
            else:
                f.write(" ".join(unique_stations))
                f.write("\n")

        logger.info(
            "Wrote station list",
            path=str(output_path),
            stations=len(unique_stations),
        )

        return len(unique_stations)


class VELFileWriter:
    """Writer for Bernese .VEL (velocity) files.

    Format: Station velocity file for plate motion.
    """

    HEADER = """VEL: VELOCITIES
--------------------------------------------------------------------------------
LOCAL GEODETIC DATUM: {datum}                 EPOCH: {epoch}
REFERENCE FRAME: {frame}

NUM  STATION NAME           VX (M/Y)       VY (M/Y)       VZ (M/Y)    FLAG

"""

    STATION_FORMAT = "{num:4d}  {name:16s}  {vx:14.6f}  {vy:14.6f}  {vz:14.6f}  {flag:s}\n"

    def __init__(
        self,
        datum: str = "IGS20",
        epoch: str = "2015.0",
        frame: str = "ITRF2020",
    ):
        """Initialize VEL file writer.

        Args:
            datum: Reference frame
            epoch: Reference epoch
            frame: Velocity reference frame
        """
        self.datum = datum
        self.epoch = epoch
        self.frame = frame
        self._stations: list[dict[str, Any]] = []

    def add_station(
        self,
        station_id: str,
        vx: float,
        vy: float,
        vz: float,
        flag: str = "A",
    ) -> None:
        """Add a station velocity.

        Args:
            station_id: 4-char station code
            vx: X velocity (m/year)
            vy: Y velocity (m/year)
            vz: Z velocity (m/year)
            flag: Station flag
        """
        self._stations.append({
            "station_id": station_id.upper()[:4],
            "vx": vx,
            "vy": vy,
            "vz": vz,
            "flag": flag,
        })

    def clear(self) -> None:
        """Clear all stations."""
        self._stations.clear()

    def write(self, output_path: Path | str) -> int:
        """Write the VEL file.

        Args:
            output_path: Output file path

        Returns:
            Number of stations written
        """
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        with open(output_path, "w", encoding="utf-8") as f:
            # Write header
            f.write(
                self.HEADER.format(
                    datum=self.datum,
                    epoch=self.epoch,
                    frame=self.frame,
                )
            )

            # Write stations
            for i, station in enumerate(self._stations, 1):
                f.write(
                    self.STATION_FORMAT.format(
                        num=i,
                        name=station["station_id"].ljust(16),
                        vx=station["vx"],
                        vy=station["vy"],
                        vz=station["vz"],
                        flag=station["flag"],
                    )
                )

        logger.info(
            "Wrote VEL file",
            path=str(output_path),
            stations=len(self._stations),
        )

        return len(self._stations)


def write_crd_file(
    stations: list[dict[str, Any]],
    output_path: Path | str,
    datum: str = "IGS20",
    epoch: str = "2015.0",
) -> int:
    """Convenience function to write a CRD file.

    Args:
        stations: List of station dicts with x, y, z coordinates
        output_path: Output file path
        datum: Reference datum
        epoch: Reference epoch

    Returns:
        Number of stations written
    """
    writer = CRDFileWriter.from_dict_list(stations, datum=datum, epoch=epoch)
    return writer.write(output_path)


def write_station_list(
    station_ids: list[str],
    output_path: Path | str,
) -> int:
    """Convenience function to write a station list file.

    Args:
        station_ids: List of station IDs
        output_path: Output file path

    Returns:
        Number of stations written
    """
    writer = StationListWriter()
    writer.add_stations(station_ids)
    return writer.write(output_path)


# =============================================================================
# CRD File Reader and Converters
# Replaces Perl crd2otl.pl and crd2staXml.pl utilities
# =============================================================================

class CRDFileReader:
    """Reader for Bernese .CRD coordinate files.

    Parses CRD files and extracts station coordinates.
    """

    def __init__(self):
        """Initialize CRD reader."""
        self._stations: list[StationCoordinate] = []
        self._datum: str = ""
        self._epoch: str = ""

    @classmethod
    def from_file(cls, filepath: Path | str) -> "CRDFileReader":
        """Read CRD file.

        Args:
            filepath: Path to CRD file

        Returns:
            CRDFileReader instance with parsed data
        """
        reader = cls()
        reader.parse(filepath)
        return reader

    def parse(self, filepath: Path | str) -> list[StationCoordinate]:
        """Parse CRD file.

        Args:
            filepath: Path to CRD file

        Returns:
            List of StationCoordinate objects
        """
        filepath = Path(filepath)

        if not filepath.exists():
            raise FileNotFoundError(f"CRD file not found: {filepath}")

        stations = []

        with open(filepath, "r", encoding="utf-8", errors="ignore") as f:
            for line_num, line in enumerate(f, 1):
                # Skip header lines (first 6 lines typically)
                if line_num <= 6:
                    # Try to extract datum and epoch from header
                    if "DATUM:" in line.upper():
                        parts = line.split(":")
                        if len(parts) >= 2:
                            self._datum = parts[1].split()[0].strip()
                    if "EPOCH:" in line.upper():
                        parts = line.split("EPOCH:")
                        if len(parts) >= 2:
                            self._epoch = parts[1].split()[0].strip()
                    continue

                # Clean up line
                line = line.strip()
                if not line:
                    continue

                # Skip lines without letters (station names have letters)
                if not any(c.isalpha() for c in line):
                    continue

                # Parse station line
                parts = line.split()
                if len(parts) < 5:
                    continue

                try:
                    # Format: NUM STATION X Y Z [FLAG]
                    # or: NUM STATION DESC X Y Z [FLAG]
                    station_id = parts[1]

                    # Find the first numeric field (X coordinate)
                    x_idx = -1
                    for i, p in enumerate(parts):
                        try:
                            float(p)
                            if abs(float(p)) > 1000:  # Coordinates are > 1000m
                                x_idx = i
                                break
                        except ValueError:
                            continue

                    if x_idx == -1 or x_idx + 2 >= len(parts):
                        continue

                    x = float(parts[x_idx])
                    y = float(parts[x_idx + 1])
                    z = float(parts[x_idx + 2])

                    # Check for flag at end
                    flag = "A"
                    if x_idx + 3 < len(parts):
                        potential_flag = parts[x_idx + 3]
                        if len(potential_flag) == 1 and potential_flag.isalpha():
                            flag = potential_flag

                    station = StationCoordinate(
                        station_id=station_id,
                        x=x,
                        y=y,
                        z=z,
                        flag=flag,
                    )
                    stations.append(station)

                except (ValueError, IndexError):
                    continue

        self._stations = stations

        logger.info(
            "Read CRD file",
            path=str(filepath),
            stations=len(stations),
        )

        return stations

    @property
    def stations(self) -> list[StationCoordinate]:
        """Get parsed stations."""
        return self._stations

    @property
    def datum(self) -> str:
        """Get reference datum."""
        return self._datum

    @property
    def epoch(self) -> str:
        """Get reference epoch."""
        return self._epoch


def crd_to_otl(
    crd_path: Path | str,
    otl_path: Path | str | None = None,
) -> int:
    """Convert CRD file to OTL format for ocean tide loading requests.

    Replaces Perl crd2otl.pl utility.

    Extracts station coordinates from CRD file and writes them in
    a format suitable for ocean tide loading coefficient requests.

    Args:
        crd_path: Path to input CRD file
        otl_path: Path to output OTL file (defaults to .OTL extension)

    Returns:
        Number of stations converted
    """
    crd_path = Path(crd_path)

    if otl_path is None:
        otl_path = crd_path.with_suffix(".OTL")
    else:
        otl_path = Path(otl_path)

    # Read CRD file
    reader = CRDFileReader.from_file(crd_path)
    stations = reader.stations

    if not stations:
        logger.warning("No stations found in CRD file", path=str(crd_path))
        return 0

    # Write OTL format
    otl_path.parent.mkdir(parents=True, exist_ok=True)

    with open(otl_path, "w", encoding="utf-8") as f:
        for station in stations:
            # Format: STATION_NAME (23 chars)  X (14.3f)  Y (14.3f)  Z (14.3f)
            f.write(
                f"{station.station_name:23s} {station.x:14.3f}  {station.y:14.3f} {station.z:14.3f}\n"
            )

    logger.info(
        "Converted CRD to OTL format",
        input=str(crd_path),
        output=str(otl_path),
        stations=len(stations),
    )

    return len(stations)


@dataclass
class StationXMLEntry:
    """Station entry for XML station configuration file."""

    four_char_name: str
    two_char_name: str = "XX"
    full_name: str = ""
    approximate_x: float = 0.0
    approximate_y: float = 0.0
    approximate_z: float = 0.0
    country: str = "XX"
    primary_net: str = "EUREF"
    provider: str = "EUREF"
    use_nrt: bool = True
    station_type: str = "EUREF"
    gmt_justify: str = "MC"
    gmt_dopt: str = "D0/0.2"

    def to_xml(self, indent: int = 8) -> str:
        """Convert to XML string.

        Args:
            indent: Number of spaces for indentation

        Returns:
            XML string representation
        """
        sp = " " * indent
        sp2 = " " * (indent + 8)

        use_nrt_str = "yes" if self.use_nrt else "no"

        xml = f"""{sp}<station>
{sp2}<fourCharName>{self.four_char_name}</fourCharName>
{sp2}<twoCharName>{self.two_char_name}</twoCharName>
{sp2}<approximate_X>{self.approximate_x}</approximate_X>
{sp2}<approximate_Y>{self.approximate_y}</approximate_Y>
{sp2}<approximate_Z>{self.approximate_z}</approximate_Z>
{sp2}<country>{self.country}</country>
{sp2}<primaryNet>{self.primary_net}</primaryNet>
{sp2}<provider>{self.provider}</provider>
{sp2}<use_nrt>{use_nrt_str}</use_nrt>
{sp2}<type>{self.station_type}</type>
{sp2}<GMT_opt>
{sp2}  <justify>{self.gmt_justify}</justify>
{sp2}  <Dopt>{self.gmt_dopt}</Dopt>
{sp2}</GMT_opt>
{sp}</station>"""

        return xml


class StationXMLWriter:
    """Writer for i-GNSS station XML configuration files.

    Creates XML configuration blocks for stations to be used in
    the i-GNSS processing system.
    """

    def __init__(
        self,
        primary_net: str = "EUREF",
        provider: str = "EUREF",
        station_type: str = "EUREF",
        country: str = "XX",
    ):
        """Initialize station XML writer.

        Args:
            primary_net: Default primary network
            provider: Default provider
            station_type: Default station type
            country: Default country code
        """
        self.primary_net = primary_net
        self.provider = provider
        self.station_type = station_type
        self.country = country
        self._entries: list[StationXMLEntry] = []

    def add_station(self, entry: StationXMLEntry) -> None:
        """Add a station entry."""
        self._entries.append(entry)

    def add_from_coordinate(self, station: StationCoordinate) -> None:
        """Add station from coordinate data.

        Args:
            station: StationCoordinate object
        """
        entry = StationXMLEntry(
            four_char_name=station.station_name,
            approximate_x=station.x,
            approximate_y=station.y,
            approximate_z=station.z,
            primary_net=self.primary_net,
            provider=self.provider,
            station_type=self.station_type,
            country=self.country,
        )
        self._entries.append(entry)

    def clear(self) -> None:
        """Clear all entries."""
        self._entries.clear()

    def write(self, output_path: Path | str) -> int:
        """Write station XML file.

        Args:
            output_path: Output file path

        Returns:
            Number of stations written
        """
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        with open(output_path, "w", encoding="utf-8") as f:
            f.write('<?xml version="1.0" encoding="UTF-8"?>\n')
            f.write("<stations>\n")

            for entry in self._entries:
                f.write(entry.to_xml())
                f.write("\n")

            f.write("</stations>\n")

        logger.info(
            "Wrote station XML file",
            path=str(output_path),
            stations=len(self._entries),
        )

        return len(self._entries)

    def to_xml_string(self) -> str:
        """Generate XML string without file I/O.

        Returns:
            Complete XML string
        """
        lines = ['<?xml version="1.0" encoding="UTF-8"?>', "<stations>"]

        for entry in self._entries:
            lines.append(entry.to_xml())

        lines.append("</stations>")

        return "\n".join(lines)


def crd_to_station_xml(
    crd_path: Path | str,
    xml_path: Path | str | None = None,
    primary_net: str = "EUREF",
    provider: str = "EUREF",
    station_type: str = "EUREF",
    country: str = "XX",
) -> int:
    """Convert CRD file to station XML configuration.

    Replaces Perl crd2staXml.pl utility.

    Reads stations from a Bernese CRD file and creates XML configuration
    blocks for the i-GNSS system.

    Args:
        crd_path: Path to input CRD file
        xml_path: Path to output XML file (defaults to .xml extension)
        primary_net: Network name
        provider: Provider name
        station_type: Station type
        country: Country code

    Returns:
        Number of stations converted
    """
    crd_path = Path(crd_path)

    if xml_path is None:
        xml_path = crd_path.with_suffix(".xml")
    else:
        xml_path = Path(xml_path)

    # Read CRD file
    reader = CRDFileReader.from_file(crd_path)
    stations = reader.stations

    if not stations:
        logger.warning("No stations found in CRD file", path=str(crd_path))
        return 0

    # Create XML writer
    writer = StationXMLWriter(
        primary_net=primary_net,
        provider=provider,
        station_type=station_type,
        country=country,
    )

    for station in stations:
        writer.add_from_coordinate(station)

    # Write XML file
    return writer.write(xml_path)


def print_station_xml_blocks(
    crd_path: Path | str,
    primary_net: str = "EUREF",
    provider: str = "EUREF",
    station_type: str = "EUREF",
) -> str:
    """Generate station XML blocks for console output.

    Useful for copy-pasting into existing XML configuration files.

    Args:
        crd_path: Path to input CRD file
        primary_net: Network name
        provider: Provider name
        station_type: Station type

    Returns:
        XML string with station blocks
    """
    crd_path = Path(crd_path)

    # Read CRD file
    reader = CRDFileReader.from_file(crd_path)
    stations = reader.stations

    if not stations:
        return ""

    # Generate XML blocks
    blocks = []
    for station in stations:
        entry = StationXMLEntry(
            four_char_name=station.station_name,
            approximate_x=station.x,
            approximate_y=station.y,
            approximate_z=station.z,
            primary_net=primary_net,
            provider=provider,
            station_type=station_type,
        )
        blocks.append(entry.to_xml())

    return "\n".join(blocks)
