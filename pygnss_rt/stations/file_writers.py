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
