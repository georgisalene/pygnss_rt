"""
Bernese GNSS Software Output File Parsers.

Parsers for reading and extracting data from BSW output files:
- TRO files: SINEX-format troposphere delay estimates (ZTD/TROTOT)
- CRD files: Coordinate files with station positions

These parsers enable automated extraction of processing results
for database storage and output generation.

Usage:
    from pygnss_rt.bsw.parsers import TROParser, CRDParser

    # Parse troposphere file
    tro_parser = TROParser()
    ztd_records = tro_parser.parse('/path/to/file.TRO')

    # Parse coordinate file
    crd_parser = CRDParser()
    coordinates = crd_parser.parse('/path/to/file.CRD')
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Iterator


# =============================================================================
# TRO (Troposphere) File Parser
# =============================================================================

@dataclass
class TROHeader:
    """Header information from a TRO file.

    Attributes:
        format_version: SINEX TRO format version (e.g., '0.01')
        agency: Agency code
        file_epoch: File creation epoch
        start_epoch: Data start epoch
        end_epoch: Data end epoch
        technique: Observation technique (e.g., 'P' for GPS/GNSS)
        observation_type: Observation type (e.g., 'MIX')
        description: File description
        software: Software used
        sampling_interval: Data sampling interval (seconds)
        sampling_trop: Troposphere sampling interval (seconds)
        mapping_function: Wet mapping function used
        elevation_cutoff: Elevation cutoff angle (degrees)
    """

    format_version: str = ""
    agency: str = ""
    file_epoch: datetime | None = None
    start_epoch: datetime | None = None
    end_epoch: datetime | None = None
    technique: str = ""
    observation_type: str = ""
    description: str = ""
    software: str = ""
    sampling_interval: int = 0
    sampling_trop: int = 0
    mapping_function: str = ""
    elevation_cutoff: float = 0.0


@dataclass
class TROStation:
    """Station coordinate from TRO file TROP/STA_COORDINATES block.

    Attributes:
        site: 4-character station ID
        point: Point ID (typically 'A')
        solution: Solution number
        technique: Technique code
        x: X coordinate (meters)
        y: Y coordinate (meters)
        z: Z coordinate (meters)
        system: Reference system (e.g., 'IGS14', 'IGS20')
        remark: Additional remarks
    """

    site: str
    point: str = "A"
    solution: int = 1
    technique: str = "P"
    x: float = 0.0
    y: float = 0.0
    z: float = 0.0
    system: str = ""
    remark: str = ""


@dataclass
class TRORecord:
    """A single troposphere estimate record.

    Represents one ZTD (TROTOT) estimate from a TRO file.

    Attributes:
        site: 4-character station ID
        epoch: Observation epoch (datetime)
        epoch_seconds: Seconds of day for the epoch
        trotot: Total tropospheric delay in mm
        stddev: Standard deviation in mm
        year: Year
        doy: Day of year
    """

    site: str
    epoch: datetime
    epoch_seconds: int
    trotot: float
    stddev: float
    year: int = 0
    doy: int = 0

    def __post_init__(self):
        """Set year and DOY from epoch if not provided."""
        if self.year == 0 and self.epoch:
            self.year = self.epoch.year
            self.doy = self.epoch.timetuple().tm_yday


@dataclass
class TROFile:
    """Parsed TRO file contents.

    Attributes:
        path: Source file path
        header: File header information
        stations: Station coordinates
        records: Troposphere estimate records
    """

    path: Path
    header: TROHeader = field(default_factory=TROHeader)
    stations: list[TROStation] = field(default_factory=list)
    records: list[TRORecord] = field(default_factory=list)

    @property
    def station_ids(self) -> list[str]:
        """Get unique station IDs."""
        return list(set(r.site for r in self.records))

    @property
    def n_records(self) -> int:
        """Get total number of records."""
        return len(self.records)

    @property
    def n_stations(self) -> int:
        """Get number of unique stations."""
        return len(self.station_ids)

    def get_station_records(self, site: str) -> list[TRORecord]:
        """Get all records for a specific station."""
        return [r for r in self.records if r.site.upper() == site.upper()]


class TROParser:
    """Parser for SINEX TRO (troposphere) files.

    Reads and parses BSW troposphere output files containing ZTD estimates.
    Supports both SINEX TRO 0.01 and 1.00 formats.

    Usage:
        parser = TROParser()
        tro_file = parser.parse('/path/to/file.TRO')

        for record in tro_file.records:
            print(f"{record.site}: ZTD={record.trotot} +/- {record.stddev} mm")
    """

    # Regex patterns for parsing
    HEADER_PATTERN = re.compile(
        r'^%=TRO\s+(\S+)\s+\S+\s+(\d+:\d+:\d+)\s+(\S+)\s+(\d+:\d+:\d+)\s+(\d+:\d+:\d+)\s+(\S)\s+(\S+)'
    )
    TROP_RECORD_PATTERN = re.compile(
        r'^\s+(\S{4})\s+(\d+):(\d+):(\d+)\s+([\d.]+)\s+([\d.]+)'
    )
    COORD_PATTERN = re.compile(
        r'^\s+(\S{4})\s+(\S)\s+(\d+)\s+(\S)\s+([-\d.]+)\s+([-\d.]+)\s+([-\d.]+)\s+(\S+)\s*(\S*)'
    )

    def __init__(self, verbose: bool = False):
        """Initialize parser.

        Args:
            verbose: Enable verbose output
        """
        self.verbose = verbose

    def parse(self, path: Path | str) -> TROFile:
        """Parse a TRO file.

        Args:
            path: Path to TRO file

        Returns:
            Parsed TROFile object

        Raises:
            FileNotFoundError: If file doesn't exist
            ValueError: If file format is invalid
        """
        path = Path(path)
        if not path.exists():
            raise FileNotFoundError(f"TRO file not found: {path}")

        tro_file = TROFile(path=path)

        # Read and parse file
        in_coords = False
        in_solution = False

        with open(path, 'r') as f:
            for line in f:
                line = line.rstrip('\n\r')

                # Parse header line
                if line.startswith('%=TRO'):
                    tro_file.header = self._parse_header_line(line)
                    continue

                # Track block boundaries
                if line.startswith('+TROP/STA_COORDINATES'):
                    in_coords = True
                    continue
                if line.startswith('-TROP/STA_COORDINATES'):
                    in_coords = False
                    continue
                if line.startswith('+TROP/SOLUTION'):
                    in_solution = True
                    continue
                if line.startswith('-TROP/SOLUTION'):
                    in_solution = False
                    continue

                # Skip comment lines
                if line.startswith('*') or line.startswith('#'):
                    continue

                # Parse coordinate lines
                if in_coords:
                    station = self._parse_coord_line(line)
                    if station:
                        tro_file.stations.append(station)
                    continue

                # Parse solution records
                if in_solution:
                    record = self._parse_solution_line(line)
                    if record:
                        tro_file.records.append(record)
                    continue

                # Parse FILE/REFERENCE block
                if 'SAMPLING INTERVAL' in line:
                    tro_file.header.sampling_interval = self._extract_int_value(line)
                elif 'SAMPLING TROP' in line:
                    tro_file.header.sampling_trop = self._extract_int_value(line)
                elif 'ELEVATION CUTOFF' in line:
                    tro_file.header.elevation_cutoff = self._extract_float_value(line)
                elif 'TROP MAPPING FUNCTION' in line:
                    tro_file.header.mapping_function = line[30:].strip()
                elif ' DESCRIPTION ' in line:
                    tro_file.header.description = line[19:].strip()
                elif ' SOFTWARE ' in line:
                    tro_file.header.software = line[19:].strip()

        if self.verbose:
            print(f"Parsed {tro_file.n_records} records from {tro_file.n_stations} stations")

        return tro_file

    def _parse_header_line(self, line: str) -> TROHeader:
        """Parse TRO header line.

        Format: %=TRO VER XYZ YY:DOY:SOD AGN YY:DOY:SOD YY:DOY:SOD T TYPE
        """
        header = TROHeader()

        match = self.HEADER_PATTERN.match(line)
        if match:
            header.format_version = match.group(1)
            header.file_epoch = self._parse_epoch(match.group(2))
            header.agency = match.group(3)
            header.start_epoch = self._parse_epoch(match.group(4))
            header.end_epoch = self._parse_epoch(match.group(5))
            header.technique = match.group(6)
            header.observation_type = match.group(7)

        return header

    def _parse_coord_line(self, line: str) -> TROStation | None:
        """Parse coordinate line from TROP/STA_COORDINATES block.

        Format: SITE PT SOLN T __STA_X_____ __STA_Y_____ __STA_Z_____ SYSTEM REMRK
        """
        match = self.COORD_PATTERN.match(line)
        if not match:
            return None

        return TROStation(
            site=match.group(1),
            point=match.group(2),
            solution=int(match.group(3)),
            technique=match.group(4),
            x=float(match.group(5)),
            y=float(match.group(6)),
            z=float(match.group(7)),
            system=match.group(8),
            remark=match.group(9) if match.group(9) else "",
        )

    def _parse_solution_line(self, line: str) -> TRORecord | None:
        """Parse solution line from TROP/SOLUTION block.

        Format: SITE YY:DOY:SOD TROTOT STDDEV
        """
        match = self.TROP_RECORD_PATTERN.match(line)
        if not match:
            return None

        site = match.group(1)
        year_2d = int(match.group(2))
        doy = int(match.group(3))
        sod = int(match.group(4))
        trotot = float(match.group(5))
        stddev = float(match.group(6))

        # Convert 2-digit year
        year = 2000 + year_2d if year_2d < 80 else 1900 + year_2d

        # Convert to datetime
        epoch = datetime(year, 1, 1) + timedelta(days=doy - 1, seconds=sod)

        return TRORecord(
            site=site,
            epoch=epoch,
            epoch_seconds=sod,
            trotot=trotot,
            stddev=stddev,
            year=year,
            doy=doy,
        )

    def _parse_epoch(self, epoch_str: str) -> datetime | None:
        """Parse epoch string YY:DOY:SOD to datetime."""
        try:
            parts = epoch_str.split(':')
            year_2d = int(parts[0])
            doy = int(parts[1])
            sod = int(parts[2])

            year = 2000 + year_2d if year_2d < 80 else 1900 + year_2d
            return datetime(year, 1, 1) + timedelta(days=doy - 1, seconds=sod)
        except (ValueError, IndexError):
            return None

    def _extract_int_value(self, line: str) -> int:
        """Extract integer value from description line."""
        try:
            # Values typically start at column 30
            value_str = line[30:].strip().split()[0]
            return int(value_str)
        except (ValueError, IndexError):
            return 0

    def _extract_float_value(self, line: str) -> float:
        """Extract float value from description line."""
        try:
            value_str = line[30:].strip().split()[0]
            return float(value_str)
        except (ValueError, IndexError):
            return 0.0

    def iter_records(self, path: Path | str) -> Iterator[TRORecord]:
        """Iterate over TRO records without loading entire file.

        Memory-efficient iterator for large files.

        Args:
            path: Path to TRO file

        Yields:
            TRORecord objects
        """
        path = Path(path)
        in_solution = False

        with open(path, 'r') as f:
            for line in f:
                line = line.rstrip('\n\r')

                if line.startswith('+TROP/SOLUTION'):
                    in_solution = True
                    continue
                if line.startswith('-TROP/SOLUTION'):
                    break

                if in_solution and not line.startswith('*'):
                    record = self._parse_solution_line(line)
                    if record:
                        yield record


# =============================================================================
# CRD (Coordinate) File Parser
# =============================================================================

@dataclass
class CRDHeader:
    """Header information from a CRD file.

    Attributes:
        creation_date: File creation date
        datum: Geodetic datum name
        epoch: Reference epoch
    """

    creation_date: datetime | None = None
    datum: str = ""
    epoch: str = ""


@dataclass
class CRDRecord:
    """A single coordinate record from a CRD file.

    Attributes:
        num: Station number in file
        station: Station name (4 characters for BSW)
        domes: DOMES number (if present)
        x: X coordinate (meters)
        y: Y coordinate (meters)
        z: Z coordinate (meters)
        flag: Coordinate flag/source
        system: Reference system
        antenna: Antenna type (from extended format)
        radome: Radome type
        receiver: Receiver type
    """

    num: int
    station: str
    domes: str = ""
    x: float = 0.0
    y: float = 0.0
    z: float = 0.0
    flag: str = ""
    system: str = ""
    antenna: str = ""
    radome: str = ""
    receiver: str = ""

    @property
    def station_id(self) -> str:
        """Get 4-character station ID."""
        return self.station[:4].upper()


@dataclass
class CRDFile:
    """Parsed CRD file contents.

    Attributes:
        path: Source file path
        header: File header
        records: Coordinate records
    """

    path: Path
    header: CRDHeader = field(default_factory=CRDHeader)
    records: list[CRDRecord] = field(default_factory=list)

    @property
    def station_ids(self) -> list[str]:
        """Get list of station IDs."""
        return [r.station_id for r in self.records]

    @property
    def n_stations(self) -> int:
        """Get number of stations."""
        return len(self.records)

    def get_station(self, station: str) -> CRDRecord | None:
        """Get coordinate record for a station.

        Args:
            station: Station ID (case-insensitive)

        Returns:
            CRDRecord if found, None otherwise
        """
        station_upper = station.upper()
        for r in self.records:
            if r.station_id == station_upper or r.station.upper().startswith(station_upper):
                return r
        return None


class CRDParser:
    """Parser for Bernese CRD (coordinate) files.

    Supports multiple CRD formats:
    - Standard BSW format with header
    - Extended format with antenna/receiver info
    - Simple format with comments

    Usage:
        parser = CRDParser()
        crd_file = parser.parse('/path/to/file.CRD')

        for record in crd_file.records:
            print(f"{record.station}: X={record.x:.3f} Y={record.y:.3f} Z={record.z:.3f}")
    """

    # Regex patterns
    # Standard BSW format: NUM  STATION NAME           X (M)          Y (M)          Z (M)     FLAG
    BSW_COORD_PATTERN = re.compile(
        r'^\s*(\d+)\s+(\S+)\s+(\S+)\s+([-\d.]+)\s+([-\d.]+)\s+([-\d.]+)\s+(\S*)'
    )

    # Extended format with eccentricities: STATION X Y Z ECC_N ECC_E ECC_U ANTENNA RADOME RECEIVER
    EXTENDED_PATTERN = re.compile(
        r'^(\S+)\s+([-\d.]+)\s+([-\d.]+)\s+([-\d.]+)\s+([-\d.]+)\s+([-\d.]+)\s+([-\d.]+)\s+(\S+)\s+(\S+)\s*(.*)?$'
    )

    # Datum line pattern
    DATUM_PATTERN = re.compile(
        r'LOCAL GEODETIC DATUM:\s+(\S+)\s+EPOCH:\s+(.+)$'
    )

    def __init__(self, verbose: bool = False):
        """Initialize parser.

        Args:
            verbose: Enable verbose output
        """
        self.verbose = verbose

    def parse(self, path: Path | str) -> CRDFile:
        """Parse a CRD file.

        Args:
            path: Path to CRD file

        Returns:
            Parsed CRDFile object

        Raises:
            FileNotFoundError: If file doesn't exist
        """
        path = Path(path)
        if not path.exists():
            raise FileNotFoundError(f"CRD file not found: {path}")

        crd_file = CRDFile(path=path)

        with open(path, 'r') as f:
            lines = f.readlines()

        # Detect format and parse accordingly
        if self._is_bsw_format(lines):
            self._parse_bsw_format(lines, crd_file)
        else:
            self._parse_extended_format(lines, crd_file)

        if self.verbose:
            print(f"Parsed {crd_file.n_stations} stations from {path}")

        return crd_file

    def _is_bsw_format(self, lines: list[str]) -> bool:
        """Check if file is standard BSW format."""
        for line in lines[:10]:
            if 'LOCAL GEODETIC DATUM' in line:
                return True
            if 'NUM  STATION' in line:
                return True
        return False

    def _parse_bsw_format(self, lines: list[str], crd_file: CRDFile) -> None:
        """Parse standard BSW CRD format."""
        in_data = False

        for line in lines:
            line = line.rstrip('\n\r')

            # Parse header date (first line often has date)
            if not crd_file.header.creation_date and line.strip():
                date_match = re.search(r'(\d{2})-([A-Z]{3})-(\d{2})\s+(\d{2}:\d{2})', line)
                if date_match:
                    try:
                        date_str = f"{date_match.group(1)}-{date_match.group(2)}-{date_match.group(3)}"
                        crd_file.header.creation_date = datetime.strptime(date_str, "%d-%b-%y")
                    except ValueError:
                        pass

            # Parse datum line
            datum_match = self.DATUM_PATTERN.search(line)
            if datum_match:
                crd_file.header.datum = datum_match.group(1)
                crd_file.header.epoch = datum_match.group(2).strip()
                continue

            # Detect start of coordinate data
            if 'NUM  STATION' in line:
                in_data = True
                continue

            # Skip empty or comment lines
            if not line.strip() or line.startswith('#') or line.startswith('-'):
                continue

            # Parse coordinate line
            if in_data or (line.strip() and line[0].isdigit()):
                record = self._parse_bsw_coord_line(line)
                if record:
                    crd_file.records.append(record)

    def _parse_extended_format(self, lines: list[str], crd_file: CRDFile) -> None:
        """Parse extended CRD format with antenna/receiver info."""
        num = 0

        for line in lines:
            line = line.rstrip('\n\r')

            # Skip comments
            if line.startswith('#') or not line.strip():
                continue

            # Parse coordinate line
            match = self.EXTENDED_PATTERN.match(line)
            if match:
                num += 1
                station = match.group(1)
                extra = match.group(10) if match.group(10) else ""

                # Parse extra field (may contain receiver type)
                receiver = ""
                if extra:
                    receiver = extra.strip()

                record = CRDRecord(
                    num=num,
                    station=station,
                    x=float(match.group(2)),
                    y=float(match.group(3)),
                    z=float(match.group(4)),
                    antenna=match.group(8),
                    radome=match.group(9),
                    receiver=receiver,
                )
                crd_file.records.append(record)

    def _parse_bsw_coord_line(self, line: str) -> CRDRecord | None:
        """Parse BSW format coordinate line.

        Format: NUM  STATION DOMES       X (M)          Y (M)          Z (M)     FLAG SYSTEM
        """
        # Try standard BSW format
        match = self.BSW_COORD_PATTERN.match(line)
        if match:
            # Check if it's a valid coordinate (not header)
            try:
                x = float(match.group(4))
                y = float(match.group(5))
                z = float(match.group(6))
            except ValueError:
                return None

            return CRDRecord(
                num=int(match.group(1)),
                station=match.group(2),
                domes=match.group(3),
                x=x,
                y=y,
                z=z,
                flag=match.group(7) if match.group(7) else "",
            )

        return None

    def iter_stations(self, path: Path | str) -> Iterator[CRDRecord]:
        """Iterate over CRD records without loading entire file.

        Args:
            path: Path to CRD file

        Yields:
            CRDRecord objects
        """
        path = Path(path)
        num = 0
        in_data = False

        with open(path, 'r') as f:
            for line in f:
                line = line.rstrip('\n\r')

                if 'NUM  STATION' in line:
                    in_data = True
                    continue

                if not line.strip() or line.startswith('#'):
                    continue

                if in_data or (line.strip() and line[0].isdigit()):
                    record = self._parse_bsw_coord_line(line)
                    if record:
                        yield record


# =============================================================================
# Convenience Functions
# =============================================================================

def parse_tro_file(path: Path | str, verbose: bool = False) -> TROFile:
    """Parse a TRO file.

    Convenience function for parsing troposphere files.

    Args:
        path: Path to TRO file
        verbose: Enable verbose output

    Returns:
        Parsed TROFile object
    """
    parser = TROParser(verbose=verbose)
    return parser.parse(path)


def parse_crd_file(path: Path | str, verbose: bool = False) -> CRDFile:
    """Parse a CRD file.

    Convenience function for parsing coordinate files.

    Args:
        path: Path to CRD file
        verbose: Enable verbose output

    Returns:
        Parsed CRDFile object
    """
    parser = CRDParser(verbose=verbose)
    return parser.parse(path)


def extract_ztd_values(
    tro_path: Path | str,
    stations: list[str] | None = None,
) -> dict[str, list[tuple[datetime, float, float]]]:
    """Extract ZTD values from TRO file.

    Convenience function to get ZTD values organized by station.

    Args:
        tro_path: Path to TRO file
        stations: List of stations to extract (None for all)

    Returns:
        Dictionary mapping station ID to list of (epoch, ztd, stddev) tuples
    """
    tro = parse_tro_file(tro_path)
    result: dict[str, list[tuple[datetime, float, float]]] = {}

    stations_upper = None
    if stations:
        stations_upper = {s.upper() for s in stations}

    for record in tro.records:
        if stations_upper and record.site.upper() not in stations_upper:
            continue

        site = record.site.upper()
        if site not in result:
            result[site] = []

        result[site].append((record.epoch, record.trotot, record.stddev))

    return result


def extract_coordinates(
    crd_path: Path | str,
    stations: list[str] | None = None,
) -> dict[str, tuple[float, float, float]]:
    """Extract coordinates from CRD file.

    Convenience function to get XYZ coordinates by station.

    Args:
        crd_path: Path to CRD file
        stations: List of stations to extract (None for all)

    Returns:
        Dictionary mapping station ID to (X, Y, Z) tuple
    """
    crd = parse_crd_file(crd_path)
    result: dict[str, tuple[float, float, float]] = {}

    stations_upper = None
    if stations:
        stations_upper = {s.upper() for s in stations}

    for record in crd.records:
        station_id = record.station_id

        if stations_upper and station_id not in stations_upper:
            continue

        result[station_id] = (record.x, record.y, record.z)

    return result
