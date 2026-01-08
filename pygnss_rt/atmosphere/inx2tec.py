"""
IONEX to TEC conversion utilities.

Converts IONEX (IONosphere EXchange) format files to TEC (Total Electron Content)
values in COST-716 format for distribution.

Replaces Perl INX2TEC.pm module.

Authors: Original Perl by Etienne J. Orliac and Richard M. Bingley (IESSG, University of Nottingham)
         Python conversion for pygnss_rt
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Optional

from pygnss_rt.utils.dates import GNSSDate
from pygnss_rt.utils.format import hour_to_alpha, alpha_to_hour
from pygnss_rt.utils.logging import get_logger


logger = get_logger(__name__)


# IONEX grid constants
# Standard global IONEX grid: 71 latitude bands x 72 longitude points = 5112 grid points
IONEX_LAT_START = 87.5      # Starting latitude (degrees)
IONEX_LAT_END = -87.5       # Ending latitude (degrees)
IONEX_LAT_STEP = -2.5       # Latitude step (degrees)
IONEX_LON_START = -180.0    # Starting longitude (degrees)
IONEX_LON_END = 180.0       # Ending longitude (degrees)
IONEX_LON_STEP = 5.0        # Longitude step (degrees)
IONEX_NUM_LAT = 71          # Number of latitude bands
IONEX_NUM_LON = 72          # Number of longitude points
IONEX_TOTAL_POINTS = 5112   # Total grid points

# Default values for COST-716 output
DEFAULT_ZTD = -9.9
DEFAULT_RMS = -9.9
DEFAULT_ZWD = -9.9
DEFAULT_IWV = -9.9
DEFAULT_PRESSURE = -9.9
DEFAULT_TEMPERATURE = -9.9
DEFAULT_HUMIDITY = -9.9
DEFAULT_GRADIENT = 999.99
DEFAULT_GRADIENT_ERROR = -9.99
DEFAULT_TEC = -99.999

# European region bounds for sub-daily filtering
EUROPE_LAT_MIN = 35.0
EUROPE_LAT_MAX = 75.0
EUROPE_LON_MIN = 350.0  # Wraps around 0
EUROPE_LON_MAX = 20.0


@dataclass
class GridPoint:
    """Single grid point with coordinates and TEC values."""

    index: int
    latitude: float  # degrees
    longitude: float  # degrees (0-360)
    height_ellipsoidal: float = 1.0
    height_orthometric: float = 0.0
    height_geoid: float = 0.0
    tec_epoch1: Optional[float] = None
    tec_epoch2: Optional[float] = None


@dataclass
class IONEXHeader:
    """IONEX file header information."""

    version: float = 1.0
    file_type: str = "I"
    system: str = "GPS"
    program: str = ""
    run_by: str = ""
    date: str = ""
    description: str = ""
    epoch_first: Optional[datetime] = None
    epoch_last: Optional[datetime] = None
    interval: int = 3600  # seconds
    num_maps: int = 0
    mapping_function: str = ""
    elevation_cutoff: float = 0.0
    base_radius: float = 6371.0  # km
    map_dimension: int = 2
    hgt1: float = 450.0  # km
    hgt2: float = 450.0
    dhgt: float = 0.0
    lat1: float = 87.5
    lat2: float = -87.5
    dlat: float = -2.5
    lon1: float = -180.0
    lon2: float = 180.0
    dlon: float = 5.0
    exponent: int = -1  # TEC values = value * 10^exponent


@dataclass
class IONEXData:
    """Complete IONEX file data."""

    header: IONEXHeader
    grid_points: list[GridPoint] = field(default_factory=list)
    tec_maps: dict[int, dict[int, float]] = field(default_factory=dict)
    # tec_maps[epoch_index][grid_point_index] = tec_value


@dataclass
class TECRecord:
    """Single TEC record for output."""

    station_id: int
    latitude: float
    longitude: float
    height_ellipsoidal: float
    height_orthometric: float
    height_geoid: float
    epoch1: datetime
    epoch2: datetime
    tec1: float
    tec2: float


class IONEXParser:
    """Parser for IONEX format files."""

    def __init__(self):
        """Initialize IONEX parser."""
        self.header = IONEXHeader()
        self.current_epoch = 0
        self.current_lat = 0.0
        self.tec_maps: dict[int, dict[int, float]] = {}

    def parse(self, file_path: Path | str) -> IONEXData:
        """Parse an IONEX file.

        Args:
            file_path: Path to IONEX file

        Returns:
            IONEXData with header and TEC maps
        """
        file_path = Path(file_path)

        if not file_path.exists():
            raise FileNotFoundError(f"IONEX file not found: {file_path}")

        self.header = IONEXHeader()
        self.tec_maps = {}
        self.current_epoch = 0

        with open(file_path, 'r') as f:
            lines = f.readlines()

        in_header = True
        in_tec_map = False
        current_grid_index = 0
        current_lat_values: list[float] = []

        i = 0
        while i < len(lines):
            line = lines[i]

            # Skip comment lines
            if line.startswith('*'):
                i += 1
                continue

            # Parse header
            if in_header:
                if 'END OF HEADER' in line:
                    in_header = False
                else:
                    self._parse_header_line(line)
                i += 1
                continue

            # Parse TEC map
            if 'START OF TEC MAP' in line:
                in_tec_map = True
                self.current_epoch = int(line[:6].strip())
                self.tec_maps[self.current_epoch] = {}
                current_grid_index = 0
                i += 1
                continue

            if 'END OF TEC MAP' in line:
                in_tec_map = False
                i += 1
                continue

            if 'EPOCH OF CURRENT MAP' in line:
                # Parse epoch: YYYY MM DD HH MM SS
                parts = line[:60].split()
                if len(parts) >= 6:
                    year = int(parts[0])
                    month = int(parts[1])
                    day = int(parts[2])
                    hour = int(parts[3])
                    minute = int(parts[4])
                    second = int(float(parts[5]))

                    epoch_dt = datetime(year, month, day, hour, minute, second)

                    if self.current_epoch == 1:
                        self.header.epoch_first = epoch_dt
                    self.header.epoch_last = epoch_dt

                i += 1
                continue

            if 'LAT/LON1/LON2/DLON/H' in line:
                # Parse latitude header
                parts = line[:60].split()
                if len(parts) >= 5:
                    self.current_lat = float(parts[0])
                current_lat_values = []
                i += 1
                continue

            if in_tec_map and not any(label in line for label in [
                'START OF', 'END OF', 'EPOCH OF', 'LAT/LON'
            ]):
                # Parse TEC values
                # Each line has up to 16 values, 5 characters each
                values = []
                for j in range(0, min(80, len(line)), 5):
                    val_str = line[j:j+5].strip()
                    if val_str:
                        try:
                            values.append(int(val_str))
                        except ValueError:
                            values.append(0)

                current_lat_values.extend(values)

                # Check if we have all longitude values for this latitude
                if len(current_lat_values) >= IONEX_NUM_LON:
                    # Store values in grid
                    lat_index = int((IONEX_LAT_START - self.current_lat) / abs(IONEX_LAT_STEP))
                    for lon_idx, tec_val in enumerate(current_lat_values[:IONEX_NUM_LON]):
                        grid_idx = lat_index * IONEX_NUM_LON + lon_idx + 1
                        self.tec_maps[self.current_epoch][grid_idx] = tec_val
                    current_lat_values = []

            i += 1

        # Build grid points
        grid_points = self._build_grid_points()

        return IONEXData(
            header=self.header,
            grid_points=grid_points,
            tec_maps=self.tec_maps,
        )

    def _parse_header_line(self, line: str) -> None:
        """Parse a single header line."""
        label = line[60:].strip() if len(line) > 60 else ""
        value = line[:60]

        if 'IONEX VERSION / TYPE' in label:
            parts = value.split()
            if parts:
                self.header.version = float(parts[0])
            if len(parts) > 1:
                self.header.file_type = parts[1]
            if len(parts) > 2:
                self.header.system = parts[2]

        elif 'PGM / RUN BY / DATE' in label:
            self.header.program = value[:20].strip()
            self.header.run_by = value[20:40].strip()
            self.header.date = value[40:60].strip()

        elif 'INTERVAL' in label:
            try:
                self.header.interval = int(float(value.split()[0]))
            except (ValueError, IndexError):
                pass

        elif '# OF MAPS IN FILE' in label:
            try:
                self.header.num_maps = int(value.split()[0])
            except (ValueError, IndexError):
                pass

        elif 'ELEVATION CUTOFF' in label:
            try:
                self.header.elevation_cutoff = float(value.split()[0])
            except (ValueError, IndexError):
                pass

        elif 'BASE RADIUS' in label:
            try:
                self.header.base_radius = float(value.split()[0])
            except (ValueError, IndexError):
                pass

        elif 'MAP DIMENSION' in label:
            try:
                self.header.map_dimension = int(value.split()[0])
            except (ValueError, IndexError):
                pass

        elif 'HGT1 / HGT2 / DHGT' in label:
            parts = value.split()
            if len(parts) >= 3:
                self.header.hgt1 = float(parts[0])
                self.header.hgt2 = float(parts[1])
                self.header.dhgt = float(parts[2])

        elif 'LAT1 / LAT2 / DLAT' in label:
            parts = value.split()
            if len(parts) >= 3:
                self.header.lat1 = float(parts[0])
                self.header.lat2 = float(parts[1])
                self.header.dlat = float(parts[2])

        elif 'LON1 / LON2 / DLON' in label:
            parts = value.split()
            if len(parts) >= 3:
                self.header.lon1 = float(parts[0])
                self.header.lon2 = float(parts[1])
                self.header.dlon = float(parts[2])

        elif 'EXPONENT' in label:
            try:
                self.header.exponent = int(value.split()[0])
            except (ValueError, IndexError):
                pass

    def _build_grid_points(self) -> list[GridPoint]:
        """Build list of grid points with coordinates."""
        grid_points = []

        idx = 1
        lat = IONEX_LAT_START
        while lat >= IONEX_LAT_END:
            lon = IONEX_LON_START
            while lon < IONEX_LON_END:
                # Convert longitude to 0-360 range
                lon_360 = lon if lon >= 0 else lon + 360

                point = GridPoint(
                    index=idx,
                    latitude=lat,
                    longitude=lon_360,
                )

                # Get TEC values from maps
                if 1 in self.tec_maps and idx in self.tec_maps[1]:
                    point.tec_epoch1 = self.tec_maps[1][idx]
                if 2 in self.tec_maps and idx in self.tec_maps[2]:
                    point.tec_epoch2 = self.tec_maps[2][idx]

                grid_points.append(point)

                lon += IONEX_LON_STEP
                idx += 1

            lat += IONEX_LAT_STEP

        return grid_points


class INX2TEC:
    """Convert IONEX files to TEC in COST-716 format.

    This class handles:
    - Parsing IONEX format ionosphere files
    - Converting TEC values to COST-716 format
    - Support for hourly and sub-hourly (15-minute) data
    """

    def __init__(
        self,
        solution_name: str = "MIGH",
        processing_center: str = "MIGH",
        processing_method: str = "BERNESE V5.4",
        orbit_source: str = "IGSULT",
    ):
        """Initialize converter.

        Args:
            solution_name: 4-character solution name
            processing_center: Processing center code
            processing_method: Processing software/method
            orbit_source: Orbit product source
        """
        self.solution_name = solution_name
        self.processing_center = processing_center
        self.processing_method = processing_method
        self.orbit_source = orbit_source
        self.parser = IONEXParser()

    def convert_hourly(
        self,
        ionex_file: Path | str,
        output_dir: Optional[Path | str] = None,
    ) -> Path:
        """Convert hourly IONEX file to COST-716 format.

        Args:
            ionex_file: Path to IONEX file
            output_dir: Output directory (defaults to same as input)

        Returns:
            Path to output COST-716 file
        """
        ionex_file = Path(ionex_file)
        output_dir = Path(output_dir) if output_dir else ionex_file.parent

        # Parse IONEX file
        data = self.parser.parse(ionex_file)

        # Extract date from filename
        date_info = self._parse_filename(ionex_file.name)

        # Build output filename
        # Format: cost_h_o_YYYYMMDDHH00_YYYYMMDDHH59_mult_XXXX.dat
        if data.header.epoch_first:
            dt = data.header.epoch_first
            output_name = (
                f"cost_h_o_{dt.year:04d}{dt.month:02d}{dt.day:02d}"
                f"{dt.hour:02d}00_{dt.year:04d}{dt.month:02d}{dt.day:02d}"
                f"{dt.hour:02d}59_mult_{self.solution_name}.dat"
            )
        else:
            output_name = f"cost_h_o_{ionex_file.stem}_mult_{self.solution_name}.dat"

        output_path = output_dir / output_name

        # Get file creation time
        creation_time = datetime.utcfromtimestamp(ionex_file.stat().st_mtime)

        # Write COST-716 file
        self._write_cost716_hourly(
            output_path,
            data,
            creation_time,
        )

        logger.info(f"Created COST-716 TEC file: {output_path}")
        return output_path

    def convert_subhourly(
        self,
        ionex_file: Path | str,
        output_dir: Optional[Path | str] = None,
        europe_only: bool = True,
    ) -> Path:
        """Convert sub-hourly (15-minute) IONEX file to COST-716 format.

        Args:
            ionex_file: Path to IONEX file
            output_dir: Output directory (defaults to same as input)
            europe_only: If True, only output European region data

        Returns:
            Path to output COST-716 file
        """
        ionex_file = Path(ionex_file)
        output_dir = Path(output_dir) if output_dir else ionex_file.parent

        # Parse IONEX file
        data = self.parser.parse(ionex_file)

        # Extract date and minute from filename
        date_info = self._parse_filename_subhourly(ionex_file.name)

        # Build output filename
        if data.header.epoch_first:
            dt = data.header.epoch_first
            minute_start = dt.minute
            minute_end = minute_start + 14
            output_name = (
                f"cost_s_o_{dt.year:04d}{dt.month:02d}{dt.day:02d}"
                f"{dt.hour:02d}{minute_start:02d}_{dt.year:04d}{dt.month:02d}{dt.day:02d}"
                f"{dt.hour:02d}{minute_end:02d}_mult_{self.solution_name}.dat"
            )
        else:
            output_name = f"cost_s_o_{ionex_file.stem}_mult_{self.solution_name}.dat"

        output_path = output_dir / output_name

        # Get file creation time
        creation_time = datetime.utcfromtimestamp(ionex_file.stat().st_mtime)

        # Write COST-716 file
        self._write_cost716_subhourly(
            output_path,
            data,
            creation_time,
            europe_only=europe_only,
        )

        logger.info(f"Created COST-716 TEC file (sub-hourly): {output_path}")
        return output_path

    def _parse_filename(self, filename: str) -> dict:
        """Parse date information from IONEX filename.

        Args:
            filename: IONEX filename

        Returns:
            Dict with year, doy, hour
        """
        # Clean filename
        name = filename
        for pattern in ['nrt105', 'nrt2', 'CAMPAIGN52']:
            name = name.replace(pattern, '')

        # Remove leading non-digits
        name = re.sub(r'^\D*', '', name)

        result = {
            'y2c': 0,
            'doy': 0,
            'hour': 0,
        }

        if len(name) >= 6:
            try:
                result['y2c'] = int(name[0:2])
                result['doy'] = int(name[2:5])
                result['hour'] = alpha_to_hour(name[5].lower())
            except (ValueError, IndexError):
                pass

        return result

    def _parse_filename_subhourly(self, filename: str) -> dict:
        """Parse date information from sub-hourly IONEX filename.

        Args:
            filename: IONEX filename

        Returns:
            Dict with year, doy, hour, minute
        """
        result = self._parse_filename(filename)

        # Clean filename to get minute indicator
        name = filename
        for pattern in ['nrt105', 'nrt2', 'CAMPAIGN52']:
            name = name.replace(pattern, '')
        name = re.sub(r'^\D*', '', name)

        # Parse minute indicator (7th character)
        if len(name) >= 7:
            minute_char = name[6]
            minute_map = {'0': 0, '1': 15, '3': 30, '4': 45}
            result['minute'] = minute_map.get(minute_char, 0)
        else:
            result['minute'] = 0

        return result

    def _write_cost716_hourly(
        self,
        output_path: Path,
        data: IONEXData,
        creation_time: datetime,
    ) -> None:
        """Write COST-716 format file for hourly data.

        Args:
            output_path: Output file path
            data: Parsed IONEX data
            creation_time: File creation timestamp
        """
        # COST-716 format constants
        format_version = "COST-716 V2.2"
        project = "E-GVAP"
        status = "OPER"
        source_met = "NONE"

        # Time parameters for hourly data
        time_inc = 60  # minutes
        time_upd = 60
        lobts = 120

        # Product confidence data
        pcdd = "FFFFFFFF"
        pdch = 0b1110101  # Processing flags

        # Number of slant delays (none for TEC-only)
        num_slant = 0
        num_samples = 2  # Two epochs per hour

        # Get epoch times
        epoch1 = data.header.epoch_first or datetime.now()
        epoch2 = datetime(
            epoch1.year, epoch1.month, epoch1.day,
            epoch1.hour, 59, epoch1.second
        )

        with open(output_path, 'w') as f:
            for point in data.grid_points:
                # Get TEC values
                tec1 = self._scale_tec(point.tec_epoch1, data.header.exponent)
                tec2 = self._scale_tec(point.tec_epoch2, data.header.exponent)

                # Write station record
                self._write_station_record(
                    f, point, epoch1, epoch2, creation_time,
                    tec1, tec2, time_inc, time_upd, lobts,
                    pcdd, pdch, num_samples, num_slant,
                )

    def _write_cost716_subhourly(
        self,
        output_path: Path,
        data: IONEXData,
        creation_time: datetime,
        europe_only: bool = True,
    ) -> None:
        """Write COST-716 format file for sub-hourly data.

        Args:
            output_path: Output file path
            data: Parsed IONEX data
            creation_time: File creation timestamp
            europe_only: If True, only include European region
        """
        # COST-716 format constants
        format_version = "COST-716 V2.2"
        project = "E-GVAP"
        status = "OPER"
        source_met = "NONE"

        # Time parameters for sub-hourly data
        time_inc = 15  # minutes
        time_upd = 15
        lobts = 120

        # Product confidence data
        pcdd = "FFFFFFFF"
        pdch = 0b1110101

        num_slant = 0
        num_samples = 2

        # Get epoch times
        epoch1 = data.header.epoch_first or datetime.now()
        epoch2 = datetime(
            epoch1.year, epoch1.month, epoch1.day,
            epoch1.hour, epoch1.minute + 14, epoch1.second
        )

        with open(output_path, 'w') as f:
            for point in data.grid_points:
                # Get TEC values, applying region filter if needed
                if europe_only:
                    tec1 = self._get_europe_tec(
                        point, point.tec_epoch1, data.header.exponent
                    )
                    tec2 = self._get_europe_tec(
                        point, point.tec_epoch2, data.header.exponent
                    )
                else:
                    tec1 = self._scale_tec(point.tec_epoch1, data.header.exponent)
                    tec2 = self._scale_tec(point.tec_epoch2, data.header.exponent)

                # Write station record
                self._write_station_record(
                    f, point, epoch1, epoch2, creation_time,
                    tec1, tec2, time_inc, time_upd, lobts,
                    pcdd, pdch, num_samples, num_slant,
                )

    def _scale_tec(
        self,
        raw_value: Optional[float],
        exponent: int,
    ) -> float:
        """Scale raw TEC value to TECU.

        Args:
            raw_value: Raw TEC value from IONEX
            exponent: IONEX exponent (typically -1)

        Returns:
            Scaled TEC value or default
        """
        if raw_value is None or raw_value == 0:
            return DEFAULT_TEC

        # IONEX stores values as integers, scale by 10^exponent
        # Then divide by 10 for final TECU (as in original Perl)
        return (raw_value * (10 ** exponent)) / 10

    def _get_europe_tec(
        self,
        point: GridPoint,
        raw_value: Optional[float],
        exponent: int,
    ) -> float:
        """Get TEC value filtered for European region.

        Args:
            point: Grid point with coordinates
            raw_value: Raw TEC value
            exponent: IONEX exponent

        Returns:
            TEC value if in Europe, else default
        """
        # Check if point is in European region
        in_lat = EUROPE_LAT_MIN <= point.latitude <= EUROPE_LAT_MAX
        in_lon = point.longitude >= EUROPE_LON_MIN or point.longitude <= EUROPE_LON_MAX

        if in_lat and in_lon:
            return self._scale_tec(raw_value, exponent)
        else:
            return DEFAULT_TEC

    def _write_station_record(
        self,
        f,
        point: GridPoint,
        epoch1: datetime,
        epoch2: datetime,
        creation_time: datetime,
        tec1: float,
        tec2: float,
        time_inc: int,
        time_upd: int,
        lobts: int,
        pcdd: str,
        pdch: int,
        num_samples: int,
        num_slant: int,
    ) -> None:
        """Write a single station record in COST-716 format.

        Args:
            f: File handle
            point: Grid point data
            epoch1: First epoch datetime
            epoch2: Second epoch datetime
            creation_time: File creation time
            tec1: TEC value for epoch 1
            tec2: TEC value for epoch 2
            time_inc: Time increment (minutes)
            time_upd: Time update (minutes)
            lobts: Length of back time series
            pcdd: Product confidence data
            pdch: Product data characteristic hex
            num_samples: Number of samples
            num_slant: Number of slant delays
        """
        # Format strings
        format_version = "COST-716 V2.2"
        project = "E-GVAP"
        status = "OPER"
        empty = ""

        # Line 1: Format, project, status
        f.write(f"{format_version:<20}{empty:<5}{project:<20}{empty:<5}{status:<20}\n")

        # Line 2: Station ID
        f.write(f"{point.index:04d}{empty:<16}{empty:<5}{empty:<60}\n")

        # Line 3: Receiver and antenna (empty for grid points)
        f.write(f"{empty:<20}{empty:<5}{empty:<20}\n")

        # Line 4: Position
        f.write(
            f"{point.latitude:12.6f}{point.longitude:12.6f}"
            f"{point.height_ellipsoidal:12.3f}{point.height_orthometric:12.3f}"
            f"{point.height_geoid:12.3f}\n"
        )

        # Line 5: Time info
        month_names = ['', 'JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN',
                       'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC']

        time_first = (
            f"{epoch1.day:02d}-{month_names[epoch1.month]}-{epoch1.year:04d} "
            f"{epoch1.hour:02d}:{epoch1.minute:02d}:{epoch1.second:02d}"
        )
        time_creation = (
            f"{creation_time.day:02d}-{month_names[creation_time.month]}-{creation_time.year:04d} "
            f"{creation_time.hour:02d}:{creation_time.minute:02d}:{creation_time.second:02d}"
        )
        f.write(f"{time_first:<20}{empty:>5}{time_creation:<20}\n")

        # Line 6: Processing info
        f.write(
            f"{self.processing_center:<20}{empty:>5}"
            f"{self.processing_method:<20}{empty:>5}"
            f"{self.orbit_source:<20}{empty:>5}{'NONE':<20}\n"
        )

        # Line 7: Time parameters
        f.write(f"{time_inc:>5}{time_upd:>5}{lobts:>5}\n")

        # Line 8: Product data characteristic hex
        f.write(f"{pdch:08X}\n")

        # Line 9: Number of samples
        f.write(f"{num_samples:>4}\n")

        # Line 10: First epoch data
        # Format: HH MM SS PCDD ZTD RMS ZWD IWV P T RH GradN GradE ErrN ErrE TEC
        f.write(
            f"{epoch1.hour:3d}{epoch1.minute:3d}{epoch1.second:3d}"
            f"{pcdd:>9}"
            f"{DEFAULT_ZTD:7.1f}{DEFAULT_RMS:7.1f}"
            f"{DEFAULT_ZWD:7.1f}{DEFAULT_IWV:7.1f}"
            f"{DEFAULT_PRESSURE:7.1f}{DEFAULT_TEMPERATURE:7.1f}{DEFAULT_HUMIDITY:7.1f}"
            f"{DEFAULT_GRADIENT:7.2f}{DEFAULT_GRADIENT:7.2f}"
            f"{DEFAULT_GRADIENT_ERROR:7.2f}{DEFAULT_GRADIENT_ERROR:7.2f}"
            f"{tec1:8.3f}\n"
        )
        f.write(f"{num_slant:>4}\n")

        # Line 11: Second epoch data
        f.write(
            f"{epoch2.hour:3d}{epoch2.minute:3d}{epoch2.second:3d}"
            f"{pcdd:>9}"
            f"{DEFAULT_ZTD:7.1f}{DEFAULT_RMS:7.1f}"
            f"{DEFAULT_ZWD:7.1f}{DEFAULT_IWV:7.1f}"
            f"{DEFAULT_PRESSURE:7.1f}{DEFAULT_TEMPERATURE:7.1f}{DEFAULT_HUMIDITY:7.1f}"
            f"{DEFAULT_GRADIENT:7.2f}{DEFAULT_GRADIENT:7.2f}"
            f"{DEFAULT_GRADIENT_ERROR:7.2f}{DEFAULT_GRADIENT_ERROR:7.2f}"
            f"{tec2:8.3f}\n"
        )
        f.write(f"{num_slant:>4}\n")

        # Separator line (100 dashes)
        f.write("-" * 100 + "\n")
        f.write("\n\n")


def convert_ionex_to_tec(
    ionex_file: Path | str,
    output_dir: Optional[Path | str] = None,
    hourly: bool = True,
    solution_name: str = "MIGH",
    europe_only: bool = True,
) -> Path:
    """Convenience function to convert IONEX to TEC.

    Args:
        ionex_file: Path to IONEX file
        output_dir: Output directory
        hourly: True for hourly, False for sub-hourly
        solution_name: Solution name code
        europe_only: For sub-hourly, only include Europe

    Returns:
        Path to output file
    """
    converter = INX2TEC(solution_name=solution_name)

    if hourly:
        return converter.convert_hourly(ionex_file, output_dir)
    else:
        return converter.convert_subhourly(ionex_file, output_dir, europe_only=europe_only)
