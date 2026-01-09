"""
Orbit and ERP product management.

Replaces Perl ORBIT.pm module with full SP3 parsing support.

Handles:
- SP3 orbit file parsing (SP3-c and SP3-d formats)
- ERP (Earth Rotation Parameters) file parsing
- Orbit product database tracking
- GPS week/DOW based filename construction
- Gap filling for missed downloads

Usage:
    from pygnss_rt.products.orbit import SP3Reader, OrbitDataManager

    # Read SP3 file
    sp3 = SP3Reader.from_file("/path/to/igs2345.sp3")
    for epoch in sp3.epochs:
        for sat_id, position in epoch.positions.items():
            print(f"{sat_id}: X={position.x}, Y={position.y}, Z={position.z}")

    # Track orbit downloads
    orbit_mgr = OrbitDataManager(db)
    orbit_mgr.maintain()
    missing = orbit_mgr.get_waiting_list()
"""

from __future__ import annotations

import gzip
import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import TYPE_CHECKING, Any, Iterator

from pygnss_rt.core.exceptions import DatabaseError, ProductNotAvailableError
from pygnss_rt.utils.dates import GNSSDate
from pygnss_rt.utils.logging import get_logger

if TYPE_CHECKING:
    from pygnss_rt.database.connection import DatabaseManager


logger = get_logger(__name__)


# =============================================================================
# SP3 Format Constants
# =============================================================================

class SP3Version(str, Enum):
    """SP3 file version."""
    SP3_A = "a"
    SP3_B = "b"
    SP3_C = "c"
    SP3_D = "d"


class OrbitType(str, Enum):
    """Orbit accuracy type."""
    FINAL = "final"
    RAPID = "rapid"
    ULTRA_RAPID = "ultra"
    PREDICTED = "predicted"


class TimeSystem(str, Enum):
    """Time system used in SP3 file."""
    GPS = "GPS"
    GLO = "GLO"  # GLONASS
    GAL = "GAL"  # Galileo
    TAI = "TAI"
    UTC = "UTC"
    BDS = "BDS"  # BeiDou
    QZS = "QZS"  # QZSS


# Satellite system prefixes
SAT_SYSTEMS = {
    "G": "GPS",
    "R": "GLONASS",
    "E": "Galileo",
    "C": "BeiDou",
    "J": "QZSS",
    "I": "IRNSS",
    "S": "SBAS",
}


# =============================================================================
# Data Classes
# =============================================================================

@dataclass
class SP3Position:
    """Satellite position and clock data for a single epoch."""

    x: float  # X coordinate in km
    y: float  # Y coordinate in km
    z: float  # Z coordinate in km
    clock: float = 0.0  # Clock correction in microseconds
    x_sigma: float | None = None  # X position accuracy (mm)
    y_sigma: float | None = None  # Y position accuracy (mm)
    z_sigma: float | None = None  # Z position accuracy (mm)
    clock_sigma: float | None = None  # Clock accuracy (ps)

    @property
    def position_m(self) -> tuple[float, float, float]:
        """Get position in meters."""
        return (self.x * 1000.0, self.y * 1000.0, self.z * 1000.0)

    @property
    def clock_sec(self) -> float:
        """Get clock correction in seconds."""
        return self.clock * 1e-6

    def distance_to(self, other: "SP3Position") -> float:
        """Calculate distance to another position in meters."""
        dx = (self.x - other.x) * 1000.0
        dy = (self.y - other.y) * 1000.0
        dz = (self.z - other.z) * 1000.0
        return (dx**2 + dy**2 + dz**2) ** 0.5


@dataclass
class SP3Velocity:
    """Satellite velocity data (optional in SP3 files)."""

    vx: float  # X velocity in dm/s
    vy: float  # Y velocity in dm/s
    vz: float  # Z velocity in dm/s
    clock_rate: float = 0.0  # Clock rate change

    @property
    def velocity_m_s(self) -> tuple[float, float, float]:
        """Get velocity in m/s."""
        return (self.vx * 0.1, self.vy * 0.1, self.vz * 0.1)


@dataclass
class SP3Epoch:
    """Single epoch of SP3 data."""

    datetime: datetime
    positions: dict[str, SP3Position] = field(default_factory=dict)
    velocities: dict[str, SP3Velocity] = field(default_factory=dict)

    @property
    def mjd(self) -> float:
        """Get Modified Julian Date."""
        return GNSSDate.from_datetime(self.datetime).mjd

    @property
    def gps_week(self) -> int:
        """Get GPS week number."""
        return GNSSDate.from_datetime(self.datetime).gps_week

    @property
    def seconds_of_week(self) -> float:
        """Get seconds of GPS week."""
        gnss_date = GNSSDate.from_datetime(self.datetime)
        return gnss_date.day_of_week * 86400 + self.datetime.hour * 3600 + \
               self.datetime.minute * 60 + self.datetime.second

    def get_satellite_ids(self, system: str | None = None) -> list[str]:
        """Get list of satellite IDs, optionally filtered by system."""
        if system is None:
            return list(self.positions.keys())
        return [sat for sat in self.positions.keys() if sat.startswith(system)]


@dataclass
class SP3Header:
    """SP3 file header information."""

    version: SP3Version = SP3Version.SP3_C
    pos_vel_flag: str = "P"  # P=position only, V=position+velocity
    start_time: datetime | None = None
    num_epochs: int = 0
    data_used: str = ""  # e.g., "ORBIT"
    coordinate_system: str = "IGS20"
    orbit_type: str = "HLM"  # Fit type
    agency: str = ""
    gps_week: int = 0
    seconds_of_week: float = 0.0
    epoch_interval: float = 900.0  # seconds
    mjd_start: int = 0
    fractional_day: float = 0.0
    num_satellites: int = 0
    satellite_ids: list[str] = field(default_factory=list)
    satellite_accuracies: dict[str, int] = field(default_factory=dict)
    time_system: TimeSystem = TimeSystem.GPS
    base_pos_vel: float = 2.0  # Base for position/velocity accuracy
    base_clk_rate: float = 2.0  # Base for clock/rate accuracy

    @property
    def has_velocities(self) -> bool:
        """Check if file contains velocity data."""
        return self.pos_vel_flag == "V"


@dataclass
class SP3File:
    """Complete SP3 file data."""

    header: SP3Header
    epochs: list[SP3Epoch] = field(default_factory=list)
    filepath: Path | None = None

    @property
    def num_epochs(self) -> int:
        """Number of epochs in file."""
        return len(self.epochs)

    @property
    def start_time(self) -> datetime | None:
        """Start time of data."""
        return self.epochs[0].datetime if self.epochs else None

    @property
    def end_time(self) -> datetime | None:
        """End time of data."""
        return self.epochs[-1].datetime if self.epochs else None

    @property
    def satellites(self) -> list[str]:
        """List of all satellites in file."""
        return self.header.satellite_ids

    def get_position(
        self,
        sat_id: str,
        epoch_time: datetime,
    ) -> SP3Position | None:
        """Get satellite position at specific epoch.

        Args:
            sat_id: Satellite ID (e.g., "G01", "R05")
            epoch_time: Epoch datetime

        Returns:
            SP3Position or None if not found
        """
        for epoch in self.epochs:
            if epoch.datetime == epoch_time:
                return epoch.positions.get(sat_id)
        return None

    def interpolate_position(
        self,
        sat_id: str,
        target_time: datetime,
        degree: int = 9,
    ) -> SP3Position | None:
        """Interpolate satellite position using Lagrange interpolation.

        Args:
            sat_id: Satellite ID
            target_time: Target datetime
            degree: Interpolation polynomial degree (default 9)

        Returns:
            Interpolated SP3Position or None
        """
        # Collect epochs with this satellite
        epochs_with_sat = [
            (e.datetime, e.positions[sat_id])
            for e in self.epochs
            if sat_id in e.positions
        ]

        if len(epochs_with_sat) < degree + 1:
            return None

        # Find nearest epochs
        target_ts = target_time.timestamp()
        epochs_with_sat.sort(key=lambda x: abs(x[0].timestamp() - target_ts))
        selected = epochs_with_sat[:degree + 1]
        selected.sort(key=lambda x: x[0])

        # Lagrange interpolation
        times = [e[0].timestamp() for e in selected]
        x_vals = [e[1].x for e in selected]
        y_vals = [e[1].y for e in selected]
        z_vals = [e[1].z for e in selected]
        clk_vals = [e[1].clock for e in selected]

        x_interp = self._lagrange_interp(times, x_vals, target_ts)
        y_interp = self._lagrange_interp(times, y_vals, target_ts)
        z_interp = self._lagrange_interp(times, z_vals, target_ts)
        clk_interp = self._lagrange_interp(times, clk_vals, target_ts)

        return SP3Position(
            x=x_interp,
            y=y_interp,
            z=z_interp,
            clock=clk_interp,
        )

    @staticmethod
    def _lagrange_interp(x: list[float], y: list[float], xi: float) -> float:
        """Lagrange polynomial interpolation."""
        n = len(x)
        result = 0.0

        for i in range(n):
            term = y[i]
            for j in range(n):
                if i != j:
                    term *= (xi - x[j]) / (x[i] - x[j])
            result += term

        return result

    def iter_epochs(self) -> Iterator[SP3Epoch]:
        """Iterate over epochs."""
        return iter(self.epochs)

    def get_epochs_for_satellite(self, sat_id: str) -> list[SP3Epoch]:
        """Get all epochs containing a specific satellite."""
        return [e for e in self.epochs if sat_id in e.positions]


# =============================================================================
# SP3 Reader
# =============================================================================

class SP3Reader:
    """Parser for SP3 orbit files.

    Supports SP3-a, SP3-b, SP3-c, and SP3-d formats.

    SP3 Format Reference:
    https://files.igs.org/pub/data/format/sp3_docu.txt
    """

    # Bad/missing value markers
    BAD_CLOCK = 999999.999999
    BAD_POSITION = 0.000000

    def __init__(self):
        """Initialize SP3 reader."""
        self._current_epoch: SP3Epoch | None = None

    @classmethod
    def from_file(cls, filepath: str | Path) -> SP3File:
        """Read SP3 file.

        Args:
            filepath: Path to SP3 file (supports .sp3, .sp3.Z, .sp3.gz)

        Returns:
            SP3File object
        """
        filepath = Path(filepath)
        reader = cls()

        # Handle compressed files
        if filepath.suffix == ".Z":
            import subprocess
            result = subprocess.run(
                ["zcat", str(filepath)],
                capture_output=True,
            )
            if result.returncode != 0:
                raise IOError(f"Failed to decompress {filepath}")
            content = result.stdout.decode("utf-8", errors="ignore")
            lines = content.splitlines()
        elif filepath.suffix == ".gz":
            with gzip.open(filepath, "rt", encoding="utf-8", errors="ignore") as f:
                lines = f.read().splitlines()
        else:
            with open(filepath, "r", encoding="utf-8", errors="ignore") as f:
                lines = f.read().splitlines()

        sp3_file = reader.parse(lines)
        sp3_file.filepath = filepath

        logger.info(
            "Read SP3 file",
            path=str(filepath),
            epochs=sp3_file.num_epochs,
            satellites=len(sp3_file.satellites),
        )

        return sp3_file

    def parse(self, lines: list[str]) -> SP3File:
        """Parse SP3 content from lines.

        Args:
            lines: List of file lines

        Returns:
            SP3File object
        """
        header = self._parse_header(lines)
        epochs = self._parse_epochs(lines, header)

        return SP3File(header=header, epochs=epochs)

    def _parse_header(self, lines: list[str]) -> SP3Header:
        """Parse SP3 header lines."""
        header = SP3Header()
        satellite_ids = []
        satellite_accuracies = {}

        for i, line in enumerate(lines):
            if not line:
                continue

            # Line 1: Version and header info
            if line.startswith("#"):
                if i == 0:
                    header.version = SP3Version(line[1].lower())
                    header.pos_vel_flag = line[2]

                    # Parse start time
                    try:
                        year = int(line[3:7])
                        month = int(line[8:10])
                        day = int(line[11:13])
                        hour = int(line[14:16])
                        minute = int(line[17:19])
                        second = float(line[20:31])
                        header.start_time = datetime(
                            year, month, day, hour, minute, int(second),
                            int((second % 1) * 1_000_000)
                        )
                    except (ValueError, IndexError):
                        pass

                    # Parse epoch count and other info
                    try:
                        header.num_epochs = int(line[32:39])
                        header.data_used = line[40:45].strip()
                        header.coordinate_system = line[46:51].strip()
                        header.orbit_type = line[52:55].strip()
                        header.agency = line[56:60].strip()
                    except (ValueError, IndexError):
                        pass

                elif i == 1:
                    # Line 2: GPS week, seconds, epoch interval, MJD
                    try:
                        header.gps_week = int(line[3:7])
                        header.seconds_of_week = float(line[8:23])
                        header.epoch_interval = float(line[24:38])
                        header.mjd_start = int(line[39:44])
                        header.fractional_day = float(line[45:60])
                    except (ValueError, IndexError):
                        pass

            # Lines 3-7: Satellite IDs (+ lines)
            elif line.startswith("+"):
                if line[1] == " " and i >= 2:
                    # Number of satellites line
                    try:
                        if i == 2:
                            header.num_satellites = int(line[3:6])
                    except ValueError:
                        pass

                    # Parse satellite IDs
                    for j in range(17):
                        start = 9 + j * 3
                        end = start + 3
                        if end <= len(line):
                            sat_id = line[start:end].strip()
                            if sat_id and sat_id != "0" and sat_id != "00":
                                # Normalize satellite ID
                                if sat_id[0].isdigit():
                                    sat_id = "G" + sat_id.zfill(2)
                                satellite_ids.append(sat_id)

                elif line[1] == "+":
                    # Satellite accuracy lines (++ lines)
                    for j in range(17):
                        start = 9 + j * 3
                        end = start + 3
                        if end <= len(line):
                            try:
                                acc = int(line[start:end].strip())
                                if len(satellite_ids) > j + (i - 7) * 17:
                                    sat_idx = j + (i - 7) * 17
                                    if sat_idx < len(satellite_ids):
                                        satellite_accuracies[satellite_ids[sat_idx]] = acc
                            except (ValueError, IndexError):
                                pass

            # Time system line
            elif line.startswith("%c"):
                parts = line.split()
                if len(parts) >= 2:
                    try:
                        header.time_system = TimeSystem(parts[1][:3])
                    except ValueError:
                        pass

            # Float base values
            elif line.startswith("%f"):
                parts = line.split()
                if len(parts) >= 3:
                    try:
                        header.base_pos_vel = float(parts[1])
                        header.base_clk_rate = float(parts[2])
                    except ValueError:
                        pass

            # End of header
            elif line.startswith("*"):
                break

        header.satellite_ids = satellite_ids
        header.satellite_accuracies = satellite_accuracies

        return header

    def _parse_epochs(self, lines: list[str], header: SP3Header) -> list[SP3Epoch]:
        """Parse SP3 epoch data."""
        epochs = []
        current_epoch: SP3Epoch | None = None

        for line in lines:
            if not line:
                continue

            # Epoch header line
            if line.startswith("*"):
                # Save previous epoch
                if current_epoch is not None:
                    epochs.append(current_epoch)

                # Parse epoch time
                try:
                    year = int(line[3:7])
                    month = int(line[8:10])
                    day = int(line[11:13])
                    hour = int(line[14:16])
                    minute = int(line[17:19])
                    second = float(line[20:31])

                    epoch_dt = datetime(
                        year, month, day, hour, minute, int(second),
                        int((second % 1) * 1_000_000)
                    )
                    current_epoch = SP3Epoch(datetime=epoch_dt)
                except (ValueError, IndexError) as e:
                    logger.warning(f"Failed to parse epoch line: {line}", error=str(e))
                    current_epoch = None

            # Position line
            elif line.startswith("P") and current_epoch is not None:
                try:
                    sat_id = line[1:4].strip()

                    # Normalize satellite ID
                    if sat_id[0].isdigit():
                        sat_id = "G" + sat_id.zfill(2)

                    x = float(line[4:18])
                    y = float(line[18:32])
                    z = float(line[32:46])
                    clock = float(line[46:60])

                    # Check for bad values
                    if abs(clock - self.BAD_CLOCK) < 0.001:
                        clock = 0.0

                    position = SP3Position(x=x, y=y, z=z, clock=clock)

                    # Parse optional accuracy values (if present)
                    if len(line) >= 73:
                        try:
                            position.x_sigma = int(line[61:63]) if line[61:63].strip() else None
                            position.y_sigma = int(line[64:66]) if line[64:66].strip() else None
                            position.z_sigma = int(line[67:69]) if line[67:69].strip() else None
                            position.clock_sigma = int(line[70:73]) if line[70:73].strip() else None
                        except ValueError:
                            pass

                    current_epoch.positions[sat_id] = position

                except (ValueError, IndexError) as e:
                    logger.warning(f"Failed to parse position line: {line}", error=str(e))

            # Velocity line
            elif line.startswith("V") and current_epoch is not None:
                try:
                    sat_id = line[1:4].strip()
                    if sat_id[0].isdigit():
                        sat_id = "G" + sat_id.zfill(2)

                    vx = float(line[4:18])
                    vy = float(line[18:32])
                    vz = float(line[32:46])
                    clock_rate = float(line[46:60])

                    velocity = SP3Velocity(vx=vx, vy=vy, vz=vz, clock_rate=clock_rate)
                    current_epoch.velocities[sat_id] = velocity

                except (ValueError, IndexError) as e:
                    logger.warning(f"Failed to parse velocity line: {line}", error=str(e))

            # End of file
            elif line.startswith("EOF"):
                break

        # Don't forget the last epoch
        if current_epoch is not None:
            epochs.append(current_epoch)

        return epochs


# =============================================================================
# SP3 Writer (for creating SP3 files)
# =============================================================================

class SP3Writer:
    """Writer for SP3 orbit files."""

    def __init__(
        self,
        version: SP3Version = SP3Version.SP3_C,
        agency: str = "UNK",
        coordinate_system: str = "IGS20",
        orbit_type: str = "HLM",
        time_system: TimeSystem = TimeSystem.GPS,
    ):
        """Initialize SP3 writer.

        Args:
            version: SP3 version
            agency: Producing agency
            coordinate_system: Reference frame
            orbit_type: Orbit fit type
            time_system: Time system
        """
        self.version = version
        self.agency = agency
        self.coordinate_system = coordinate_system
        self.orbit_type = orbit_type
        self.time_system = time_system

    def write(self, sp3_file: SP3File, output_path: Path | str) -> None:
        """Write SP3 file.

        Args:
            sp3_file: SP3 data to write
            output_path: Output file path
        """
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        lines = []

        # Header
        lines.extend(self._format_header(sp3_file))

        # Epochs
        for epoch in sp3_file.epochs:
            lines.extend(self._format_epoch(epoch))

        # EOF
        lines.append("EOF")

        with open(output_path, "w", encoding="utf-8") as f:
            f.write("\n".join(lines))

        logger.info(
            "Wrote SP3 file",
            path=str(output_path),
            epochs=len(sp3_file.epochs),
        )

    def _format_header(self, sp3_file: SP3File) -> list[str]:
        """Format SP3 header lines."""
        lines = []
        header = sp3_file.header

        if not sp3_file.epochs:
            raise ValueError("Cannot write SP3 file with no epochs")

        start = sp3_file.epochs[0].datetime
        gnss_date = GNSSDate.from_datetime(start)

        # Line 1
        line1 = f"#{self.version.value}{header.pos_vel_flag}"
        line1 += f"{start.year:4d} {start.month:2d} {start.day:2d}"
        line1 += f" {start.hour:2d} {start.minute:2d} {start.second:11.8f}"
        line1 += f" {len(sp3_file.epochs):7d} {'ORBIT':5s}"
        line1 += f" {self.coordinate_system:5s} {self.orbit_type:3s} {self.agency:4s}"
        lines.append(line1)

        # Line 2
        sow = gnss_date.day_of_week * 86400 + start.hour * 3600 + start.minute * 60 + start.second
        line2 = f"##{gnss_date.gps_week:5d}{sow:15.8f}"
        line2 += f"{header.epoch_interval:14.8f}"
        line2 += f"{int(gnss_date.mjd):5d}{gnss_date.mjd % 1:15.13f}"
        lines.append(line2)

        # Satellite ID lines (+ lines)
        satellites = header.satellite_ids
        num_sat_lines = (len(satellites) + 16) // 17

        for i in range(max(5, num_sat_lines)):
            if i == 0:
                line = f"+  {len(satellites):3d}   "
            else:
                line = "+        "

            for j in range(17):
                idx = i * 17 + j
                if idx < len(satellites):
                    line += f"{satellites[idx]:>3s}"
                else:
                    line += "  0"
            lines.append(line)

        # Satellite accuracy lines (++ lines)
        for i in range(max(5, num_sat_lines)):
            line = "++       "
            for j in range(17):
                idx = i * 17 + j
                if idx < len(satellites):
                    acc = header.satellite_accuracies.get(satellites[idx], 0)
                    line += f"{acc:3d}"
                else:
                    line += "  0"
            lines.append(line)

        # Comment lines
        lines.append(f"%c {self.time_system.value}  cc GPS ccc cccc cccc cccc cccc ccccc ccccc ccccc ccccc")
        lines.append("%c cc cc ccc ccc cccc cccc cccc cccc ccccc ccccc ccccc ccccc")
        lines.append(f"%f  {header.base_pos_vel:.7f}  {header.base_clk_rate:.9f}  0.00000000000  0.000000000000000")
        lines.append("%f  0.0000000  0.000000000  0.00000000000  0.000000000000000")
        lines.append("%i    0    0    0    0      0      0      0      0         0")
        lines.append("%i    0    0    0    0      0      0      0      0         0")
        lines.append("/* GENERATED BY PYGNSS_RT")
        lines.append("/*")
        lines.append("/*")
        lines.append("/*")

        return lines

    def _format_epoch(self, epoch: SP3Epoch) -> list[str]:
        """Format epoch data lines."""
        lines = []
        dt = epoch.datetime

        # Epoch header
        sec = dt.second + dt.microsecond / 1_000_000
        line = f"*  {dt.year:4d} {dt.month:2d} {dt.day:2d} {dt.hour:2d} {dt.minute:2d}{sec:11.8f}"
        lines.append(line)

        # Position lines
        for sat_id, pos in sorted(epoch.positions.items()):
            line = f"P{sat_id:>3s}{pos.x:14.6f}{pos.y:14.6f}{pos.z:14.6f}{pos.clock:14.6f}"
            lines.append(line)

        # Velocity lines (if present)
        for sat_id, vel in sorted(epoch.velocities.items()):
            line = f"V{sat_id:>3s}{vel.vx:14.6f}{vel.vy:14.6f}{vel.vz:14.6f}{vel.clock_rate:14.6f}"
            lines.append(line)

        return lines


# =============================================================================
# Orbit Product Status Tracking
# =============================================================================

class OrbitStatus(str, Enum):
    """Status values for orbit product records."""

    WAITING = "Waiting"
    DOWNLOADED = "Downloaded"
    VERIFIED = "Verified"
    FAILED = "Failed"
    TOO_LATE = "Too Late"


@dataclass
class OrbitEntry:
    """Orbit/ERP product database entry."""

    provider: str  # IGS, CODE, etc.
    product_type: str  # orbit, erp, clock
    tier: str  # final, rapid, ultra
    gps_week: int
    day_of_week: int
    mjd: float
    status: OrbitStatus = OrbitStatus.WAITING
    filename: str | None = None
    local_path: str | None = None
    file_size: int | None = None
    download_time: datetime | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None

    @property
    def wwwwd(self) -> str:
        """Get GPS week + day of week string (e.g., '23451')."""
        return f"{self.gps_week:04d}{self.day_of_week}"

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "provider": self.provider,
            "product_type": self.product_type,
            "tier": self.tier,
            "gps_week": self.gps_week,
            "day_of_week": self.day_of_week,
            "mjd": self.mjd,
            "status": self.status.value,
            "filename": self.filename,
            "local_path": self.local_path,
            "file_size": self.file_size,
        }


class OrbitDataManager:
    """Manages orbit/ERP product tracking in database.

    Replaces Perl ORBIT.pm database functionality.

    Tracks orbit products across multiple providers and tiers:
    - IGS final, rapid, ultra-rapid orbits
    - CODE final orbits
    - ERP (Earth Rotation Parameters)
    - Clock files
    """

    TABLE_NAME = "orbit_products"

    def __init__(self, db: "DatabaseManager"):
        """Initialize orbit data manager.

        Args:
            db: Database manager instance
        """
        self.db = db

    def table_exists(self) -> bool:
        """Check if orbit products table exists."""
        row = self.db.fetchone(
            f"SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '{self.TABLE_NAME}'"
        )
        return row is not None and row[0] > 0

    def create_table(self) -> None:
        """Create the orbit products tracking table."""
        self.db.execute(f"""
            CREATE TABLE IF NOT EXISTS {self.TABLE_NAME} (
                provider VARCHAR NOT NULL,
                product_type VARCHAR NOT NULL,
                tier VARCHAR NOT NULL,
                gps_week INTEGER NOT NULL,
                day_of_week INTEGER NOT NULL,
                mjd DOUBLE NOT NULL,
                status VARCHAR(30) DEFAULT '{OrbitStatus.WAITING.value}',
                filename VARCHAR,
                local_path VARCHAR,
                file_size BIGINT,
                download_time TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (provider, product_type, tier, gps_week, day_of_week)
            )
        """)

        # Create indexes
        self.db.execute(
            f"CREATE INDEX IF NOT EXISTS idx_orbit_mjd ON {self.TABLE_NAME}(mjd)"
        )
        self.db.execute(
            f"CREATE INDEX IF NOT EXISTS idx_orbit_status ON {self.TABLE_NAME}(status)"
        )
        self.db.execute(
            f"CREATE INDEX IF NOT EXISTS idx_orbit_week ON {self.TABLE_NAME}(gps_week)"
        )

    def ensure_table(self) -> None:
        """Ensure the orbit products table exists."""
        if not self.table_exists():
            self.create_table()

    def maintain(
        self,
        provider: str = "IGS",
        product_type: str = "orbit",
        tier: str = "final",
        reference_date: GNSSDate | None = None,
    ) -> int:
        """Maintain orbit tracking table by adding entry for current day.

        Args:
            provider: Product provider
            product_type: Product type (orbit, erp, clock)
            tier: Product tier (final, rapid, ultra)
            reference_date: Reference date (defaults to today)

        Returns:
            Number of entries added
        """
        if reference_date is None:
            reference_date = GNSSDate.now()

        # Check if entry already exists
        existing = self.db.fetchone(
            f"""
            SELECT 1 FROM {self.TABLE_NAME}
            WHERE provider = ? AND product_type = ? AND tier = ?
              AND gps_week = ? AND day_of_week = ?
            """,
            (provider, product_type, tier, reference_date.gps_week, reference_date.day_of_week),
        )

        if existing:
            return 0

        # Add new entry
        self.db.execute(
            f"""
            INSERT INTO {self.TABLE_NAME}
            (provider, product_type, tier, gps_week, day_of_week, mjd, status)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                provider,
                product_type,
                tier,
                reference_date.gps_week,
                reference_date.day_of_week,
                reference_date.mjd,
                OrbitStatus.WAITING.value,
            ),
        )

        return 1

    def fill_gaps(
        self,
        provider: str = "IGS",
        product_type: str = "orbit",
        tier: str = "final",
        days_back: int = 30,
        reference_date: GNSSDate | None = None,
    ) -> int:
        """Fill gaps in orbit tracking table.

        Args:
            provider: Product provider
            product_type: Product type
            tier: Product tier
            days_back: Number of days to check back
            reference_date: Reference date

        Returns:
            Number of entries added
        """
        if reference_date is None:
            reference_date = GNSSDate.now()

        # Get min and max MJD in table
        row = self.db.fetchone(
            f"""
            SELECT MIN(mjd), MAX(mjd) FROM {self.TABLE_NAME}
            WHERE provider = ? AND product_type = ? AND tier = ?
            """,
            (provider, product_type, tier),
        )

        if row is None or row[0] is None:
            # Table empty, start from days_back
            start_mjd = reference_date.mjd - days_back
            end_mjd = reference_date.mjd
        else:
            start_mjd = row[0]
            end_mjd = row[1]

        added = 0
        current_mjd = start_mjd

        while current_mjd <= end_mjd:
            current_date = GNSSDate.from_mjd(current_mjd)

            # Check if entry exists
            existing = self.db.fetchone(
                f"""
                SELECT 1 FROM {self.TABLE_NAME}
                WHERE provider = ? AND product_type = ? AND tier = ?
                  AND gps_week = ? AND day_of_week = ?
                """,
                (provider, product_type, tier, current_date.gps_week, current_date.day_of_week),
            )

            if not existing:
                self.db.execute(
                    f"""
                    INSERT INTO {self.TABLE_NAME}
                    (provider, product_type, tier, gps_week, day_of_week, mjd, status)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        provider,
                        product_type,
                        tier,
                        current_date.gps_week,
                        current_date.day_of_week,
                        current_date.mjd,
                        OrbitStatus.WAITING.value,
                    ),
                )
                added += 1

            current_mjd += 1

        return added

    def get_waiting_list(
        self,
        provider: str | None = None,
        product_type: str | None = None,
        tier: str | None = None,
        limit: int | None = None,
    ) -> list[OrbitEntry]:
        """Get list of products waiting for download.

        Args:
            provider: Optional provider filter
            product_type: Optional product type filter
            tier: Optional tier filter
            limit: Optional limit on results

        Returns:
            List of OrbitEntry objects
        """
        conditions = ["status = ?"]
        params: list[Any] = [OrbitStatus.WAITING.value]

        if provider:
            conditions.append("provider = ?")
            params.append(provider)
        if product_type:
            conditions.append("product_type = ?")
            params.append(product_type)
        if tier:
            conditions.append("tier = ?")
            params.append(tier)

        where_clause = " AND ".join(conditions)

        query = f"""
            SELECT provider, product_type, tier, gps_week, day_of_week, mjd,
                   status, filename, local_path, file_size, download_time,
                   created_at, updated_at
            FROM {self.TABLE_NAME}
            WHERE {where_clause}
            ORDER BY mjd
        """

        if limit:
            query += f" LIMIT {limit}"

        rows = self.db.fetchall(query, tuple(params))

        return [
            OrbitEntry(
                provider=row[0],
                product_type=row[1],
                tier=row[2],
                gps_week=row[3],
                day_of_week=row[4],
                mjd=row[5],
                status=OrbitStatus(row[6]),
                filename=row[7],
                local_path=row[8],
                file_size=row[9],
                download_time=row[10],
                created_at=row[11],
                updated_at=row[12],
            )
            for row in rows
        ]

    def update_downloaded(
        self,
        provider: str,
        product_type: str,
        tier: str,
        gps_week: int,
        day_of_week: int,
        filename: str,
        local_path: str,
        file_size: int | None = None,
    ) -> bool:
        """Mark a product as downloaded.

        Args:
            provider: Product provider
            product_type: Product type
            tier: Product tier
            gps_week: GPS week
            day_of_week: Day of GPS week
            filename: Downloaded filename
            local_path: Local file path
            file_size: File size in bytes

        Returns:
            True if updated
        """
        self.db.execute(
            f"""
            UPDATE {self.TABLE_NAME}
            SET status = ?,
                filename = ?,
                local_path = ?,
                file_size = ?,
                download_time = CURRENT_TIMESTAMP,
                updated_at = CURRENT_TIMESTAMP
            WHERE provider = ? AND product_type = ? AND tier = ?
              AND gps_week = ? AND day_of_week = ?
            """,
            (
                OrbitStatus.DOWNLOADED.value,
                filename,
                local_path,
                file_size,
                provider,
                product_type,
                tier,
                gps_week,
                day_of_week,
            ),
        )
        return True

    def update_failed(
        self,
        provider: str,
        product_type: str,
        tier: str,
        gps_week: int,
        day_of_week: int,
    ) -> bool:
        """Mark a product as failed.

        Args:
            provider: Product provider
            product_type: Product type
            tier: Product tier
            gps_week: GPS week
            day_of_week: Day of GPS week

        Returns:
            True if updated
        """
        self.db.execute(
            f"""
            UPDATE {self.TABLE_NAME}
            SET status = ?, updated_at = CURRENT_TIMESTAMP
            WHERE provider = ? AND product_type = ? AND tier = ?
              AND gps_week = ? AND day_of_week = ?
            """,
            (
                OrbitStatus.FAILED.value,
                provider,
                product_type,
                tier,
                gps_week,
                day_of_week,
            ),
        )
        return True

    def set_too_late(
        self,
        late_days: int = 30,
        reference_date: GNSSDate | None = None,
    ) -> int:
        """Mark old waiting entries as 'Too Late'.

        Args:
            late_days: Days threshold for "too late"
            reference_date: Reference date

        Returns:
            Number of entries updated
        """
        if reference_date is None:
            reference_date = GNSSDate.now()

        cutoff_mjd = reference_date.mjd - late_days

        # Count entries to update
        count_row = self.db.fetchone(
            f"""
            SELECT COUNT(*) FROM {self.TABLE_NAME}
            WHERE status = ? AND mjd < ?
            """,
            (OrbitStatus.WAITING.value, cutoff_mjd),
        )
        count = count_row[0] if count_row else 0

        # Update
        self.db.execute(
            f"""
            UPDATE {self.TABLE_NAME}
            SET status = ?, updated_at = CURRENT_TIMESTAMP
            WHERE status = ? AND mjd < ?
            """,
            (OrbitStatus.TOO_LATE.value, OrbitStatus.WAITING.value, cutoff_mjd),
        )

        return count

    def cleanup_old_entries(self, days_to_keep: int = 180) -> int:
        """Remove old entries.

        Args:
            days_to_keep: Number of days of data to retain

        Returns:
            Number of entries deleted
        """
        cutoff_mjd = GNSSDate.now().mjd - days_to_keep

        count_row = self.db.fetchone(
            f"SELECT COUNT(*) FROM {self.TABLE_NAME} WHERE mjd < ?",
            (cutoff_mjd,),
        )
        count = count_row[0] if count_row else 0

        self.db.execute(
            f"DELETE FROM {self.TABLE_NAME} WHERE mjd < ?",
            (cutoff_mjd,),
        )

        return count


# =============================================================================
# Utility Functions
# =============================================================================

def build_orbit_filename(
    provider: str,
    tier: str,
    gps_week: int,
    day_of_week: int,
    product_type: str = "orbit",
) -> str:
    """Build standard orbit product filename.

    Args:
        provider: Product provider (IGS, CODE, etc.)
        tier: Product tier (final, rapid, ultra)
        gps_week: GPS week number
        day_of_week: Day of GPS week (0-6)
        product_type: Product type

    Returns:
        Filename string
    """
    if provider.upper() == "IGS":
        if tier == "final":
            prefix = "igs"
        elif tier == "rapid":
            prefix = "igr"
        else:
            prefix = "igu"

        if product_type == "orbit":
            return f"{prefix}{gps_week:04d}{day_of_week}.sp3.Z"
        elif product_type == "erp":
            return f"{prefix}{gps_week:04d}7.erp.Z"
        elif product_type == "clock":
            return f"{prefix}{gps_week:04d}{day_of_week}.clk.Z"

    elif provider.upper() == "CODE":
        if product_type == "orbit":
            return f"COD{gps_week:04d}{day_of_week}.EPH.Z"
        elif product_type == "erp":
            return f"COD{gps_week:04d}7.ERP.Z"

    # Generic fallback
    return f"{provider.lower()}{gps_week:04d}{day_of_week}.sp3.Z"


def parse_orbit_filename(filename: str) -> dict[str, Any] | None:
    """Parse orbit product filename to extract metadata.

    Args:
        filename: Orbit filename (e.g., 'igs23451.sp3.Z')

    Returns:
        Dictionary with provider, gps_week, day_of_week, etc. or None
    """
    # Remove compression extension
    base = filename.replace(".Z", "").replace(".gz", "")

    # IGS pattern: igs/igr/iguWWWWD.sp3
    match = re.match(r"(igs|igr|igu)(\d{4})(\d)\.(\w+)", base, re.IGNORECASE)
    if match:
        prefix, week, dow, ext = match.groups()
        tier_map = {"igs": "final", "igr": "rapid", "igu": "ultra"}
        return {
            "provider": "IGS",
            "tier": tier_map.get(prefix.lower(), "unknown"),
            "gps_week": int(week),
            "day_of_week": int(dow),
            "extension": ext,
        }

    # CODE pattern: CODWWWWD.EPH
    match = re.match(r"COD(\d{4})(\d)\.(\w+)", base)
    if match:
        week, dow, ext = match.groups()
        return {
            "provider": "CODE",
            "tier": "final",
            "gps_week": int(week),
            "day_of_week": int(dow),
            "extension": ext,
        }

    return None
