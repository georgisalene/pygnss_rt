"""
Bernese .STA file parser.

Parses BSW station information files to extract receiver/antenna
specifications and benchmark heights for specific dates.

Replaces Perl BSWSTA.pm module.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from pygnss_rt.utils.dates import mjd_from_date
from pygnss_rt.utils.logging import get_logger


logger = get_logger(__name__)


@dataclass
class StationRecord:
    """Station equipment record from BSW .STA file."""

    station_name: str
    flag: str
    from_mjd: float
    to_mjd: float
    receiver_type: str
    antenna_type: str
    receiver_number: str
    antenna_number: str
    north_offset: float
    east_offset: float
    up_offset: float  # Antenna height above benchmark
    description: str
    remark: str

    @property
    def from_date(self) -> datetime:
        """Get start date as datetime."""
        from pygnss_rt.utils.dates import date_from_mjd
        return date_from_mjd(self.from_mjd)

    @property
    def to_date(self) -> datetime:
        """Get end date as datetime."""
        from pygnss_rt.utils.dates import date_from_mjd
        return date_from_mjd(self.to_mjd)


class BSWStationFile:
    """Parser for Bernese .STA files.

    The .STA file contains station equipment history including:
    - Receiver type and serial number
    - Antenna type and serial number
    - Antenna eccentricities (north, east, up)
    - Valid date ranges for each configuration
    """

    # Column positions for TYPE 002 records (receiver/antenna info)
    # Based on BSW 5.4 format
    COLUMNS = {
        "station_name": (0, 16),
        "flag": (22, 25),
        "from": (27, 46),
        "to": (48, 67),
        "receiver_type": (69, 89),
        "antenna_type": (121, 141),
        "receiver_number": (113, 119),
        "antenna_number": (165, 171),
        "north": (173, 181),
        "east": (183, 191),
        "up": (193, 201),
        "description": (203, 225),
        "remark": (227, 251),
    }

    def __init__(self):
        """Initialize BSW station file parser."""
        self._records: dict[str, list[StationRecord]] = {}

    def load(self, sta_file: Path | str) -> int:
        """Load and parse a .STA file.

        Args:
            sta_file: Path to the .STA file

        Returns:
            Number of records loaded
        """
        path = Path(sta_file)
        if not path.exists():
            raise FileNotFoundError(f"STA file not found: {path}")

        logger.info("Loading BSW STA file", path=str(path))

        in_type2_section = False
        in_type3_section = False
        record_count = 0

        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            for line_num, line in enumerate(f, 1):
                # Detect section markers
                if "RECEIVER TYPE" in line and "RECEIVER SERIAL NBR" in line:
                    in_type2_section = True
                    continue
                if "TYPE 003: HANDLING OF STATION PROBLEMS" in line:
                    in_type3_section = True
                    in_type2_section = False
                    continue

                # Parse TYPE 002 records
                if in_type2_section and not in_type3_section:
                    if not line.strip():
                        continue
                    if line.startswith("*") or "STATION NAME" in line:
                        continue

                    try:
                        record = self._parse_line(line)
                        if record:
                            station_key = record.station_name.strip().upper()
                            if station_key not in self._records:
                                self._records[station_key] = []
                            self._records[station_key].append(record)
                            record_count += 1
                    except Exception as e:
                        logger.warning(
                            "Failed to parse STA line",
                            line_num=line_num,
                            error=str(e),
                        )

        logger.info(
            "Loaded BSW STA file",
            path=str(path),
            stations=len(self._records),
            records=record_count,
        )

        return record_count

    def _parse_line(self, line: str) -> StationRecord | None:
        """Parse a single TYPE 002 record line."""
        if len(line) < 100:
            return None

        def extract(col_name: str) -> str:
            """Extract column value from line."""
            start, end = self.COLUMNS[col_name]
            if len(line) > start:
                return line[start:min(end, len(line))].strip()
            return ""

        station_name = extract("station_name")
        if not station_name:
            return None

        # Parse dates
        from_str = extract("from")
        to_str = extract("to")

        from_mjd = self._parse_datetime_to_mjd(from_str)
        if to_str:
            to_mjd = self._parse_datetime_to_mjd(to_str)
        else:
            # Open-ended record - use far future date
            to_mjd = mjd_from_date(2099, 12, 31, 23, 59, 59)

        # Parse numeric offsets
        def parse_float(s: str) -> float:
            try:
                return float(s) if s else 0.0
            except ValueError:
                return 0.0

        return StationRecord(
            station_name=station_name,
            flag=extract("flag"),
            from_mjd=from_mjd,
            to_mjd=to_mjd,
            receiver_type=extract("receiver_type"),
            antenna_type=extract("antenna_type"),
            receiver_number=extract("receiver_number"),
            antenna_number=extract("antenna_number"),
            north_offset=parse_float(extract("north")),
            east_offset=parse_float(extract("east")),
            up_offset=parse_float(extract("up")),
            description=extract("description"),
            remark=extract("remark"),
        )

    def _parse_datetime_to_mjd(self, dt_str: str) -> float:
        """Parse datetime string 'YYYY MM DD HH MN SS' to MJD."""
        if not dt_str or len(dt_str) < 10:
            return 0.0

        parts = dt_str.split()
        if len(parts) < 3:
            return 0.0

        try:
            year = int(parts[0])
            month = int(parts[1])
            day = int(parts[2])
            hour = int(parts[3]) if len(parts) > 3 else 0
            minute = int(parts[4]) if len(parts) > 4 else 0
            second = int(parts[5]) if len(parts) > 5 else 0

            return mjd_from_date(year, month, day, hour, minute, second)
        except (ValueError, IndexError):
            return 0.0

    def get_record(
        self,
        station: str,
        year: int,
        month: int,
        day: int,
    ) -> StationRecord | None:
        """Get station record valid for a specific date.

        Args:
            station: Station name (4-char code)
            year: Year
            month: Month
            day: Day

        Returns:
            StationRecord if found, None otherwise
        """
        station_key = station.strip().upper()

        # Also try with spaces padded to match BSW format
        if len(station_key) == 4:
            # Try both exact match and with trailing spaces
            keys_to_try = [
                station_key,
                f"{station_key}            ",  # 16 chars total
                station_key.ljust(16),
            ]
        else:
            keys_to_try = [station_key]

        records = None
        for key in keys_to_try:
            if key in self._records:
                records = self._records[key]
                break

        if not records:
            logger.warning(
                "Station not found in STA file",
                station=station,
            )
            return None

        # Calculate MJD for query date
        query_mjd = mjd_from_date(year, month, day)

        # Find record valid for this date
        for record in records:
            if record.from_mjd <= query_mjd <= record.to_mjd:
                return record

        logger.warning(
            "No valid record for station/date",
            station=station,
            date=f"{year}-{month:02d}-{day:02d}",
            mjd=query_mjd,
        )
        return None

    def get_antenna_height(
        self,
        station: str,
        year: int,
        month: int,
        day: int,
    ) -> float | None:
        """Get antenna height (up offset) for station at date.

        Args:
            station: Station name
            year: Year
            month: Month
            day: Day

        Returns:
            Antenna height in meters, or None if not found
        """
        record = self.get_record(station, year, month, day)
        if record:
            return record.up_offset
        return None

    def get_stations(self) -> list[str]:
        """Get list of all station names in file."""
        return list(self._records.keys())

    def __len__(self) -> int:
        """Return number of stations."""
        return len(self._records)

    def __contains__(self, station: str) -> bool:
        """Check if station exists in file."""
        return station.strip().upper() in self._records
