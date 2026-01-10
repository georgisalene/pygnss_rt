"""
Bernese STA File Writer - Port of DB2BSWSta52.pm from i-BSWSTA.

Generates Bernese GNSS Software .STA (station information) files
from parsed site log data.

The STA file format contains:
- TYPE 001: Station renaming information
- TYPE 002: Station equipment information (receiver/antenna/eccentricities)
- TYPE 003: Station problem handling
- TYPE 004: Coordinate constraints
- TYPE 005: Station type handling

Author: Original Perl by E.J. Orliac (University of Nottingham)
        Python port for pygnss_rt
"""

import logging
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Optional

from pygnss_rt.stations.site_log_parser import SiteLogData, ReceiverInfo, AntennaInfo

logger = logging.getLogger(__name__)

# Far future date for open-ended validity
FAR_FUTURE = datetime(2099, 12, 31, 23, 59, 59)

# MJD reference (Modified Julian Date epoch: Nov 17, 1858)
MJD_EPOCH = datetime(1858, 11, 17)


def datetime_to_mjd(dt: datetime) -> float:
    """Convert datetime to Modified Julian Date.

    Port of Perl conversion from DB2BSWSta52.pm.

    Args:
        dt: Datetime object

    Returns:
        Modified Julian Date as float
    """
    delta = dt - MJD_EPOCH
    return delta.days + delta.seconds / 86400.0


def mjd_to_datetime(mjd: float) -> datetime:
    """Convert Modified Julian Date to datetime.

    Args:
        mjd: Modified Julian Date

    Returns:
        Datetime object
    """
    from datetime import timedelta
    days = int(mjd)
    frac = mjd - days
    seconds = int(frac * 86400)
    return MJD_EPOCH + timedelta(days=days, seconds=seconds)


@dataclass
class STAEvent:
    """Represents a station equipment change event for STA file."""
    start_date: datetime
    end_date: datetime
    receiver_type: str
    receiver_serial: str
    antenna_type: str
    antenna_serial: str
    radome_type: str
    north_ecc: float
    east_ecc: float
    up_ecc: float
    site_name: str = ""

    @property
    def start_mjd(self) -> float:
        """Get start date as Modified Julian Date."""
        return datetime_to_mjd(self.start_date)

    @property
    def end_mjd(self) -> float:
        """Get end date as Modified Julian Date."""
        return datetime_to_mjd(self.end_date)


@dataclass
class STAStationInfo:
    """Station information for STA file generation."""
    station_id: str  # 4-char ID
    domes_number: str = ""
    site_name: str = ""
    events: list[STAEvent] = field(default_factory=list)

    @property
    def full_station_name(self) -> str:
        """Get full station name with optional DOMES."""
        return f"{self.station_id.upper()} {self.domes_number}".strip()

    @property
    def station_name_no_domes(self) -> str:
        """Get station name without DOMES."""
        return self.station_id.upper()


class STAFileWriter:
    """
    Write Bernese STA files from site log data.

    Port of DB2BSWSta52::writeSTA from i-BSWSTA.
    """

    def __init__(self, use_domes: bool = False):
        """
        Initialize STA file writer.

        Args:
            use_domes: Whether to include DOMES numbers in station names
        """
        self.use_domes = use_domes

    def write_sta_file(
        self,
        output_path: str | Path,
        station_data: list[SiteLogData],
        title: str = "i-BSWSTA generated"
    ) -> int:
        """
        Write a STA file from site log data.

        Args:
            output_path: Path for output .STA file
            station_data: List of parsed site log data
            title: Title line for the STA file

        Returns:
            Number of stations written
        """
        output_path = Path(output_path)

        # Build station info from site log data
        stations = self._build_station_info(station_data)

        if not stations:
            logger.warning("No valid stations to write")
            return 0

        # Write the STA file
        now = datetime.utcnow()
        with open(output_path, 'w') as f:
            self._write_header(f, title, now)
            self._write_type_001(f, stations)
            self._write_type_002(f, stations)
            self._write_type_003(f)
            self._write_type_004(f)
            self._write_type_005(f)

        logger.info(f"Wrote STA file with {len(stations)} stations: {output_path}")
        return len(stations)

    def _build_station_info(self, station_data: list[SiteLogData]) -> list[STAStationInfo]:
        """Build STA station info from parsed site log data."""
        stations = []

        for data in station_data:
            if not data.station_id:
                continue

            info = STAStationInfo(
                station_id=data.station_id.upper(),
                domes_number=data.domes_number,
                site_name=data.site_identification.site_name[:22] if data.site_identification.site_name else ""
            )

            # Build equipment change events
            events = self._build_events(data)
            if events:
                info.events = events
                stations.append(info)

        return sorted(stations, key=lambda s: s.station_id.lower())

    def _build_events(self, data: SiteLogData) -> list[STAEvent]:
        """
        Build equipment change events from receiver and antenna history.

        This implements the logic from DB2BSWSta52.pm that determines
        validity periods based on overlapping receiver and antenna
        installation dates.

        Edge cases handled (from Perl DB2BSWSta.pm):
        1. Last equipment without date_removed -> use FAR_FUTURE
        2. Date alignment: adjust dates to avoid gaps/overlaps
        3. Same-day changes: use time component to disambiguate
        4. Equipment with only one receiver or antenna entry
        5. Merge consecutive events with identical equipment
        """
        if not data.receivers or not data.antennas:
            return []

        events = []

        # Sort receivers and antennas by install date
        receivers = sorted(
            [r for r in data.receivers if r.date_installed],
            key=lambda r: r.date_installed
        )
        antennas = sorted(
            [a for a in data.antennas if a.date_installed],
            key=lambda a: a.date_installed
        )

        if not receivers or not antennas:
            return []

        # Get all change dates (installation dates AND removal dates)
        all_dates = set()
        for r in receivers:
            if r.date_installed:
                all_dates.add(r.date_installed)
            if r.date_removed and r.date_removed < FAR_FUTURE:
                all_dates.add(r.date_removed)
        for a in antennas:
            if a.date_installed:
                all_dates.add(a.date_installed)
            if a.date_removed and a.date_removed < FAR_FUTURE:
                all_dates.add(a.date_removed)

        all_dates = sorted(all_dates)

        # For each period, determine active receiver and antenna
        for i, start_date in enumerate(all_dates):
            # Find active receiver at this date
            active_receiver = self._find_active_equipment(receivers, start_date)

            # Find active antenna at this date
            active_antenna = self._find_active_equipment(antennas, start_date)

            if not active_receiver or not active_antenna:
                continue

            # Determine end date
            if i + 1 < len(all_dates):
                # End at next change date (minus 1 second for boundary)
                from datetime import timedelta
                end_date = all_dates[i + 1] - timedelta(seconds=1)
            else:
                # Last period - use earliest of removed dates or far future
                rec_end = active_receiver.date_removed or FAR_FUTURE
                ant_end = active_antenna.date_removed or FAR_FUTURE
                end_date = min(rec_end, ant_end)

            # Skip if end_date is before or equal to start_date (invalid period)
            if end_date <= start_date:
                continue

            # Create event
            event = STAEvent(
                start_date=start_date,
                end_date=end_date,
                receiver_type=self._clean_receiver_type(active_receiver.receiver_type),
                receiver_serial=self._clean_serial(active_receiver.serial_number),
                antenna_type=self._format_antenna_type(
                    active_antenna.antenna_type,
                    active_antenna.radome_type
                ),
                antenna_serial=self._clean_serial(active_antenna.serial_number),
                radome_type=active_antenna.radome_type or "NONE",
                north_ecc=active_antenna.marker_arp_north_ecc,
                east_ecc=active_antenna.marker_arp_east_ecc,
                up_ecc=active_antenna.marker_arp_up_ecc,
                site_name=data.site_identification.site_name[:22] if data.site_identification.site_name else ""
            )
            events.append(event)

        # Merge consecutive events with identical equipment
        events = self._merge_identical_events(events)

        return events

    def _find_active_equipment(self, equipment_list: list, at_date: datetime):
        """Find the equipment that was active at a given date.

        Port of Perl DB2BSWSta.pm equipment lookup logic.

        Args:
            equipment_list: List of receivers or antennas
            at_date: The date to check

        Returns:
            The active equipment item, or None
        """
        active = None
        for eq in equipment_list:
            if eq.date_installed and eq.date_installed <= at_date:
                removed = eq.date_removed or FAR_FUTURE
                # Use > instead of >= to handle exact boundary dates correctly
                if removed > at_date:
                    # If we already have an active one, prefer the more recent
                    if active is None or eq.date_installed > active.date_installed:
                        active = eq
        return active

    def _merge_identical_events(self, events: list[STAEvent]) -> list[STAEvent]:
        """Merge consecutive events that have identical equipment.

        This handles cases where a date boundary exists but the equipment
        didn't actually change (e.g., only metadata updated).

        Args:
            events: List of events sorted by start_date

        Returns:
            List with consecutive identical events merged
        """
        if len(events) < 2:
            return events

        merged = [events[0]]

        for event in events[1:]:
            prev = merged[-1]

            # Check if equipment is identical
            if (prev.receiver_type == event.receiver_type and
                    prev.receiver_serial == event.receiver_serial and
                    prev.antenna_type == event.antenna_type and
                    prev.antenna_serial == event.antenna_serial and
                    prev.north_ecc == event.north_ecc and
                    prev.east_ecc == event.east_ecc and
                    prev.up_ecc == event.up_ecc):
                # Extend the previous event
                prev.end_date = event.end_date
            else:
                merged.append(event)

        return merged

    def _clean_receiver_type(self, receiver_type: str) -> str:
        """Clean receiver type string for STA file."""
        if not receiver_type:
            return ""
        # Take first 20 characters, remove extra spaces
        return receiver_type.strip()[:20]

    def _clean_serial(self, serial: str) -> str:
        """Clean serial number for STA file."""
        if not serial:
            return "999999"

        # Remove problematic characters
        serial = serial.replace('?', '').replace('=', '').replace('.', '')
        serial = serial.replace('-', '').replace('_', '').replace('#', '')
        serial = serial.replace(' ', '').replace('/', '').replace('(', '').replace(')', '')

        # Remove letters (Bernese prefers numeric)
        serial = ''.join(c for c in serial if c.isdigit())

        if not serial:
            return "999999"

        return serial[-6:]  # Last 6 digits

    def _format_antenna_type(self, antenna_type: str, radome_type: str) -> str:
        """
        Format antenna type with radome for STA file.

        Antenna type must be exactly 20 characters with radome at the end.
        """
        if not antenna_type:
            return ""

        # Get just the antenna name (first word typically)
        parts = antenna_type.strip().split()
        ant_name = parts[0] if parts else antenna_type

        # Radome is 4 characters, default to NONE
        radome = (radome_type or "NONE").strip()[:4]

        # Pad antenna name to fit with radome
        max_ant_len = 20 - len(radome)
        ant_name = ant_name[:max_ant_len]

        # Build 20-char string
        padding = 20 - len(ant_name) - len(radome)
        return f"{ant_name}{' ' * padding}{radome}"

    def _write_header(self, f, title: str, now: datetime) -> None:
        """Write STA file header."""
        date_str = now.strftime("%d-%m-%Y %H:%M")
        f.write(f"{title:<63} {date_str}\n")
        f.write("-" * 80 + "\n\n")
        f.write("FORMAT VERSION: 1.01\n")
        f.write("TECHNIQUE:      GNSS\n\n")

    def _write_type_001(self, f, stations: list[STAStationInfo]) -> None:
        """Write TYPE 001: Station renaming section."""
        f.write("TYPE 001: RENAMING OF STATIONS\n")
        f.write("-" * 30 + "\n\n")
        f.write("STATION NAME          FLG          FROM                   TO         "
                "OLD STATION NAME      REMARK\n")
        f.write("****************      ***  YYYY MM DD HH MM SS  YYYY MM DD HH MM SS  "
                "****************      ************************\n")

        for station in stations:
            if station.events:
                name = station.full_station_name if self.use_domes else station.station_name_no_domes
                old_name = f"{station.station_id.lower()}*"
                f.write(f"{name:<16}      001  "
                        f"{'':19}  "
                        f"{'':19}  "
                        f"{old_name.upper():<16}      "
                        f"{'i-BSWSTA generated':<24}\n")

        f.write("\n\n")

    def _write_type_002(self, f, stations: list[STAStationInfo]) -> None:
        """Write TYPE 002: Station information section."""
        f.write("TYPE 002: STATION INFORMATION\n")
        f.write("-" * 29 + "\n\n")

        f.write("STATION NAME          FLG          FROM                   TO         "
                "RECEIVER TYPE         RECEIVER SERIAL NBR   REC #   "
                "ANTENNA TYPE          ANTENNA SERIAL NBR    ANT #    "
                "NORTH      EAST      UP      DESCRIPTION             REMARK\n")
        f.write("****************      ***  YYYY MM DD HH MM SS  YYYY MM DD HH MM SS  "
                "********************  ********************  ******  "
                "********************  ********************  ******  "
                "***.****  ***.****  ***.****  **********************  ************************\n")

        for station in stations:
            name = station.full_station_name if self.use_domes else station.station_name_no_domes

            for event in station.events:
                # Format dates
                start = event.start_date
                end = event.end_date

                start_str = f"{start.year:4d} {start.month:02d} {start.day:02d} " \
                           f"{start.hour:02d} {start.minute:02d} {start.second:02d}"

                if end.year >= 2099:
                    end_str = " " * 19
                else:
                    end_str = f"{end.year:4d} {end.month:02d} {end.day:02d} " \
                             f"{end.hour:02d} {end.minute:02d} {end.second:02d}"

                # Write line
                f.write(f"{name:<16}      001  ")
                f.write(f"{start_str}  ")
                f.write(f"{end_str}  ")
                f.write(f"{event.receiver_type:<20}  ")
                f.write(f"{event.receiver_serial:>20}  ")
                f.write(f"{event.receiver_serial[-6:]:>6}  ")
                f.write(f"{event.antenna_type:<20}  ")
                f.write(f"{event.antenna_serial:>20}  ")
                f.write(f"{event.antenna_serial[-6:]:>6}  ")
                f.write(f"{event.north_ecc:8.4f}  ")
                f.write(f"{event.east_ecc:8.4f}  ")
                f.write(f"{event.up_ecc:8.4f}  ")
                f.write(f"{event.site_name:<22}  ")
                f.write("i-BSWSTA generated\n")

        f.write("\n\n")

    def _write_type_003(self, f) -> None:
        """Write TYPE 003: Station problems section (empty)."""
        f.write("TYPE 003: HANDLING OF STATION PROBLEMS\n")
        f.write("-" * 38 + "\n\n")
        f.write("STATION NAME          FLG          FROM                   TO         REMARK\n")
        f.write("****************      ***  YYYY MM DD HH MM SS  YYYY MM DD HH MM SS  "
                "************************************************************\n\n\n")

    def _write_type_004(self, f) -> None:
        """Write TYPE 004: Coordinate constraints section (empty)."""
        f.write("TYPE 004: STATION COORDINATES AND VELOCITIES (ADDNEQ)\n")
        f.write("-" * 53 + "\n")
        f.write("                                            RELATIVE CONSTR. POSITION     "
                "RELATIVE CONSTR. VELOCITY\n")
        f.write("STATION NAME 1        STATION NAME 2        NORTH     EAST      UP        "
                "NORTH     EAST      UP\n")
        f.write("****************      ****************      **.*****  **.*****  **.*****  "
                "**.*****  **.*****  **.*****\n\n\n")

    def _write_type_005(self, f) -> None:
        """Write TYPE 005: Station types section (empty)."""
        f.write("TYPE 005: HANDLING STATION TYPES\n")
        f.write("-" * 32 + "\n\n")
        f.write("STATION NAME          FLG  FROM                 TO                   "
                "MARKER TYPE           REMARK\n")
        f.write("****************      ***  YYYY MM DD HH MM SS  YYYY MM DD HH MM SS  "
                "********************  ************************\n\n\n")


def write_sta_file(
    output_path: str | Path,
    station_data: list[SiteLogData],
    use_domes: bool = False,
    title: str = "i-BSWSTA generated"
) -> int:
    """
    Convenience function to write a STA file.

    Args:
        output_path: Path for output .STA file
        station_data: List of parsed site log data
        use_domes: Whether to include DOMES numbers
        title: Title line for the STA file

    Returns:
        Number of stations written
    """
    writer = STAFileWriter(use_domes=use_domes)
    return writer.write_sta_file(output_path, station_data, title)


def write_sta_from_directory(
    site_log_dir: str | Path,
    output_path: str | Path,
    use_domes: bool = False,
    title: str = "i-BSWSTA generated"
) -> int:
    """
    Parse site logs from a directory and write a STA file.

    Args:
        site_log_dir: Directory containing .log files
        output_path: Path for output .STA file
        use_domes: Whether to include DOMES numbers
        title: Title line for the STA file

    Returns:
        Number of stations written
    """
    from pygnss_rt.stations.site_log_parser import parse_site_logs_directory

    # Parse all site logs
    parsed = parse_site_logs_directory(site_log_dir)

    # Write STA file
    station_data = list(parsed.values())
    return write_sta_file(output_path, station_data, use_domes, title)
