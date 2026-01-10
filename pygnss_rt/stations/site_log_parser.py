"""
IGS Site Log Parser - Port of ASCII2XML.pm from i-BSWSTA.

Parses IGS/GNSS site log ASCII files and extracts station metadata,
receiver/antenna history, and other equipment information.

This module provides:
- SiteLogParser: Parse IGS site log ASCII files
- SiteLogData: Structured representation of site log data
- Equipment history tracking (receivers, antennas)
- Date parsing and normalization

Author: Original Perl by E.J. Orliac (University of Nottingham)
        Python port for pygnss_rt
"""

import re
import logging
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass
class ReceiverInfo:
    """GNSS receiver information from site log."""
    receiver_type: str = ""
    satellite_system: str = ""
    serial_number: str = ""
    firmware_version: str = ""
    elevation_cutoff: str = ""
    date_installed: Optional[datetime] = None
    date_removed: Optional[datetime] = None
    temperature_stabilization: str = ""
    notes: str = ""


@dataclass
class AntennaInfo:
    """GNSS antenna information from site log."""
    antenna_type: str = ""
    serial_number: str = ""
    antenna_reference_point: str = ""
    marker_arp_up_ecc: float = 0.0
    marker_arp_north_ecc: float = 0.0
    marker_arp_east_ecc: float = 0.0
    alignment_from_true_north: str = ""
    radome_type: str = ""
    radome_serial_number: str = ""
    antenna_cable_type: str = ""
    antenna_cable_length: str = ""
    date_installed: Optional[datetime] = None
    date_removed: Optional[datetime] = None
    notes: str = ""


@dataclass
class SiteIdentification:
    """Site identification information."""
    site_name: str = ""
    four_character_id: str = ""
    nine_character_id: str = ""
    monument_inscription: str = ""
    iers_domes_number: str = ""
    cdp_number: str = ""
    monument_description: str = ""
    height_of_monument: str = ""
    monument_foundation: str = ""
    foundation_depth: str = ""
    marker_description: str = ""
    date_installed: Optional[datetime] = None
    geologic_characteristic: str = ""
    bedrock_type: str = ""
    bedrock_condition: str = ""
    fracture_spacing: str = ""
    fault_zones_nearby: str = ""
    notes: str = ""


@dataclass
class SiteLocation:
    """Site location information."""
    city: str = ""
    state: str = ""
    country: str = ""
    tectonic_plate: str = ""
    x_coordinate: float = 0.0
    y_coordinate: float = 0.0
    z_coordinate: float = 0.0
    latitude: float = 0.0
    longitude: float = 0.0
    elevation: float = 0.0
    notes: str = ""


@dataclass
class ContactInfo:
    """Contact information from site log."""
    agency: str = ""
    preferred_abbreviation: str = ""
    mailing_address: str = ""
    contact_name: str = ""
    telephone_primary: str = ""
    telephone_secondary: str = ""
    fax: str = ""
    email: str = ""
    notes: str = ""


@dataclass
class MeteorologicalSensor:
    """Meteorological sensor information."""
    sensor_type: str = ""  # humidity, pressure, temperature, water_vapor
    model: str = ""
    manufacturer: str = ""
    serial_number: str = ""
    height_diff_to_antenna: str = ""
    calibration_date: str = ""
    effective_dates: str = ""
    data_sampling_interval: str = ""
    accuracy: str = ""
    aspiration: str = ""
    distance_to_antenna: str = ""  # For water vapor radiometers
    notes: str = ""


@dataclass
class SurveyedLocalTie:
    """Surveyed local ties to other monuments (Section 5)."""
    tied_marker_name: str = ""
    tied_marker_usage: str = ""
    tied_marker_cdp_number: str = ""
    tied_marker_domes_number: str = ""
    differential_dx: float = 0.0  # Differential components (m)
    differential_dy: float = 0.0
    differential_dz: float = 0.0
    accuracy_mm: str = ""
    survey_method: str = ""
    date_measured: Optional[datetime] = None
    notes: str = ""


@dataclass
class FrequencyStandard:
    """Frequency standard information (Section 6)."""
    standard_type: str = ""
    input_frequency: str = ""
    effective_dates: str = ""
    notes: str = ""


@dataclass
class CollocationInformation:
    """Collocation/instrumentation information (Section 7)."""
    instrumentation_type: str = ""
    status: str = ""
    effective_dates: str = ""
    notes: str = ""


@dataclass
class RadioInterference:
    """Radio interference information (Section 9)."""
    radio_interferences: str = ""
    observed_degradations: str = ""
    effective_dates: str = ""
    notes: str = ""


@dataclass
class MultipathSource:
    """Multipath source information (Section 10)."""
    multipath_sources: str = ""
    effective_dates: str = ""
    notes: str = ""


@dataclass
class SignalObstruction:
    """Signal obstruction information (Section 11)."""
    signal_obstructions: str = ""
    effective_dates: str = ""
    notes: str = ""


@dataclass
class LocalEpisodicEvent:
    """Local episodic events (Section 12)."""
    event_date: Optional[datetime] = None
    event_description: str = ""


@dataclass
class MoreInformation:
    """Additional information (Section 13)."""
    primary_data_center: str = ""
    secondary_data_center: str = ""
    url_for_more_information: str = ""
    hardcopy_on_file: str = ""
    site_map: str = ""
    site_diagram: str = ""
    horizon_mask: str = ""
    monument_description: str = ""
    site_pictures: str = ""
    notes: str = ""
    antenna_graphics: str = ""


@dataclass
class SiteLogData:
    """Complete parsed site log data."""
    # Form info
    prepared_by: str = ""
    date_prepared: Optional[datetime] = None
    report_type: str = ""

    # Site identification
    site_identification: SiteIdentification = field(default_factory=SiteIdentification)

    # Site location
    site_location: SiteLocation = field(default_factory=SiteLocation)

    # Equipment history
    receivers: list[ReceiverInfo] = field(default_factory=list)
    antennas: list[AntennaInfo] = field(default_factory=list)

    # Section 5: Surveyed local ties
    surveyed_local_ties: list[SurveyedLocalTie] = field(default_factory=list)

    # Section 6: Frequency standards
    frequency_standards: list[FrequencyStandard] = field(default_factory=list)

    # Section 7: Collocation information
    collocation_info: list[CollocationInformation] = field(default_factory=list)

    # Section 8: Meteorological sensors
    humidity_sensors: list[MeteorologicalSensor] = field(default_factory=list)
    pressure_sensors: list[MeteorologicalSensor] = field(default_factory=list)
    temperature_sensors: list[MeteorologicalSensor] = field(default_factory=list)
    water_vapor_sensors: list[MeteorologicalSensor] = field(default_factory=list)

    # Section 9: Radio interferences
    radio_interferences: list[RadioInterference] = field(default_factory=list)

    # Section 10: Multipath sources
    multipath_sources: list[MultipathSource] = field(default_factory=list)

    # Section 11: Signal obstructions (old format - before contact)
    signal_obstructions: list[SignalObstruction] = field(default_factory=list)

    # Section 12: Local episodic events (old format - before responsible agency)
    episodic_events: list[LocalEpisodicEvent] = field(default_factory=list)

    # Contact info
    contact_agency: ContactInfo = field(default_factory=ContactInfo)
    responsible_agency: ContactInfo = field(default_factory=ContactInfo)

    # Section 13: More information
    more_information: MoreInformation = field(default_factory=MoreInformation)

    # Source file
    source_file: str = ""

    # Validation flags
    _has_duplicate_dates: bool = False
    _validation_warnings: list[str] = field(default_factory=list)

    @property
    def station_id(self) -> str:
        """Get the 4-character station ID."""
        if self.site_identification.four_character_id:
            return self.site_identification.four_character_id.upper()[:4]
        elif self.site_identification.nine_character_id:
            return self.site_identification.nine_character_id.upper()[:4]
        return ""

    @property
    def domes_number(self) -> str:
        """Get IERS DOMES number."""
        return self.site_identification.iers_domes_number

    @property
    def current_receiver(self) -> Optional[ReceiverInfo]:
        """Get the current (latest) receiver."""
        if not self.receivers:
            return None
        # Sort by date_installed, return the one with no date_removed or latest date_removed
        active = [r for r in self.receivers if r.date_removed is None]
        if active:
            return max(active, key=lambda r: r.date_installed or datetime.min)
        return max(self.receivers, key=lambda r: r.date_installed or datetime.min)

    @property
    def current_antenna(self) -> Optional[AntennaInfo]:
        """Get the current (latest) antenna."""
        if not self.antennas:
            return None
        active = [a for a in self.antennas if a.date_removed is None]
        if active:
            return max(active, key=lambda a: a.date_installed or datetime.min)
        return max(self.antennas, key=lambda a: a.date_installed or datetime.min)


class SiteLogParser:
    """
    Parse IGS site log ASCII files.

    Port of ASCII2XML.pm from i-BSWSTA.
    """

    # Date format patterns
    DATE_PATTERNS = [
        (r'(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2})Z', '%Y-%m-%dT%H:%MZ'),
        (r'(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2})', '%Y-%m-%dT%H:%M'),
        (r'(\d{4})-(\d{2})-(\d{2})', '%Y-%m-%d'),
        (r'(\d{2})-(\w{3})-(\d{4})', '%d-%b-%Y'),
    ]

    # Block number patterns for section detection
    SECTION_PATTERNS = {
        'form': r'^0\.\s+',
        'site_identification': r'^1\.\s+',
        'site_location': r'^2\.\s+',
        'receiver': r'^3\.(\d+)\s+',
        'antenna': r'^4\.(\d+)\s+',
        'local_ties': r'^5\.(\d+)\s+',
        'frequency_standard': r'^6\.(\d+)\s+',
        'collocation': r'^7\.(\d+)\s+',
        'meteorological': r'^8\.(\d+)\.(\d+)\s+',
        'local_interference': r'^9\.(\d+)\s+',
        'multipath': r'^10\.(\d+)\s+',
        'signal_obstruction': r'^11\.(\d+)\s+',
        'episodic_events': r'^12\.(\d+)\s+',
        'contact': r'^11\.\s+|^12\.\s+',  # Old format uses 11/12
        'responsible': r'^12\.\s+',
        'more_info': r'^13\.\s+',
    }

    def __init__(self):
        """Initialize the parser."""
        self._current_section = None
        self._current_subsection = 0

    def parse_file(self, file_path: str | Path) -> SiteLogData:
        """
        Parse a site log file.

        Args:
            file_path: Path to the ASCII site log file

        Returns:
            SiteLogData object with parsed information
        """
        file_path = Path(file_path)
        if not file_path.exists():
            raise FileNotFoundError(f"Site log file not found: {file_path}")

        # Read file content
        try:
            content = file_path.read_text(encoding='utf-8', errors='replace')
        except UnicodeDecodeError:
            content = file_path.read_text(encoding='latin-1', errors='replace')

        # Clean up DOS line endings
        content = content.replace('\r\n', '\n').replace('\r', '\n')

        return self.parse_content(content, str(file_path))

    def parse_content(self, content: str, source_file: str = "") -> SiteLogData:
        """
        Parse site log content.

        Args:
            content: ASCII site log content
            source_file: Source file path for reference

        Returns:
            SiteLogData object with parsed information
        """
        data = SiteLogData(source_file=source_file)
        lines = content.split('\n')

        # Track current section and collect lines
        current_section = None
        section_lines: list[str] = []
        section_number = None

        for line in lines:
            line = line.rstrip()

            # Check for section headers
            new_section, new_number = self._detect_section(line)

            if new_section:
                # Process previous section
                if current_section and section_lines:
                    self._process_section(data, current_section, section_number, section_lines)

                current_section = new_section
                section_number = new_number
                section_lines = [line]
            elif current_section:
                section_lines.append(line)

        # Process last section
        if current_section and section_lines:
            self._process_section(data, current_section, section_number, section_lines)

        # Validate and fix station ID
        self._validate_station_id(data)

        # Detect and correct duplicate dates
        self._fix_duplicate_dates(data)

        # Validate equipment has required dates
        self._validate_equipment_dates(data)

        return data

    def _detect_section(self, line: str) -> tuple[Optional[str], Optional[str]]:
        """Detect which section a line belongs to."""
        line = line.strip()

        # Form Information
        if re.match(r'^0\.\s+Form', line, re.IGNORECASE):
            return 'form', None

        # Site Identification
        if re.match(r'^1\.\s+Site Identification', line, re.IGNORECASE):
            return 'site_identification', None

        # Site Location
        if re.match(r'^2\.\s+Site Location', line, re.IGNORECASE):
            return 'site_location', None

        # GNSS Receiver (numbered subsections)
        match = re.match(r'^3\.(\d+)\s+', line)
        if match:
            return 'receiver', match.group(1)

        # GNSS Antenna (numbered subsections)
        match = re.match(r'^4\.(\d+)\s+', line)
        if match:
            return 'antenna', match.group(1)

        # Section 5: Surveyed Local Ties
        match = re.match(r'^5\.(\d+)\s+', line)
        if match:
            return 'surveyed_local_tie', match.group(1)

        # Section 6: Frequency Standard
        match = re.match(r'^6\.(\d+)\s+', line)
        if match:
            return 'frequency_standard', match.group(1)

        # Section 7: Collocation Information
        match = re.match(r'^7\.(\d+)\s+', line)
        if match:
            return 'collocation', match.group(1)

        # Section 8: Meteorological sensors
        match = re.match(r'^8\.1\.(\d+)\s+', line)  # Humidity
        if match:
            return 'humidity_sensor', match.group(1)

        match = re.match(r'^8\.2\.(\d+)\s+', line)  # Pressure
        if match:
            return 'pressure_sensor', match.group(1)

        match = re.match(r'^8\.3\.(\d+)\s+', line)  # Temperature
        if match:
            return 'temperature_sensor', match.group(1)

        match = re.match(r'^8\.4\.(\d+)\s+', line)  # Water Vapor
        if match:
            return 'water_vapor_sensor', match.group(1)

        # Section 9: Radio Interferences (local interference sources)
        match = re.match(r'^9\.(\d+)\s+', line)
        if match:
            return 'radio_interference', match.group(1)

        # Section 10: Multipath Sources
        match = re.match(r'^10\.(\d+)\s+', line)
        if match:
            return 'multipath_source', match.group(1)

        # Section 11: Signal Obstructions (note: old format uses 11 for contact)
        match = re.match(r'^11\.(\d+)\s+', line)
        if match:
            return 'signal_obstruction', match.group(1)

        # Contact Agency (old format section 11, new format different)
        if re.match(r'^11\.\s+On-Site.*Contact', line, re.IGNORECASE):
            return 'contact_agency', None

        # Section 12: Local Episodic Events (note: old format uses 12 for responsible)
        match = re.match(r'^12\.(\d+)\s+', line)
        if match:
            return 'episodic_event', match.group(1)

        # Responsible Agency
        if re.match(r'^12\.\s+Responsible Agency', line, re.IGNORECASE):
            return 'responsible_agency', None

        # Section 13: More Information
        if re.match(r'^13\.\s+More Information', line, re.IGNORECASE):
            return 'more_information', None

        return None, None

    def _process_section(self, data: SiteLogData, section: str, number: Optional[str],
                         lines: list[str]) -> None:
        """Process a section's lines and update data."""
        # Join lines to extract key-value pairs
        section_text = '\n'.join(lines)

        if section == 'form':
            self._parse_form(data, section_text)
        elif section == 'site_identification':
            self._parse_site_identification(data, section_text)
        elif section == 'site_location':
            self._parse_site_location(data, section_text)
        elif section == 'receiver':
            receiver = self._parse_receiver(section_text)
            if receiver and receiver.date_installed:
                data.receivers.append(receiver)
        elif section == 'antenna':
            antenna = self._parse_antenna(section_text)
            if antenna and antenna.date_installed:
                data.antennas.append(antenna)
        # Section 5: Surveyed Local Ties
        elif section == 'surveyed_local_tie':
            tie = self._parse_surveyed_local_tie(section_text)
            if tie:
                data.surveyed_local_ties.append(tie)
        # Section 6: Frequency Standard
        elif section == 'frequency_standard':
            std = self._parse_frequency_standard(section_text)
            if std:
                data.frequency_standards.append(std)
        # Section 7: Collocation Information
        elif section == 'collocation':
            coll = self._parse_collocation(section_text)
            if coll:
                data.collocation_info.append(coll)
        # Section 8: Meteorological sensors
        elif section == 'humidity_sensor':
            sensor = self._parse_met_sensor(section_text, 'humidity')
            if sensor:
                data.humidity_sensors.append(sensor)
        elif section == 'pressure_sensor':
            sensor = self._parse_met_sensor(section_text, 'pressure')
            if sensor:
                data.pressure_sensors.append(sensor)
        elif section == 'temperature_sensor':
            sensor = self._parse_met_sensor(section_text, 'temperature')
            if sensor:
                data.temperature_sensors.append(sensor)
        elif section == 'water_vapor_sensor':
            sensor = self._parse_met_sensor(section_text, 'water_vapor')
            if sensor:
                data.water_vapor_sensors.append(sensor)
        # Section 9: Radio Interferences
        elif section == 'radio_interference':
            interf = self._parse_radio_interference(section_text)
            if interf:
                data.radio_interferences.append(interf)
        # Section 10: Multipath Sources
        elif section == 'multipath_source':
            mp = self._parse_multipath_source(section_text)
            if mp:
                data.multipath_sources.append(mp)
        # Section 11: Signal Obstructions
        elif section == 'signal_obstruction':
            obs = self._parse_signal_obstruction(section_text)
            if obs:
                data.signal_obstructions.append(obs)
        # Section 12: Local Episodic Events
        elif section == 'episodic_event':
            event = self._parse_episodic_event(section_text)
            if event:
                data.episodic_events.append(event)
        # Contact sections
        elif section == 'contact_agency':
            data.contact_agency = self._parse_contact(section_text)
        elif section == 'responsible_agency':
            data.responsible_agency = self._parse_contact(section_text)
        # Section 13: More Information
        elif section == 'more_information':
            data.more_information = self._parse_more_information(section_text)

    def _extract_value(self, text: str, pattern: str) -> str:
        """Extract a value from text using a pattern."""
        match = re.search(pattern + r'\s*:\s*(.+?)(?:\n|$)', text, re.IGNORECASE | re.MULTILINE)
        if match:
            value = match.group(1).strip()
            # Clean up placeholders
            if value in ('(A4)', '(A9)', '(CCYY-MM-DDThh:mmZ)', ''):
                return ""
            return self._clean_value(value)
        return ""

    def _extract_multiline(self, text: str, pattern: str) -> str:
        """Extract a multiline value."""
        match = re.search(pattern + r'\s*:\s*(.+?)(?=\n\s*\w.*?:|$)',
                         text, re.IGNORECASE | re.DOTALL)
        if match:
            value = match.group(1).strip()
            if value.startswith('(') and value.endswith(')'):
                return ""
            return self._clean_value(value)
        return ""

    def _clean_value(self, value: str) -> str:
        """Clean and normalize a value.

        Port of watchValue() from ASCII2XML.pm with all character replacements
        and location name corrections.
        """
        if not value:
            return ""

        # Remove special characters and normalize (matching Perl watchValue)
        # Accented characters
        value = value.replace('ü', 'u').replace('ö', 'o').replace('ä', 'a')
        value = value.replace('é', 'e').replace('è', 'e').replace('ê', 'e').replace('ë', 'e')
        value = value.replace('à', 'a').replace('â', 'a').replace('á', 'a')
        value = value.replace('ô', 'o').replace('ò', 'o').replace('ó', 'o')
        value = value.replace('ù', 'u').replace('û', 'u').replace('ú', 'u')
        value = value.replace('ç', 'c').replace('ñ', 'n')
        value = value.replace('í', 'i').replace('ì', 'i').replace('î', 'i')
        value = value.replace('ý', 'y').replace('ÿ', 'y')
        value = value.replace('ß', 'ss')

        # Degree and special symbols
        value = value.replace('°', ' deg ')
        value = value.replace('±', '')
        value = value.replace('º', ' deg ')
        value = value.replace('′', "'").replace('″', '"')

        # Location name corrections (from Perl watchValue)
        if re.match(r'^Finist', value):
            value = 'Finistere'
        elif re.match(r'^Pont-de-Buis', value, re.IGNORECASE):
            value = 'Pont-de-Buis-les-Quirmech'
        elif re.match(r'^Tup.', value, re.IGNORECASE):
            value = 'Tupa'
        elif re.match(r'^S.o\s*Lu[ií]s', value, re.IGNORECASE):
            value = 'Sao Luis'
        elif re.match(r'^São\s*Lu', value, re.IGNORECASE):
            value = 'Sao Luis'
        elif re.match(r'^Concepci', value, re.IGNORECASE):
            value = 'Concepcion'
        elif re.match(r'^Bogot', value, re.IGNORECASE):
            value = 'Bogota'
        elif re.match(r'^Bras.lia', value, re.IGNORECASE):
            value = 'Brasilia'

        # Remove placeholder text
        if value in ('(multiple lines)', 'CCYY-MM-DDThh:mmZ', '(A4)', '(A9)',
                     '(CCYY)', '(DDD)', '(sec)', '(m)', '(deg)', '(+/- m)',
                     '(see instructions in header)'):
            return ""

        return value.strip()

    def _parse_date(self, date_str: str) -> Optional[datetime]:
        """Parse a date string to datetime.

        Port of formatDateFull() from ASCII2XML.pm with all edge cases.
        Handles 20+ date format variations found in IGS site logs.
        """
        if not date_str:
            return None

        date_str = date_str.strip()

        # Skip placeholder dates
        if 'CCYY' in date_str or date_str == '0000-00-00':
            return None
        if date_str.startswith('(') and date_str.endswith(')'):
            return None

        # Pre-processing: normalize common variations (from Perl formatDateFull)

        # Handle "Thh:mmZ" without date (set to midnight)
        if re.match(r'^T\d{2}:\d{2}Z?$', date_str):
            return None  # Invalid - no date component

        # Remove trailing parenthesis: "2023-01-01Thh:mmZ)" -> "2023-01-01Thh:mmZ"
        date_str = re.sub(r'\)$', '', date_str)

        # Remove extra characters around Z
        date_str = re.sub(r'Z+$', 'Z', date_str)

        # TU -> Z conversion (Temps Universel)
        date_str = re.sub(r'\s*TU\s*$', 'Z', date_str)
        date_str = re.sub(r'\s*UT\s*$', 'Z', date_str)

        # GMT -> normalize
        if ' GMT' in date_str:
            date_str = date_str.replace(' GMT', '')
            # "YYYY-MM-DD HH:MM GMT" -> "YYYY-MM-DD HH:MM"
            match = re.match(r'(\d{4}-\d{2}-\d{2})\s+(\d{2}:\d{2})', date_str)
            if match:
                date_str = f"{match.group(1)}T{match.group(2)}Z"

        # Fix missing hour in "YYYY-MM-DD T:mm:ss Z" pattern
        date_str = re.sub(r'T:(\d{2})', r'T00:\1', date_str)

        # Normalize spacing around T
        date_str = re.sub(r'\s+T', 'T', date_str)
        date_str = re.sub(r'T\s+', 'T', date_str)

        # Handle "YYYY-MM-DDThh:mm" without Z
        if re.match(r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}$', date_str):
            date_str += 'Z'

        # Handle "YYYY-MM-DDThh:mm:ss" without Z
        if re.match(r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$', date_str):
            date_str += 'Z'

        # Handle "YYYY-MM-DD hh:mm" space instead of T
        match = re.match(r'^(\d{4}-\d{2}-\d{2})\s+(\d{2}:\d{2}(:\d{2})?)$', date_str)
        if match:
            date_str = f"{match.group(1)}T{match.group(2)}Z"

        # Handle "DD-Mon-YYYY" format (e.g., "15-Jan-2020")
        month_abbrevs = {
            'jan': '01', 'feb': '02', 'mar': '03', 'apr': '04',
            'may': '05', 'jun': '06', 'jul': '07', 'aug': '08',
            'sep': '09', 'oct': '10', 'nov': '11', 'dec': '12'
        }
        match = re.match(r'^(\d{1,2})-([A-Za-z]{3})-(\d{4})$', date_str)
        if match:
            day, mon, year = match.groups()
            mon_num = month_abbrevs.get(mon.lower())
            if mon_num:
                date_str = f"{year}-{mon_num}-{int(day):02d}T00:00Z"

        # Handle "Mon-DD-YYYY" format
        match = re.match(r'^([A-Za-z]{3})-(\d{1,2})-(\d{4})$', date_str)
        if match:
            mon, day, year = match.groups()
            mon_num = month_abbrevs.get(mon.lower())
            if mon_num:
                date_str = f"{year}-{mon_num}-{int(day):02d}T00:00Z"

        # Handle date-only format "YYYY-MM-DD" - add time
        if re.match(r'^\d{4}-\d{2}-\d{2}$', date_str):
            date_str += 'T00:00Z'

        # Try various formats
        formats = [
            '%Y-%m-%dT%H:%M:%SZ',
            '%Y-%m-%dT%H:%MZ',
            '%Y-%m-%dT%H:%M:%S',
            '%Y-%m-%dT%H:%M',
            '%Y-%m-%d',
            '%d-%b-%Y',
            '%Y-%m-%d %H:%M:%S',
            '%Y-%m-%d %H:%M',
            '%Y/%m/%d',
            '%d/%m/%Y',
        ]

        for fmt in formats:
            try:
                return datetime.strptime(date_str, fmt)
            except ValueError:
                continue

        # Log warning for unparseable dates
        if date_str and not date_str.startswith('('):
            logger.debug(f"Could not parse date: {date_str}")
        return None

    def _parse_float(self, value: str) -> float:
        """Parse a float value, handling various formats."""
        if not value:
            return 0.0

        # Remove non-numeric characters except . - +
        value = re.sub(r'[^\d.\-+]', '', value)

        try:
            return float(value)
        except ValueError:
            return 0.0

    def _parse_form(self, data: SiteLogData, text: str) -> None:
        """Parse form information section."""
        data.prepared_by = self._extract_value(text, r'Prepared by')
        date_str = self._extract_value(text, r'Date Prepared')
        data.date_prepared = self._parse_date(date_str)
        data.report_type = self._extract_value(text, r'Report Type')

    def _parse_site_identification(self, data: SiteLogData, text: str) -> None:
        """Parse site identification section."""
        si = data.site_identification
        si.site_name = self._extract_value(text, r'Site Name')
        si.four_character_id = self._extract_value(text, r'Four Character ID')
        si.nine_character_id = self._extract_value(text, r'Nine Character ID')
        si.monument_inscription = self._extract_value(text, r'Monument Inscription')
        si.iers_domes_number = self._extract_value(text, r'IERS DOMES Number')
        si.cdp_number = self._extract_value(text, r'CDP Number')
        si.monument_description = self._extract_value(text, r'Monument Description')
        si.height_of_monument = self._extract_value(text, r'Height of the Monument')
        si.monument_foundation = self._extract_value(text, r'Monument Foundation')
        si.foundation_depth = self._extract_value(text, r'Foundation Depth')
        si.marker_description = self._extract_value(text, r'Marker Description')

        date_str = self._extract_value(text, r'Date Installed')
        si.date_installed = self._parse_date(date_str)

        si.geologic_characteristic = self._extract_value(text, r'Geologic Characteristic')
        si.bedrock_type = self._extract_value(text, r'Bedrock Type')
        si.bedrock_condition = self._extract_value(text, r'Bedrock Condition')
        si.fracture_spacing = self._extract_value(text, r'Fracture Spacing')
        si.fault_zones_nearby = self._extract_value(text, r'Fault zones nearby')
        si.notes = self._extract_multiline(text, r'Additional Information')

    def _parse_site_location(self, data: SiteLogData, text: str) -> None:
        """Parse site location section."""
        sl = data.site_location
        sl.city = self._extract_value(text, r'City or Town')
        sl.state = self._extract_value(text, r'State or Province')
        sl.country = self._extract_value(text, r'Country')
        sl.tectonic_plate = self._extract_value(text, r'Tectonic Plate')

        sl.x_coordinate = self._parse_float(self._extract_value(text, r'X coordinate \(m\)'))
        sl.y_coordinate = self._parse_float(self._extract_value(text, r'Y coordinate \(m\)'))
        sl.z_coordinate = self._parse_float(self._extract_value(text, r'Z coordinate \(m\)'))

        # Also try without escaped parentheses
        if sl.x_coordinate == 0:
            sl.x_coordinate = self._parse_float(self._extract_value(text, r'X coordinate'))
        if sl.y_coordinate == 0:
            sl.y_coordinate = self._parse_float(self._extract_value(text, r'Y coordinate'))
        if sl.z_coordinate == 0:
            sl.z_coordinate = self._parse_float(self._extract_value(text, r'Z coordinate'))

        sl.notes = self._extract_multiline(text, r'Additional Information')

    def _parse_receiver(self, text: str) -> Optional[ReceiverInfo]:
        """Parse receiver information."""
        receiver = ReceiverInfo()

        receiver.receiver_type = self._extract_value(text, r'Receiver Type')
        receiver.satellite_system = self._extract_value(text, r'Satellite System')
        receiver.serial_number = self._extract_value(text, r'Serial Number')
        receiver.firmware_version = self._extract_value(text, r'Firmware Version')
        receiver.elevation_cutoff = self._extract_value(text, r'Elevation Cutoff')

        date_str = self._extract_value(text, r'Date Installed')
        receiver.date_installed = self._parse_date(date_str)

        date_str = self._extract_value(text, r'Date Removed')
        receiver.date_removed = self._parse_date(date_str)

        receiver.temperature_stabilization = self._extract_value(text, r'Temperature Stabiliz')
        receiver.notes = self._extract_multiline(text, r'Additional Information')

        # Skip if no receiver type or install date
        if not receiver.receiver_type or not receiver.date_installed:
            return None

        return receiver

    def _parse_antenna(self, text: str) -> Optional[AntennaInfo]:
        """Parse antenna information."""
        antenna = AntennaInfo()

        antenna.antenna_type = self._extract_value(text, r'Antenna Type')
        antenna.serial_number = self._extract_value(text, r'Serial Number')
        antenna.antenna_reference_point = self._extract_value(text, r'Antenna Reference Point')

        antenna.marker_arp_up_ecc = self._parse_float(
            self._extract_value(text, r'Marker->ARP Up Ecc'))
        antenna.marker_arp_north_ecc = self._parse_float(
            self._extract_value(text, r'Marker->ARP North Ecc'))
        antenna.marker_arp_east_ecc = self._parse_float(
            self._extract_value(text, r'Marker->ARP East Ecc'))

        antenna.alignment_from_true_north = self._extract_value(text, r'Alignment from True N')
        antenna.radome_type = self._extract_value(text, r'Antenna Radome Type')
        antenna.radome_serial_number = self._extract_value(text, r'Radome Serial Number')
        antenna.antenna_cable_type = self._extract_value(text, r'Antenna Cable Type')
        antenna.antenna_cable_length = self._extract_value(text, r'Antenna Cable Length')

        date_str = self._extract_value(text, r'Date Installed')
        antenna.date_installed = self._parse_date(date_str)

        date_str = self._extract_value(text, r'Date Removed')
        antenna.date_removed = self._parse_date(date_str)

        antenna.notes = self._extract_multiline(text, r'Additional Information')

        # Skip if no antenna type or install date
        if not antenna.antenna_type or not antenna.date_installed:
            return None

        return antenna

    def _parse_met_sensor(self, text: str, sensor_type: str) -> Optional[MeteorologicalSensor]:
        """Parse meteorological sensor information."""
        sensor = MeteorologicalSensor(sensor_type=sensor_type)

        if sensor_type == 'humidity':
            sensor.model = self._extract_value(text, r'Humidity Sensor Model')
        elif sensor_type == 'pressure':
            sensor.model = self._extract_value(text, r'Pressure Sensor Model')
        elif sensor_type == 'temperature':
            sensor.model = self._extract_value(text, r'Temp\. Sensor Model')
        elif sensor_type == 'water_vapor':
            sensor.model = self._extract_value(text, r'Water Vapor Radiometer')

        sensor.manufacturer = self._extract_value(text, r'Manufacturer')
        sensor.serial_number = self._extract_value(text, r'Serial Number')
        sensor.height_diff_to_antenna = self._extract_value(text, r'Height Diff to Ant')
        sensor.calibration_date = self._extract_value(text, r'Calibration date')
        sensor.effective_dates = self._extract_value(text, r'Effective Dates')
        sensor.data_sampling_interval = self._extract_value(text, r'Data Sampling Interval')
        sensor.accuracy = self._extract_value(text, r'Accuracy')
        sensor.aspiration = self._extract_value(text, r'Aspiration')
        sensor.notes = self._extract_multiline(text, r'Notes')

        if not sensor.model:
            return None

        return sensor

    def _parse_contact(self, text: str) -> ContactInfo:
        """Parse contact information."""
        contact = ContactInfo()

        contact.agency = self._extract_multiline(text, r'Agency')
        contact.preferred_abbreviation = self._extract_value(text, r'Preferred Abbreviation')
        contact.mailing_address = self._extract_multiline(text, r'Mailing Address')
        contact.contact_name = self._extract_value(text, r'Contact Name')
        contact.telephone_primary = self._extract_value(text, r'Telephone \(primary\)')
        contact.telephone_secondary = self._extract_value(text, r'Telephone \(secondary\)')
        contact.fax = self._extract_value(text, r'Fax')
        contact.email = self._extract_value(text, r'E-mail')
        contact.notes = self._extract_multiline(text, r'Additional Information')

        return contact

    # =========================================================================
    # Section 5-13 Parsing Methods (Port of ASCII2XML.pm)
    # =========================================================================

    def _parse_surveyed_local_tie(self, text: str) -> Optional[SurveyedLocalTie]:
        """Parse surveyed local ties information (Section 5)."""
        tie = SurveyedLocalTie()

        tie.tied_marker_name = self._extract_value(text, r'Tied Marker Name')
        tie.tied_marker_usage = self._extract_value(text, r'Tied Marker Usage')
        tie.tied_marker_cdp_number = self._extract_value(text, r'Tied Marker CDP Number')
        tie.tied_marker_domes_number = self._extract_value(text, r'Tied Marker DOMES Number')

        # Differential components (dx, dy, dz in meters)
        tie.differential_dx = self._parse_float(
            self._extract_value(text, r'dx \(m\)'))
        tie.differential_dy = self._parse_float(
            self._extract_value(text, r'dy \(m\)'))
        tie.differential_dz = self._parse_float(
            self._extract_value(text, r'dz \(m\)'))

        tie.accuracy_mm = self._extract_value(text, r'Accuracy \(mm\)')
        tie.survey_method = self._extract_value(text, r'Survey method')

        date_str = self._extract_value(text, r'Date Measured')
        tie.date_measured = self._parse_date(date_str)

        tie.notes = self._extract_multiline(text, r'Additional Information')

        # Only return if we have meaningful data
        if not tie.tied_marker_name:
            return None

        return tie

    def _parse_frequency_standard(self, text: str) -> Optional[FrequencyStandard]:
        """Parse frequency standard information (Section 6)."""
        std = FrequencyStandard()

        std.standard_type = self._extract_value(text, r'Standard Type')
        std.input_frequency = self._extract_value(text, r'Input Frequency')
        std.effective_dates = self._extract_value(text, r'Effective Dates')
        std.notes = self._extract_multiline(text, r'Notes')

        if not std.standard_type:
            return None

        return std

    def _parse_collocation(self, text: str) -> Optional[CollocationInformation]:
        """Parse collocation/instrumentation information (Section 7)."""
        coll = CollocationInformation()

        coll.instrumentation_type = self._extract_value(text, r'Instrumentation Type')
        coll.status = self._extract_value(text, r'Status')
        coll.effective_dates = self._extract_value(text, r'Effective Dates')
        coll.notes = self._extract_multiline(text, r'Notes')

        if not coll.instrumentation_type:
            return None

        return coll

    def _parse_radio_interference(self, text: str) -> Optional[RadioInterference]:
        """Parse radio interference information (Section 9)."""
        interf = RadioInterference()

        interf.radio_interferences = self._extract_value(text, r'Radio Interferences')
        # Note: Original Perl has typo "Degredations" - handle both
        interf.observed_degradations = self._extract_value(text, r'Observed Degr[ae]dations')
        if not interf.observed_degradations:
            interf.observed_degradations = self._extract_value(text, r'Observed Degradations')
        interf.effective_dates = self._extract_value(text, r'Effective Dates')
        interf.notes = self._extract_multiline(text, r'Additional Information')

        if not interf.radio_interferences:
            return None

        return interf

    def _parse_multipath_source(self, text: str) -> Optional[MultipathSource]:
        """Parse multipath source information (Section 10)."""
        mp = MultipathSource()

        mp.multipath_sources = self._extract_value(text, r'Multipath Sources')
        mp.effective_dates = self._extract_value(text, r'Effective Dates')
        mp.notes = self._extract_multiline(text, r'Additional Information')

        if not mp.multipath_sources:
            return None

        return mp

    def _parse_signal_obstruction(self, text: str) -> Optional[SignalObstruction]:
        """Parse signal obstruction information (Section 11)."""
        obs = SignalObstruction()

        obs.signal_obstructions = self._extract_value(text, r'Signal Obstructions')
        obs.effective_dates = self._extract_value(text, r'Effective Dates')
        obs.notes = self._extract_multiline(text, r'Additional Information')

        if not obs.signal_obstructions:
            return None

        return obs

    def _parse_episodic_event(self, text: str) -> Optional[LocalEpisodicEvent]:
        """Parse local episodic event (Section 12)."""
        event = LocalEpisodicEvent()

        date_str = self._extract_value(text, r'Date')
        event.event_date = self._parse_date(date_str)
        event.event_description = self._extract_multiline(text, r'Event')

        if not event.event_description and not event.event_date:
            return None

        return event

    def _parse_more_information(self, text: str) -> MoreInformation:
        """Parse additional information (Section 13)."""
        info = MoreInformation()

        info.primary_data_center = self._extract_value(text, r'Primary Data Center')
        info.secondary_data_center = self._extract_value(text, r'Secondary Data Center')
        info.url_for_more_information = self._extract_value(text, r'URL for More Information')
        info.hardcopy_on_file = self._extract_value(text, r'Hardcopy on File')
        info.site_map = self._extract_value(text, r'Site Map')
        info.site_diagram = self._extract_value(text, r'Site Diagram')
        info.horizon_mask = self._extract_value(text, r'Horizon Mask')
        info.monument_description = self._extract_value(text, r'Monument Description')
        info.site_pictures = self._extract_value(text, r'Site Pictures')
        info.notes = self._extract_multiline(text, r'Additional Information')
        info.antenna_graphics = self._extract_multiline(text, r'Antenna Graphics with Dimensions')

        return info

    def _validate_station_id(self, data: SiteLogData) -> None:
        """Validate and fix station ID."""
        si = data.site_identification

        # Prefer 4-char ID, fall back to first 4 of 9-char ID
        if len(si.four_character_id) == 4:
            return

        if len(si.nine_character_id) >= 4:
            si.four_character_id = si.nine_character_id[:4].upper()
            logger.info(f"Using first 4 chars of nine_character_id: {si.four_character_id}")
        elif si.four_character_id:
            # Trim/pad to 4 characters
            si.four_character_id = si.four_character_id.strip().upper()[:4]

    def _fix_duplicate_dates(self, data: SiteLogData) -> None:
        """Detect and correct duplicate installation dates.

        Port of checkDoubleDate() from ASCII2XML.pm (lines 924-994).

        When two receivers or antennas have the same installation date,
        this usually indicates a firmware update or minor change. We adjust
        the date_removed of the earlier equipment to match date_installed
        of the later one, and may add 1 second to disambiguate.
        """
        # Fix duplicate receiver dates
        if len(data.receivers) > 1:
            data.receivers = self._fix_equipment_duplicate_dates(
                data.receivers, 'receiver', data
            )

        # Fix duplicate antenna dates
        if len(data.antennas) > 1:
            data.antennas = self._fix_equipment_duplicate_dates(
                data.antennas, 'antenna', data
            )

    def _fix_equipment_duplicate_dates(
        self,
        equipment_list: list,
        equipment_type: str,
        data: SiteLogData
    ) -> list:
        """Fix duplicate dates in a list of equipment.

        Based on Perl checkDoubleDate() logic:
        1. Sort by date_installed
        2. For consecutive items with same date_installed:
           - Set date_removed of earlier item to date_installed of later
           - Add 1 second to later item's date_installed to disambiguate
        3. Track duplicate dates for validation warnings
        """
        if len(equipment_list) < 2:
            return equipment_list

        # Sort by date_installed (None dates go to end)
        sorted_list = sorted(
            equipment_list,
            key=lambda e: e.date_installed or datetime.max
        )

        # Detect and fix duplicates
        from datetime import timedelta

        i = 0
        while i < len(sorted_list) - 1:
            curr = sorted_list[i]
            next_eq = sorted_list[i + 1]

            if (curr.date_installed and next_eq.date_installed and
                    curr.date_installed == next_eq.date_installed):

                # Log warning
                data._has_duplicate_dates = True
                data._validation_warnings.append(
                    f"Duplicate {equipment_type} date: {curr.date_installed} "
                    f"({getattr(curr, 'receiver_type', None) or getattr(curr, 'antenna_type', '')})"
                )

                # Set curr's date_removed to next's date_installed
                curr.date_removed = next_eq.date_installed

                # Add 1 second to next's date_installed to disambiguate
                next_eq.date_installed = next_eq.date_installed + timedelta(seconds=1)

                logger.debug(
                    f"Fixed duplicate {equipment_type} date: "
                    f"adjusted next to {next_eq.date_installed}"
                )

            i += 1

        return sorted_list

    def _validate_equipment_dates(self, data: SiteLogData) -> None:
        """Validate that all equipment has required date_installed.

        Port of validation logic from ASCII2XML.pm.

        Equipment without date_installed is filtered out and a warning is logged.
        This ensures STA file generation only includes valid equipment.
        """
        # Validate receivers
        invalid_receivers = [
            r for r in data.receivers
            if not r.date_installed
        ]
        if invalid_receivers:
            for r in invalid_receivers:
                data._validation_warnings.append(
                    f"Receiver without date_installed: {r.receiver_type}"
                )
            # Filter out invalid (already done in _parse_receiver, but double-check)
            data.receivers = [r for r in data.receivers if r.date_installed]

        # Validate antennas
        invalid_antennas = [
            a for a in data.antennas
            if not a.date_installed
        ]
        if invalid_antennas:
            for a in invalid_antennas:
                data._validation_warnings.append(
                    f"Antenna without date_installed: {a.antenna_type}"
                )
            data.antennas = [a for a in data.antennas if a.date_installed]

        # Set date_removed for equipment that doesn't have it
        # (should be handled by STA writer, but ensure continuity)
        self._ensure_date_continuity(data.receivers)
        self._ensure_date_continuity(data.antennas)

    def _ensure_date_continuity(self, equipment_list: list) -> None:
        """Ensure equipment has continuous date ranges.

        For each equipment item without date_removed, set it to the
        date_installed of the next item (equipment change).
        """
        if len(equipment_list) < 2:
            return

        # Sort by date_installed
        sorted_list = sorted(
            equipment_list,
            key=lambda e: e.date_installed or datetime.max
        )

        for i in range(len(sorted_list) - 1):
            curr = sorted_list[i]
            next_eq = sorted_list[i + 1]

            # If current has no date_removed, use next's date_installed
            if curr.date_removed is None and next_eq.date_installed:
                curr.date_removed = next_eq.date_installed


def parse_site_log(file_path: str | Path) -> SiteLogData:
    """
    Convenience function to parse a site log file.

    Args:
        file_path: Path to the site log file

    Returns:
        SiteLogData with parsed information
    """
    parser = SiteLogParser()
    return parser.parse_file(file_path)


def parse_site_logs_directory(directory: str | Path) -> dict[str, SiteLogData]:
    """
    Parse all site log files in a directory.

    Args:
        directory: Path to directory containing .log files

    Returns:
        Dictionary mapping station ID to SiteLogData
    """
    directory = Path(directory)
    parser = SiteLogParser()
    results = {}

    for log_file in sorted(directory.glob("*.log")):
        try:
            data = parser.parse_file(log_file)
            if data.station_id:
                # If duplicate, keep the one with more recent date
                existing = results.get(data.station_id.lower())
                if existing:
                    existing_date = existing.date_prepared or datetime.min
                    new_date = data.date_prepared or datetime.min
                    if new_date > existing_date:
                        results[data.station_id.lower()] = data
                else:
                    results[data.station_id.lower()] = data
        except Exception as e:
            logger.warning(f"Failed to parse {log_file}: {e}")

    return results
