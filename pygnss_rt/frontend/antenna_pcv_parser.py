#!/usr/bin/env python3
"""
Antenna PCV/PCO Parser and Visualizer

Extracts antenna information from STA files and PCV/PCO values from Bernese PCV files.
Creates visualizations of antenna phase center variations.
"""

import os
import re
import numpy as np
import pandas as pd
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple
from pathlib import Path


@dataclass
class PCOEntry:
    """Phase Center Offset entry"""
    system: str  # G, R, E, C, J
    freq_code: str  # 01, 02, 05, etc.
    north_mm: float
    east_mm: float
    up_mm: float


@dataclass
class PCVEntry:
    """Phase Center Variation entry (zenith-dependent)"""
    system: str
    freq_code: str
    zenith_angles: List[float]  # 0, 5, 10, ..., 90
    pcv_values: List[float]  # mm


@dataclass
class AntennaCalibration:
    """Full antenna calibration data"""
    antenna_type: str
    radome: str
    serial_number: str  # "0" for type mean
    method: str  # ROBOT, CHAMBER, COPIED, CONVERTED, etc.
    date: str
    pco: Dict[str, PCOEntry] = field(default_factory=dict)  # keyed by "G01", "G02", etc.
    pcv: Dict[str, PCVEntry] = field(default_factory=dict)

    @property
    def is_individual(self) -> bool:
        return self.serial_number != "0" and self.serial_number != ""


@dataclass
class StationAntenna:
    """Station antenna information from STA file"""
    station_id: str
    antenna_type: str
    radome: str
    serial_number: str
    start_date: str
    end_date: str
    receiver_type: str
    height_offset: float  # ARP to marker height


def parse_sta_file(sta_path: str, stations: List[str]) -> Dict[str, StationAntenna]:
    """
    Parse Bernese STA file to extract current antenna for specified stations.

    Returns dict mapping station_id to StationAntenna.
    """
    station_antennas = {}
    stations_upper = [s.upper() for s in stations]

    with open(sta_path, 'r') as f:
        content = f.read()

    # Find TYPE 002 section
    in_type2 = False
    lines = content.split('\n')

    for line in lines:
        if 'TYPE 002:' in line:
            in_type2 = True
            continue
        if in_type2 and 'TYPE 003:' in line:
            break
        if not in_type2:
            continue

        # Parse station line
        # Format: STATION FLAG START_DATE END_DATE RECEIVER ANTENNA SERIAL ...
        if len(line) < 100:
            continue

        station_id = line[0:4].strip()
        if station_id.upper() not in stations_upper:
            continue

        # Check if this is current (no end date or end date in future)
        # The format varies, but typically end date starts around position 45
        try:
            # Parse the line - it's fixed width format
            # Columns: 0-20 station, 22-24 flag, 26-45 start, 47-66 end, then receiver, antenna...
            parts = line.split()
            if len(parts) < 10:
                continue

            # Look for empty end date (current entry)
            # End date is typically blank or contains spaces
            end_date_str = line[47:66].strip() if len(line) > 66 else ""

            if not end_date_str or end_date_str == "":
                # This is the current antenna
                # Parse antenna type and radome
                # Antenna typically starts around column 93
                antenna_type = line[93:113].strip() if len(line) > 113 else ""
                radome = line[113:133].strip() if len(line) > 133 else ""

                # Try to extract from parts if column parsing fails
                if not antenna_type and len(parts) >= 8:
                    # Receiver is typically parts[6], antenna is parts[7]
                    receiver_type = parts[6] if len(parts) > 6 else ""
                    antenna_type = parts[7] if len(parts) > 7 else ""
                    radome = parts[8] if len(parts) > 8 else "NONE"
                else:
                    receiver_type = line[67:93].strip() if len(line) > 93 else ""

                # Extract serial number
                serial = ""
                if len(parts) > 9:
                    serial = parts[9] if parts[9].isdigit() or len(parts[9]) > 4 else ""

                station_antennas[station_id.upper()] = StationAntenna(
                    station_id=station_id.upper(),
                    antenna_type=antenna_type.split()[0] if antenna_type else "",
                    radome=radome.split()[0] if radome else "NONE",
                    serial_number=serial,
                    start_date=line[26:45].strip(),
                    end_date="",
                    receiver_type=receiver_type.split()[0] if receiver_type else "",
                    height_offset=0.0
                )
        except Exception:
            continue

    return station_antennas


def parse_pcv_file(pcv_path: str, antenna_types: List[Tuple[str, str, str]]) -> Dict[str, AntennaCalibration]:
    """
    Parse Bernese PCV file to extract calibrations for specified antennas.

    Args:
        pcv_path: Path to PCV file
        antenna_types: List of (antenna_type, radome, serial) tuples to extract
                      Use serial="0" or "" for type mean

    Returns:
        Dict mapping "ANTENNA_TYPE RADOME SERIAL" to AntennaCalibration
    """
    calibrations = {}

    with open(pcv_path, 'r') as f:
        content = f.read()

    lines = content.split('\n')
    i = 0

    # Build lookup set for faster matching
    lookup = set()
    for ant, rad, ser in antenna_types:
        lookup.add((ant.upper(), rad.upper()))
        if ser:
            lookup.add((ant.upper(), rad.upper(), ser))

    while i < len(lines):
        line = lines[i]

        # Look for antenna header line
        # Format: ANTENNA_TYPE RADOME SERIAL SYS FRQ ...
        if len(line) > 20 and line[0] != ' ' and line[0] != '*':
            parts = line.split()
            if len(parts) >= 6:
                ant_type = parts[0]
                radome = parts[1]
                serial = parts[2] if len(parts) > 2 else "0"

                # Check if we want this antenna
                key = (ant_type.upper(), radome.upper())
                key_with_serial = (ant_type.upper(), radome.upper(), serial)

                if key in lookup or key_with_serial in lookup:
                    # Parse this calibration
                    method = parts[-2] if len(parts) > 8 else "UNKNOWN"
                    date = parts[-1] if len(parts) > 8 else ""

                    cal_key = f"{ant_type} {radome} {serial}"

                    if cal_key not in calibrations:
                        calibrations[cal_key] = AntennaCalibration(
                            antenna_type=ant_type,
                            radome=radome,
                            serial_number=serial,
                            method=method,
                            date=date
                        )

                    # Skip to PCO section
                    i += 1
                    while i < len(lines) and 'NORTH MM' not in lines[i]:
                        i += 1

                    # Skip header lines
                    i += 2

                    # Parse PCO entries
                    while i < len(lines):
                        pco_line = lines[i]
                        if not pco_line.strip() or pco_line[0] == '*':
                            break
                        if len(pco_line) < 30:
                            break

                        pco_parts = pco_line.split()
                        if len(pco_parts) >= 5:
                            try:
                                sys_freq = pco_parts[0]  # e.g., "G01", "G02", "E01"
                                sys = sys_freq[0]
                                freq = sys_freq[1:3]

                                # Skip the "0" column
                                north = float(pco_parts[2])
                                east = float(pco_parts[3])
                                up = float(pco_parts[4])

                                pco_key = f"{sys}{freq}"
                                calibrations[cal_key].pco[pco_key] = PCOEntry(
                                    system=sys,
                                    freq_code=freq,
                                    north_mm=north,
                                    east_mm=east,
                                    up_mm=up
                                )
                            except (ValueError, IndexError):
                                pass
                        i += 1
                    continue
        i += 1

    return calibrations


def get_current_antennas_for_stations(sta_path: str, stations: List[str]) -> pd.DataFrame:
    """
    Get current antenna information for a list of stations.

    Returns DataFrame with columns: station, antenna_type, radome, serial, receiver
    """
    antennas = parse_sta_file(sta_path, stations)

    data = []
    for station in stations:
        sta_upper = station.upper()
        if sta_upper in antennas:
            ant = antennas[sta_upper]
            data.append({
                'Station': ant.station_id,
                'Antenna': ant.antenna_type,
                'Radome': ant.radome,
                'Serial': ant.serial_number,
                'Receiver': ant.receiver_type,
                'Individual Cal': 'Yes' if ant.serial_number and ant.serial_number != '0' else 'No'
            })
        else:
            data.append({
                'Station': sta_upper,
                'Antenna': 'NOT FOUND',
                'Radome': '',
                'Serial': '',
                'Receiver': '',
                'Individual Cal': ''
            })

    return pd.DataFrame(data)


def extract_pco_for_stations(sta_path: str, pcv_path: str, stations: List[str]) -> pd.DataFrame:
    """
    Extract PCO values for the antennas used by specified stations.

    Returns DataFrame with PCO values for each frequency.
    """
    # Get antenna info
    antennas = parse_sta_file(sta_path, stations)

    # Build list of antenna types to look up
    antenna_types = []
    for sta_upper, ant in antennas.items():
        if ant.antenna_type:
            # Always get type mean
            antenna_types.append((ant.antenna_type, ant.radome, "0"))
            # Also get individual if available
            if ant.serial_number and ant.serial_number != "0":
                antenna_types.append((ant.antenna_type, ant.radome, ant.serial_number))

    # Parse PCV file
    calibrations = parse_pcv_file(pcv_path, antenna_types)

    # Build result
    data = []
    for station in stations:
        sta_upper = station.upper()
        if sta_upper not in antennas:
            continue

        ant = antennas[sta_upper]

        # Look for calibration - prefer individual, fall back to type mean
        cal_key_indiv = f"{ant.antenna_type} {ant.radome} {ant.serial_number}"
        cal_key_mean = f"{ant.antenna_type} {ant.radome} 0"

        cal = calibrations.get(cal_key_indiv) or calibrations.get(cal_key_mean)

        if cal:
            row = {
                'Station': sta_upper,
                'Antenna': ant.antenna_type,
                'Radome': ant.radome,
                'Serial': cal.serial_number,
                'Method': cal.method,
                'Individual': 'Yes' if cal.is_individual else 'No'
            }

            # Add PCO values for each frequency
            for freq_key in ['G01', 'G02', 'G05', 'E01', 'E05', 'R01', 'R02']:
                if freq_key in cal.pco:
                    pco = cal.pco[freq_key]
                    row[f'{freq_key}_N'] = pco.north_mm
                    row[f'{freq_key}_E'] = pco.east_mm
                    row[f'{freq_key}_U'] = pco.up_mm
                else:
                    row[f'{freq_key}_N'] = None
                    row[f'{freq_key}_E'] = None
                    row[f'{freq_key}_U'] = None

            data.append(row)

    return pd.DataFrame(data)


# Station list (38 stations)
MONITORED_STATIONS = [
    "0531", "0929", "0935", "ARL0", "BASC", "BOR1", "BRST", "BRUX",
    "DARE", "ECH2", "ENTZ", "ERPL", "EUSK", "GRAS", "GRAZ", "HERS",
    "JOZE", "KBG2", "LONG", "LROC", "MABO", "MAR6", "MATE", "MLVL",
    "ONSA", "POTS", "ROUL", "STAS", "THNV", "TLSE", "TRI2", "TROS",
    "WALF", "WARN", "WSRT", "WTZR", "YEBE", "ZIM2"
]


def main():
    """Main function to extract and display antenna information."""
    sta_path = "/home/ahunegnaw/Python_IGNSS/i-GNSS/pygnss_rt/station_data/IGS20_54.STA"
    pcv_path = "/home/ahunegnaw/Python_IGNSS/i-GNSS/pygnss_rt/station_data/ANTENNA_I20.PCV"

    print("=" * 80)
    print("ANTENNA INFORMATION FOR 38 MONITORED STATIONS")
    print("=" * 80)

    # Get antenna info
    df_antennas = get_current_antennas_for_stations(sta_path, MONITORED_STATIONS)
    print("\nCurrent Antenna Setup:")
    print(df_antennas.to_string(index=False))

    # Get PCO values
    print("\n" + "=" * 80)
    print("PCO VALUES (mm)")
    print("=" * 80)

    df_pco = extract_pco_for_stations(sta_path, pcv_path, MONITORED_STATIONS)
    if len(df_pco) > 0:
        # Print subset of columns
        cols = ['Station', 'Antenna', 'Radome', 'Method', 'Individual',
                'G01_N', 'G01_E', 'G01_U', 'G02_N', 'G02_E', 'G02_U']
        existing_cols = [c for c in cols if c in df_pco.columns]
        print(df_pco[existing_cols].to_string(index=False))

    # Antenna type summary
    print("\n" + "=" * 80)
    print("ANTENNA TYPE SUMMARY")
    print("=" * 80)
    antenna_counts = df_antennas['Antenna'].value_counts()
    for ant, count in antenna_counts.items():
        print(f"  {ant}: {count} stations")


if __name__ == "__main__":
    main()
