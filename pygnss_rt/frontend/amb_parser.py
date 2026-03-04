"""
Ambiguity Resolution Summary File Parser

Parses Bernese-style AMB summary files from DD processing.
Example file: AMB_20253020.SUM.gz

File structure:
- Widelane (WL) Ambiguity Resolution section (#AR_WL)
- Narrowlane (NL) Ambiguity Resolution section (#AR_NL)
- Satellite-wise Ambiguity Resolution Statistics

Data includes:
- Baseline statistics (station pairs, length, resolution rates)
- System totals (GPS, Galileo, GLONASS)
- Per-satellite statistics
"""

import os
import gzip
import re
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class BaselineAmbiguity:
    """Ambiguity resolution statistics for a single baseline"""
    file: str
    sta1: str
    sta2: str
    length_km: float
    amb_before: int
    rms_before: float
    amb_after: int
    rms_after: float
    resolution_pct: float
    system: str  # G, E, G E, etc.
    max_residual: float
    rms_residual: float
    qcb: str  # Quarter-cycle bias handling
    frq: str  # GLONASS frequency handling
    receiver1: str
    receiver2: str
    ar_type: str  # 'WL' or 'NL'


@dataclass
class SystemTotal:
    """Total ambiguity resolution statistics for a constellation system"""
    system: str  # G (GPS), E (Galileo), R (GLONASS), G E, etc.
    baselines: int
    length_km: float
    amb_before: int
    rms_before: float
    amb_after: int
    rms_after: float
    resolution_pct: float
    max_residual: float
    rms_residual: float
    ar_type: str  # 'WL' or 'NL'


@dataclass
class SatelliteAmbiguity:
    """Ambiguity resolution statistics for a single satellite"""
    prn: int
    system: str  # 'G' (GPS 1-32), 'R' (GLONASS 101-128), 'E' (Galileo 201-236)
    amb_total: int
    l1l2_solved: int
    l1l2_rel_pct1: float  # First rel% column
    l1l2_rel_pct2: float  # Second rel% column (cumulative)
    l5_solved: int
    l5_rel_pct1: float
    l5_rel_pct2: float


@dataclass
class AmbiguitySummary:
    """Complete ambiguity resolution summary for a day"""
    year: int
    doy: int
    wl_baselines: list[BaselineAmbiguity] = field(default_factory=list)
    nl_baselines: list[BaselineAmbiguity] = field(default_factory=list)
    l5_baselines: list[BaselineAmbiguity] = field(default_factory=list)  # Phase-Based Widelane (L5)
    l3_baselines: list[BaselineAmbiguity] = field(default_factory=list)  # Phase-Based Narrowlane (L3)
    qif_baselines: list[BaselineAmbiguity] = field(default_factory=list)  # Quasi-Ionosphere-Free (QIF)
    wl_totals: list[SystemTotal] = field(default_factory=list)
    nl_totals: list[SystemTotal] = field(default_factory=list)
    l5_totals: list[SystemTotal] = field(default_factory=list)
    l3_totals: list[SystemTotal] = field(default_factory=list)
    qif_totals: list[SystemTotal] = field(default_factory=list)
    satellite_stats: list[SatelliteAmbiguity] = field(default_factory=list)
    gps_total: Optional[dict] = None
    glo_total: Optional[dict] = None
    gal_total: Optional[dict] = None


def parse_system_code(sys_str: str) -> str:
    """Parse system code from AMB file format"""
    sys_str = sys_str.strip()
    if sys_str == 'G':
        return 'GPS'
    elif sys_str == 'E':
        return 'Galileo'
    elif sys_str == 'R':
        return 'GLONASS'
    elif 'G' in sys_str and 'E' in sys_str:
        return 'GPS+Galileo'
    elif 'G' in sys_str and 'R' in sys_str:
        return 'GPS+GLONASS'
    elif 'E' in sys_str and 'R' in sys_str:
        return 'Galileo+GLONASS'
    elif 'G' in sys_str and 'R' in sys_str and 'E' in sys_str:
        return 'GPS+GLO+GAL'
    return sys_str


def get_satellite_system(prn: int) -> str:
    """Determine satellite system from PRN number"""
    if 1 <= prn <= 32:
        return 'GPS'
    elif 101 <= prn <= 128:
        return 'GLONASS'
    elif 201 <= prn <= 250:
        return 'Galileo'
    return 'Unknown'


def parse_amb_summary_file(filepath: str, year: int, doy: int) -> Optional[AmbiguitySummary]:
    """
    Parse an AMB summary file.

    Args:
        filepath: Path to AMB summary file (can be .gz compressed)
        year: Year of processing
        doy: Day of year

    Returns:
        AmbiguitySummary with all parsed data, or None if parsing fails
    """
    summary = AmbiguitySummary(year=year, doy=doy)

    # Handle gzip compressed files
    if filepath.endswith('.gz'):
        open_func = lambda f: gzip.open(f, 'rt')
    else:
        open_func = lambda f: open(f, 'r')

    try:
        with open_func(filepath) as f:
            lines = f.readlines()

        current_section = None

        for line in lines:
            line = line.rstrip('\n')

            # Detect section changes
            if 'Code-Based Widelane (WL) Ambiguity Resolution' in line:
                current_section = 'WL'
                continue
            elif 'Code-Based Narrowlane (NL) Ambiguity Resolution' in line:
                current_section = 'NL'
                continue
            elif 'Satellite-wise' in line or 'PRN |' in line:
                current_section = 'SAT'
                continue

            # Parse WL/NL/L5/L3/QIF baseline lines (marked with #AR_WL, #AR_NL, #AR_L5, #AR_L3, #AR_QIF)
            if '#AR_WL' in line or '#AR_NL' in line or '#AR_L5' in line or '#AR_L3' in line or '#AR_QIF' in line:
                baseline = parse_baseline_line(line)
                if baseline:
                    if baseline.ar_type == 'WL':
                        summary.wl_baselines.append(baseline)
                    elif baseline.ar_type == 'NL':
                        summary.nl_baselines.append(baseline)
                    elif baseline.ar_type == 'L5':
                        summary.l5_baselines.append(baseline)
                    elif baseline.ar_type == 'L3':
                        summary.l3_baselines.append(baseline)
                    elif baseline.ar_type == 'QIF':
                        summary.qif_baselines.append(baseline)
                continue

            # Parse total lines
            if line.strip().startswith('Tot:'):
                total = parse_total_line(line, current_section or 'WL')
                if total:
                    if total.ar_type == 'WL':
                        summary.wl_totals.append(total)
                    else:
                        summary.nl_totals.append(total)
                continue

            # Parse satellite statistics
            if current_section == 'SAT':
                # Look for satellite stat lines (start with PRN number or system name)
                if line.strip().startswith(('GPS', 'GLO', 'GAL')):
                    # Parse system total line
                    parts = line.split('|')
                    if len(parts) >= 3:
                        sys_name = parts[0].strip()
                        try:
                            amb_total = int(parts[1].strip())
                            # Parse L1&L2 stats
                            l1l2_parts = parts[2].strip().split()
                            # Parse L5 stats
                            l5_parts = parts[3].strip().split() if len(parts) > 3 else []

                            stat_dict = {
                                'amb_total': amb_total,
                                'l1l2_solved': int(l1l2_parts[0]) if l1l2_parts else 0,
                                'l1l2_rel_pct': float(l1l2_parts[2]) if len(l1l2_parts) > 2 else 0,
                                'l5_solved': int(l5_parts[0]) if l5_parts else 0,
                                'l5_rel_pct': float(l5_parts[2]) if len(l5_parts) > 2 else 0,
                            }

                            if sys_name == 'GPS':
                                summary.gps_total = stat_dict
                            elif sys_name == 'GLO':
                                summary.glo_total = stat_dict
                            elif sys_name == 'GAL':
                                summary.gal_total = stat_dict
                        except (ValueError, IndexError):
                            continue
                elif re.match(r'^\s*\d+\s*\|', line):
                    # Parse individual satellite line
                    sat_stat = parse_satellite_line(line)
                    if sat_stat:
                        summary.satellite_stats.append(sat_stat)

        return summary

    except Exception as e:
        print(f"Error parsing AMB file {filepath}: {e}")
        return None


def parse_baseline_line(line: str) -> Optional[BaselineAmbiguity]:
    """Parse a baseline ambiguity line from WL, NL, L5, L3, or QIF section"""
    try:
        # Determine AR type
        if '#AR_WL' in line:
            ar_type = 'WL'
        elif '#AR_NL' in line:
            ar_type = 'NL'
        elif '#AR_L5' in line:
            ar_type = 'L5'
        elif '#AR_L3' in line:
            ar_type = 'L3'
        elif '#AR_QIF' in line:
            ar_type = 'QIF'
        else:
            ar_type = 'WL'  # Default

        # Remove the marker
        line = line.replace('#AR_WL', '').replace('#AR_NL', '').replace('#AR_L5', '').replace('#AR_L3', '').replace('#AR_QIF', '')

        # Split the line
        parts = line.split()
        if len(parts) < 12:
            return None

        # Parse fields
        # Format: File Sta1 Sta2 Length Before#Amb RMS After#Amb RMS Res% Sys Max RMS QCB FRQ Recv1 Recv2
        file = parts[0]
        sta1 = parts[1]
        sta2 = parts[2]
        length_km = float(parts[3])
        amb_before = int(parts[4])
        rms_before = float(parts[5])
        amb_after = int(parts[6])
        rms_after = float(parts[7])
        resolution_pct = float(parts[8])

        # System can be "G", "E", "G E", etc.
        # Find system by looking for G, E, R patterns
        sys_idx = 9
        system = parts[sys_idx]
        if sys_idx + 1 < len(parts) and parts[sys_idx + 1] in ('E', 'R', 'G'):
            system = system + ' ' + parts[sys_idx + 1]
            sys_idx += 1

        # Max and RMS residuals
        max_idx = sys_idx + 1
        max_residual = float(parts[max_idx]) if max_idx < len(parts) else 0.0
        rms_residual = float(parts[max_idx + 1]) if max_idx + 1 < len(parts) else 0.0

        # QCB and FRQ
        qcb_idx = max_idx + 2
        qcb = parts[qcb_idx] if qcb_idx < len(parts) else '-'
        frq = parts[qcb_idx + 1] if qcb_idx + 1 < len(parts) else '-'

        # Receivers - join remaining parts
        recv_idx = qcb_idx + 2
        receiver_parts = parts[recv_idx:] if recv_idx < len(parts) else []

        # Try to find receiver names (typically have model names)
        receiver1 = ''
        receiver2 = ''
        if receiver_parts:
            # Find the split point (receivers usually have different names)
            mid = len(receiver_parts) // 2
            receiver1 = ' '.join(receiver_parts[:mid]) if mid > 0 else ''
            receiver2 = ' '.join(receiver_parts[mid:]) if mid < len(receiver_parts) else ''

        return BaselineAmbiguity(
            file=file,
            sta1=sta1,
            sta2=sta2,
            length_km=length_km,
            amb_before=amb_before,
            rms_before=rms_before,
            amb_after=amb_after,
            rms_after=rms_after,
            resolution_pct=resolution_pct,
            system=system,
            max_residual=max_residual,
            rms_residual=rms_residual,
            qcb=qcb,
            frq=frq,
            receiver1=receiver1,
            receiver2=receiver2,
            ar_type=ar_type
        )
    except (ValueError, IndexError) as e:
        return None


def parse_total_line(line: str, ar_type: str) -> Optional[SystemTotal]:
    """Parse a total line from WL or NL section"""
    try:
        # Check for AR type markers
        if '#AR_WL' in line:
            ar_type = 'WL'
            line = line.replace('#AR_WL', '')
        elif '#AR_NL' in line:
            ar_type = 'NL'
            line = line.replace('#AR_NL', '')

        # Format: "Tot:  22      0.000  1287  0.0    48  0.3  96.3 G       0.164  0.069"
        # Remove "Tot:" prefix
        line = line.replace('Tot:', '').strip()
        parts = line.split()

        if len(parts) < 9:
            return None

        baselines = int(parts[0])
        length_km = float(parts[1])
        amb_before = int(parts[2])
        rms_before = float(parts[3])
        amb_after = int(parts[4])
        rms_after = float(parts[5])
        resolution_pct = float(parts[6])

        # System
        sys_idx = 7
        system = parts[sys_idx]
        if sys_idx + 1 < len(parts) and parts[sys_idx + 1] in ('E', 'R', 'G'):
            system = system + ' ' + parts[sys_idx + 1]
            sys_idx += 1

        # Max and RMS
        max_residual = float(parts[sys_idx + 1]) if sys_idx + 1 < len(parts) else 0.0
        rms_residual = float(parts[sys_idx + 2]) if sys_idx + 2 < len(parts) else 0.0

        return SystemTotal(
            system=system,
            baselines=baselines,
            length_km=length_km,
            amb_before=amb_before,
            rms_before=rms_before,
            amb_after=amb_after,
            rms_after=rms_after,
            resolution_pct=resolution_pct,
            max_residual=max_residual,
            rms_residual=rms_residual,
            ar_type=ar_type
        )
    except (ValueError, IndexError):
        return None


def parse_satellite_line(line: str) -> Optional[SatelliteAmbiguity]:
    """Parse a satellite statistics line"""
    try:
        # Format: "  1 |     129 |      92   21.7   68.0 |      70   30.5   52.5"
        parts = line.split('|')
        if len(parts) < 4:
            return None

        prn = int(parts[0].strip())
        amb_total = int(parts[1].strip())

        # L1&L2 stats
        l1l2_parts = parts[2].strip().split()
        l1l2_solved = int(l1l2_parts[0]) if l1l2_parts else 0
        l1l2_rel_pct1 = float(l1l2_parts[1]) if len(l1l2_parts) > 1 else 0.0
        l1l2_rel_pct2 = float(l1l2_parts[2]) if len(l1l2_parts) > 2 else 0.0

        # L5 stats
        l5_parts = parts[3].strip().split()
        l5_solved = int(l5_parts[0]) if l5_parts else 0
        l5_rel_pct1 = float(l5_parts[1]) if len(l5_parts) > 1 else 0.0
        l5_rel_pct2 = float(l5_parts[2]) if len(l5_parts) > 2 else 0.0

        system = get_satellite_system(prn)

        return SatelliteAmbiguity(
            prn=prn,
            system=system,
            amb_total=amb_total,
            l1l2_solved=l1l2_solved,
            l1l2_rel_pct1=l1l2_rel_pct1,
            l1l2_rel_pct2=l1l2_rel_pct2,
            l5_solved=l5_solved,
            l5_rel_pct1=l5_rel_pct1,
            l5_rel_pct2=l5_rel_pct2
        )
    except (ValueError, IndexError):
        return None


def get_amb_summary_path(base_dir: str, year: int, doy: int,
                         prefix: str = 'AMB') -> Optional[str]:
    """
    Get the path to an AMB summary file.

    Args:
        base_dir: Base directory for DD results
        year: Year
        doy: Day of year
        prefix: File prefix (default 'AMB')

    Returns:
        Path to AMB file if it exists, None otherwise
    """
    # Try different path patterns
    patterns = [
        f"{base_dir}/{year}/{doy}/OUT/{prefix}_{year}{doy:03d}0.SUM.gz",
        f"{base_dir}/{year}/{doy:03d}/OUT/{prefix}_{year}{doy:03d}0.SUM.gz",
        f"{base_dir}/{year}/{doy}/OUT/{prefix}_{year}{doy:03d}0.SUM",
        f"{base_dir}/{year}/{doy:03d}/OUT/{prefix}_{year}{doy:03d}0.SUM",
    ]

    for path in patterns:
        if os.path.exists(path):
            return path

    return None


def get_unique_baselines(baselines: list[BaselineAmbiguity],
                         system_filter: Optional[str] = None) -> list[BaselineAmbiguity]:
    """
    Get unique baselines (combined G E entries only, not individual G and E).

    Args:
        baselines: List of baseline ambiguity objects
        system_filter: Optional system filter ('G', 'E', 'G E', etc.)

    Returns:
        Filtered list of baselines
    """
    if system_filter:
        return [b for b in baselines if b.system.strip() == system_filter]

    # Return combined entries (G E) preferentially
    combined = {}
    for b in baselines:
        key = (b.sta1, b.sta2)
        if 'G' in b.system and 'E' in b.system:
            combined[key] = b
        elif key not in combined:
            combined[key] = b

    return list(combined.values())


if __name__ == "__main__":
    # Test the parser
    import sys

    test_file = "/home/ahunegnaw/GPSRESULTS/ahunegnaw/OCT2025_MGX/2025/302/OUT/AMB_20253020.SUM.gz"

    if os.path.exists(test_file):
        print(f"Parsing: {test_file}")
        summary = parse_amb_summary_file(test_file, 2025, 302)

        if summary:
            print(f"\nWidelane (WL) baselines: {len(summary.wl_baselines)}")
            print(f"Narrowlane (NL) baselines: {len(summary.nl_baselines)}")
            print(f"WL totals: {len(summary.wl_totals)}")
            print(f"NL totals: {len(summary.nl_totals)}")
            print(f"Satellite stats: {len(summary.satellite_stats)}")

            if summary.wl_totals:
                print("\nWL System Totals:")
                for t in summary.wl_totals:
                    print(f"  {t.system}: {t.resolution_pct:.1f}% ({t.amb_before - t.amb_after}/{t.amb_before} resolved)")

            if summary.nl_totals:
                print("\nNL System Totals:")
                for t in summary.nl_totals:
                    print(f"  {t.system}: {t.resolution_pct:.1f}% ({t.amb_before - t.amb_after}/{t.amb_before} resolved)")

            if summary.gps_total:
                print(f"\nGPS satellite total: {summary.gps_total}")
            if summary.gal_total:
                print(f"Galileo satellite total: {summary.gal_total}")
            if summary.glo_total:
                print(f"GLONASS satellite total: {summary.glo_total}")
        else:
            print("Failed to parse AMB file")
    else:
        print(f"Test file not found: {test_file}")
