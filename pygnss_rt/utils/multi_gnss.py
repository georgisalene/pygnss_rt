"""
Multi-GNSS Constellation Handling Module.

Provides comprehensive support for all major GNSS constellations:
- GPS (G): US Global Positioning System
- GLONASS (R): Russian Global Navigation Satellite System
- Galileo (E): European Global Navigation Satellite System
- BeiDou (C): Chinese BeiDou Navigation Satellite System
- QZSS (J): Japanese Quasi-Zenith Satellite System
- SBAS (S): Satellite-Based Augmentation Systems
- IRNSS/NavIC (I): Indian Regional Navigation Satellite System

Features:
- Constellation-specific frequency definitions
- Signal and observation code mappings
- GLONASS frequency channel handling (FDMA)
- Satellite numbering schemes
- Time system conversions
- Inter-system bias handling
- Multi-GNSS RINEX support

Usage:
    from pygnss_rt.utils.multi_gnss import (
        GNSSConstellation,
        GPSSignal,
        GLONASSSignal,
        get_frequency,
        convert_prn,
    )

    # Get L1 frequency for GPS
    freq = get_frequency(GNSSConstellation.GPS, 'L1')

    # GLONASS frequency with channel
    freq_r = get_glonass_frequency('L1', channel=5)
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Optional

logger = logging.getLogger(__name__)


# =============================================================================
# GNSS Constellation Definitions
# =============================================================================

class GNSSConstellation(str, Enum):
    """GNSS satellite constellations."""

    GPS = "G"        # US Global Positioning System
    GLONASS = "R"    # Russian GLONASS
    GALILEO = "E"    # European Galileo
    BEIDOU = "C"     # Chinese BeiDou
    QZSS = "J"       # Japanese QZSS
    SBAS = "S"       # SBAS (WAAS, EGNOS, MSAS, GAGAN)
    IRNSS = "I"      # Indian IRNSS/NavIC
    MIXED = "M"      # Multi-constellation

    @classmethod
    def from_prn(cls, prn: str) -> "GNSSConstellation":
        """Get constellation from PRN string (e.g., 'G01', 'R05')."""
        if not prn:
            raise ValueError("Empty PRN string")

        system_char = prn[0].upper()
        try:
            return cls(system_char)
        except ValueError:
            # Try numeric PRN (RINEX 2 style)
            if prn.isdigit():
                prn_num = int(prn)
                if 1 <= prn_num <= 32:
                    return cls.GPS
            raise ValueError(f"Unknown constellation for PRN: {prn}")

    @property
    def full_name(self) -> str:
        """Get full constellation name."""
        names = {
            "G": "GPS (Global Positioning System)",
            "R": "GLONASS (Global Navigation Satellite System)",
            "E": "Galileo",
            "C": "BeiDou Navigation Satellite System",
            "J": "QZSS (Quasi-Zenith Satellite System)",
            "S": "SBAS (Satellite-Based Augmentation System)",
            "I": "IRNSS/NavIC",
            "M": "Mixed/Multi-GNSS",
        }
        return names.get(self.value, self.value)

    @property
    def satellite_range(self) -> tuple[int, int]:
        """Get valid satellite PRN range for this constellation."""
        ranges = {
            "G": (1, 32),     # GPS: G01-G32
            "R": (1, 27),     # GLONASS: R01-R27
            "E": (1, 36),     # Galileo: E01-E36
            "C": (1, 63),     # BeiDou: C01-C63 (including GEO, IGSO, MEO)
            "J": (1, 10),     # QZSS: J01-J10
            "S": (120, 158),  # SBAS: S120-S158 (PRN 120-158)
            "I": (1, 14),     # IRNSS: I01-I14
        }
        return ranges.get(self.value, (1, 99))


# =============================================================================
# Signal and Frequency Definitions
# =============================================================================

# Speed of light (m/s)
SPEED_OF_LIGHT = 299792458.0

# GPS Frequencies (Hz)
GPS_L1_FREQ = 1575.42e6      # L1 C/A, L1C
GPS_L2_FREQ = 1227.60e6      # L2 P(Y), L2C
GPS_L5_FREQ = 1176.45e6      # L5

# GLONASS Base Frequencies (Hz) - FDMA
GLONASS_G1_BASE = 1602.0e6   # G1 base
GLONASS_G1_STEP = 0.5625e6   # G1 channel step
GLONASS_G2_BASE = 1246.0e6   # G2 base
GLONASS_G2_STEP = 0.4375e6   # G2 channel step
GLONASS_G3_FREQ = 1202.025e6 # G3 (CDMA)

# Galileo Frequencies (Hz)
GALILEO_E1_FREQ = 1575.42e6  # E1 (same as GPS L1)
GALILEO_E5A_FREQ = 1176.45e6 # E5a (same as GPS L5)
GALILEO_E5B_FREQ = 1207.14e6 # E5b
GALILEO_E5_FREQ = 1191.795e6 # E5 (AltBOC)
GALILEO_E6_FREQ = 1278.75e6  # E6

# BeiDou Frequencies (Hz)
BEIDOU_B1_FREQ = 1561.098e6  # B1 (legacy)
BEIDOU_B1C_FREQ = 1575.42e6  # B1C (same as GPS L1)
BEIDOU_B2A_FREQ = 1176.45e6  # B2a (same as GPS L5)
BEIDOU_B2B_FREQ = 1207.14e6  # B2b (same as Galileo E5b)
BEIDOU_B2_FREQ = 1191.795e6  # B2 (B2a+B2b)
BEIDOU_B3_FREQ = 1268.52e6   # B3

# QZSS Frequencies (Hz) - Same as GPS
QZSS_L1_FREQ = 1575.42e6
QZSS_L2_FREQ = 1227.60e6
QZSS_L5_FREQ = 1176.45e6
QZSS_L6_FREQ = 1278.75e6     # LEX signal

# SBAS Frequencies (Hz)
SBAS_L1_FREQ = 1575.42e6
SBAS_L5_FREQ = 1176.45e6

# IRNSS/NavIC Frequencies (Hz)
IRNSS_L5_FREQ = 1176.45e6
IRNSS_S_FREQ = 2492.028e6


class GPSSignal(str, Enum):
    """GPS signals and observation codes."""
    L1CA = "C1C"   # L1 C/A code pseudorange
    L1P = "C1W"    # L1 P(Y) code pseudorange
    L1C = "C1L"    # L1C pilot
    L1X = "C1X"    # L1C combined
    L2P = "C2W"    # L2 P(Y) code
    L2C = "C2L"    # L2C (L) code
    L2M = "C2S"    # L2C (M) code
    L2X = "C2X"    # L2C combined
    L5I = "C5I"    # L5 I code
    L5Q = "C5Q"    # L5 Q code
    L5X = "C5X"    # L5 combined


class GLONASSSignal(str, Enum):
    """GLONASS signals and observation codes."""
    G1CA = "C1C"   # G1 C/A code
    G1P = "C1P"    # G1 P code
    G2CA = "C2C"   # G2 C/A code
    G2P = "C2P"    # G2 P code
    G3I = "C3I"    # G3 I code (CDMA)
    G3Q = "C3Q"    # G3 Q code (CDMA)
    G3X = "C3X"    # G3 combined


class GalileoSignal(str, Enum):
    """Galileo signals and observation codes."""
    E1A = "C1A"    # E1 PRS
    E1B = "C1B"    # E1 OS data
    E1C = "C1C"    # E1 OS pilot
    E1X = "C1X"    # E1 combined
    E5AI = "C5I"   # E5a I
    E5AQ = "C5Q"   # E5a Q
    E5AX = "C5X"   # E5a combined
    E5BI = "C7I"   # E5b I
    E5BQ = "C7Q"   # E5b Q
    E5BX = "C7X"   # E5b combined
    E5I = "C8I"    # E5 I (AltBOC)
    E5Q = "C8Q"    # E5 Q (AltBOC)
    E5X = "C8X"    # E5 combined
    E6A = "C6A"    # E6 PRS
    E6B = "C6B"    # E6 CS data
    E6C = "C6C"    # E6 CS pilot
    E6X = "C6X"    # E6 combined


class BeiDouSignal(str, Enum):
    """BeiDou signals and observation codes."""
    B1I = "C2I"    # B1I (legacy)
    B1Q = "C2Q"    # B1Q
    B1X = "C2X"    # B1 combined
    B1CD = "C1D"   # B1C data
    B1CP = "C1P"   # B1C pilot
    B1CX = "C1X"   # B1C combined
    B2AI = "C5I"   # B2a I
    B2AQ = "C5Q"   # B2a Q
    B2AX = "C5X"   # B2a combined
    B2BI = "C7I"   # B2b I
    B2BQ = "C7Q"   # B2b Q
    B2BX = "C7X"   # B2b combined
    B2I = "C7I"    # B2I (legacy)
    B2Q = "C7Q"    # B2Q
    B3I = "C6I"    # B3I
    B3Q = "C6Q"    # B3Q
    B3X = "C6X"    # B3 combined


class QZSSSignal(str, Enum):
    """QZSS signals and observation codes."""
    L1CA = "C1C"   # L1 C/A
    L1C = "C1X"    # L1C
    L2S = "C2S"    # L2C-M
    L2L = "C2L"    # L2C-L
    L2X = "C2X"    # L2C combined
    L5I = "C5I"    # L5 I
    L5Q = "C5Q"    # L5 Q
    L5X = "C5X"    # L5 combined
    L6D = "C6S"    # LEX data
    L6P = "C6L"    # LEX pilot
    L6X = "C6X"    # LEX combined


class IRNSSSignal(str, Enum):
    """IRNSS/NavIC signals and observation codes."""
    L5A = "C5A"    # L5 SPS
    L5B = "C5B"    # L5 RS
    L5C = "C5C"    # L5 combined
    L5X = "C5X"    # L5 combined
    SA = "C9A"     # S-band A
    SB = "C9B"     # S-band B
    SC = "C9C"     # S-band C
    SX = "C9X"     # S-band combined


# =============================================================================
# Frequency Lookup Tables
# =============================================================================

# Frequency mapping for each constellation and band
FREQUENCY_TABLE: dict[str, dict[str, float]] = {
    "G": {  # GPS
        "1": GPS_L1_FREQ, "L1": GPS_L1_FREQ,
        "2": GPS_L2_FREQ, "L2": GPS_L2_FREQ,
        "5": GPS_L5_FREQ, "L5": GPS_L5_FREQ,
    },
    "R": {  # GLONASS (base frequencies, use get_glonass_frequency for FDMA)
        "1": GLONASS_G1_BASE, "G1": GLONASS_G1_BASE,
        "2": GLONASS_G2_BASE, "G2": GLONASS_G2_BASE,
        "3": GLONASS_G3_FREQ, "G3": GLONASS_G3_FREQ,
    },
    "E": {  # Galileo
        "1": GALILEO_E1_FREQ, "E1": GALILEO_E1_FREQ,
        "5": GALILEO_E5A_FREQ, "E5a": GALILEO_E5A_FREQ,
        "7": GALILEO_E5B_FREQ, "E5b": GALILEO_E5B_FREQ,
        "8": GALILEO_E5_FREQ, "E5": GALILEO_E5_FREQ,
        "6": GALILEO_E6_FREQ, "E6": GALILEO_E6_FREQ,
    },
    "C": {  # BeiDou
        "2": BEIDOU_B1_FREQ, "B1": BEIDOU_B1_FREQ,
        "1": BEIDOU_B1C_FREQ, "B1C": BEIDOU_B1C_FREQ,
        "5": BEIDOU_B2A_FREQ, "B2a": BEIDOU_B2A_FREQ,
        "7": BEIDOU_B2B_FREQ, "B2b": BEIDOU_B2B_FREQ,
        "8": BEIDOU_B2_FREQ, "B2": BEIDOU_B2_FREQ,
        "6": BEIDOU_B3_FREQ, "B3": BEIDOU_B3_FREQ,
    },
    "J": {  # QZSS
        "1": QZSS_L1_FREQ, "L1": QZSS_L1_FREQ,
        "2": QZSS_L2_FREQ, "L2": QZSS_L2_FREQ,
        "5": QZSS_L5_FREQ, "L5": QZSS_L5_FREQ,
        "6": QZSS_L6_FREQ, "L6": QZSS_L6_FREQ,
    },
    "S": {  # SBAS
        "1": SBAS_L1_FREQ, "L1": SBAS_L1_FREQ,
        "5": SBAS_L5_FREQ, "L5": SBAS_L5_FREQ,
    },
    "I": {  # IRNSS
        "5": IRNSS_L5_FREQ, "L5": IRNSS_L5_FREQ,
        "9": IRNSS_S_FREQ, "S": IRNSS_S_FREQ,
    },
}


# =============================================================================
# GLONASS FDMA Channel Handling
# =============================================================================

# GLONASS frequency channels (-7 to +6, with 0 shared by two satellites)
# As of 2024, standard channel assignments
GLONASS_CHANNEL_TABLE: dict[int, int] = {
    1: 1, 2: -4, 3: 5, 4: 6, 5: 1, 6: -4, 7: 5, 8: 6,
    9: -6, 10: -7, 11: 0, 12: -1, 13: -2, 14: -7, 15: 0, 16: -1,
    17: 4, 18: -3, 19: 3, 20: 2, 21: 4, 22: -3, 23: 3, 24: 2,
    25: -2, 26: -6, 27: 0,
}


@dataclass
class GLONASSChannel:
    """GLONASS frequency channel information."""

    slot: int  # Orbital slot (1-24)
    channel: int  # Frequency channel (-7 to +6)
    g1_freq: float  # G1 frequency (Hz)
    g2_freq: float  # G2 frequency (Hz)

    @classmethod
    def from_slot(cls, slot: int, channel: Optional[int] = None) -> "GLONASSChannel":
        """Create from orbital slot number.

        Args:
            slot: GLONASS orbital slot (1-27)
            channel: Override channel number (uses table if not specified)

        Returns:
            GLONASSChannel with frequencies
        """
        if channel is None:
            channel = GLONASS_CHANNEL_TABLE.get(slot, 0)

        g1_freq = GLONASS_G1_BASE + channel * GLONASS_G1_STEP
        g2_freq = GLONASS_G2_BASE + channel * GLONASS_G2_STEP

        return cls(
            slot=slot,
            channel=channel,
            g1_freq=g1_freq,
            g2_freq=g2_freq,
        )


def get_glonass_frequency(
    band: str,
    channel: int = 0,
    slot: Optional[int] = None,
) -> float:
    """Get GLONASS frequency for specific channel.

    GLONASS uses FDMA for G1/G2 signals, where each satellite
    has a unique frequency based on its channel number.

    Args:
        band: Frequency band ('G1', 'G2', 'G3', '1', '2', '3')
        channel: Frequency channel number (-7 to +6)
        slot: Orbital slot (alternative to channel)

    Returns:
        Frequency in Hz
    """
    if slot is not None:
        channel = GLONASS_CHANNEL_TABLE.get(slot, 0)

    band = band.upper().replace("L", "G")

    if band in ("G1", "1"):
        return GLONASS_G1_BASE + channel * GLONASS_G1_STEP
    elif band in ("G2", "2"):
        return GLONASS_G2_BASE + channel * GLONASS_G2_STEP
    elif band in ("G3", "3"):
        return GLONASS_G3_FREQ  # CDMA, no channel dependency
    else:
        raise ValueError(f"Unknown GLONASS band: {band}")


# =============================================================================
# Satellite and PRN Handling
# =============================================================================

@dataclass
class Satellite:
    """GNSS satellite information."""

    constellation: GNSSConstellation
    prn: int
    svn: Optional[int] = None  # Space Vehicle Number
    block: Optional[str] = None  # Block type (IIF, III, etc.)
    launch_date: Optional[datetime] = None
    is_active: bool = True

    @property
    def prn_string(self) -> str:
        """Get formatted PRN string (e.g., 'G01', 'R05')."""
        return f"{self.constellation.value}{self.prn:02d}"

    @classmethod
    def from_prn(cls, prn_str: str) -> "Satellite":
        """Create from PRN string."""
        constellation = GNSSConstellation.from_prn(prn_str)

        # Extract numeric part
        if prn_str[0].isalpha():
            prn_num = int(prn_str[1:])
        else:
            prn_num = int(prn_str)

        return cls(constellation=constellation, prn=prn_num)


def parse_prn(prn_str: str) -> tuple[GNSSConstellation, int]:
    """Parse PRN string to constellation and number.

    Args:
        prn_str: PRN string (e.g., 'G01', 'R05', '15')

    Returns:
        Tuple of (constellation, prn_number)
    """
    if not prn_str:
        raise ValueError("Empty PRN string")

    prn_str = prn_str.strip()

    if prn_str[0].isalpha():
        constellation = GNSSConstellation(prn_str[0].upper())
        prn_num = int(prn_str[1:])
    else:
        # RINEX 2 style - assume GPS
        prn_num = int(prn_str)
        constellation = GNSSConstellation.GPS

    return constellation, prn_num


def format_prn(
    constellation: GNSSConstellation,
    prn: int,
    width: int = 3,
) -> str:
    """Format PRN as standard string.

    Args:
        constellation: GNSS constellation
        prn: PRN number
        width: Total width (default 3 for 'G01')

    Returns:
        Formatted PRN string
    """
    prn_str = f"{constellation.value}{prn:02d}"
    return prn_str.rjust(width)


def convert_prn(
    prn: str | int,
    from_system: Optional[str] = None,
    to_system: str = "rinex3",
) -> str:
    """Convert PRN between different formats.

    Args:
        prn: Input PRN (e.g., 'G01', 1, 'GPS-01')
        from_system: Input format ('rinex2', 'rinex3', 'nmea', None for auto)
        to_system: Output format ('rinex2', 'rinex3', 'nmea', 'full')

    Returns:
        Converted PRN string
    """
    # Parse input
    if isinstance(prn, int):
        constellation = GNSSConstellation.GPS
        prn_num = prn
    elif isinstance(prn, str):
        constellation, prn_num = parse_prn(prn)
    else:
        raise ValueError(f"Invalid PRN type: {type(prn)}")

    # Format output
    if to_system == "rinex2":
        # RINEX 2: numeric for GPS, letter+num for others
        if constellation == GNSSConstellation.GPS:
            return f"{prn_num:02d}"
        else:
            return f"{constellation.value}{prn_num:02d}"

    elif to_system == "rinex3":
        return f"{constellation.value}{prn_num:02d}"

    elif to_system == "nmea":
        # NMEA uses different PRN ranges
        if constellation == GNSSConstellation.GPS:
            return str(prn_num)
        elif constellation == GNSSConstellation.GLONASS:
            return str(prn_num + 64)
        elif constellation == GNSSConstellation.SBAS:
            return str(prn_num)  # Already 120-158
        else:
            return str(prn_num)

    elif to_system == "full":
        return f"{constellation.full_name} PRN {prn_num}"

    else:
        return f"{constellation.value}{prn_num:02d}"


# =============================================================================
# Frequency and Wavelength Functions
# =============================================================================

def get_frequency(
    constellation: GNSSConstellation | str,
    band: str | int,
    channel: int = 0,
) -> float:
    """Get carrier frequency for a constellation and band.

    Args:
        constellation: GNSS constellation
        band: Frequency band (1, 2, 5, 'L1', 'E5a', etc.)
        channel: GLONASS channel number (for FDMA)

    Returns:
        Frequency in Hz
    """
    if isinstance(constellation, str):
        constellation = GNSSConstellation(constellation)

    band_str = str(band)

    # Special handling for GLONASS FDMA
    if constellation == GNSSConstellation.GLONASS and band_str in ("1", "2", "G1", "G2"):
        return get_glonass_frequency(band_str, channel)

    # Look up in frequency table
    system_freqs = FREQUENCY_TABLE.get(constellation.value, {})
    freq = system_freqs.get(band_str)

    if freq is None:
        # Try uppercase/different format
        freq = system_freqs.get(band_str.upper())

    if freq is None:
        raise ValueError(
            f"Unknown frequency band {band} for {constellation.value}"
        )

    return freq


def get_wavelength(
    constellation: GNSSConstellation | str,
    band: str | int,
    channel: int = 0,
) -> float:
    """Get carrier wavelength for a constellation and band.

    Args:
        constellation: GNSS constellation
        band: Frequency band
        channel: GLONASS channel number

    Returns:
        Wavelength in meters
    """
    freq = get_frequency(constellation, band, channel)
    return SPEED_OF_LIGHT / freq


def get_ionosphere_free_combination(
    f1: float,
    f2: float,
    obs1: float,
    obs2: float,
) -> float:
    """Compute ionosphere-free linear combination.

    Args:
        f1: First frequency (Hz)
        f2: Second frequency (Hz)
        obs1: Observation on first frequency
        obs2: Observation on second frequency

    Returns:
        Ionosphere-free combination value
    """
    f1_sq = f1 * f1
    f2_sq = f2 * f2

    return (f1_sq * obs1 - f2_sq * obs2) / (f1_sq - f2_sq)


def get_geometry_free_combination(obs1: float, obs2: float) -> float:
    """Compute geometry-free linear combination (L4).

    Args:
        obs1: Observation on first frequency
        obs2: Observation on second frequency

    Returns:
        Geometry-free combination value
    """
    return obs1 - obs2


def get_wide_lane_combination(
    f1: float,
    f2: float,
    obs1: float,
    obs2: float,
) -> float:
    """Compute wide-lane linear combination (L6).

    Args:
        f1: First frequency (Hz)
        f2: Second frequency (Hz)
        obs1: Observation on first frequency (cycles)
        obs2: Observation on second frequency (cycles)

    Returns:
        Wide-lane combination in cycles
    """
    return (f1 * obs1 - f2 * obs2) / (f1 - f2)


def get_narrow_lane_combination(
    f1: float,
    f2: float,
    obs1: float,
    obs2: float,
) -> float:
    """Compute narrow-lane linear combination.

    Args:
        f1: First frequency (Hz)
        f2: Second frequency (Hz)
        obs1: Observation on first frequency (cycles)
        obs2: Observation on second frequency (cycles)

    Returns:
        Narrow-lane combination in cycles
    """
    return (f1 * obs1 + f2 * obs2) / (f1 + f2)


# =============================================================================
# Observation Code Handling
# =============================================================================

@dataclass
class ObservationCode:
    """RINEX observation code definition."""

    code: str  # Full code (e.g., 'C1C', 'L2W', 'S5X')
    constellation: GNSSConstellation
    obs_type: str  # C=Code, L=Phase, D=Doppler, S=SNR
    band: int  # Frequency band number
    attribute: str  # Tracking mode/signal attribute

    @classmethod
    def parse(cls, code: str, constellation: GNSSConstellation = GNSSConstellation.GPS) -> "ObservationCode":
        """Parse RINEX 3 observation code.

        Format: TBX where:
            T = Observation type (C, L, D, S)
            B = Band/frequency (1, 2, 5, 6, 7, 8)
            X = Attribute (C, S, L, X, P, W, Y, M, etc.)

        Args:
            code: Observation code (e.g., 'C1C', 'L2W')
            constellation: GNSS constellation for context

        Returns:
            ObservationCode object
        """
        if len(code) < 2:
            raise ValueError(f"Invalid observation code: {code}")

        obs_type = code[0]
        band = int(code[1]) if code[1].isdigit() else 0
        attribute = code[2] if len(code) > 2 else ""

        return cls(
            code=code,
            constellation=constellation,
            obs_type=obs_type,
            band=band,
            attribute=attribute,
        )

    @property
    def frequency(self) -> float:
        """Get frequency for this observation."""
        return get_frequency(self.constellation, self.band)

    @property
    def wavelength(self) -> float:
        """Get wavelength for this observation."""
        return get_wavelength(self.constellation, self.band)

    @property
    def is_code(self) -> bool:
        """Check if this is a code/pseudorange observation."""
        return self.obs_type == "C"

    @property
    def is_phase(self) -> bool:
        """Check if this is a carrier phase observation."""
        return self.obs_type == "L"

    @property
    def is_snr(self) -> bool:
        """Check if this is a signal-to-noise ratio observation."""
        return self.obs_type == "S"

    @property
    def is_doppler(self) -> bool:
        """Check if this is a Doppler observation."""
        return self.obs_type == "D"


# RINEX 2 to RINEX 3 observation code mapping
RINEX2_TO_RINEX3_OBS: dict[str, dict[str, str]] = {
    "G": {  # GPS
        "C1": "C1C", "P1": "C1W", "L1": "L1C", "S1": "S1C", "D1": "D1C",
        "C2": "C2X", "P2": "C2W", "L2": "L2W", "S2": "S2W", "D2": "D2W",
        "C5": "C5X", "L5": "L5X", "S5": "S5X", "D5": "D5X",
    },
    "R": {  # GLONASS
        "C1": "C1C", "P1": "C1P", "L1": "L1C", "S1": "S1C", "D1": "D1C",
        "C2": "C2C", "P2": "C2P", "L2": "L2C", "S2": "S2C", "D2": "D2C",
    },
    "E": {  # Galileo
        "C1": "C1X", "L1": "L1X", "S1": "S1X", "D1": "D1X",
        "C5": "C5X", "L5": "L5X", "S5": "S5X", "D5": "D5X",
        "C7": "C7X", "L7": "L7X", "S7": "S7X", "D7": "D7X",
        "C8": "C8X", "L8": "L8X", "S8": "S8X", "D8": "D8X",
        "C6": "C6X", "L6": "L6X", "S6": "S6X", "D6": "D6X",
    },
    "C": {  # BeiDou
        "C2": "C2I", "L2": "L2I", "S2": "S2I", "D2": "D2I",
        "C7": "C7I", "L7": "L7I", "S7": "S7I", "D7": "D7I",
        "C6": "C6I", "L6": "L6I", "S6": "S6I", "D6": "D6I",
    },
}


def convert_obs_code(
    code: str,
    from_version: int = 2,
    to_version: int = 3,
    constellation: GNSSConstellation = GNSSConstellation.GPS,
) -> str:
    """Convert observation code between RINEX versions.

    Args:
        code: Observation code
        from_version: Source RINEX version
        to_version: Target RINEX version
        constellation: GNSS constellation for context

    Returns:
        Converted observation code
    """
    if from_version == 2 and to_version == 3:
        system_map = RINEX2_TO_RINEX3_OBS.get(constellation.value, {})
        return system_map.get(code, code)

    elif from_version == 3 and to_version == 2:
        # Reverse mapping
        system_map = RINEX2_TO_RINEX3_OBS.get(constellation.value, {})
        for r2, r3 in system_map.items():
            if r3 == code:
                return r2
        return code[:2]  # Fallback: just return first two chars

    return code


# =============================================================================
# Time System Handling
# =============================================================================

class TimeSystem(str, Enum):
    """GNSS time systems."""

    GPS = "GPS"      # GPS Time
    GLONASS = "GLO"  # GLONASS Time (UTC+3, leap seconds)
    GALILEO = "GAL"  # Galileo System Time (same as GPS)
    BEIDOU = "BDT"   # BeiDou Time
    QZSS = "QZS"     # QZSS Time (same as GPS)
    IRNSS = "IRN"    # IRNSS Time
    UTC = "UTC"      # Coordinated Universal Time
    TAI = "TAI"      # International Atomic Time


# Time offsets relative to GPS Time (seconds)
# Note: These are approximate and should be updated
TIME_OFFSETS: dict[str, float] = {
    "GPS": 0.0,
    "GLO": 0.0,  # GLONASS is aligned with UTC, needs leap seconds
    "GAL": 0.0,  # Galileo is synchronized with GPS
    "BDT": 14.0,  # BeiDou is 14 seconds behind GPS (as of 2006)
    "QZS": 0.0,  # QZSS is synchronized with GPS
    "IRN": 0.0,  # IRNSS is synchronized with GPS
}

# GPS-UTC leap seconds (cumulative)
GPS_UTC_LEAP_SECONDS = 18  # As of 2017


@dataclass
class GNSSTime:
    """Multi-GNSS time representation."""

    time_system: TimeSystem
    week: int
    seconds_of_week: float

    @property
    def total_seconds(self) -> float:
        """Total seconds since reference epoch."""
        return self.week * 604800.0 + self.seconds_of_week

    def to_gps_time(self) -> "GNSSTime":
        """Convert to GPS time."""
        if self.time_system == TimeSystem.GPS:
            return self

        offset = TIME_OFFSETS.get(self.time_system.value, 0.0)
        new_sow = self.seconds_of_week + offset
        new_week = self.week

        # Handle week rollover
        while new_sow >= 604800.0:
            new_sow -= 604800.0
            new_week += 1
        while new_sow < 0:
            new_sow += 604800.0
            new_week -= 1

        return GNSSTime(
            time_system=TimeSystem.GPS,
            week=new_week,
            seconds_of_week=new_sow,
        )

    def to_datetime(self) -> datetime:
        """Convert to Python datetime (UTC)."""
        # GPS epoch: January 6, 1980, 00:00:00 UTC
        gps_epoch = datetime(1980, 1, 6, 0, 0, 0)

        gps_time = self.to_gps_time()
        total_seconds = gps_time.total_seconds

        # Subtract leap seconds for UTC
        total_seconds -= GPS_UTC_LEAP_SECONDS

        return gps_epoch + timedelta(seconds=total_seconds)


def get_constellation_time_system(constellation: GNSSConstellation) -> TimeSystem:
    """Get native time system for a constellation.

    Args:
        constellation: GNSS constellation

    Returns:
        Native time system
    """
    time_systems = {
        GNSSConstellation.GPS: TimeSystem.GPS,
        GNSSConstellation.GLONASS: TimeSystem.GLONASS,
        GNSSConstellation.GALILEO: TimeSystem.GALILEO,
        GNSSConstellation.BEIDOU: TimeSystem.BEIDOU,
        GNSSConstellation.QZSS: TimeSystem.QZSS,
        GNSSConstellation.IRNSS: TimeSystem.IRNSS,
        GNSSConstellation.SBAS: TimeSystem.GPS,
    }
    return time_systems.get(constellation, TimeSystem.GPS)


# =============================================================================
# Inter-System Biases
# =============================================================================

@dataclass
class InterSystemBias:
    """Inter-system bias between two constellations."""

    reference: GNSSConstellation
    target: GNSSConstellation
    signal_ref: str  # Reference signal (e.g., 'C1C')
    signal_target: str  # Target signal
    bias_value: float  # Bias in nanoseconds
    bias_std: float = 0.0  # Standard deviation
    epoch: Optional[datetime] = None


@dataclass
class DifferentialCodeBias:
    """Differential code bias for a satellite."""

    prn: str
    constellation: GNSSConstellation
    obs_code1: str
    obs_code2: str
    bias_value: float  # Bias in nanoseconds
    bias_std: float = 0.0


# =============================================================================
# Multi-GNSS Observation Handler
# =============================================================================

@dataclass
class MultiGNSSObservation:
    """Container for multi-GNSS observations at an epoch."""

    epoch: datetime
    observations: dict[str, dict[str, float]] = field(default_factory=dict)
    # Format: {PRN: {obs_code: value}}

    def get_satellites(self, constellation: Optional[GNSSConstellation] = None) -> list[str]:
        """Get list of satellites with observations.

        Args:
            constellation: Filter by constellation (None for all)

        Returns:
            List of PRN strings
        """
        if constellation is None:
            return list(self.observations.keys())

        return [
            prn for prn in self.observations
            if prn[0] == constellation.value
        ]

    def get_observation(self, prn: str, obs_code: str) -> Optional[float]:
        """Get observation value.

        Args:
            prn: Satellite PRN
            obs_code: Observation code

        Returns:
            Observation value or None
        """
        sat_obs = self.observations.get(prn, {})
        return sat_obs.get(obs_code)

    def get_pseudorange(self, prn: str, band: int = 1) -> Optional[float]:
        """Get pseudorange observation for a satellite.

        Args:
            prn: Satellite PRN
            band: Frequency band

        Returns:
            Pseudorange in meters or None
        """
        sat_obs = self.observations.get(prn, {})

        # Try common code observation types
        for attr in ["C", "W", "X", "P", "I", "Q"]:
            code = f"C{band}{attr}"
            if code in sat_obs:
                return sat_obs[code]

        return None

    def get_phase(self, prn: str, band: int = 1) -> Optional[float]:
        """Get carrier phase observation.

        Args:
            prn: Satellite PRN
            band: Frequency band

        Returns:
            Phase in cycles or None
        """
        sat_obs = self.observations.get(prn, {})

        for attr in ["C", "W", "X", "P", "I", "Q"]:
            code = f"L{band}{attr}"
            if code in sat_obs:
                return sat_obs[code]

        return None

    def count_observations(self) -> dict[str, int]:
        """Count observations by constellation.

        Returns:
            Dict mapping constellation code to count
        """
        counts: dict[str, int] = {}

        for prn in self.observations:
            system = prn[0] if prn else "?"
            counts[system] = counts.get(system, 0) + 1

        return counts


# =============================================================================
# Constellation Configuration
# =============================================================================

@dataclass
class ConstellationConfig:
    """Configuration for processing a specific constellation."""

    constellation: GNSSConstellation
    enabled: bool = True
    signals: list[str] = field(default_factory=list)  # Observation codes to use
    min_elevation: float = 10.0  # Minimum elevation (degrees)
    weight: float = 1.0  # Relative weight in solution

    @classmethod
    def gps_standard(cls) -> "ConstellationConfig":
        """Standard GPS configuration."""
        return cls(
            constellation=GNSSConstellation.GPS,
            signals=["C1C", "C2W", "L1C", "L2W"],
        )

    @classmethod
    def glonass_standard(cls) -> "ConstellationConfig":
        """Standard GLONASS configuration."""
        return cls(
            constellation=GNSSConstellation.GLONASS,
            signals=["C1C", "C2C", "L1C", "L2C"],
        )

    @classmethod
    def galileo_standard(cls) -> "ConstellationConfig":
        """Standard Galileo configuration."""
        return cls(
            constellation=GNSSConstellation.GALILEO,
            signals=["C1X", "C5X", "L1X", "L5X"],
        )

    @classmethod
    def beidou_standard(cls) -> "ConstellationConfig":
        """Standard BeiDou configuration."""
        return cls(
            constellation=GNSSConstellation.BEIDOU,
            signals=["C2I", "C7I", "L2I", "L7I"],
        )


@dataclass
class MultiGNSSConfig:
    """Configuration for multi-GNSS processing."""

    constellations: list[ConstellationConfig] = field(default_factory=list)
    estimate_isb: bool = True  # Estimate inter-system biases
    reference_system: GNSSConstellation = GNSSConstellation.GPS

    @classmethod
    def gps_only(cls) -> "MultiGNSSConfig":
        """GPS-only configuration."""
        return cls(
            constellations=[ConstellationConfig.gps_standard()],
            estimate_isb=False,
        )

    @classmethod
    def gps_glonass(cls) -> "MultiGNSSConfig":
        """GPS+GLONASS configuration."""
        return cls(
            constellations=[
                ConstellationConfig.gps_standard(),
                ConstellationConfig.glonass_standard(),
            ],
        )

    @classmethod
    def full_multi_gnss(cls) -> "MultiGNSSConfig":
        """Full multi-GNSS configuration (GPS+GLO+GAL+BDS)."""
        return cls(
            constellations=[
                ConstellationConfig.gps_standard(),
                ConstellationConfig.glonass_standard(),
                ConstellationConfig.galileo_standard(),
                ConstellationConfig.beidou_standard(),
            ],
        )

    def get_enabled_systems(self) -> list[GNSSConstellation]:
        """Get list of enabled constellations."""
        return [c.constellation for c in self.constellations if c.enabled]


# =============================================================================
# Convenience Functions
# =============================================================================

def list_constellations() -> list[tuple[str, str]]:
    """List all supported constellations.

    Returns:
        List of (code, name) tuples
    """
    return [(c.value, c.full_name) for c in GNSSConstellation if c != GNSSConstellation.MIXED]


def get_all_frequencies(constellation: GNSSConstellation) -> dict[str, float]:
    """Get all frequencies for a constellation.

    Args:
        constellation: GNSS constellation

    Returns:
        Dict mapping band name to frequency
    """
    return FREQUENCY_TABLE.get(constellation.value, {}).copy()


def is_valid_prn(prn: str) -> bool:
    """Check if PRN string is valid.

    Args:
        prn: PRN string

    Returns:
        True if valid
    """
    try:
        constellation, prn_num = parse_prn(prn)
        min_prn, max_prn = constellation.satellite_range
        return min_prn <= prn_num <= max_prn
    except (ValueError, KeyError):
        return False


def get_constellation_signals(
    constellation: GNSSConstellation,
) -> list[str]:
    """Get common observation codes for a constellation.

    Args:
        constellation: GNSS constellation

    Returns:
        List of observation codes
    """
    signals = {
        GNSSConstellation.GPS: ["C1C", "C1W", "C2W", "C2L", "C5X", "L1C", "L2W", "L5X"],
        GNSSConstellation.GLONASS: ["C1C", "C1P", "C2C", "C2P", "C3X", "L1C", "L2C", "L3X"],
        GNSSConstellation.GALILEO: ["C1X", "C5X", "C7X", "C8X", "C6X", "L1X", "L5X", "L7X"],
        GNSSConstellation.BEIDOU: ["C2I", "C1X", "C5X", "C7X", "C6I", "L2I", "L5X", "L7X"],
        GNSSConstellation.QZSS: ["C1C", "C2X", "C5X", "C6X", "L1C", "L2X", "L5X"],
        GNSSConstellation.IRNSS: ["C5A", "C5B", "C9A", "L5A", "L9A"],
        GNSSConstellation.SBAS: ["C1C", "C5X", "L1C", "L5X"],
    }
    return signals.get(constellation, [])
