"""
Formatting utilities for GNSS data.

Provides number formatting, zero-padding, and alpha-numeric
time conversions used throughout GNSS processing.

Replaces Perl FORMAT.pm module.
"""

from __future__ import annotations

from typing import Union


def round_to_precision(value: float, precision: int) -> float:
    """Round a number to specified decimal places.

    Args:
        value: Number to round
        precision: Number of decimal places

    Returns:
        Rounded number
    """
    return round(value, precision)


def zero_pad(value: Union[int, str], width: int) -> str:
    """Zero-pad a value to specified width.

    Args:
        value: Value to pad
        width: Target width

    Returns:
        Zero-padded string

    Raises:
        ValueError: If value is longer than width
    """
    s = str(value)
    if len(s) > width:
        raise ValueError(f"Value '{s}' is longer than width {width}")
    return s.zfill(width)


def format_width(value: Union[int, str], width: int) -> str:
    """Format value to specified width with zero padding.

    Alias for zero_pad for compatibility.
    """
    return zero_pad(value, width)


# Hour alpha conversion (a-x for hours 0-23)
HOUR_TO_ALPHA = [
    'a', 'b', 'c', 'd', 'e', 'f',
    'g', 'h', 'i', 'j', 'k', 'l',
    'm', 'n', 'o', 'p', 'q', 'r',
    's', 't', 'u', 'v', 'w', 'x',
]

ALPHA_TO_HOUR = {alpha: hour for hour, alpha in enumerate(HOUR_TO_ALPHA)}


def hour_to_alpha(hour: int) -> str:
    """Convert hour (0-23) to alpha character (a-x).

    Args:
        hour: Hour (0-23)

    Returns:
        Single character 'a'-'x'

    Raises:
        ValueError: If hour is out of range
    """
    if not 0 <= hour <= 23:
        raise ValueError(f"Hour must be 0-23, got {hour}")
    return HOUR_TO_ALPHA[hour]


def alpha_to_hour(alpha: str) -> int:
    """Convert alpha character to hour.

    Args:
        alpha: Single character 'a'-'x'

    Returns:
        Hour (0-23)

    Raises:
        ValueError: If alpha is invalid
    """
    alpha_lower = alpha.lower()
    if alpha_lower not in ALPHA_TO_HOUR:
        raise ValueError(f"Alpha must be a-x, got {alpha}")
    return ALPHA_TO_HOUR[alpha_lower]


# Sub-hourly (15-minute) alpha conversion for high-rate data
SUBHOUR_TO_ALPHA = [
    'a00', 'a15', 'a30', 'a45',
    'b00', 'b15', 'b30', 'b45',
    'c00', 'c15', 'c30', 'c45',
    'd00', 'd15', 'd30', 'd45',
    'e00', 'e15', 'e30', 'e45',
    'f00', 'f15', 'f30', 'f45',
    'g00', 'g15', 'g30', 'g45',
    'h00', 'h15', 'h30', 'h45',
    'i00', 'i15', 'i30', 'i45',
    'j00', 'j15', 'j30', 'j45',
    'k00', 'k15', 'k30', 'k45',
    'l00', 'l15', 'l30', 'l45',
    'm00', 'm15', 'm30', 'm45',
    'n00', 'n15', 'n30', 'n45',
    'o00', 'o15', 'o30', 'o45',
    'p00', 'p15', 'p30', 'p45',
    'q00', 'q15', 'q30', 'q45',
    'r00', 'r15', 'r30', 'r45',
    's00', 's15', 's30', 's45',
    't00', 't15', 't30', 't45',
    'u00', 'u15', 'u30', 'u45',
    'v00', 'v15', 'v30', 'v45',
    'w00', 'w15', 'w30', 'w45',
    'x00', 'x15', 'x30', 'x45',
]

ALPHA_TO_SUBHOUR = {alpha: idx for idx, alpha in enumerate(SUBHOUR_TO_ALPHA)}


def subhour_to_alpha(index: int) -> str:
    """Convert sub-hourly index (0-95) to alpha notation.

    Args:
        index: Sub-hourly index (0-95)

    Returns:
        Alpha notation like 'a00', 'a15', etc.

    Raises:
        ValueError: If index is out of range
    """
    if not 0 <= index <= 95:
        raise ValueError(f"Sub-hourly index must be 0-95, got {index}")
    return SUBHOUR_TO_ALPHA[index]


def alpha_to_subhour(alpha: str) -> int:
    """Convert sub-hourly alpha notation to index.

    Args:
        alpha: Alpha notation like 'a00', 'a15', etc.

    Returns:
        Sub-hourly index (0-95)

    Raises:
        ValueError: If alpha is invalid
    """
    alpha_lower = alpha.lower()
    if alpha_lower not in ALPHA_TO_SUBHOUR:
        raise ValueError(f"Invalid sub-hourly alpha: {alpha}")
    return ALPHA_TO_SUBHOUR[alpha_lower]


def subhour_to_time(index: int) -> tuple[int, int]:
    """Convert sub-hourly index to hour and minute.

    Args:
        index: Sub-hourly index (0-95)

    Returns:
        Tuple of (hour, minute)
    """
    hour = index // 4
    minute = (index % 4) * 15
    return hour, minute


def time_to_subhour(hour: int, minute: int) -> int:
    """Convert hour and minute to sub-hourly index.

    Args:
        hour: Hour (0-23)
        minute: Minute (0, 15, 30, or 45)

    Returns:
        Sub-hourly index (0-95)

    Raises:
        ValueError: If minute is not 0, 15, 30, or 45
    """
    if minute not in (0, 15, 30, 45):
        raise ValueError(f"Minute must be 0, 15, 30, or 45, got {minute}")
    return hour * 4 + minute // 15


# Small meteorological time format (HHMM)
SMALL_MET_TIMES = [
    '0000', '0015', '0030', '0045',
    '0100', '0115', '0130', '0145',
    '0200', '0215', '0230', '0245',
    '0300', '0315', '0330', '0345',
    '0400', '0415', '0430', '0445',
    '0500', '0515', '0530', '0545',
    '0600', '0615', '0630', '0645',
    '0700', '0715', '0730', '0745',
    '0800', '0815', '0830', '0845',
    '0900', '0915', '0930', '0945',
    '1000', '1015', '1030', '1045',
    '1100', '1115', '1130', '1145',
    '1200', '1215', '1230', '1245',
    '1300', '1315', '1330', '1345',
    '1400', '1415', '1430', '1445',
    '1500', '1515', '1530', '1545',
    '1600', '1615', '1630', '1645',
    '1700', '1715', '1730', '1745',
    '1800', '1815', '1830', '1845',
    '1900', '1915', '1930', '1945',
    '2000', '2015', '2030', '2045',
    '2100', '2115', '2130', '2145',
    '2200', '2215', '2230', '2245',
    '2300', '2315', '2330', '2345',
]

SMALL_MET_TO_INDEX = {t: i for i, t in enumerate(SMALL_MET_TIMES)}


def small_met_to_index(time_str: str) -> int:
    """Convert small meteorological time string to index.

    Args:
        time_str: Time string like '0000', '0015', etc.

    Returns:
        Index (0-95)
    """
    if time_str not in SMALL_MET_TO_INDEX:
        raise ValueError(f"Invalid small met time: {time_str}")
    return SMALL_MET_TO_INDEX[time_str]


def index_to_small_met(index: int) -> str:
    """Convert index to small meteorological time string.

    Args:
        index: Index (0-95)

    Returns:
        Time string like '0000', '0015', etc.
    """
    if not 0 <= index <= 95:
        raise ValueError(f"Index must be 0-95, got {index}")
    return SMALL_MET_TIMES[index]


def year_2c_to_4c(year_2c: int) -> int:
    """Convert 2-digit year to 4-digit year.

    Uses pivot year of 70 (1970-2069).

    Args:
        year_2c: 2-digit year (0-99)

    Returns:
        4-digit year
    """
    if year_2c > 70:
        return 1900 + year_2c
    else:
        return 2000 + year_2c


def year_4c_to_2c(year_4c: int) -> int:
    """Convert 4-digit year to 2-digit year.

    Args:
        year_4c: 4-digit year

    Returns:
        2-digit year (0-99)
    """
    return year_4c % 100
