"""
Date and time utilities for GNSS processing.

Provides conversions between various GNSS time systems:
- Modified Julian Date (MJD)
- GPS Week and Day of Week
- Year and Day of Year (DOY)
- Calendar dates

Replaces Perl modules: DATES.pm, GMTTIME.pm
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import ClassVar


# Constants
GPS_EPOCH_MJD = 44244.0  # January 6, 1980
MJD_OFFSET = 2400000.5   # Offset from JD to MJD


def mjd_from_date(
    year: int,
    month: int,
    day: int,
    hour: int = 0,
    minute: int = 0,
    second: float = 0.0,
) -> float:
    """Calculate Modified Julian Date from calendar date.

    Args:
        year: Year (e.g., 2024)
        month: Month (1-12)
        day: Day of month (1-31)
        hour: Hour (0-23)
        minute: Minute (0-59)
        second: Second (0-59.999...)

    Returns:
        MJD as float
    """
    # Algorithm from astronomical computing
    a = (14 - month) // 12
    y = year + 4800 - a
    m = month + 12 * a - 3

    # Julian Day Number
    jdn = day + (153 * m + 2) // 5 + 365 * y + y // 4 - y // 100 + y // 400 - 32045

    # Add fractional day
    frac = (hour + minute / 60.0 + second / 3600.0) / 24.0

    # Convert to MJD
    return jdn - MJD_OFFSET + frac


def date_from_mjd(mjd: float) -> datetime:
    """Convert MJD to datetime.

    Args:
        mjd: Modified Julian Date

    Returns:
        datetime object (UTC)
    """
    jd = mjd + MJD_OFFSET

    # Algorithm from astronomical computing
    z = int(jd + 0.5)
    f = jd + 0.5 - z

    if z < 2299161:
        a = z
    else:
        alpha = int((z - 1867216.25) / 36524.25)
        a = z + 1 + alpha - alpha // 4

    b = a + 1524
    c = int((b - 122.1) / 365.25)
    d = int(365.25 * c)
    e = int((b - d) / 30.6001)

    day = b - d - int(30.6001 * e)
    month = e - 1 if e < 14 else e - 13
    year = c - 4716 if month > 2 else c - 4715

    # Extract time
    hours_float = f * 24.0
    hour = int(hours_float)
    minutes_float = (hours_float - hour) * 60.0
    minute = int(minutes_float)
    second = int((minutes_float - minute) * 60.0)

    return datetime(year, month, day, hour, minute, second, tzinfo=timezone.utc)


def gps_week_from_mjd(mjd: float) -> tuple[int, int]:
    """Calculate GPS week and day of week from MJD.

    Args:
        mjd: Modified Julian Date

    Returns:
        Tuple of (GPS week, day of week) where Sunday=0
    """
    days_since_epoch = mjd - GPS_EPOCH_MJD
    gps_week = int(days_since_epoch / 7)
    day_of_week = int(days_since_epoch) % 7
    return gps_week, day_of_week


def mjd_from_gps_week(gps_week: int, day_of_week: int, seconds: float = 0.0) -> float:
    """Calculate MJD from GPS week and day of week.

    Args:
        gps_week: GPS week number
        day_of_week: Day of week (0=Sunday)
        seconds: Seconds into the day

    Returns:
        MJD as float
    """
    return GPS_EPOCH_MJD + gps_week * 7 + day_of_week + seconds / 86400.0


def doy_from_date(year: int, month: int, day: int) -> int:
    """Calculate day of year from calendar date.

    Args:
        year: Year
        month: Month (1-12)
        day: Day of month (1-31)

    Returns:
        Day of year (1-366)
    """
    dt = datetime(year, month, day)
    return dt.timetuple().tm_yday


def date_from_doy(year: int, doy: int) -> tuple[int, int]:
    """Convert year and DOY to month and day.

    Args:
        year: Year
        doy: Day of year (1-366)

    Returns:
        Tuple of (month, day)
    """
    dt = datetime(year, 1, 1) + timedelta(days=doy - 1)
    return dt.month, dt.day


def hour_to_alpha(hour: int) -> str:
    """Convert hour (0-23) to alpha character (a-x).

    Args:
        hour: Hour (0-23)

    Returns:
        Single character 'a'-'x'
    """
    if not 0 <= hour <= 23:
        raise ValueError(f"Hour must be 0-23, got {hour}")
    return chr(ord("a") + hour)


def alpha_to_hour(alpha: str) -> int:
    """Convert alpha character to hour.

    Args:
        alpha: Single character 'a'-'x'

    Returns:
        Hour (0-23)
    """
    if len(alpha) != 1 or not "a" <= alpha.lower() <= "x":
        raise ValueError(f"Alpha must be a-x, got {alpha}")
    return ord(alpha.lower()) - ord("a")


@dataclass
class GNSSDate:
    """Unified GNSS date representation.

    Supports multiple time representations commonly used in GNSS:
    - Calendar (year, month, day, hour, minute, second)
    - Modified Julian Date (MJD)
    - GPS Week and Day of Week
    - Year and Day of Year (DOY)
    """

    year: int
    month: int
    day: int
    hour: int = 0
    minute: int = 0
    second: float = 0.0

    # Class constants
    GPS_EPOCH_MJD: ClassVar[float] = 44244.0

    def __post_init__(self) -> None:
        """Validate date components."""
        if not 1970 <= self.year <= 2100:
            raise ValueError(f"Year {self.year} out of range")
        if not 1 <= self.month <= 12:
            raise ValueError(f"Month {self.month} out of range")
        if not 1 <= self.day <= 31:
            raise ValueError(f"Day {self.day} out of range")
        if not 0 <= self.hour <= 23:
            raise ValueError(f"Hour {self.hour} out of range")
        if not 0 <= self.minute <= 59:
            raise ValueError(f"Minute {self.minute} out of range")
        if not 0 <= self.second < 60:
            raise ValueError(f"Second {self.second} out of range")

    @property
    def mjd(self) -> float:
        """Get Modified Julian Date."""
        return mjd_from_date(
            self.year, self.month, self.day,
            self.hour, self.minute, self.second
        )

    @property
    def gps_week(self) -> int:
        """Get GPS week number."""
        week, _ = gps_week_from_mjd(self.mjd)
        return week

    @property
    def day_of_week(self) -> int:
        """Get day of week (0=Sunday)."""
        _, dow = gps_week_from_mjd(self.mjd)
        return dow

    @property
    def doy(self) -> int:
        """Get day of year (1-366)."""
        return doy_from_date(self.year, self.month, self.day)

    @property
    def hour_alpha(self) -> str:
        """Get hour as alpha character (a-x)."""
        return hour_to_alpha(self.hour)

    @property
    def datetime(self) -> datetime:
        """Get as datetime object."""
        return datetime(
            self.year, self.month, self.day,
            self.hour, self.minute, int(self.second),
            tzinfo=timezone.utc
        )

    @property
    def yyddd(self) -> str:
        """Get as YYDDD format (2-digit year + DOY)."""
        return f"{self.year % 100:02d}{self.doy:03d}"

    @property
    def yyyyddd(self) -> str:
        """Get as YYYYDDD format (4-digit year + DOY)."""
        return f"{self.year:04d}{self.doy:03d}"

    @classmethod
    def from_mjd(cls, mjd: float) -> GNSSDate:
        """Create from Modified Julian Date."""
        dt = date_from_mjd(mjd)
        return cls(
            dt.year, dt.month, dt.day,
            dt.hour, dt.minute, dt.second
        )

    @classmethod
    def from_gps_week(
        cls,
        gps_week: int,
        day_of_week: int,
        hour: int = 0,
    ) -> GNSSDate:
        """Create from GPS week and day of week."""
        mjd = mjd_from_gps_week(gps_week, day_of_week, hour * 3600.0)
        return cls.from_mjd(mjd)

    @classmethod
    def from_doy(cls, year: int, doy: int, hour: int = 0) -> GNSSDate:
        """Create from year and day of year."""
        month, day = date_from_doy(year, doy)
        return cls(year, month, day, hour)

    @classmethod
    def from_datetime(cls, dt: datetime) -> GNSSDate:
        """Create from datetime object."""
        return cls(
            dt.year, dt.month, dt.day,
            dt.hour, dt.minute, float(dt.second)
        )

    @classmethod
    def now(cls) -> GNSSDate:
        """Create for current UTC time."""
        return cls.from_datetime(datetime.now(timezone.utc))

    def add_hours(self, hours: int) -> GNSSDate:
        """Return new GNSSDate with hours added."""
        new_dt = self.datetime + timedelta(hours=hours)
        return GNSSDate.from_datetime(new_dt)

    def add_days(self, days: int) -> GNSSDate:
        """Return new GNSSDate with days added."""
        new_dt = self.datetime + timedelta(days=days)
        return GNSSDate.from_datetime(new_dt)

    def __str__(self) -> str:
        """String representation."""
        return f"{self.year:04d}-{self.month:02d}-{self.day:02d} {self.hour:02d}:{self.minute:02d}"

    def __repr__(self) -> str:
        """Detailed representation."""
        return (
            f"GNSSDate({self.year}, {self.month}, {self.day}, "
            f"{self.hour}, {self.minute}, {self.second})"
        )
