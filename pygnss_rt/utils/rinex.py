"""
RINEX filename parsing utilities.

Provides functions to extract metadata from various RINEX and
GNSS product filename conventions.

Replaces relevant parts of Perl UTIL.pm module.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from pygnss_rt.utils.format import year_2c_to_4c, alpha_to_hour, alpha_to_subhour


@dataclass
class RINEXFileInfo:
    """Information extracted from RINEX filename."""

    station: str
    year: int
    doy: int
    hour: Optional[int] = None
    minute: Optional[int] = None
    session: Optional[str] = None
    file_type: Optional[str] = None  # 'o' for obs, 'n' for nav, etc.
    compression: Optional[str] = None
    rinex_version: int = 2  # 2 or 3


@dataclass
class OrbitFileInfo:
    """Information extracted from orbit product filename."""

    provider: str
    gps_week: int
    day_of_week: int
    hour: Optional[int] = None
    doy: Optional[int] = None
    product_type: Optional[str] = None  # 'sp3', 'clk', etc.


@dataclass
class ERPFileInfo:
    """Information extracted from ERP product filename."""

    provider: str
    gps_week: int
    day_of_week: int
    hour: Optional[int] = None


@dataclass
class DCBFileInfo:
    """Information extracted from DCB filename."""

    year_2c: int
    month: int
    year_4c: int
    mjd: float


@dataclass
class BIAFileInfo:
    """Information extracted from BIA (bias) filename."""

    provider: str
    gps_week: int
    day_of_week: int
    doy: int
    hour: int


def parse_rinex2_filename(filename: str) -> RINEXFileInfo:
    """Parse RINEX 2.x filename format.

    Format: ssssdddf.yyt[.Z|.gz]
    where:
        ssss = 4-char station code
        ddd  = day of year
        f    = file sequence (0, a-x for hourly)
        yy   = 2-digit year
        t    = file type (o=obs, n=nav, m=met)

    Args:
        filename: RINEX filename

    Returns:
        RINEXFileInfo with extracted data
    """
    base = Path(filename).name

    # Remove compression extensions
    compression = None
    if base.endswith('.Z'):
        compression = 'Z'
        base = base[:-2]
    elif base.endswith('.gz'):
        compression = 'gz'
        base = base[:-3]

    # Parse: ssssdddf.yyt
    parts = base.split('.')
    if len(parts) < 2:
        raise ValueError(f"Invalid RINEX 2 filename: {filename}")

    name_part = parts[0]
    ext_part = parts[1]

    station = name_part[0:4].lower()
    doy = int(name_part[4:7])
    session = name_part[7:8] if len(name_part) > 7 else '0'

    # Parse hour from session character
    hour = None
    if session != '0' and session.isalpha():
        hour = alpha_to_hour(session)

    year_2c = int(ext_part[0:2])
    year = year_2c_to_4c(year_2c)
    file_type = ext_part[2:3] if len(ext_part) > 2 else None

    return RINEXFileInfo(
        station=station,
        year=year,
        doy=doy,
        hour=hour,
        session=session,
        file_type=file_type,
        compression=compression,
        rinex_version=2,
    )


def parse_rinex3_filename(filename: str) -> RINEXFileInfo:
    """Parse RINEX 3.x/4.x long filename format.

    Format: XXXXMRCCC_K_YYYYDDDHHMM_01H_30S_MO.rnx[.gz]
    where:
        XXXX = 4-char station code
        M    = monument/marker number
        R    = receiver number
        CCC  = country code
        K    = data source (R=receiver, S=stream, U=unknown)
        YYYY = year
        DDD  = day of year
        HH   = hour
        MM   = minute
        01H  = duration
        30S  = sample interval
        MO   = observation type

    Args:
        filename: RINEX filename

    Returns:
        RINEXFileInfo with extracted data
    """
    base = Path(filename).name

    # Remove compression extensions
    compression = None
    for ext in ['.gz', '.Z', '.zip']:
        if base.lower().endswith(ext):
            compression = ext[1:]
            base = base[:-len(ext)]
            break

    # Remove .rnx or .crx extension
    if base.lower().endswith('.rnx') or base.lower().endswith('.crx'):
        file_type = base[-3:-2]  # 'r' or 'c'
        base = base[:-4]

    # Parse underscore-separated parts
    parts = base.split('_')
    if len(parts) < 4:
        raise ValueError(f"Invalid RINEX 3 filename: {filename}")

    # Station info: XXXXMRCCC
    station_part = parts[0]
    station = station_part[0:4].lower()

    # Skip data source (parts[1])

    # Date/time: YYYYDDDHHMM
    datetime_part = parts[2]
    year = int(datetime_part[0:4])
    doy = int(datetime_part[4:7])
    hour = int(datetime_part[7:9]) if len(datetime_part) > 7 else 0
    minute = int(datetime_part[9:11]) if len(datetime_part) > 9 else 0

    return RINEXFileInfo(
        station=station,
        year=year,
        doy=doy,
        hour=hour,
        minute=minute,
        compression=compression,
        rinex_version=3,
    )


def parse_rinex_filename(filename: str) -> RINEXFileInfo:
    """Parse RINEX filename (auto-detect version).

    Args:
        filename: RINEX filename

    Returns:
        RINEXFileInfo with extracted data
    """
    base = Path(filename).name

    # RINEX 3 long filenames contain underscores and are > 30 chars
    if '_' in base and len(base) > 30:
        return parse_rinex3_filename(filename)
    else:
        return parse_rinex2_filename(filename)


def parse_orbit_filename(filename: str) -> OrbitFileInfo:
    """Parse orbit product filename.

    Supports formats:
    - Legacy: igsWWWWD.sp3.Z (igs=provider, WWWW=week, D=dow)
    - IGS long: IGS0OPSFIN_YYYYDDD0000_01D_15M_ORB.SP3.gz

    Args:
        filename: Orbit filename

    Returns:
        OrbitFileInfo with extracted data
    """
    base = Path(filename).name

    # Remove extensions
    for ext in ['.Z', '.gz', '.sp3', '.SP3', '.eph', '.EPH']:
        if base.endswith(ext):
            base = base[:-len(ext)]

    # Check for IGS long format
    if '_' in base and len(base) > 20:
        # IGS0OPSFIN_YYYYDDD0000_01D_15M_ORB
        parts = base.split('_')
        provider = parts[0][:3]

        datetime_part = parts[1]
        year = int(datetime_part[0:4])
        doy = int(datetime_part[4:7])
        hour = int(datetime_part[7:9]) if len(datetime_part) > 9 else 0

        # Calculate GPS week and day of week
        from pygnss_rt.utils.dates import GNSSDate
        gd = GNSSDate.from_doy(year, doy, hour)

        return OrbitFileInfo(
            provider=provider,
            gps_week=gd.gps_week,
            day_of_week=gd.day_of_week,
            hour=hour,
            doy=doy,
        )
    else:
        # Legacy format: igsWWWWD or igsWWWWD_HH
        provider = base[0:3]
        gps_week = int(base[3:7])
        day_of_week = int(base[7:8])

        hour = None
        if len(base) > 8 and base[8] == '_':
            hour = int(base[9:11])

        return OrbitFileInfo(
            provider=provider,
            gps_week=gps_week,
            day_of_week=day_of_week,
            hour=hour,
        )


def parse_erp_filename(filename: str) -> ERPFileInfo:
    """Parse ERP product filename.

    Supports formats:
    - Legacy: igsWWWW7.erp.Z
    - IGS long: IGS0OPSFIN_YYYYDDD0000_01D_01D_ERP.ERP.gz

    Args:
        filename: ERP filename

    Returns:
        ERPFileInfo with extracted data
    """
    base = Path(filename).name

    # Remove extensions
    for ext in ['.Z', '.gz', '.erp', '.ERP']:
        if base.endswith(ext):
            base = base[:-len(ext)]

    # Check for IGS long format
    if '_' in base and len(base) > 20:
        parts = base.split('_')
        provider = parts[0][:3]

        datetime_part = parts[1]
        year = int(datetime_part[0:4])
        doy = int(datetime_part[4:7])
        hour = int(datetime_part[7:9]) if len(datetime_part) > 9 else 0

        from pygnss_rt.utils.dates import GNSSDate
        gd = GNSSDate.from_doy(year, doy, hour)

        return ERPFileInfo(
            provider=provider,
            gps_week=gd.gps_week,
            day_of_week=gd.day_of_week,
            hour=hour,
        )
    else:
        # Legacy format
        provider = base[0:3]
        gps_week = int(base[3:7])
        day_of_week = int(base[7:8])

        return ERPFileInfo(
            provider=provider,
            gps_week=gps_week,
            day_of_week=day_of_week,
        )


def parse_dcb_filename(filename: str) -> DCBFileInfo:
    """Parse DCB (Differential Code Bias) filename.

    Format: P1C1YYMM.DCB.Z

    Args:
        filename: DCB filename

    Returns:
        DCBFileInfo with extracted data
    """
    base = Path(filename).name

    # Remove extensions
    for ext in ['.Z', '.gz', '.DCB', '.dcb']:
        if base.endswith(ext):
            base = base[:-len(ext)]

    # Parse P1C1YYMM
    year_2c = int(base[4:6])
    month = int(base[6:8])
    year = year_2c_to_4c(year_2c)

    from pygnss_rt.utils.dates import mjd_from_date
    mjd = mjd_from_date(year, month, 1)

    return DCBFileInfo(
        year_2c=year_2c,
        month=month,
        year_4c=year,
        mjd=mjd,
    )


def parse_bia_filename(filename: str) -> BIAFileInfo:
    """Parse BIA (bias) filename.

    Format: CAS0MGXRAP_YYYYDDDHH00_01D_01D_OSB.BIA.gz

    Args:
        filename: BIA filename

    Returns:
        BIAFileInfo with extracted data
    """
    base = Path(filename).name

    # Remove extensions
    for ext in ['.Z', '.gz', '.BIA', '.bia']:
        if base.endswith(ext):
            base = base[:-len(ext)]

    # Parse underscore-separated parts
    parts = base.split('_')
    provider = parts[0][:3] if parts else "UNK"

    if len(parts) > 1:
        datetime_part = parts[1]
        year = int(datetime_part[0:4])
        doy = int(datetime_part[4:7])
        hour = int(datetime_part[7:9]) if len(datetime_part) > 9 else 0

        from pygnss_rt.utils.dates import GNSSDate
        gd = GNSSDate.from_doy(year, doy, hour)

        return BIAFileInfo(
            provider=provider,
            gps_week=gd.gps_week,
            day_of_week=gd.day_of_week,
            doy=doy,
            hour=hour,
        )
    else:
        raise ValueError(f"Invalid BIA filename: {filename}")


def build_rinex2_filename(
    station: str,
    year: int,
    doy: int,
    hour: Optional[int] = None,
    file_type: str = 'o',
    compression: Optional[str] = None,
) -> str:
    """Build RINEX 2.x filename.

    Args:
        station: 4-char station code
        year: Year
        doy: Day of year
        hour: Hour (0-23) for hourly files, None for daily
        file_type: File type ('o', 'n', 'm')
        compression: Compression extension ('Z', 'gz', or None)

    Returns:
        Formatted filename
    """
    from pygnss_rt.utils.format import hour_to_alpha

    station = station.lower()[:4]
    year_2c = year % 100

    if hour is not None:
        session = hour_to_alpha(hour)
    else:
        session = '0'

    filename = f"{station}{doy:03d}{session}.{year_2c:02d}{file_type}"

    if compression:
        filename += f".{compression}"

    return filename


def build_orbit_filename(
    provider: str,
    gps_week: int,
    day_of_week: int,
    tier: str = 'final',
    extension: str = 'sp3',
    compression: str = 'Z',
) -> str:
    """Build orbit product filename.

    Args:
        provider: Provider code (IGS, CODE, etc.)
        gps_week: GPS week number
        day_of_week: Day of week (0-6)
        tier: Product tier (final, rapid, ultra)
        extension: File extension
        compression: Compression extension

    Returns:
        Formatted filename
    """
    prefix_map = {
        ('IGS', 'final'): 'igs',
        ('IGS', 'rapid'): 'igr',
        ('IGS', 'ultra'): 'igu',
        ('CODE', 'final'): 'COD',
        ('CODE', 'rapid'): 'COR',
    }

    prefix = prefix_map.get((provider.upper(), tier.lower()), provider.lower()[:3])

    filename = f"{prefix}{gps_week:04d}{day_of_week}.{extension}"

    if compression:
        filename += f".{compression}"

    return filename


# =============================================================================
# Additional Filename Parsers (from UTIL.pm)
# =============================================================================

@dataclass
class IONFileInfo:
    """Information extracted from ionosphere product filename."""

    provider: str
    gps_week: int
    day_of_week: int
    doy: Optional[int] = None
    hour: int = 0


@dataclass
class MetFileInfo:
    """Information extracted from meteorological filename."""

    year: int
    year_2c: int
    doy: int
    month: int
    day: int
    hour: int  # or minute for subhourly
    compression: Optional[str] = None
    is_subhourly: bool = False


def parse_ion_filename(filename: str) -> IONFileInfo:
    """Parse ionosphere product filename.

    Supports formats:
    - Legacy: igsWWWWD.ion.Z
    - IGS long: IGS0OPSFIN_YYYYDDD0000_01D_02H_ION.ION.gz

    Args:
        filename: Ionosphere filename

    Returns:
        IONFileInfo with extracted data
    """
    base = Path(filename).name

    # Remove extensions
    for ext in ['.Z', '.gz', '.ion', '.ION', '.inx', '.INX']:
        if base.endswith(ext):
            base = base[:-len(ext)]

    # Check for IGS long format
    if '_' in base and len(base) > 20:
        parts = base.split('_')
        provider = parts[0][:11] if len(parts[0]) >= 11 else parts[0]

        datetime_part = parts[1]
        year = int(datetime_part[0:4])
        doy = int(datetime_part[4:7])
        hour = int(datetime_part[7:9]) if len(datetime_part) > 9 else 0

        from pygnss_rt.utils.dates import GNSSDate
        gd = GNSSDate.from_doy(year, doy, hour)

        return IONFileInfo(
            provider=provider,
            gps_week=gd.gps_week,
            day_of_week=gd.day_of_week,
            doy=doy,
            hour=hour,
        )
    else:
        # Legacy format: igsWWWWD or codWWWWD
        provider = base[0:3]
        gps_week = int(base[3:7])
        day_of_week = int(base[7:8])

        return IONFileInfo(
            provider=provider,
            gps_week=gps_week,
            day_of_week=day_of_week,
        )


def parse_met_filename(filename: str, subhourly: bool = False) -> MetFileInfo:
    """Parse meteorological data filename.

    Format: metYYYYMMDDHH.gz (hourly)
            metYYYYMMDDHHMM.gz (subhourly)

    Args:
        filename: Meteorological filename
        subhourly: Whether to parse subhourly format

    Returns:
        MetFileInfo with extracted data
    """
    base = Path(filename).name

    # Remove 'met' prefix
    if base.lower().startswith('met'):
        base = base[3:]

    # Remove compression extension
    compression = None
    for ext in ['.gz', '.Z', '.zip']:
        if base.endswith(ext):
            compression = ext[1:]
            base = base[:-len(ext)]
            break

    # Parse date/time
    year = int(base[0:4])
    month = int(base[4:6])
    day = int(base[6:8])

    if subhourly:
        # HHMM format
        time_str = base[8:12]
        hour = int(time_str) if len(time_str) == 4 else int(time_str[:2])
    else:
        # HH format
        hour = int(base[8:10]) if len(base) >= 10 else 0

    # Calculate DOY
    from pygnss_rt.utils.dates import doy_from_date
    doy = doy_from_date(year, month, day)

    return MetFileInfo(
        year=year,
        year_2c=year % 100,
        doy=doy,
        month=month,
        day=day,
        hour=hour,
        compression=compression,
        is_subhourly=subhourly,
    )


def build_rinex3_filename(
    station: str,
    year: int,
    doy: int,
    hour: int = 0,
    minute: int = 0,
    duration: str = "01D",
    interval: str = "30S",
    data_type: str = "MO",
    country: str = "XXX",
    source: str = "R",
    monument: int = 0,
    receiver: int = 0,
    file_ext: str = "rnx",
    compression: Optional[str] = "gz",
) -> str:
    """Build RINEX 3.x long filename.

    Format: XXXXMRCCC_K_YYYYDDDHHMM_DUR_INT_TYP.ext[.gz]

    Args:
        station: 4-char station code
        year: Year
        doy: Day of year
        hour: Hour
        minute: Minute
        duration: Duration code (01D, 01H, 15M)
        interval: Sample interval (30S, 01S, 15M)
        data_type: Data type (MO=obs, MN=nav, MM=met)
        country: 3-char country code
        source: Data source (R=receiver, S=stream, U=unknown)
        monument: Monument number
        receiver: Receiver number
        file_ext: File extension (rnx, crx)
        compression: Compression extension

    Returns:
        Formatted filename
    """
    station = station.upper()[:4]

    filename = (
        f"{station}{monument}{receiver}{country}_"
        f"{source}_"
        f"{year:04d}{doy:03d}{hour:02d}{minute:02d}_"
        f"{duration}_{interval}_{data_type}.{file_ext}"
    )

    if compression:
        filename += f".{compression}"

    return filename


def build_met_filename(
    year: int,
    month: int,
    day: int,
    hour: int,
    minute: Optional[int] = None,
    compression: Optional[str] = "gz",
) -> str:
    """Build meteorological data filename.

    Args:
        year: Year
        month: Month
        day: Day
        hour: Hour
        minute: Minute (for subhourly)
        compression: Compression extension

    Returns:
        Formatted filename
    """
    if minute is not None:
        # Subhourly format
        filename = f"met{year:04d}{month:02d}{day:02d}{hour:02d}{minute:02d}"
    else:
        # Hourly format
        filename = f"met{year:04d}{month:02d}{day:02d}{hour:02d}"

    if compression:
        filename += f".{compression}"

    return filename


def detect_rinex_version(filename: str) -> int:
    """Detect RINEX version from filename.

    Args:
        filename: RINEX filename

    Returns:
        RINEX version (2, 3, or 4)
    """
    base = Path(filename).name

    # RINEX 3/4 long format
    if '_' in base and len(base) > 30:
        # Check for RINEX 4 indicators
        if base.lower().endswith('.rnx') or base.lower().endswith('.crx'):
            return 3  # Could be 4, but format is same
        return 3

    # RINEX 2 short format
    return 2


def get_file_type(filename: str) -> str:
    """Get RINEX file type from filename.

    Args:
        filename: RINEX filename

    Returns:
        File type code (O=obs, N=nav, M=met, etc.)
    """
    base = Path(filename).name.lower()

    # Remove compression
    for ext in ['.gz', '.z', '.zip', '.bz2']:
        if base.endswith(ext):
            base = base[:-len(ext)]

    # Check RINEX 3 style
    if '_mo.' in base or base.endswith('_mo'):
        return 'O'
    elif '_mn.' in base or base.endswith('_mn'):
        return 'N'
    elif '_mm.' in base or base.endswith('_mm'):
        return 'M'

    # Check RINEX 2 style extension
    parts = base.split('.')
    if len(parts) >= 2:
        ext = parts[-1]
        if len(ext) >= 3:
            type_char = ext[2].upper()
            if type_char in 'ONMG':
                return type_char

    return 'O'  # Default to observation


def is_observation_file(filename: str) -> bool:
    """Check if filename is an observation file.

    Args:
        filename: RINEX filename

    Returns:
        True if observation file
    """
    return get_file_type(filename) == 'O'


def is_navigation_file(filename: str) -> bool:
    """Check if filename is a navigation file.

    Args:
        filename: RINEX filename

    Returns:
        True if navigation file
    """
    return get_file_type(filename) in ('N', 'G')


def is_meteorological_file(filename: str) -> bool:
    """Check if filename is a meteorological file.

    Args:
        filename: RINEX filename

    Returns:
        True if meteorological file
    """
    return get_file_type(filename) == 'M'
