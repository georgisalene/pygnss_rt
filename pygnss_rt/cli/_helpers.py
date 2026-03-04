"""
Shared helper functions for CLI commands.
"""

from __future__ import annotations

import click


def parse_date(date_str: str) -> "GNSSDate":
    """Parse date string to GNSSDate.

    Supports formats:
    - YYYY-MM-DD
    - YYYY/DOY
    - YYYYDOY

    Args:
        date_str: Date string to parse

    Returns:
        GNSSDate object

    Raises:
        click.BadParameter: If date format is invalid
    """
    from pygnss_rt.utils.dates import GNSSDate

    # Try YYYY-MM-DD
    if "-" in date_str:
        parts = date_str.split("-")
        if len(parts) == 3:
            return GNSSDate(int(parts[0]), int(parts[1]), int(parts[2]))

    # Try YYYY/DOY
    if "/" in date_str:
        parts = date_str.split("/")
        if len(parts) == 2:
            return GNSSDate.from_doy(int(parts[0]), int(parts[1]))

    # Try YYYYDOY
    if len(date_str) == 7 and date_str.isdigit():
        return GNSSDate.from_doy(int(date_str[:4]), int(date_str[4:]))

    raise click.BadParameter(f"Invalid date format: {date_str}")
