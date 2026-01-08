"""Tests for date/time utilities."""

import pytest
from datetime import datetime, timezone

from pygnss_rt.utils.dates import (
    GNSSDate,
    mjd_from_date,
    date_from_mjd,
    gps_week_from_mjd,
    mjd_from_gps_week,
    doy_from_date,
    hour_to_alpha,
    alpha_to_hour,
)


class TestMJDConversions:
    """Test MJD conversion functions."""

    def test_mjd_from_known_date(self):
        """Test MJD calculation for known dates."""
        # January 1, 2000 00:00:00 UTC = MJD 51544
        mjd = mjd_from_date(2000, 1, 1)
        assert abs(mjd - 51544.0) < 0.001

    def test_date_from_mjd(self):
        """Test converting MJD back to date."""
        mjd = 51544.0  # Jan 1, 2000
        dt = date_from_mjd(mjd)
        assert dt.year == 2000
        assert dt.month == 1
        assert dt.day == 1

    def test_mjd_roundtrip(self):
        """Test MJD conversion roundtrip."""
        original = datetime(2024, 6, 15, 12, 30, 45, tzinfo=timezone.utc)
        mjd = mjd_from_date(
            original.year, original.month, original.day,
            original.hour, original.minute, original.second
        )
        result = date_from_mjd(mjd)

        assert result.year == original.year
        assert result.month == original.month
        assert result.day == original.day
        assert result.hour == original.hour


class TestGPSWeek:
    """Test GPS week calculations."""

    def test_gps_week_epoch(self):
        """Test GPS week at GPS epoch."""
        # GPS epoch: Jan 6, 1980 = MJD 44244
        week, dow = gps_week_from_mjd(44244.0)
        assert week == 0
        assert dow == 0

    def test_gps_week_known_date(self):
        """Test GPS week for known date."""
        # Jan 1, 2024 is GPS week 2295, dow 1 (Monday)
        mjd = mjd_from_date(2024, 1, 1)
        week, dow = gps_week_from_mjd(mjd)
        assert week == 2295
        assert dow == 1

    def test_mjd_from_gps_week(self):
        """Test converting GPS week back to MJD."""
        mjd = mjd_from_gps_week(2295, 1)
        dt = date_from_mjd(mjd)
        assert dt.year == 2024
        assert dt.month == 1
        assert dt.day == 1


class TestGNSSDate:
    """Test GNSSDate class."""

    def test_create_from_calendar(self):
        """Test creating GNSSDate from calendar date."""
        date = GNSSDate(2024, 6, 15, 12, 30, 0)

        assert date.year == 2024
        assert date.month == 6
        assert date.day == 15
        assert date.hour == 12
        assert date.minute == 30

    def test_from_mjd(self):
        """Test creating GNSSDate from MJD."""
        date = GNSSDate.from_mjd(51544.5)  # Jan 1, 2000 12:00

        assert date.year == 2000
        assert date.month == 1
        assert date.day == 1
        assert date.hour == 12

    def test_from_gps_week(self):
        """Test creating GNSSDate from GPS week."""
        date = GNSSDate.from_gps_week(2295, 1, 12)  # Jan 1, 2024 12:00

        assert date.year == 2024
        assert date.month == 1
        assert date.day == 1
        assert date.hour == 12

    def test_from_doy(self):
        """Test creating GNSSDate from year and DOY."""
        date = GNSSDate.from_doy(2024, 1)  # Jan 1, 2024

        assert date.year == 2024
        assert date.month == 1
        assert date.day == 1
        assert date.doy == 1

    def test_add_hours(self):
        """Test adding hours to GNSSDate."""
        date = GNSSDate(2024, 1, 1, 23)
        new_date = date.add_hours(2)

        assert new_date.year == 2024
        assert new_date.month == 1
        assert new_date.day == 2
        assert new_date.hour == 1

    def test_add_days(self):
        """Test adding days to GNSSDate."""
        date = GNSSDate(2024, 12, 31)
        new_date = date.add_days(1)

        assert new_date.year == 2025
        assert new_date.month == 1
        assert new_date.day == 1

    def test_hour_alpha(self):
        """Test hour to alpha conversion."""
        date = GNSSDate(2024, 1, 1, 0)
        assert date.hour_alpha == "a"

        date = GNSSDate(2024, 1, 1, 23)
        assert date.hour_alpha == "x"


class TestHourAlpha:
    """Test hour to alpha conversion functions."""

    def test_hour_to_alpha(self):
        """Test hour to alpha conversion."""
        assert hour_to_alpha(0) == "a"
        assert hour_to_alpha(12) == "m"
        assert hour_to_alpha(23) == "x"

    def test_alpha_to_hour(self):
        """Test alpha to hour conversion."""
        assert alpha_to_hour("a") == 0
        assert alpha_to_hour("m") == 12
        assert alpha_to_hour("x") == 23

    def test_roundtrip(self):
        """Test hour/alpha conversion roundtrip."""
        for hour in range(24):
            alpha = hour_to_alpha(hour)
            result = alpha_to_hour(alpha)
            assert result == hour


class TestDOY:
    """Test day of year calculations."""

    def test_doy_jan_1(self):
        """Test DOY for January 1."""
        assert doy_from_date(2024, 1, 1) == 1

    def test_doy_dec_31_leap(self):
        """Test DOY for December 31 in leap year."""
        assert doy_from_date(2024, 12, 31) == 366

    def test_doy_dec_31_non_leap(self):
        """Test DOY for December 31 in non-leap year."""
        assert doy_from_date(2023, 12, 31) == 365
