"""
Database Maintenance Callers.

Replaces Perl call_*_maintain.pl scripts:
- call_HD_maintain.pl / call_HD_maintain_BSW54.pl
- call_SD_maintain.pl
- call_MET_maintain.pl
- call_SM_maintain.pl

These callers orchestrate database maintenance operations:
1. Collect station lists from all network XML files
2. Create tables for new stations
3. Add new hourly/daily entries (maintain)
4. Fill gaps from service interruptions
5. Mark old entries as "Too Late"

Usage:
    from pygnss_rt.database.maintenance import (
        HDMaintenanceCaller,
        run_hd_maintenance,
    )

    # Run hourly data maintenance
    caller = HDMaintenanceCaller()
    stats = caller.run()

    # Or use convenience function
    stats = run_hd_maintenance(verbose=True)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any

from pygnss_rt.database.connection import DatabaseManager
from pygnss_rt.database.hourly_data import HourlyDataManager, HDStatistics
from pygnss_rt.database.daily_data import DailyDataManager, SDStatistics
from pygnss_rt.database.subhourly_met import SubhourlyMetManager, SMStatistics
from pygnss_rt.stations.station import StationManager
from pygnss_rt.utils.dates import GNSSDate
from pygnss_rt.utils.logging import get_logger, IGNSSPrinter


logger = get_logger(__name__)


class MaintenanceType(str, Enum):
    """Types of database maintenance."""

    HOURLY_DATA = "HD"      # Hourly RINEX data
    DAILY_DATA = "SD"       # Sub-hourly (15-min) RINEX data
    HOURLY_MET = "MET"      # Hourly meteorological data
    SUBHOURLY_MET = "SM"    # Sub-hourly (15-min) MET data


@dataclass
class MaintenanceConfig:
    """Configuration for maintenance operations."""

    # Networks to include
    networks: list[str] = field(default_factory=lambda: [
        "IGS", "EUREF", "OSGB", "RGP", "NRCAN",
        "IRISH", "ICELANDIC", "SCIENTIFIC", "SUPERSITE",
    ])

    # Only include NRT stations
    use_nrt: bool = True

    # Latency settings for "too late" marking
    late_day: int = 0
    late_hour: int = 1
    late_15min: int = 2

    # Gap filling settings
    fill_gap_late_day: int = 0
    fill_gap_late_hour: int = 2

    # Provider-specific latency overrides
    provider_latency: dict[str, tuple[int, int]] = field(default_factory=lambda: {
        "MO": (0, 1),
        "IESSG": (0, 1),
        "IGS": (0, 1),
        "EUREF": (0, 1),
    })

    verbose: bool = False


@dataclass
class MaintenanceStats:
    """Statistics from maintenance operation."""

    maintenance_type: MaintenanceType
    start_time: datetime
    end_time: datetime
    stations_processed: int
    tables_created: int
    entries_added: int
    gaps_filled: int
    marked_too_late: int
    errors: list[str] = field(default_factory=list)

    @property
    def runtime_seconds(self) -> float:
        return (self.end_time - self.start_time).total_seconds()

    def __str__(self) -> str:
        return (
            f"{self.maintenance_type.value} Maintenance:\n"
            f"  Runtime: {self.runtime_seconds:.1f}s\n"
            f"  Stations: {self.stations_processed}\n"
            f"  Tables created: {self.tables_created}\n"
            f"  Entries added: {self.entries_added}\n"
            f"  Gaps filled: {self.gaps_filled}\n"
            f"  Marked too late: {self.marked_too_late}\n"
            f"  Errors: {len(self.errors)}"
        )


class HDMaintenanceCaller:
    """Hourly Data maintenance caller.

    Replaces Perl call_HD_maintain.pl and call_HD_maintain_BSW54.pl.

    Performs the following operations for each station:
    1. Creates HD table if it doesn't exist
    2. Adds new hourly entries (maintain)
    3. Fills gaps from service interruptions
    4. Marks old waiting entries as "Too Late"

    The stations are collected from multiple network XML configuration files
    and combined into a unique list.
    """

    def __init__(
        self,
        config: MaintenanceConfig | None = None,
        db_manager: DatabaseManager | None = None,
    ):
        """Initialize HD maintenance caller.

        Args:
            config: Maintenance configuration
            db_manager: Database manager (created if None)
        """
        self.config = config or MaintenanceConfig()
        self.db = db_manager or DatabaseManager()
        self.hd_manager = HourlyDataManager(self.db)
        self.station_manager = StationManager(self.db)
        self.printer = IGNSSPrinter()

    def get_all_stations(self) -> list[str]:
        """Collect stations from all configured networks.

        Combines station lists from IGS, EUREF, OSGB, RGP, NRCAN,
        Ireland, Iceland, Scientific, Supersite networks.

        Returns:
            Sorted unique list of station IDs
        """
        all_stations: set[str] = set()

        for network in self.config.networks:
            try:
                stations = self.station_manager.get_stations_by_network(
                    network=network,
                    use_nrt=self.config.use_nrt,
                )
                all_stations.update(stations)
            except Exception as e:
                logger.warning(f"Failed to get stations for {network}: {e}")

        return sorted(all_stations)

    def get_provider_for_station(self, station_id: str) -> str | None:
        """Get data provider for a station.

        Checks each network to find the station's provider.

        Args:
            station_id: Station ID

        Returns:
            Provider name or None
        """
        for network in self.config.networks:
            try:
                provider = self.station_manager.get_station_provider(
                    station_id=station_id,
                    network=network,
                )
                if provider:
                    return provider
            except Exception:
                pass

        return None

    def get_latency_for_provider(self, provider: str | None) -> tuple[int, int]:
        """Get latency settings for a provider.

        Args:
            provider: Provider name

        Returns:
            Tuple of (late_day, late_hour)
        """
        if provider and provider in self.config.provider_latency:
            return self.config.provider_latency[provider]
        return (self.config.late_day, self.config.late_hour)

    def run(
        self,
        reference_date: GNSSDate | None = None,
    ) -> MaintenanceStats:
        """Run HD maintenance.

        Args:
            reference_date: Reference date/time (defaults to now)

        Returns:
            Maintenance statistics
        """
        start_time = datetime.utcnow()

        if reference_date is None:
            reference_date = GNSSDate.now()

        if self.config.verbose:
            self.printer.info("i-GNSS at work... Please wait!")

        logger.info("Starting HD maintenance")

        # Ensure table exists
        self.hd_manager.ensure_table()

        # Get all stations
        stations = self.get_all_stations()
        n_stations = len(stations)

        if self.config.verbose:
            print(f"\nMaintaining {n_stations} stations...")

        stats = MaintenanceStats(
            maintenance_type=MaintenanceType.HOURLY_DATA,
            start_time=start_time,
            end_time=start_time,
            stations_processed=n_stations,
            tables_created=0,
            entries_added=0,
            gaps_filled=0,
            marked_too_late=0,
        )

        # Process each station
        for i, station in enumerate(stations, 1):
            try:
                station_stats = self._process_station(
                    station, reference_date, i, n_stations
                )

                stats.entries_added += station_stats.get("entries_added", 0)
                stats.gaps_filled += station_stats.get("gaps_filled", 0)
                stats.marked_too_late += station_stats.get("marked_too_late", 0)

            except Exception as e:
                error_msg = f"Error processing {station}: {e}"
                logger.error(error_msg)
                stats.errors.append(error_msg)

        stats.end_time = datetime.utcnow()

        logger.info(
            "HD maintenance completed",
            stations=n_stations,
            entries_added=stats.entries_added,
            gaps_filled=stats.gaps_filled,
            too_late=stats.marked_too_late,
            runtime=stats.runtime_seconds,
        )

        if self.config.verbose:
            print(f"\n{stats}")

        return stats

    def _process_station(
        self,
        station_id: str,
        reference_date: GNSSDate,
        index: int,
        total: int,
    ) -> dict[str, int]:
        """Process maintenance for a single station.

        Args:
            station_id: Station ID
            reference_date: Reference date
            index: Current station index
            total: Total stations

        Returns:
            Dictionary with maintenance counts
        """
        if self.config.verbose:
            pct = index / total * 100
            print(f"Maintaining station {station_id:4s} {index:3d}/{total:3d} = {pct:4.1f}%")

        result = {
            "entries_added": 0,
            "gaps_filled": 0,
            "marked_too_late": 0,
        }

        # Add new hourly entries (maintain)
        added = self.hd_manager.maintain(
            station_ids=[station_id],
            reference_date=reference_date,
        )
        result["entries_added"] = added

        # Fill gaps
        gaps = self.hd_manager.fill_gap(
            station_ids=[station_id],
            late_day=self.config.fill_gap_late_day,
            late_hour=self.config.fill_gap_late_hour,
            reference_date=reference_date,
        )
        result["gaps_filled"] = gaps

        # Get provider-specific latency
        provider = self.get_provider_for_station(station_id)
        late_day, late_hour = self.get_latency_for_provider(provider)

        if provider is None and self.config.verbose:
            print(f"  WARNING: no provider defined for station {station_id}")

        # Mark too late files
        # Note: This is done globally, not per-station in the Python version
        # The Perl version did this per-station but it's more efficient globally

        return result

    def finalize(self) -> int:
        """Finalize maintenance by marking all too-late files.

        Called after all stations are processed to mark files as too late.

        Returns:
            Number of files marked as too late
        """
        return self.hd_manager.set_too_late_files(
            late_day=self.config.late_day,
            late_hour=self.config.late_hour,
        )


class SDMaintenanceCaller:
    """Sub-hourly (15-minute) RINEX data maintenance caller.

    Replaces Perl call_SD_maintain.pl.
    """

    def __init__(
        self,
        config: MaintenanceConfig | None = None,
        db_manager: DatabaseManager | None = None,
    ):
        """Initialize SD maintenance caller."""
        self.config = config or MaintenanceConfig()
        self.db = db_manager or DatabaseManager()
        self.sd_manager = DailyDataManager(self.db)
        self.station_manager = StationManager(self.db)
        self.printer = IGNSSPrinter()

    def get_all_stations(self) -> list[str]:
        """Collect stations from all configured networks."""
        all_stations: set[str] = set()

        for network in self.config.networks:
            try:
                stations = self.station_manager.get_stations_by_network(
                    network=network,
                    use_nrt=self.config.use_nrt,
                )
                all_stations.update(stations)
            except Exception as e:
                logger.warning(f"Failed to get stations for {network}: {e}")

        return sorted(all_stations)

    def run(
        self,
        reference_date: GNSSDate | None = None,
    ) -> MaintenanceStats:
        """Run SD maintenance."""
        start_time = datetime.utcnow()

        if reference_date is None:
            reference_date = GNSSDate.now()

        logger.info("Starting SD maintenance")

        # Ensure table exists
        self.sd_manager.ensure_table()

        stations = self.get_all_stations()
        n_stations = len(stations)

        stats = MaintenanceStats(
            maintenance_type=MaintenanceType.DAILY_DATA,
            start_time=start_time,
            end_time=start_time,
            stations_processed=n_stations,
            tables_created=0,
            entries_added=0,
            gaps_filled=0,
            marked_too_late=0,
        )

        # Maintain all stations
        added = self.sd_manager.maintain(
            station_ids=stations,
            reference_date=reference_date,
        )
        stats.entries_added = added

        # Fill gaps
        gaps = self.sd_manager.fill_gap(
            station_ids=stations,
            late_day=self.config.fill_gap_late_day,
            reference_date=reference_date,
        )
        stats.gaps_filled = gaps

        # Mark too late
        too_late = self.sd_manager.set_too_late_files(
            late_day=self.config.late_day,
        )
        stats.marked_too_late = too_late

        stats.end_time = datetime.utcnow()

        logger.info(
            "SD maintenance completed",
            stations=n_stations,
            entries_added=stats.entries_added,
            runtime=stats.runtime_seconds,
        )

        return stats


class SMMaintenanceCaller:
    """Sub-hourly MET (15-minute meteorological) maintenance caller.

    Replaces Perl call_SM_maintain.pl.
    """

    def __init__(
        self,
        config: MaintenanceConfig | None = None,
        db_manager: DatabaseManager | None = None,
    ):
        """Initialize SM maintenance caller."""
        self.config = config or MaintenanceConfig()
        self.db = db_manager or DatabaseManager()
        self.sm_manager = SubhourlyMetManager(self.db)
        self.printer = IGNSSPrinter()

    def run(
        self,
        reference_date: GNSSDate | None = None,
    ) -> MaintenanceStats:
        """Run SM maintenance."""
        start_time = datetime.utcnow()

        if reference_date is None:
            reference_date = GNSSDate.now()

        logger.info("Starting SM maintenance")

        stats = MaintenanceStats(
            maintenance_type=MaintenanceType.SUBHOURLY_MET,
            start_time=start_time,
            end_time=start_time,
            stations_processed=0,
            tables_created=0,
            entries_added=0,
            gaps_filled=0,
            marked_too_late=0,
        )

        # Maintain (add new entries)
        added = self.sm_manager.maintain(reference_date=reference_date)
        stats.entries_added = added

        # Fill gaps
        gaps = self.sm_manager.fill_gap(
            late_day=self.config.fill_gap_late_day,
            late_15min=self.config.late_15min,
            reference_date=reference_date,
        )
        stats.gaps_filled = gaps

        # Mark too late
        too_late = self.sm_manager.set_too_late_files(
            late_day=self.config.late_day,
            late_15min=self.config.late_15min,
        )
        stats.marked_too_late = too_late

        stats.end_time = datetime.utcnow()

        logger.info(
            "SM maintenance completed",
            entries_added=stats.entries_added,
            gaps_filled=stats.gaps_filled,
            too_late=stats.marked_too_late,
            runtime=stats.runtime_seconds,
        )

        return stats


# Convenience functions

def run_hd_maintenance(
    verbose: bool = False,
    late_day: int = 0,
    late_hour: int = 1,
) -> MaintenanceStats:
    """Run hourly data maintenance.

    Convenience function for HD maintenance.

    Args:
        verbose: Enable verbose output
        late_day: Days before marking as too late
        late_hour: Hours before marking as too late

    Returns:
        Maintenance statistics
    """
    config = MaintenanceConfig(
        verbose=verbose,
        late_day=late_day,
        late_hour=late_hour,
    )

    caller = HDMaintenanceCaller(config=config)
    stats = caller.run()

    # Finalize (mark too late globally)
    too_late = caller.finalize()
    stats.marked_too_late = too_late

    return stats


def run_sd_maintenance(
    verbose: bool = False,
    late_day: int = 3,
) -> MaintenanceStats:
    """Run sub-hourly RINEX data maintenance.

    Args:
        verbose: Enable verbose output
        late_day: Days before marking as too late

    Returns:
        Maintenance statistics
    """
    config = MaintenanceConfig(
        verbose=verbose,
        late_day=late_day,
    )

    caller = SDMaintenanceCaller(config=config)
    return caller.run()


def run_sm_maintenance(
    verbose: bool = False,
    late_day: int = 0,
    late_15min: int = 2,
) -> MaintenanceStats:
    """Run sub-hourly MET maintenance.

    Args:
        verbose: Enable verbose output
        late_day: Days before marking as too late
        late_15min: 15-min periods before marking as too late

    Returns:
        Maintenance statistics
    """
    config = MaintenanceConfig(
        verbose=verbose,
        late_day=late_day,
        late_15min=late_15min,
    )

    caller = SMMaintenanceCaller(config=config)
    return caller.run()


def run_all_maintenance(verbose: bool = False) -> dict[str, MaintenanceStats]:
    """Run all database maintenance operations.

    Args:
        verbose: Enable verbose output

    Returns:
        Dictionary mapping maintenance type to statistics
    """
    results: dict[str, MaintenanceStats] = {}

    # HD maintenance
    try:
        results["HD"] = run_hd_maintenance(verbose=verbose)
    except Exception as e:
        logger.error(f"HD maintenance failed: {e}")

    # SD maintenance
    try:
        results["SD"] = run_sd_maintenance(verbose=verbose)
    except Exception as e:
        logger.error(f"SD maintenance failed: {e}")

    # SM maintenance
    try:
        results["SM"] = run_sm_maintenance(verbose=verbose)
    except Exception as e:
        logger.error(f"SM maintenance failed: {e}")

    return results


def main() -> int:
    """Command-line entry point.

    Usage:
        python -m pygnss_rt.database.maintenance [type] [options]

    Returns:
        Exit code (0 for success)
    """
    import argparse

    parser = argparse.ArgumentParser(
        description="Run database maintenance operations",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Maintenance types:
  hd  - Hourly RINEX data (call_HD_maintain.pl)
  sd  - Sub-hourly RINEX data (call_SD_maintain.pl)
  sm  - Sub-hourly MET data (call_SM_maintain.pl)
  all - Run all maintenance operations

Examples:
  python -m pygnss_rt.database.maintenance hd --verbose
  python -m pygnss_rt.database.maintenance all
        """,
    )

    parser.add_argument(
        "type",
        nargs="?",
        default="hd",
        choices=["hd", "sd", "sm", "all"],
        help="Maintenance type (default: hd)",
    )

    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable verbose output",
    )

    parser.add_argument(
        "--late-day",
        type=int,
        default=0,
        help="Days before marking as too late",
    )

    parser.add_argument(
        "--late-hour",
        type=int,
        default=1,
        help="Hours before marking as too late (HD only)",
    )

    args = parser.parse_args()

    if args.type == "all":
        results = run_all_maintenance(verbose=args.verbose)
        print("\n" + "=" * 60)
        print("MAINTENANCE SUMMARY")
        print("=" * 60)
        for mtype, stats in results.items():
            print(f"\n{stats}")
        return 0

    elif args.type == "hd":
        stats = run_hd_maintenance(
            verbose=args.verbose,
            late_day=args.late_day,
            late_hour=args.late_hour,
        )

    elif args.type == "sd":
        stats = run_sd_maintenance(
            verbose=args.verbose,
            late_day=args.late_day,
        )

    elif args.type == "sm":
        stats = run_sm_maintenance(
            verbose=args.verbose,
            late_day=args.late_day,
        )

    print(f"\n{stats}")
    return 0 if not stats.errors else 1


if __name__ == "__main__":
    import sys
    sys.exit(main())
