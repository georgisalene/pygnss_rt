"""
Station Download Callers.

Replaces the Perl call_download_*.pl scripts:
- call_download_IGS_stations.pl
- call_download_IGS_stations_sd.pl (subhourly)
- call_download_EUREF_stations.pl
- call_download_OSGB_stations.pl
- call_download_RGP_stations.pl
- call_download_NRCAN_stations.pl
- call_download_irish_stations.pl
- call_download_icelandic_stations.pl
- call_download_scientific_stations.pl
- call_download_supersite_stations.pl

These callers orchestrate the download of RINEX data from various providers,
updating the database tracking tables as files are downloaded.

Usage:
    from pygnss_rt.data_access.download_callers import (
        IGSDownloadCaller,
        EUREFDownloadCaller,
        OSGBDownloadCaller,
        run_download_job,
    )

    # Run IGS hourly download
    caller = IGSDownloadCaller()
    stats = caller.run()

    # Or use convenience function
    stats = run_download_job("igs", data_type="hourly")
"""

from __future__ import annotations

import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any

from pygnss_rt.data_access.station_downloader import (
    StationDownloader,
    DownloadResult,
    DownloadTask,
    RINEXType,
    ProviderConfig,
    DEFAULT_PROVIDERS,
)
from pygnss_rt.database.connection import DatabaseManager
from pygnss_rt.database.hourly_data import HourlyDataManager, HDStatus
from pygnss_rt.database.daily_data import DailyDataManager, SDStatus
from pygnss_rt.database.subhourly_met import SubhourlyMetManager, SMStatus
from pygnss_rt.stations.station import StationManager
from pygnss_rt.utils.dates import GNSSDate
from pygnss_rt.utils.logging import get_logger


logger = get_logger(__name__)


class DataType(str, Enum):
    """Data download types."""

    HOURLY = "hourly"
    DAILY = "daily"
    SUBHOURLY = "subhourly"


@dataclass
class DownloadJobConfig:
    """Configuration for a download job."""

    # Station selection
    station_list_xml: str | None = None  # Path to station XML
    station_type: str = ""  # e.g., "IGS", "EUREF", "OSGB"
    use_nrt: bool = True  # Filter by use_nrt=yes

    # Download settings
    data_type: DataType = DataType.HOURLY
    destination_dir: str = "/data/rinex"
    tree_type: str = "yyyy/yydoy"  # Directory structure

    # Provider settings
    ftp_site: str = "CDDIS"  # Primary FTP site
    fallback_sites: list[str] = field(default_factory=list)

    # Database settings
    database_name: str = "HD54"
    late_day: int = 0
    late_hour: int = 1
    late_15min: int = 2  # For subhourly data

    # Processing options
    parallel_downloads: int = 4
    max_retries: int = 3
    verbose: bool = False


@dataclass
class DownloadJobStats:
    """Statistics from a download job."""

    job_name: str
    start_time: datetime
    end_time: datetime
    stations_processed: int
    files_waiting: int
    files_downloaded: int
    files_failed: int
    files_too_late: int
    bytes_downloaded: int
    providers_used: dict[str, int] = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)

    @property
    def runtime_seconds(self) -> float:
        return (self.end_time - self.start_time).total_seconds()

    @property
    def success_rate(self) -> float:
        total = self.files_downloaded + self.files_failed
        if total == 0:
            return 0.0
        return self.files_downloaded / total

    def __str__(self) -> str:
        return (
            f"DownloadJob '{self.job_name}':\n"
            f"  Runtime: {self.runtime_seconds:.1f}s\n"
            f"  Stations: {self.stations_processed}\n"
            f"  Waiting: {self.files_waiting}\n"
            f"  Downloaded: {self.files_downloaded}\n"
            f"  Failed: {self.files_failed}\n"
            f"  Too Late: {self.files_too_late}\n"
            f"  Success Rate: {self.success_rate:.1%}"
        )


class BaseDownloadCaller(ABC):
    """Base class for download callers.

    Subclasses implement specific station network downloads.
    """

    def __init__(
        self,
        config: DownloadJobConfig | None = None,
        db_manager: DatabaseManager | None = None,
    ):
        """Initialize download caller.

        Args:
            config: Job configuration
            db_manager: Database manager (created if None)
        """
        self.config = config or self._default_config()
        self.db = db_manager or DatabaseManager()

        # Initialize managers
        self.station_manager = StationManager(self.db)

        if self.config.data_type == DataType.HOURLY:
            self.data_manager = HourlyDataManager(self.db)
        elif self.config.data_type == DataType.DAILY:
            self.data_manager = DailyDataManager(self.db)
        else:  # SUBHOURLY
            self.data_manager = SubhourlyMetManager(self.db)

        # Initialize downloader
        self.downloader = StationDownloader(
            download_dir=self.config.destination_dir,
            max_retries=self.config.max_retries,
            parallel_downloads=self.config.parallel_downloads,
            verbose=self.config.verbose,
        )

    @abstractmethod
    def _default_config(self) -> DownloadJobConfig:
        """Get default configuration for this caller."""
        pass

    @abstractmethod
    def get_station_list(self) -> list[str]:
        """Get list of stations to download.

        Returns:
            List of 4-character station IDs
        """
        pass

    @property
    def job_name(self) -> str:
        """Get job name for logging."""
        return f"{self.__class__.__name__}"

    def get_waiting_files(self, station: str) -> list[dict]:
        """Get list of waiting files for a station.

        Args:
            station: Station ID

        Returns:
            List of waiting file info dictionaries
        """
        if self.config.data_type == DataType.HOURLY:
            entries = self.data_manager.get_waiting_list(station=station)
            return [
                {
                    "year": e.year,
                    "doy": e.doy,
                    "hour": e.hour,
                    "station": station,
                }
                for e in entries
            ]
        elif self.config.data_type == DataType.SUBHOURLY:
            entries = self.data_manager.get_waiting_list(station=station)
            return [
                {
                    "year": e.year,
                    "doy": e.doy,
                    "hour": e.hour,
                    "quarter": e.quarter,
                    "station": station,
                }
                for e in entries
            ]
        else:  # DAILY
            entries = self.data_manager.get_waiting_list(station=station)
            return [
                {
                    "year": e.year,
                    "doy": e.doy,
                    "station": station,
                }
                for e in entries
            ]

    def update_downloaded(
        self,
        station: str,
        result: DownloadResult,
    ) -> bool:
        """Update database for downloaded file.

        Args:
            station: Station ID
            result: Download result

        Returns:
            True if update successful
        """
        task = result.task

        if self.config.data_type == DataType.HOURLY:
            return self.data_manager.update_downloaded(
                station=station,
                year=task.year,
                doy=task.doy,
                hour=task.hour,
                rinex_file=str(result.local_path) if result.local_path else "",
            )
        elif self.config.data_type == DataType.SUBHOURLY:
            quarter = getattr(task, "quarter", 0)
            return self.data_manager.update_downloaded(
                year=task.year,
                doy=task.doy,
                hour=task.hour,
                quarter=quarter,
                met_file=str(result.local_path) if result.local_path else "",
            )
        else:  # DAILY
            return self.data_manager.update_downloaded(
                station=station,
                year=task.year,
                doy=task.doy,
                rinex_file=str(result.local_path) if result.local_path else "",
            )

    def run(self) -> DownloadJobStats:
        """Run the download job.

        Returns:
            Job statistics
        """
        start_time = datetime.utcnow()

        logger.info(
            "Starting download job",
            job=self.job_name,
            data_type=self.config.data_type.value,
        )

        # Get station list
        stations = self.get_station_list()
        logger.info(f"Processing {len(stations)} stations")

        if self.config.verbose:
            print(f"\n{len(stations)} stations to process:")
            for i in range(0, len(stations), 10):
                print("  " + " ".join(stations[i:i+10]))
            print()

        stats = DownloadJobStats(
            job_name=self.job_name,
            start_time=start_time,
            end_time=start_time,
            stations_processed=len(stations),
            files_waiting=0,
            files_downloaded=0,
            files_failed=0,
            files_too_late=0,
            bytes_downloaded=0,
        )

        # Process each station
        for station in sorted(stations):
            station_stats = self._process_station(station)

            stats.files_waiting += station_stats.get("waiting", 0)
            stats.files_downloaded += station_stats.get("downloaded", 0)
            stats.files_failed += station_stats.get("failed", 0)
            stats.bytes_downloaded += station_stats.get("bytes", 0)

            for prov, count in station_stats.get("providers", {}).items():
                stats.providers_used[prov] = stats.providers_used.get(prov, 0) + count

        # Mark too-late files
        too_late_count = self._mark_too_late_files()
        stats.files_too_late = too_late_count

        stats.end_time = datetime.utcnow()

        logger.info(
            "Download job completed",
            job=self.job_name,
            runtime=stats.runtime_seconds,
            downloaded=stats.files_downloaded,
            failed=stats.files_failed,
        )

        if self.config.verbose:
            print(str(stats))

        return stats

    def _process_station(self, station: str) -> dict[str, Any]:
        """Process downloads for a single station.

        Args:
            station: Station ID

        Returns:
            Station processing statistics
        """
        # Get waiting files
        waiting = self.get_waiting_files(station)
        n_waiting = len(waiting)

        if self.config.verbose:
            print(f"\nStation {station.upper()}: {n_waiting} waiting file(s)")

        if n_waiting == 0:
            return {"waiting": 0, "downloaded": 0, "failed": 0, "bytes": 0, "providers": {}}

        # Build download tasks
        tasks = []
        for w in waiting:
            if self.config.data_type == DataType.HOURLY:
                task = DownloadTask(
                    station_id=station,
                    year=w["year"],
                    doy=w["doy"],
                    hour=w["hour"],
                    rinex_type=RINEXType.HOURLY,
                )
            elif self.config.data_type == DataType.SUBHOURLY:
                task = DownloadTask(
                    station_id=station,
                    year=w["year"],
                    doy=w["doy"],
                    hour=w["hour"],
                    rinex_type=RINEXType.SUBHOURLY,
                )
                # Store quarter for later
                task.quarter = w.get("quarter", 0)
            else:
                task = DownloadTask(
                    station_id=station,
                    year=w["year"],
                    doy=w["doy"],
                    hour=None,
                    rinex_type=RINEXType.DAILY,
                )
            tasks.append(task)

        # Download files
        providers = [self.config.ftp_site] + self.config.fallback_sites
        results = self.downloader._download_batch(tasks, providers)

        # Update database for successful downloads
        downloaded = 0
        failed = 0
        total_bytes = 0
        providers_used: dict[str, int] = {}

        for result in results:
            if result.success:
                self.update_downloaded(station, result)
                downloaded += 1
                total_bytes += result.file_size

                prov = result.provider_used
                providers_used[prov] = providers_used.get(prov, 0) + 1
            else:
                failed += 1

        return {
            "waiting": n_waiting,
            "downloaded": downloaded,
            "failed": failed,
            "bytes": total_bytes,
            "providers": providers_used,
        }

    def _mark_too_late_files(self) -> int:
        """Mark files as too late based on configuration.

        Returns:
            Number of files marked as too late
        """
        if self.config.data_type == DataType.HOURLY:
            return self.data_manager.set_too_late_files(
                late_day=self.config.late_day,
                late_hour=self.config.late_hour,
            )
        elif self.config.data_type == DataType.SUBHOURLY:
            return self.data_manager.set_too_late_files(
                late_day=self.config.late_day,
                late_15min=self.config.late_15min,
            )
        else:
            return self.data_manager.set_too_late_files(
                late_day=self.config.late_day,
            )

    def close(self) -> None:
        """Clean up resources."""
        self.downloader.close()


class IGSDownloadCaller(BaseDownloadCaller):
    """Download caller for IGS stations (hourly data).

    Replaces call_download_IGS_stations.pl
    """

    def _default_config(self) -> DownloadJobConfig:
        return DownloadJobConfig(
            station_type="IGS",
            data_type=DataType.HOURLY,
            destination_dir="/data/hourlyData",
            ftp_site="CDDIS",
            fallback_sites=["IGN_HOURLY", "BKGE_HOURLY"],
            database_name="HD54",
            late_day=0,
            late_hour=1,
        )

    def get_station_list(self) -> list[str]:
        """Get IGS station list from XML configuration."""
        # Get IGS core stations + general IGS stations
        igs_core = self.station_manager.get_stations_by_network(
            network="IGS",
            station_type="core",
            use_nrt=self.config.use_nrt,
        )
        igs_general = self.station_manager.get_stations_by_network(
            network="IGS",
            use_nrt=self.config.use_nrt,
        )

        stations = list(set(igs_core + igs_general))
        return sorted(stations)


class IGSSubhourlyDownloadCaller(BaseDownloadCaller):
    """Download caller for IGS stations (15-minute subhourly data).

    Replaces call_download_IGS_stations_sd.pl
    """

    def _default_config(self) -> DownloadJobConfig:
        return DownloadJobConfig(
            station_type="IGS",
            data_type=DataType.SUBHOURLY,
            destination_dir="/data/subhourlyData",
            ftp_site="CDDIS",
            fallback_sites=["IGN_HOURLY"],
            database_name="SD52",
            late_day=0,
            late_15min=1,
        )

    def get_station_list(self) -> list[str]:
        """Get IGS station list for subhourly data."""
        igs_core = self.station_manager.get_stations_by_network(
            network="IGS",
            station_type="core",
            use_nrt=self.config.use_nrt,
        )
        igs_general = self.station_manager.get_stations_by_network(
            network="IGS",
            use_nrt=self.config.use_nrt,
        )

        stations = list(set(igs_core + igs_general))
        return sorted(stations)


class EUREFDownloadCaller(BaseDownloadCaller):
    """Download caller for EUREF stations.

    Replaces call_download_EUREF_stations.pl
    """

    def _default_config(self) -> DownloadJobConfig:
        return DownloadJobConfig(
            station_type="EUREF",
            data_type=DataType.HOURLY,
            destination_dir="/data/hourlyData",
            ftp_site="BKGE_HOURLY",
            fallback_sites=["CDDIS"],
            database_name="HD54",
            late_day=0,
            late_hour=1,
        )

    def get_station_list(self) -> list[str]:
        """Get EUREF station list."""
        return self.station_manager.get_stations_by_network(
            network="EUREF",
            use_nrt=self.config.use_nrt,
        )


class OSGBDownloadCaller(BaseDownloadCaller):
    """Download caller for OSGB (Ordnance Survey GB) stations.

    Replaces call_download_OSGB_stations.pl
    """

    def _default_config(self) -> DownloadJobConfig:
        return DownloadJobConfig(
            station_type="OSGB",
            data_type=DataType.HOURLY,
            destination_dir="/data/hourlyData",
            ftp_site="OSGB",
            fallback_sites=[],
            database_name="HD54",
            late_day=0,
            late_hour=1,
        )

    def get_station_list(self) -> list[str]:
        """Get OSGB station list."""
        return self.station_manager.get_stations_by_network(
            network="OSGB",
            use_nrt=self.config.use_nrt,
        )


class RGPDownloadCaller(BaseDownloadCaller):
    """Download caller for RGP (French) stations.

    Replaces call_download_RGP_stations.pl
    """

    def _default_config(self) -> DownloadJobConfig:
        return DownloadJobConfig(
            station_type="RGP",
            data_type=DataType.HOURLY,
            destination_dir="/data/hourlyData",
            ftp_site="RGP",
            fallback_sites=["IGN_HOURLY"],
            database_name="HD54",
            late_day=0,
            late_hour=1,
        )

    def get_station_list(self) -> list[str]:
        """Get RGP station list."""
        return self.station_manager.get_stations_by_network(
            network="RGP",
            use_nrt=self.config.use_nrt,
        )


class NRCANDownloadCaller(BaseDownloadCaller):
    """Download caller for NRCAN (Canadian) stations.

    Replaces call_download_NRCAN_stations.pl
    """

    def _default_config(self) -> DownloadJobConfig:
        return DownloadJobConfig(
            station_type="NRCAN",
            data_type=DataType.HOURLY,
            destination_dir="/data/hourlyData",
            ftp_site="CDDIS",
            fallback_sites=[],
            database_name="HD54",
            late_day=0,
            late_hour=1,
        )

    def get_station_list(self) -> list[str]:
        """Get NRCAN station list."""
        return self.station_manager.get_stations_by_network(
            network="NRCAN",
            use_nrt=self.config.use_nrt,
        )


class IrishDownloadCaller(BaseDownloadCaller):
    """Download caller for Irish stations.

    Replaces call_download_irish_stations.pl
    """

    def _default_config(self) -> DownloadJobConfig:
        return DownloadJobConfig(
            station_type="IRISH",
            data_type=DataType.HOURLY,
            destination_dir="/data/hourlyData",
            ftp_site="CDDIS",
            fallback_sites=["BKGE_HOURLY"],
            database_name="HD54",
            late_day=0,
            late_hour=1,
        )

    def get_station_list(self) -> list[str]:
        """Get Irish station list."""
        return self.station_manager.get_stations_by_network(
            network="IRISH",
            use_nrt=self.config.use_nrt,
        )


class IcelandicDownloadCaller(BaseDownloadCaller):
    """Download caller for Icelandic stations.

    Replaces call_download_icelandic_stations.pl
    """

    def _default_config(self) -> DownloadJobConfig:
        return DownloadJobConfig(
            station_type="ICELANDIC",
            data_type=DataType.HOURLY,
            destination_dir="/data/hourlyData",
            ftp_site="CDDIS",
            fallback_sites=["BKGE_HOURLY"],
            database_name="HD54",
            late_day=0,
            late_hour=1,
        )

    def get_station_list(self) -> list[str]:
        """Get Icelandic station list."""
        return self.station_manager.get_stations_by_network(
            network="ICELANDIC",
            use_nrt=self.config.use_nrt,
        )


class ScientificDownloadCaller(BaseDownloadCaller):
    """Download caller for scientific research stations.

    Replaces call_download_scientific_stations.pl
    """

    def _default_config(self) -> DownloadJobConfig:
        return DownloadJobConfig(
            station_type="SCIENTIFIC",
            data_type=DataType.HOURLY,
            destination_dir="/data/hourlyData",
            ftp_site="CDDIS",
            fallback_sites=["SOPAC"],
            database_name="HD54",
            late_day=0,
            late_hour=2,  # Longer latency for scientific stations
        )

    def get_station_list(self) -> list[str]:
        """Get scientific station list."""
        return self.station_manager.get_stations_by_network(
            network="SCIENTIFIC",
            use_nrt=self.config.use_nrt,
        )


class SupersiteDownloadCaller(BaseDownloadCaller):
    """Download caller for supersite stations.

    Replaces call_download_supersite_stations.pl
    """

    def _default_config(self) -> DownloadJobConfig:
        return DownloadJobConfig(
            station_type="SUPERSITE",
            data_type=DataType.HOURLY,
            destination_dir="/data/hourlyData",
            ftp_site="CDDIS",
            fallback_sites=["IGN_HOURLY"],
            database_name="HD54",
            late_day=0,
            late_hour=1,
        )

    def get_station_list(self) -> list[str]:
        """Get supersite station list."""
        return self.station_manager.get_stations_by_network(
            network="SUPERSITE",
            use_nrt=self.config.use_nrt,
        )


# Registry of all download callers
DOWNLOAD_CALLERS: dict[str, type[BaseDownloadCaller]] = {
    "igs": IGSDownloadCaller,
    "igs_sd": IGSSubhourlyDownloadCaller,
    "igs_subhourly": IGSSubhourlyDownloadCaller,
    "euref": EUREFDownloadCaller,
    "osgb": OSGBDownloadCaller,
    "rgp": RGPDownloadCaller,
    "nrcan": NRCANDownloadCaller,
    "irish": IrishDownloadCaller,
    "icelandic": IcelandicDownloadCaller,
    "scientific": ScientificDownloadCaller,
    "supersite": SupersiteDownloadCaller,
}


def run_download_job(
    network: str,
    data_type: str = "hourly",
    destination_dir: str | None = None,
    verbose: bool = False,
    **kwargs: Any,
) -> DownloadJobStats:
    """Run a download job for a specific network.

    Convenience function to run download jobs without instantiating classes.

    Args:
        network: Network name (igs, euref, osgb, etc.)
        data_type: Data type (hourly, daily, subhourly)
        destination_dir: Override destination directory
        verbose: Enable verbose output
        **kwargs: Additional configuration options

    Returns:
        Download job statistics

    Raises:
        ValueError: If network is not recognized
    """
    network_lower = network.lower()

    if network_lower not in DOWNLOAD_CALLERS:
        available = ", ".join(sorted(DOWNLOAD_CALLERS.keys()))
        raise ValueError(
            f"Unknown network '{network}'. Available: {available}"
        )

    caller_class = DOWNLOAD_CALLERS[network_lower]

    # Build custom config if needed
    config = caller_class(config=None, db_manager=None)._default_config()

    if data_type:
        config.data_type = DataType(data_type)
    if destination_dir:
        config.destination_dir = destination_dir
    config.verbose = verbose

    # Apply additional kwargs
    for key, value in kwargs.items():
        if hasattr(config, key):
            setattr(config, key, value)

    # Create and run caller
    caller = caller_class(config=config)
    try:
        return caller.run()
    finally:
        caller.close()


def run_all_download_jobs(
    networks: list[str] | None = None,
    verbose: bool = False,
) -> dict[str, DownloadJobStats]:
    """Run download jobs for multiple networks.

    Args:
        networks: List of networks to process (all if None)
        verbose: Enable verbose output

    Returns:
        Dictionary mapping network name to job statistics
    """
    if networks is None:
        networks = list(DOWNLOAD_CALLERS.keys())

    results: dict[str, DownloadJobStats] = {}

    for network in networks:
        try:
            stats = run_download_job(network, verbose=verbose)
            results[network] = stats
        except Exception as e:
            logger.error(f"Download job failed for {network}: {e}")

    return results


def main() -> int:
    """Command-line entry point.

    Usage:
        python -m pygnss_rt.data_access.download_callers [network] [options]

    Returns:
        Exit code (0 for success)
    """
    import argparse

    parser = argparse.ArgumentParser(
        description="Run station data download jobs",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=f"""
Available networks:
  {', '.join(sorted(DOWNLOAD_CALLERS.keys()))}

Examples:
  python -m pygnss_rt.data_access.download_callers igs
  python -m pygnss_rt.data_access.download_callers euref --verbose
  python -m pygnss_rt.data_access.download_callers igs_sd --type subhourly
        """,
    )

    parser.add_argument(
        "network",
        nargs="?",
        default="igs",
        help="Network to download (default: igs)",
    )

    parser.add_argument(
        "--type", "-t",
        dest="data_type",
        default="hourly",
        choices=["hourly", "daily", "subhourly"],
        help="Data type to download",
    )

    parser.add_argument(
        "--dest", "-d",
        dest="destination_dir",
        help="Destination directory",
    )

    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable verbose output",
    )

    parser.add_argument(
        "--all", "-a",
        action="store_true",
        help="Run all download jobs",
    )

    args = parser.parse_args()

    if args.all:
        results = run_all_download_jobs(verbose=args.verbose)
        print("\n" + "=" * 60)
        print("SUMMARY")
        print("=" * 60)
        for network, stats in results.items():
            print(f"\n{network}: {stats.files_downloaded} downloaded, {stats.files_failed} failed")
        return 0

    stats = run_download_job(
        network=args.network,
        data_type=args.data_type,
        destination_dir=args.destination_dir,
        verbose=args.verbose,
    )

    print("\n" + str(stats))
    return 0 if stats.files_failed == 0 else 1


if __name__ == "__main__":
    import sys
    sys.exit(main())
