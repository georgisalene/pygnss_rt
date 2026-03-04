"""
Data file management for GNSS processing.

This module handles RINEX data file management (hourly, daily, subhourly).
Extracted from orchestrator_main.py for better modularity.
"""

from __future__ import annotations

from pathlib import Path

from pygnss_rt.database import DatabaseManager, HourlyDataManager
from pygnss_rt.processing.processing_config import ProcessingConfig, ProcessingType
from pygnss_rt.utils.logging import get_logger


logger = get_logger(__name__)


class DataManager:
    """Manage RINEX data files for processing.

    Handles hourly, daily, and subhourly data.
    Replaces Perl get_list_of_hourly_files, get_list_of_daily_files,
    get_list_of_available_hourly_files, compose_list_from_list_and_comp.
    """

    def __init__(self, config: ProcessingConfig, db_manager: DatabaseManager | None = None):
        """Initialize data manager.

        Args:
            config: Processing configuration
            db_manager: Optional database manager for availability checks
        """
        self.config = config
        self._db_manager = db_manager

    @staticmethod
    def compose_list_with_compression(
        file_list: list[str],
        compression: str = ".Z",
    ) -> list[str]:
        """Add compression extension to file list.

        Replaces Perl compose_list_from_list_and_comp.

        Args:
            file_list: List of filenames
            compression: Compression extension to add

        Returns:
            List of filenames with compression extension
        """
        return [f"{f}{compression}" for f in file_list]

    def get_requested_files(
        self,
        stations: list[str],
        compression: str = ".Z",
        include_compression: bool = True,
    ) -> list[str]:
        """Get list of files needed for processing.

        Replaces Perl get_list_of_hourly_files and get_list_of_daily_files.

        Args:
            stations: List of station IDs
            compression: Compression extension
            include_compression: Whether to include compression in filename

        Returns:
            List of required filenames
        """
        if not self.config.gnss_date:
            return []

        gd = self.config.gnss_date
        files = []

        if self.config.proc_type == ProcessingType.HOURLY:
            # Hourly file naming: ssssdddhh.yyd (Hatanaka compressed)
            hour = getattr(gd, "hour", 0)
            hour_alpha = self._hour_to_alpha(hour)
            for sta in stations:
                # Short format with hour letter
                filename = f"{sta.lower()}{gd.doy:03d}{hour_alpha}.{gd.year % 100:02d}d"
                if include_compression:
                    filename = f"{filename}{compression}"
                files.append(filename)

        elif self.config.proc_type == ProcessingType.DAILY:
            # Daily file naming: ssssdddn.yyo or ssssdddn.yyd
            for sta in stations:
                # Use session character (0 for daily, or from config)
                session_char = "0"
                filename = f"{sta.lower()}{gd.doy:03d}{session_char}.{gd.year % 100:02d}d"
                if include_compression:
                    filename = f"{filename}{compression}"
                files.append(filename)

        elif self.config.proc_type == ProcessingType.SUBHOURLY:
            # Subhourly file naming (15-minute files)
            hour = getattr(gd, "hour", 0)
            minute = getattr(gd, "minute", 0)
            # Round to nearest 15-minute boundary
            minute_block = (minute // 15) * 15
            for sta in stations:
                filename = f"{sta.lower()}{gd.doy:03d}{hour:02d}{minute_block:02d}.{gd.year % 100:02d}d"
                if include_compression:
                    filename = f"{filename}{compression}"
                files.append(filename)

        return files

    def _hour_to_alpha(self, hour: int) -> str:
        """Convert hour (0-23) to alpha character (a-x).

        Args:
            hour: Hour of day (0-23)

        Returns:
            Corresponding alpha character
        """
        return chr(ord('a') + hour)

    def get_available_files_from_db(
        self,
        stations: list[str],
        compression: str = ".Z",
    ) -> list[str]:
        """Get list of available files by checking database.

        Replaces Perl get_list_of_available_hourly_files.

        Args:
            stations: List of station IDs
            compression: Compression extension

        Returns:
            List of available filenames
        """
        if not self._db_manager or not self.config.gnss_date:
            return []

        available = []
        gd = self.config.gnss_date

        try:
            hd_manager = HourlyDataManager(self._db_manager)

            for sta in stations:
                # Query database for file status
                status = hd_manager.get_file_status(
                    station=sta,
                    mjd=gd.mjd,
                )

                # Only include if status indicates available (not 'Waiting' or 'Too Late')
                if status and status not in ("Waiting", "Too Late"):
                    if self.config.proc_type == ProcessingType.HOURLY:
                        hour = getattr(gd, "hour", 0)
                        hour_alpha = self._hour_to_alpha(hour)
                        filename = f"{sta.lower()}{gd.doy:03d}{hour_alpha}.{gd.year % 100:02d}d{compression}"
                    else:
                        filename = f"{sta.lower()}{gd.doy:03d}0.{gd.year % 100:02d}d{compression}"

                    available.append(filename)

        except Exception as e:
            logger.warning(f"DB check failed: {e}")

        return available

    def get_available_files(
        self,
        requested: list[str],
        check_db: bool = True,
    ) -> tuple[list[str], list[str]]:
        """Check which files are available.

        Args:
            requested: List of requested filenames
            check_db: Whether to also check database

        Returns:
            Tuple of (available files, missing files)
        """
        available = []
        missing = []

        if not self.config.data_dir:
            return [], requested

        for filename in requested:
            found = False

            # Check various possible locations
            search_paths = [
                self.config.data_dir / filename,
            ]

            # Add year/doy organized path
            if self.config.gnss_date:
                gd = self.config.gnss_date
                search_paths.extend([
                    self.config.data_dir / str(gd.year) / f"{gd.year % 100:02d}{gd.doy:03d}" / filename,
                    self.config.data_dir / str(gd.year) / str(gd.doy) / filename,
                    self.config.data_dir / str(gd.year) / filename,
                ])

            for check_path in search_paths:
                if check_path.exists():
                    available.append(filename)
                    found = True
                    break

            if not found:
                missing.append(filename)

        return available, missing

    def calculate_success_rate(
        self,
        available: list[str],
        requested: list[str],
    ) -> float:
        """Calculate download/availability success rate.

        Args:
            available: List of available files
            requested: List of requested files

        Returns:
            Success rate as percentage (0-100)
        """
        if not requested:
            return 0.0
        return len(available) / len(requested) * 100
