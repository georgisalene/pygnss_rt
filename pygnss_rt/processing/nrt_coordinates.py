"""
Dynamic NRT Coordinate File Management.

Handles dynamic coordinate file paths that are updated daily for
Near Real-Time processing. Coordinates are stored in files with
date-based naming: DNR{YY}{DOY}0.CRD (main) and ANR{YY}{DOY}0.CRD (backup).

This replaces the Perl coordinate path construction:
    $args{infoCRD} = "/home/nrt105/data54/nrtCoord/DNR".$args{y2c}.$args{doy}.'0.CRD';

Usage:
    from pygnss_rt.processing.nrt_coordinates import NRTCoordinateManager

    manager = NRTCoordinateManager(base_dir="/home/nrt105/data54/nrtCoord")
    crd_path = manager.get_coordinate_file(year=2024, doy=260)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any


@dataclass
class NRTCoordinateConfig:
    """Configuration for NRT coordinate file management.

    Corresponds to the Perl args structure for coordinate handling.
    """

    base_dir: Path = field(default_factory=lambda: Path("/home/nrt105/data54/nrtCoord"))

    # File naming pattern prefixes
    main_prefix: str = "DNR"  # Main coordinate file prefix
    backup_prefix: str = "ANR"  # Backup/archive coordinate file prefix

    # File suffix
    suffix: str = "0.CRD"  # Hour designation (0=daily) + extension

    # Whether to remove stations without coordinates
    remove_if_no_coord: bool = True

    # Fallback static coordinate file (used when NRT not available)
    static_fallback: Path | None = None

    def __post_init__(self) -> None:
        """Ensure base_dir is a Path object."""
        if isinstance(self.base_dir, str):
            self.base_dir = Path(self.base_dir)
        if self.static_fallback and isinstance(self.static_fallback, str):
            self.static_fallback = Path(self.static_fallback)


@dataclass
class CoordinateFileInfo:
    """Information about a coordinate file."""

    file_path: Path
    year: int
    doy: int
    file_type: str  # "main" or "backup"
    exists: bool = False
    station_count: int = 0


class NRTCoordinateManager:
    """Manages dynamic NRT coordinate files.

    Provides path construction and file availability checking for
    Near Real-Time coordinate files that are updated daily.
    """

    def __init__(
        self,
        config: NRTCoordinateConfig | None = None,
        base_dir: str | Path | None = None,
        verbose: bool = False,
    ) -> None:
        """Initialize NRT coordinate manager.

        Args:
            config: Full configuration object
            base_dir: Base directory for coordinate files (shortcut)
            verbose: Enable verbose output
        """
        if config:
            self.config = config
        elif base_dir:
            self.config = NRTCoordinateConfig(base_dir=Path(base_dir))
        else:
            self.config = NRTCoordinateConfig()

        self.verbose = verbose

    def _format_path(self, prefix: str, year: int, doy: int) -> Path:
        """Format coordinate file path.

        Args:
            prefix: File prefix (DNR or ANR)
            year: 4-digit year
            doy: Day of year

        Returns:
            Full path to coordinate file
        """
        y2 = year % 100  # 2-digit year
        filename = f"{prefix}{y2:02d}{doy:03d}{self.config.suffix}"
        return self.config.base_dir / filename

    def get_main_coordinate_file(self, year: int, doy: int) -> Path:
        """Get path to main coordinate file for given date.

        Main file uses DNR prefix: DNR{YY}{DOY}0.CRD

        Args:
            year: 4-digit year
            doy: Day of year (1-366)

        Returns:
            Path to main coordinate file
        """
        return self._format_path(self.config.main_prefix, year, doy)

    def get_backup_coordinate_file(self, year: int, doy: int) -> Path:
        """Get path to backup coordinate file for given date.

        Backup file uses ANR prefix: ANR{YY}{DOY}0.CRD

        Args:
            year: 4-digit year
            doy: Day of year (1-366)

        Returns:
            Path to backup coordinate file
        """
        return self._format_path(self.config.backup_prefix, year, doy)

    def get_coordinate_file(
        self,
        year: int,
        doy: int,
        prefer_backup: bool = False,
    ) -> Path:
        """Get best available coordinate file for given date.

        Checks main file first (DNR), falls back to backup (ANR),
        then to static fallback if configured.

        Args:
            year: 4-digit year
            doy: Day of year
            prefer_backup: If True, check backup before main

        Returns:
            Path to best available coordinate file

        Raises:
            FileNotFoundError: If no coordinate file is available
        """
        main_path = self.get_main_coordinate_file(year, doy)
        backup_path = self.get_backup_coordinate_file(year, doy)

        if prefer_backup:
            paths = [backup_path, main_path]
        else:
            paths = [main_path, backup_path]

        # Check for existing files
        for path in paths:
            if path.exists():
                if self.verbose:
                    print(f"  Using coordinate file: {path}")
                return path

        # Try static fallback
        if self.config.static_fallback and self.config.static_fallback.exists():
            if self.verbose:
                print(f"  Using static fallback: {self.config.static_fallback}")
            return self.config.static_fallback

        # No file found
        raise FileNotFoundError(
            f"No coordinate file found for {year}/{doy:03d}. "
            f"Checked: {main_path}, {backup_path}"
        )

    def get_coordinate_files_info(
        self,
        year: int,
        doy: int,
    ) -> dict[str, CoordinateFileInfo]:
        """Get information about all coordinate files for a date.

        Args:
            year: 4-digit year
            doy: Day of year

        Returns:
            Dictionary with 'main' and 'backup' CoordinateFileInfo objects
        """
        main_path = self.get_main_coordinate_file(year, doy)
        backup_path = self.get_backup_coordinate_file(year, doy)

        return {
            "main": CoordinateFileInfo(
                file_path=main_path,
                year=year,
                doy=doy,
                file_type="main",
                exists=main_path.exists(),
                station_count=self._count_stations(main_path) if main_path.exists() else 0,
            ),
            "backup": CoordinateFileInfo(
                file_path=backup_path,
                year=year,
                doy=doy,
                file_type="backup",
                exists=backup_path.exists(),
                station_count=self._count_stations(backup_path) if backup_path.exists() else 0,
            ),
        }

    def _count_stations(self, crd_path: Path) -> int:
        """Count stations in a CRD file.

        CRD files have station entries starting with 4-character station ID.

        Args:
            crd_path: Path to CRD file

        Returns:
            Number of stations in file
        """
        if not crd_path.exists():
            return 0

        count = 0
        try:
            with open(crd_path, "r") as f:
                for line in f:
                    # CRD file format: station entries have format
                    # NUM  STAID  X  Y  Z  FLAG
                    parts = line.strip().split()
                    if len(parts) >= 5:
                        # Check if second field looks like a station ID (4 chars)
                        if len(parts[1]) == 4 and parts[1].isalnum():
                            count += 1
        except Exception:
            pass

        return count

    def check_availability(
        self,
        start_year: int,
        start_doy: int,
        end_year: int,
        end_doy: int,
    ) -> list[dict[str, Any]]:
        """Check coordinate file availability for a date range.

        Args:
            start_year: Start year
            start_doy: Start day of year
            end_year: End year
            end_doy: End day of year

        Returns:
            List of availability records
        """
        results = []

        # Convert to datetime for iteration
        start_dt = datetime(start_year, 1, 1) + timedelta(days=start_doy - 1)
        end_dt = datetime(end_year, 1, 1) + timedelta(days=end_doy - 1)

        current = start_dt
        while current <= end_dt:
            year = current.year
            doy = current.timetuple().tm_yday

            info = self.get_coordinate_files_info(year, doy)

            results.append({
                "year": year,
                "doy": doy,
                "main_exists": info["main"].exists,
                "backup_exists": info["backup"].exists,
                "main_path": str(info["main"].file_path),
                "backup_path": str(info["backup"].file_path),
                "station_count": max(info["main"].station_count, info["backup"].station_count),
            })

            current += timedelta(days=1)

        return results

    def get_latest_available(
        self,
        max_days_back: int = 7,
        reference_year: int | None = None,
        reference_doy: int | None = None,
    ) -> tuple[int, int, Path] | None:
        """Find the most recent available coordinate file.

        Args:
            max_days_back: Maximum days to search backwards
            reference_year: Reference year (default: current)
            reference_doy: Reference DOY (default: current)

        Returns:
            Tuple of (year, doy, path) or None if not found
        """
        if reference_year is None or reference_doy is None:
            now = datetime.utcnow()
            reference_year = now.year
            reference_doy = now.timetuple().tm_yday

        ref_dt = datetime(reference_year, 1, 1) + timedelta(days=reference_doy - 1)

        for days_back in range(max_days_back + 1):
            check_dt = ref_dt - timedelta(days=days_back)
            year = check_dt.year
            doy = check_dt.timetuple().tm_yday

            try:
                path = self.get_coordinate_file(year, doy)
                return (year, doy, path)
            except FileNotFoundError:
                continue

        return None

    def build_bsw_args(self, year: int, doy: int) -> dict[str, Any]:
        """Build Bernese-style args dictionary for coordinates.

        Args:
            year: 4-digit year
            doy: Day of year

        Returns:
            Dictionary with infoCRD, infoCRA, remIfNoCoord keys
        """
        return {
            "infoCRD": str(self.get_main_coordinate_file(year, doy)),
            "infoCRA": str(self.get_backup_coordinate_file(year, doy)),
            "remIfNoCoord": "yes" if self.config.remove_if_no_coord else "no",
        }


def create_nrt_coordinate_config(
    base_dir: str | Path = "/home/nrt105/data54/nrtCoord",
    main_prefix: str = "DNR",
    backup_prefix: str = "ANR",
    static_fallback: str | Path | None = None,
) -> NRTCoordinateConfig:
    """Convenience function to create NRT coordinate configuration.

    Args:
        base_dir: Base directory for coordinate files
        main_prefix: Main file prefix (default: DNR)
        backup_prefix: Backup file prefix (default: ANR)
        static_fallback: Optional static fallback file

    Returns:
        NRTCoordinateConfig instance
    """
    return NRTCoordinateConfig(
        base_dir=Path(base_dir),
        main_prefix=main_prefix,
        backup_prefix=backup_prefix,
        static_fallback=Path(static_fallback) if static_fallback else None,
    )


# Default configuration for NRDDP TRO processing
NRDDP_TRO_COORDINATES = NRTCoordinateConfig(
    base_dir=Path("/home/nrt105/data54/nrtCoord"),
    main_prefix="DNR",
    backup_prefix="ANR",
    remove_if_no_coord=True,
)

# Configuration with static IGS20 fallback
NRDDP_TRO_WITH_FALLBACK = NRTCoordinateConfig(
    base_dir=Path("/home/nrt105/data54/nrtCoord"),
    main_prefix="DNR",
    backup_prefix="ANR",
    remove_if_no_coord=True,
    static_fallback=Path("/home/ahunegnaw/GPSUSER54_LANT/STA/IGS20_54.CRD"),
)
