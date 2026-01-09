"""
NEQ (Normal Equation) Stacking for GNSS Processing.

Implements NEQ stacking functionality for combining multiple hourly solutions
into a more robust final solution. This is essential for Near Real-Time
Double Difference Processing (NRDDP) troposphere estimation.

NEQ stacking combines normal equation files (.NQ0) from previous hours to:
1. Improve parameter estimation through temporal averaging
2. Provide continuity constraints across session boundaries
3. Enable sliding window processing for real-time applications

Naming Schemes:
- P1_yydoyU: Standard hourly format (e.g., P1_24260A.NQ0)
- P1_yydoyUUU: Sub-hourly format with minutes (e.g., P1_24260A15.NQ0)

Usage:
    from pygnss_rt.processing.neq_stacking import NEQStacker, NEQStackingConfig

    config = NEQStackingConfig(
        enabled=True,
        n_hours_to_stack=4,
        name_scheme="P1_yydoyU",
    )

    stacker = NEQStacker(config)
    neq_files = stacker.get_neq_files_to_stack(
        current_mjdh=60560.5,  # MJD with hour fraction
        archive_dir="/home/user/data54/campaigns/tro",
        session_suffix="NR",
    )

    # Copy NEQ files to campaign directory
    stacker.copy_neq_files_to_campaign(
        neq_files=neq_files,
        campaign_sol_dir="/campaigns/24260ANR/SOL",
    )
"""

from __future__ import annotations

import gzip
import shutil
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Any


class NEQNameScheme(str, Enum):
    """NEQ file naming schemes."""

    HOURLY = "P1_yydoyU"  # Standard hourly: P1_24260A.NQ0
    SUB_HOURLY = "P1_yydoyUUU"  # Sub-hourly with minutes: P1_24260A15.NQ0


@dataclass
class NEQStackingConfig:
    """Configuration for NEQ stacking.

    Corresponds to the Perl %args{COMBNEQ} structure:
        $args{COMBNEQ} = {
            yesORno    => 'yes',
            n2stack    => 4,
            nameScheme => 'P1_yydoyU',
        };
    """

    enabled: bool = False
    n_hours_to_stack: int = 4  # Number of previous hours to stack
    name_scheme: NEQNameScheme | str = NEQNameScheme.HOURLY

    # Archive organization
    archive_organization: str = "yyyy/doy"  # Directory structure

    # Compression
    compression: str = "gzip"  # Compression utility used

    # Session suffix for directory naming
    session_suffix: str = "NR"  # e.g., "NR" for NRDDP, "H" for hourly

    def __post_init__(self) -> None:
        """Normalize name_scheme to enum."""
        if isinstance(self.name_scheme, str):
            if self.name_scheme == "P1_yydoyU":
                self.name_scheme = NEQNameScheme.HOURLY
            elif self.name_scheme == "P1_yydoyUUU":
                self.name_scheme = NEQNameScheme.SUB_HOURLY

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> NEQStackingConfig:
        """Create config from Perl-style dictionary.

        Args:
            d: Dictionary with keys like 'yesORno', 'n2stack', 'nameScheme'

        Returns:
            NEQStackingConfig instance
        """
        return cls(
            enabled=d.get("yesORno", "no").lower() == "yes",
            n_hours_to_stack=int(d.get("n2stack", 4)),
            name_scheme=d.get("nameScheme", "P1_yydoyU"),
        )


@dataclass
class NEQFileInfo:
    """Information about an NEQ file to be stacked."""

    file_path: Path
    session_name: str
    year: int
    doy: int
    hour_char: str  # a-x for hours 0-23
    minutes: str = ""  # For sub-hourly: "00", "15", "30", "45"
    compressed: bool = False
    exists: bool = False

    @property
    def base_name(self) -> str:
        """Get the base NEQ filename without path or compression."""
        name = f"P1_{self.year % 100:02d}{self.doy:03d}{self.hour_char.upper()}"
        if self.minutes:
            name += self.minutes
        return name + ".NQ0"


def mjdh_to_components(mjdh: float) -> tuple[int, int, int, str]:
    """Convert MJD with hour fraction to date components.

    Args:
        mjdh: Modified Julian Date with hour fraction (e.g., 60560.5 for noon)

    Returns:
        Tuple of (year, doy, hour, hour_char)
        hour_char is 'a'-'x' for hours 0-23
    """
    # Convert MJD to datetime
    # MJD epoch is November 17, 1858
    mjd_epoch = datetime(1858, 11, 17)
    dt = mjd_epoch + timedelta(days=mjdh)

    year = dt.year
    doy = dt.timetuple().tm_yday
    hour = dt.hour

    # Hour character: a=0, b=1, ..., x=23
    hour_char = chr(ord('a') + hour)

    return year, doy, hour, hour_char


def components_to_mjdh(year: int, doy: int, hour: int = 0) -> float:
    """Convert date components to MJD with hour fraction.

    Args:
        year: 4-digit year
        doy: Day of year (1-366)
        hour: Hour of day (0-23)

    Returns:
        Modified Julian Date with hour fraction
    """
    # Create datetime from year and DOY
    dt = datetime(year, 1, 1) + timedelta(days=doy - 1, hours=hour)

    # Convert to MJD
    mjd_epoch = datetime(1858, 11, 17)
    mjd = (dt - mjd_epoch).total_seconds() / 86400.0

    return mjd


class NEQStacker:
    """Handles NEQ file stacking for hourly processing.

    Manages the discovery, retrieval, and decompression of NEQ files
    from previous hours for stacking in ADDNEQ2.
    """

    def __init__(
        self,
        config: NEQStackingConfig | None = None,
        verbose: bool = False,
    ) -> None:
        """Initialize NEQ stacker.

        Args:
            config: Stacking configuration
            verbose: Enable verbose output
        """
        self.config = config or NEQStackingConfig()
        self.verbose = verbose

    def get_neq_files_to_stack(
        self,
        current_mjdh: float,
        archive_dir: str | Path,
        session_suffix: str | None = None,
    ) -> list[NEQFileInfo]:
        """Get list of NEQ files to stack from previous hours.

        Args:
            current_mjdh: Current processing MJD with hour fraction
            archive_dir: Root archive directory (e.g., /data54/campaigns/tro)
            session_suffix: Override session suffix (default from config)

        Returns:
            List of NEQFileInfo objects for files to stack
        """
        if not self.config.enabled:
            return []

        archive_path = Path(archive_dir)
        suffix = session_suffix or self.config.session_suffix
        neq_files = []

        if self.config.name_scheme == NEQNameScheme.HOURLY:
            neq_files = self._get_hourly_neq_files(
                current_mjdh, archive_path, suffix
            )
        elif self.config.name_scheme == NEQNameScheme.SUB_HOURLY:
            neq_files = self._get_subhourly_neq_files(
                current_mjdh, archive_path, suffix
            )

        return neq_files

    def _get_hourly_neq_files(
        self,
        current_mjdh: float,
        archive_path: Path,
        session_suffix: str,
    ) -> list[NEQFileInfo]:
        """Get NEQ files for hourly stacking scheme.

        For P1_yydoyU scheme, retrieves files from previous N hours.

        Args:
            current_mjdh: Current MJD with hour fraction
            archive_path: Archive root directory
            session_suffix: Session suffix for directory naming

        Returns:
            List of NEQFileInfo for previous hours
        """
        neq_files = []

        for i in range(1, self.config.n_hours_to_stack + 1):
            # Go back i hours
            prev_mjdh = current_mjdh - (i / 24.0)
            year, doy, hour, hour_char = mjdh_to_components(prev_mjdh)

            # Build session name: YYDOYH + suffix (e.g., 24260ANR)
            session_name = f"{year % 100:02d}{doy:03d}{hour_char.upper()}{session_suffix}"

            # Build NEQ filename
            neq_base = f"P1_{year % 100:02d}{doy:03d}{hour_char.upper()}.NQ0"

            # Build full path with organization
            if self.config.archive_organization == "yyyy/doy":
                neq_dir = archive_path / str(year) / f"{doy:03d}" / session_name / "SOL"
            else:
                neq_dir = archive_path / session_name / "SOL"

            # Check for compressed version first
            neq_path_gz = neq_dir / f"{neq_base}.gz"
            neq_path = neq_dir / neq_base

            neq_info = NEQFileInfo(
                file_path=neq_path_gz if neq_path_gz.exists() else neq_path,
                session_name=session_name,
                year=year,
                doy=doy,
                hour_char=hour_char,
                compressed=neq_path_gz.exists(),
                exists=neq_path_gz.exists() or neq_path.exists(),
            )

            neq_files.append(neq_info)

            if self.verbose:
                status = "available" if neq_info.exists else "MISSING"
                print(f"  NEQ {i}: {neq_info.file_path} [{status}]")

        return neq_files

    def _get_subhourly_neq_files(
        self,
        current_mjdh: float,
        archive_path: Path,
        session_suffix: str,
    ) -> list[NEQFileInfo]:
        """Get NEQ files for sub-hourly stacking scheme.

        For P1_yydoyUUU scheme, retrieves files at 15-minute intervals:
        00, 15, 30, 45 minutes for each hour.

        Args:
            current_mjdh: Current MJD with hour fraction
            archive_path: Archive root directory
            session_suffix: Session suffix

        Returns:
            List of NEQFileInfo for sub-hourly periods
        """
        neq_files = []
        minute_suffixes = ["00", "15", "30", "45"]
        # Corresponding session suffix for each 15-min period
        minute_session_suffix = {"45": "4", "30": "3", "15": "1", "00": "0"}

        for i in range(self.config.n_hours_to_stack + 1):
            # Go back i hours
            prev_mjdh = current_mjdh - (i / 24.0)
            year, doy, hour, hour_char = mjdh_to_components(prev_mjdh)

            for minutes in minute_suffixes:
                # Session uses minute-based suffix: e.g., 24260A4 for :45
                min_suffix = minute_session_suffix[minutes]
                session_name = f"{year % 100:02d}{doy:03d}{hour_char.upper()}{min_suffix}"

                # NEQ file includes minutes: P1_24260A45.NQ0
                neq_base = f"P1_{doy:03d}{hour_char.upper()}{minutes}.NQ0"

                # Build full path
                if self.config.archive_organization == "yyyy/doy":
                    neq_dir = archive_path / str(year) / f"{doy:03d}" / session_name / "SOL"
                else:
                    neq_dir = archive_path / session_name / "SOL"

                neq_path_gz = neq_dir / f"{neq_base}.gz"
                neq_path = neq_dir / neq_base

                neq_info = NEQFileInfo(
                    file_path=neq_path_gz if neq_path_gz.exists() else neq_path,
                    session_name=session_name,
                    year=year,
                    doy=doy,
                    hour_char=hour_char,
                    minutes=minutes,
                    compressed=neq_path_gz.exists(),
                    exists=neq_path_gz.exists() or neq_path.exists(),
                )

                neq_files.append(neq_info)

                if self.verbose:
                    status = "available" if neq_info.exists else "MISSING"
                    print(f"  NEQ {session_name}/{minutes}: {neq_info.file_path} [{status}]")

        return neq_files

    def copy_neq_files_to_campaign(
        self,
        neq_files: list[NEQFileInfo],
        campaign_sol_dir: str | Path,
        decompress: bool = True,
    ) -> list[Path]:
        """Copy NEQ files to campaign SOL directory.

        Args:
            neq_files: List of NEQ files to copy
            campaign_sol_dir: Destination SOL directory
            decompress: Decompress gzipped files after copying

        Returns:
            List of paths to copied files in campaign directory
        """
        sol_dir = Path(campaign_sol_dir)
        sol_dir.mkdir(parents=True, exist_ok=True)

        copied_files = []

        for neq_info in neq_files:
            if not neq_info.exists:
                print(f"  Skipping missing NEQ: {neq_info.file_path}")
                continue

            # Destination filename
            dest_name = neq_info.base_name
            if neq_info.compressed:
                dest_name += ".gz"

            dest_path = sol_dir / dest_name

            # Copy file
            try:
                shutil.copy2(neq_info.file_path, dest_path)

                if self.verbose:
                    print(f"  Copied: {neq_info.file_path.name} -> {dest_path}")

                # Decompress if needed
                if decompress and neq_info.compressed:
                    decompressed_path = sol_dir / neq_info.base_name
                    with gzip.open(dest_path, "rb") as f_in:
                        with open(decompressed_path, "wb") as f_out:
                            shutil.copyfileobj(f_in, f_out)
                    dest_path.unlink()  # Remove compressed version
                    dest_path = decompressed_path

                    if self.verbose:
                        print(f"  Decompressed: {decompressed_path.name}")

                copied_files.append(dest_path)

            except Exception as e:
                print(f"  Error copying {neq_info.file_path}: {e}")

        return copied_files

    def get_stacking_summary(
        self,
        neq_files: list[NEQFileInfo],
    ) -> dict[str, Any]:
        """Get summary of NEQ stacking operation.

        Args:
            neq_files: List of NEQ files

        Returns:
            Summary dictionary with counts and status
        """
        available = [f for f in neq_files if f.exists]
        missing = [f for f in neq_files if not f.exists]

        return {
            "total_requested": len(neq_files),
            "available": len(available),
            "missing": len(missing),
            "available_files": [str(f.file_path) for f in available],
            "missing_files": [str(f.file_path) for f in missing],
            "name_scheme": str(self.config.name_scheme.value),
            "n_hours_to_stack": self.config.n_hours_to_stack,
        }


def create_neq_stacking_config(
    enabled: bool = True,
    n_hours: int = 4,
    name_scheme: str = "P1_yydoyU",
    archive_org: str = "yyyy/doy",
    session_suffix: str = "NR",
) -> NEQStackingConfig:
    """Convenience function to create NEQ stacking configuration.

    Args:
        enabled: Whether stacking is enabled
        n_hours: Number of hours to stack
        name_scheme: NEQ naming scheme
        archive_org: Archive directory organization
        session_suffix: Session directory suffix

    Returns:
        NEQStackingConfig instance
    """
    return NEQStackingConfig(
        enabled=enabled,
        n_hours_to_stack=n_hours,
        name_scheme=name_scheme,
        archive_organization=archive_org,
        session_suffix=session_suffix,
    )


# Default configurations for common use cases
NRDDP_TRO_STACKING = NEQStackingConfig(
    enabled=True,
    n_hours_to_stack=4,
    name_scheme=NEQNameScheme.HOURLY,
    archive_organization="yyyy/doy",
    session_suffix="NR",
)

NRDDP_TRO_SUBHOURLY_STACKING = NEQStackingConfig(
    enabled=True,
    n_hours_to_stack=4,
    name_scheme=NEQNameScheme.SUB_HOURLY,
    archive_organization="yyyy/doy",
    session_suffix="H",
)

# No stacking (for daily PPP)
NO_STACKING = NEQStackingConfig(enabled=False)
