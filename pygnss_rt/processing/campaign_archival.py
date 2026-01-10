"""
Campaign Archival Module (DCM - Data Campaign Management).

Provides functionality for managing Bernese GNSS Software campaigns:
- Campaign directory cleanup
- Campaign compression (gzip/compress)
- Campaign archival to organized directory structure
- Campaign restoration from archive

Replaces Perl IGNSS::dcm, IGNSS::clean_campaign, IGNSS::compress_campaign,
IGNSS::move_campaign functionality.

Usage:
    from pygnss_rt.processing.campaign_archival import (
        CampaignArchiver,
        CampaignArchiveConfig,
        archive_campaign,
    )

    # Quick archival
    result = archive_campaign(
        session="TEST2024",
        campaign_dir="/data/GPSDATA/CAMPAIGN54",
        archive_dir="/data/ARCHIVE",
    )

    # Detailed configuration
    config = CampaignArchiveConfig(
        session="TEST2024",
        campaign_dir=Path("/data/GPSDATA/CAMPAIGN54"),
        archive_dir=Path("/data/ARCHIVE"),
        organization="yyyy/doy",
        compression_method="gzip",
        directories_to_clean=["BPE", "OUT", "SOL"],
    )
    archiver = CampaignArchiver(config)
    result = archiver.archive()
"""

from __future__ import annotations

import gzip
import logging
import os
import shutil
import subprocess
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Optional

logger = logging.getLogger(__name__)


class CompressionMethod(str, Enum):
    """Compression methods for campaign archival."""

    GZIP = "gzip"
    COMPRESS = "compress"  # Unix compress (.Z)
    NONE = "none"


class ArchiveOrganization(str, Enum):
    """Archive directory organization schemes."""

    YYYY_DOY = "yyyy/doy"  # Year/DOY subdirectories
    YYYY_MM = "yyyy/mm"    # Year/Month subdirectories
    YYYY = "yyyy"          # Year only
    FLAT = "flat"          # No subdirectories


class ArchiveStatus(str, Enum):
    """Campaign archive operation status."""

    SUCCESS = "success"
    PARTIAL = "partial"
    FAILED = "failed"
    SKIPPED = "skipped"


# Default directories to clean before archival (large intermediate files)
DEFAULT_DIRS_TO_CLEAN = [
    "BPE",      # BPE processing files
    "OUT",      # Output files (can be regenerated)
    "SOL",      # Solution files (keep in some cases)
]

# Files to remove from RAW directory before archival
DEFAULT_RAW_PATTERNS_TO_CLEAN = [
    "*.SMT",    # RINEX summary files
    "*.RNX",    # Uncompressed RINEX (keep compressed)
    "*O",       # Uncompressed observation files
    "*.0",      # Hourly files
]


@dataclass
class CampaignArchiveConfig:
    """Configuration for campaign archival."""

    session: str  # Campaign session name
    campaign_dir: Path  # Base campaign directory ($P)
    archive_dir: Path  # Archive destination directory

    # Date information (for organization)
    year: Optional[int] = None
    doy: Optional[int] = None
    month: Optional[int] = None

    # Organization and compression
    organization: ArchiveOrganization = ArchiveOrganization.YYYY_DOY
    compression_method: CompressionMethod = CompressionMethod.GZIP

    # Cleanup configuration
    directories_to_clean: list[str] = field(default_factory=lambda: DEFAULT_DIRS_TO_CLEAN.copy())
    raw_patterns_to_clean: list[str] = field(default_factory=lambda: DEFAULT_RAW_PATTERNS_TO_CLEAN.copy())
    clean_raw: bool = True

    # Behavior options
    replace_existing: bool = True  # Replace if archive exists
    dry_run: bool = False  # Don't actually perform operations


@dataclass
class CampaignArchiveResult:
    """Result of campaign archival operation."""

    status: ArchiveStatus
    session: str
    source_path: Path
    archive_path: Optional[Path] = None

    # Statistics
    files_compressed: int = 0
    files_removed: int = 0
    bytes_before: int = 0
    bytes_after: int = 0

    # Timing
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None

    # Issues
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    @property
    def compression_ratio(self) -> float:
        """Calculate compression ratio."""
        if self.bytes_before == 0:
            return 0.0
        return 1.0 - (self.bytes_after / self.bytes_before)

    @property
    def duration_seconds(self) -> float:
        """Get operation duration in seconds."""
        if self.start_time and self.end_time:
            return (self.end_time - self.start_time).total_seconds()
        return 0.0

    def summary(self) -> str:
        """Generate summary string."""
        lines = [
            f"Campaign Archival: {self.session}",
            f"  Status: {self.status.value}",
            f"  Source: {self.source_path}",
            f"  Archive: {self.archive_path or 'N/A'}",
            f"  Files compressed: {self.files_compressed}",
            f"  Files removed: {self.files_removed}",
            f"  Size before: {self.bytes_before / 1024 / 1024:.1f} MB",
            f"  Size after: {self.bytes_after / 1024 / 1024:.1f} MB",
            f"  Compression: {self.compression_ratio * 100:.1f}%",
            f"  Duration: {self.duration_seconds:.1f}s",
        ]

        if self.errors:
            lines.append("  Errors:")
            for err in self.errors:
                lines.append(f"    - {err}")

        if self.warnings:
            lines.append("  Warnings:")
            for warn in self.warnings:
                lines.append(f"    - {warn}")

        return "\n".join(lines)


class CampaignArchiver:
    """
    Campaign archival manager.

    Handles cleanup, compression, and archival of Bernese GNSS campaigns.
    """

    def __init__(self, config: CampaignArchiveConfig):
        """Initialize archiver with configuration.

        Args:
            config: Archival configuration
        """
        self.config = config
        self._result: Optional[CampaignArchiveResult] = None

    @property
    def session_path(self) -> Path:
        """Get full path to campaign session directory."""
        return self.config.campaign_dir / self.config.session

    def archive(self) -> CampaignArchiveResult:
        """Perform full campaign archival.

        Steps:
        1. Validate campaign exists
        2. Clean specified directories
        3. Clean RAW directory
        4. Compress campaign files
        5. Move to archive location

        Returns:
            CampaignArchiveResult with operation details
        """
        self._result = CampaignArchiveResult(
            status=ArchiveStatus.SUCCESS,
            session=self.config.session,
            source_path=self.session_path,
            start_time=datetime.now(),
        )

        try:
            # Validate
            if not self._validate():
                self._result.status = ArchiveStatus.FAILED
                return self._result

            # Calculate initial size
            self._result.bytes_before = self._get_directory_size(self.session_path)

            # Clean directories
            self._clean_directories()

            # Clean RAW directory
            if self.config.clean_raw:
                self._clean_raw_directory()

            # Compress
            self._compress_campaign()

            # Calculate size after compression
            self._result.bytes_after = self._get_directory_size(self.session_path)

            # Move to archive
            archive_path = self._move_to_archive()
            self._result.archive_path = archive_path

            # Check for partial success
            if self._result.errors:
                self._result.status = ArchiveStatus.PARTIAL

        except Exception as e:
            logger.exception(f"Campaign archival failed: {e}")
            self._result.errors.append(str(e))
            self._result.status = ArchiveStatus.FAILED

        self._result.end_time = datetime.now()
        return self._result

    def clean_only(self) -> CampaignArchiveResult:
        """Clean campaign without archiving.

        Returns:
            CampaignArchiveResult
        """
        self._result = CampaignArchiveResult(
            status=ArchiveStatus.SUCCESS,
            session=self.config.session,
            source_path=self.session_path,
            start_time=datetime.now(),
        )

        try:
            if not self._validate():
                self._result.status = ArchiveStatus.FAILED
                return self._result

            self._result.bytes_before = self._get_directory_size(self.session_path)
            self._clean_directories()

            if self.config.clean_raw:
                self._clean_raw_directory()

            self._result.bytes_after = self._get_directory_size(self.session_path)

        except Exception as e:
            logger.exception(f"Campaign cleanup failed: {e}")
            self._result.errors.append(str(e))
            self._result.status = ArchiveStatus.FAILED

        self._result.end_time = datetime.now()
        return self._result

    def compress_only(self) -> CampaignArchiveResult:
        """Compress campaign without moving.

        Returns:
            CampaignArchiveResult
        """
        self._result = CampaignArchiveResult(
            status=ArchiveStatus.SUCCESS,
            session=self.config.session,
            source_path=self.session_path,
            start_time=datetime.now(),
        )

        try:
            if not self._validate():
                self._result.status = ArchiveStatus.FAILED
                return self._result

            self._result.bytes_before = self._get_directory_size(self.session_path)
            self._compress_campaign()
            self._result.bytes_after = self._get_directory_size(self.session_path)

        except Exception as e:
            logger.exception(f"Campaign compression failed: {e}")
            self._result.errors.append(str(e))
            self._result.status = ArchiveStatus.FAILED

        self._result.end_time = datetime.now()
        return self._result

    def _validate(self) -> bool:
        """Validate configuration and paths."""
        if not self.session_path.exists():
            self._result.errors.append(f"Campaign directory not found: {self.session_path}")
            return False

        if not self.session_path.is_dir():
            self._result.errors.append(f"Not a directory: {self.session_path}")
            return False

        return True

    def _clean_directories(self) -> None:
        """Clean specified directories."""
        for dir_name in self.config.directories_to_clean:
            dir_path = self.session_path / dir_name

            if not dir_path.exists():
                continue

            if self.config.dry_run:
                logger.info(f"[DRY RUN] Would remove: {dir_path}")
                continue

            try:
                # Count files before removal
                file_count = sum(1 for _ in dir_path.rglob("*") if _.is_file())

                shutil.rmtree(dir_path)
                self._result.files_removed += file_count

                logger.info(f"Removed directory: {dir_path} ({file_count} files)")

            except Exception as e:
                self._result.warnings.append(f"Could not remove {dir_path}: {e}")
                logger.warning(f"Failed to remove {dir_path}: {e}")

    def _clean_raw_directory(self) -> None:
        """Clean temporary files from RAW directory."""
        raw_dir = self.session_path / "RAW"

        if not raw_dir.exists():
            return

        for pattern in self.config.raw_patterns_to_clean:
            for filepath in raw_dir.glob(pattern):
                if self.config.dry_run:
                    logger.info(f"[DRY RUN] Would remove: {filepath}")
                    continue

                try:
                    filepath.unlink()
                    self._result.files_removed += 1
                except Exception as e:
                    self._result.warnings.append(f"Could not remove {filepath}: {e}")

    def _compress_campaign(self) -> None:
        """Compress campaign files."""
        if self.config.compression_method == CompressionMethod.NONE:
            return

        if self.config.dry_run:
            logger.info(f"[DRY RUN] Would compress: {self.session_path}")
            return

        if self.config.compression_method == CompressionMethod.GZIP:
            self._compress_with_gzip()
        elif self.config.compression_method == CompressionMethod.COMPRESS:
            self._compress_with_compress()

    def _compress_with_gzip(self) -> None:
        """Compress files using gzip."""
        # Find all uncompressed files
        for filepath in self.session_path.rglob("*"):
            if not filepath.is_file():
                continue

            # Skip already compressed files
            if filepath.suffix in (".gz", ".Z", ".zip", ".bz2"):
                continue

            # Skip small files
            if filepath.stat().st_size < 1024:  # Skip < 1KB
                continue

            try:
                compressed_path = Path(str(filepath) + ".gz")

                with open(filepath, "rb") as f_in:
                    with gzip.open(compressed_path, "wb") as f_out:
                        shutil.copyfileobj(f_in, f_out)

                # Remove original
                filepath.unlink()
                self._result.files_compressed += 1

            except Exception as e:
                self._result.warnings.append(f"Failed to compress {filepath}: {e}")

    def _compress_with_compress(self) -> None:
        """Compress files using Unix compress command."""
        try:
            cmd = ["compress", "-r", str(self.session_path)]
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
            )

            if result.returncode != 0:
                self._result.warnings.append(f"Compress warning: {result.stderr}")

        except FileNotFoundError:
            self._result.errors.append("compress command not found, using gzip instead")
            self._compress_with_gzip()

    def _move_to_archive(self) -> Optional[Path]:
        """Move campaign to archive location.

        Returns:
            Archive path if successful
        """
        # Determine archive subdirectory based on organization
        archive_subdir = self._get_archive_subdir()
        archive_base = self.config.archive_dir / archive_subdir

        if self.config.dry_run:
            logger.info(f"[DRY RUN] Would move to: {archive_base / self.config.session}")
            return archive_base / self.config.session

        # Create archive directory
        archive_base.mkdir(parents=True, exist_ok=True)

        destination = archive_base / self.config.session

        # Handle existing archive
        if destination.exists():
            if self.config.replace_existing:
                self._result.warnings.append(f"Replacing existing archive: {destination}")
                shutil.rmtree(destination)
            else:
                self._result.errors.append(f"Archive already exists: {destination}")
                return None

        # Move campaign
        try:
            shutil.move(str(self.session_path), str(destination))
            logger.info(f"Campaign archived to: {destination}")
            return destination

        except Exception as e:
            self._result.errors.append(f"Failed to move campaign: {e}")
            return None

    def _get_archive_subdir(self) -> str:
        """Get archive subdirectory based on organization scheme."""
        year = self.config.year or datetime.now().year
        doy = self.config.doy or 1
        month = self.config.month or 1

        if self.config.organization == ArchiveOrganization.YYYY_DOY:
            return f"{year}/{doy:03d}"
        elif self.config.organization == ArchiveOrganization.YYYY_MM:
            return f"{year}/{month:02d}"
        elif self.config.organization == ArchiveOrganization.YYYY:
            return str(year)
        else:  # FLAT
            return ""

    def _get_directory_size(self, path: Path) -> int:
        """Calculate total size of directory in bytes."""
        total = 0
        try:
            for filepath in path.rglob("*"):
                if filepath.is_file():
                    total += filepath.stat().st_size
        except Exception:
            pass
        return total


# =============================================================================
# Campaign Restoration
# =============================================================================

@dataclass
class CampaignRestoreConfig:
    """Configuration for campaign restoration."""

    session: str
    archive_dir: Path  # Archive location
    restore_dir: Path  # Destination for restoration ($P)

    # Decompression
    decompress: bool = True

    # Options
    overwrite_existing: bool = False


def restore_campaign(config: CampaignRestoreConfig) -> CampaignArchiveResult:
    """Restore a campaign from archive.

    Args:
        config: Restoration configuration

    Returns:
        CampaignArchiveResult with operation details
    """
    result = CampaignArchiveResult(
        status=ArchiveStatus.SUCCESS,
        session=config.session,
        source_path=config.archive_dir / config.session,
        start_time=datetime.now(),
    )

    archive_path = config.archive_dir / config.session
    restore_path = config.restore_dir / config.session

    try:
        # Validate archive exists
        if not archive_path.exists():
            result.errors.append(f"Archive not found: {archive_path}")
            result.status = ArchiveStatus.FAILED
            return result

        # Check destination
        if restore_path.exists():
            if config.overwrite_existing:
                result.warnings.append(f"Overwriting existing: {restore_path}")
                shutil.rmtree(restore_path)
            else:
                result.errors.append(f"Destination exists: {restore_path}")
                result.status = ArchiveStatus.FAILED
                return result

        # Copy from archive
        shutil.copytree(archive_path, restore_path)
        result.archive_path = restore_path

        # Decompress if needed
        if config.decompress:
            _decompress_directory(restore_path)

        result.bytes_after = _get_directory_size(restore_path)

    except Exception as e:
        result.errors.append(str(e))
        result.status = ArchiveStatus.FAILED

    result.end_time = datetime.now()
    return result


def _decompress_directory(path: Path) -> int:
    """Decompress all compressed files in directory.

    Returns:
        Number of files decompressed
    """
    count = 0

    for filepath in path.rglob("*.gz"):
        try:
            output_path = filepath.with_suffix("")

            with gzip.open(filepath, "rb") as f_in:
                with open(output_path, "wb") as f_out:
                    shutil.copyfileobj(f_in, f_out)

            filepath.unlink()
            count += 1

        except Exception as e:
            logger.warning(f"Failed to decompress {filepath}: {e}")

    # Also handle .Z files
    for filepath in path.rglob("*.Z"):
        try:
            subprocess.run(
                ["uncompress", str(filepath)],
                capture_output=True,
                check=True,
            )
            count += 1
        except Exception as e:
            logger.warning(f"Failed to uncompress {filepath}: {e}")

    return count


def _get_directory_size(path: Path) -> int:
    """Calculate total size of directory."""
    total = 0
    for filepath in path.rglob("*"):
        if filepath.is_file():
            total += filepath.stat().st_size
    return total


# =============================================================================
# Convenience Functions
# =============================================================================

def archive_campaign(
    session: str,
    campaign_dir: str | Path,
    archive_dir: str | Path,
    year: Optional[int] = None,
    doy: Optional[int] = None,
    organization: str = "yyyy/doy",
    compression: str = "gzip",
    clean_dirs: Optional[list[str]] = None,
    dry_run: bool = False,
) -> CampaignArchiveResult:
    """Archive a campaign with simplified interface.

    Args:
        session: Campaign session name
        campaign_dir: Base campaign directory ($P)
        archive_dir: Archive destination
        year: Year for organization (auto-detected if None)
        doy: Day of year for organization
        organization: Archive organization scheme
        compression: Compression method ('gzip', 'compress', 'none')
        clean_dirs: Directories to clean before archiving
        dry_run: If True, don't actually perform operations

    Returns:
        CampaignArchiveResult
    """
    config = CampaignArchiveConfig(
        session=session,
        campaign_dir=Path(campaign_dir),
        archive_dir=Path(archive_dir),
        year=year,
        doy=doy,
        organization=ArchiveOrganization(organization),
        compression_method=CompressionMethod(compression),
        directories_to_clean=clean_dirs or DEFAULT_DIRS_TO_CLEAN.copy(),
        dry_run=dry_run,
    )

    archiver = CampaignArchiver(config)
    return archiver.archive()


def clean_campaign(
    session: str,
    campaign_dir: str | Path,
    directories: Optional[list[str]] = None,
    clean_raw: bool = True,
) -> CampaignArchiveResult:
    """Clean campaign directories.

    Args:
        session: Campaign session name
        campaign_dir: Base campaign directory
        directories: Directories to clean
        clean_raw: Clean RAW directory files

    Returns:
        CampaignArchiveResult
    """
    config = CampaignArchiveConfig(
        session=session,
        campaign_dir=Path(campaign_dir),
        archive_dir=Path("/tmp"),  # Not used for clean only
        directories_to_clean=directories or DEFAULT_DIRS_TO_CLEAN.copy(),
        clean_raw=clean_raw,
    )

    archiver = CampaignArchiver(config)
    return archiver.clean_only()


def compress_campaign(
    session: str,
    campaign_dir: str | Path,
    method: str = "gzip",
) -> CampaignArchiveResult:
    """Compress campaign files.

    Args:
        session: Campaign session name
        campaign_dir: Base campaign directory
        method: Compression method

    Returns:
        CampaignArchiveResult
    """
    config = CampaignArchiveConfig(
        session=session,
        campaign_dir=Path(campaign_dir),
        archive_dir=Path("/tmp"),  # Not used
        compression_method=CompressionMethod(method),
    )

    archiver = CampaignArchiver(config)
    return archiver.compress_only()


def list_archived_campaigns(
    archive_dir: str | Path,
    year: Optional[int] = None,
) -> list[dict[str, Any]]:
    """List archived campaigns.

    Args:
        archive_dir: Archive base directory
        year: Filter by year

    Returns:
        List of campaign info dictionaries
    """
    archive_path = Path(archive_dir)
    campaigns = []

    if not archive_path.exists():
        return campaigns

    # Search for campaigns
    search_path = archive_path / str(year) if year else archive_path

    for path in search_path.rglob("*"):
        if path.is_dir():
            # Check if it looks like a campaign directory
            if (path / "RAW").exists() or (path / "STA").exists():
                # Calculate size
                size = _get_directory_size(path)

                # Try to determine date from path
                rel_path = path.relative_to(archive_path)
                parts = rel_path.parts

                campaign_year = None
                campaign_doy = None

                if len(parts) >= 2:
                    try:
                        campaign_year = int(parts[0])
                        campaign_doy = int(parts[1])
                    except ValueError:
                        pass

                campaigns.append({
                    "session": path.name,
                    "path": str(path),
                    "year": campaign_year,
                    "doy": campaign_doy,
                    "size_mb": size / 1024 / 1024,
                    "is_compressed": any(path.rglob("*.gz")) or any(path.rglob("*.Z")),
                })

    return sorted(campaigns, key=lambda x: (x.get("year") or 0, x.get("doy") or 0))
