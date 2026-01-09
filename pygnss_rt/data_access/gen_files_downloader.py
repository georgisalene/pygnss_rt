"""
Bernese GNSS Software (BSW) GEN files downloader.

Replaces Perl genFilesDownloader*.pm variants (6 files):
- genFilesDownloader.pm
- genFilesDownloader54.pm
- genFilesDownloader54_02.pm
- genFilesDownloader54_03.pm
- genFilesDownloader54_04.pm
- genFilesDownloader54_1.pm

Downloads BSW GEN configuration files from CODE FTP server.
"""

from __future__ import annotations

import os
import shutil
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path

from pygnss_rt.data_access.ftp_client import FTPClient
from pygnss_rt.data_access.ftp_config import FTPServerConfig, load_ftp_config
from pygnss_rt.utils.logging import get_logger, ignss_print, MessageType


logger = get_logger(__name__)


class BSWVersion(str, Enum):
    """Bernese Software version."""

    BSW52 = "52"
    BSW54 = "54"


@dataclass
class GENFileSpec:
    """Specification for a GEN file to download.

    Attributes:
        filename: Name of the file to download
        remote_dir: Remote directory (CONFIG or REF)
        copy_to_info: Whether to also copy to IGNSS/info directory
        year_suffix: If True, append current year to filename (e.g., SAT_{year}.CRX)
    """

    filename: str
    remote_dir: str = "CONFIG"
    copy_to_info: bool = True
    year_suffix: bool = False


@dataclass
class GENDownloadResult:
    """Result of downloading GEN files.

    Attributes:
        total_files: Number of files attempted
        downloaded: Number of files successfully downloaded
        failed: Number of files that failed to download
        copied_to_info: Number of files copied to info directory
        errors: List of error messages
    """

    total_files: int = 0
    downloaded: int = 0
    failed: int = 0
    copied_to_info: int = 0
    errors: list[str] = field(default_factory=list)

    @property
    def success_rate(self) -> float:
        """Calculate download success rate."""
        if self.total_files == 0:
            return 0.0
        return self.downloaded / self.total_files * 100


@dataclass
class GENDownloaderConfig:
    """Configuration for GEN files downloader.

    Attributes:
        bsw_version: Bernese Software version (52 or 54)
        ftp_host: FTP server hostname (default: CODE server)
        ftp_config_path: Path to FTP config XML (alternative to ftp_host)
        bern_dir: Bernese installation directory
        ignss_info_dir: i-GNSS info directory
        max_retries: Maximum FTP connection retry attempts
        timeout: FTP timeout in seconds
    """

    bsw_version: BSWVersion = BSWVersion.BSW54
    ftp_host: str = "ftp.aiub.unibe.ch"
    ftp_config_path: Path | None = None
    bern_dir: Path | None = None
    ignss_info_dir: Path | None = None
    max_retries: int = 3
    timeout: int = 60

    def __post_init__(self):
        """Set default directories from environment."""
        if self.bern_dir is None:
            home = Path(os.environ.get("HOME", ""))
            self.bern_dir = home / f"BERN{self.bsw_version.value}" / "GLOBAL" / "CONFIG"

        if self.ignss_info_dir is None:
            ignss = os.environ.get("IGNSS", "")
            if ignss:
                self.ignss_info_dir = Path(ignss) / "info"


# Default GEN files for BSW54
DEFAULT_CONFIG_FILES: list[GENFileSpec] = [
    GENFileSpec("DATUM.BSW", "CONFIG", copy_to_info=False),
    GENFileSpec("GPSUTC.BSW", "CONFIG", copy_to_info=False),
    GENFileSpec("OBSERV.SEL", "CONFIG", copy_to_info=True),
    GENFileSpec("SATELLIT_I20.SAT", "CONFIG", copy_to_info=False),
    GENFileSpec("FREQINFO.FRQ", "CONFIG", copy_to_info=False),
]

DEFAULT_REF_FILES: list[GENFileSpec] = [
    GENFileSpec("ANTENNA_I20.PCV", "REF", copy_to_info=True),
    GENFileSpec("I20.ATX", "REF", copy_to_info=True),
]

# SAT_YYYY.CRX is special - year suffix
SAT_CRX_FILE = GENFileSpec("SAT", "CONFIG", copy_to_info=False, year_suffix=True)


class GENFilesDownloader:
    """Downloader for BSW GEN configuration files.

    Downloads configuration files from CODE FTP server for Bernese GNSS Software.

    Files downloaded from CONFIG directory:
        - DATUM.BSW: Datum definitions
        - GPSUTC.BSW: GPS-UTC time conversion
        - OBSERV.SEL: Observable selection
        - SATELLIT_I20.SAT: Satellite information (IGS20)
        - FREQINFO.FRQ: Frequency information
        - SAT_YYYY.CRX: Satellite CRX file for current year

    Files downloaded from REF directory:
        - ANTENNA_I20.PCV: Antenna phase center variations (IGS20)
        - I20.ATX: Antenna calibrations (IGS20)

    Usage:
        from pygnss_rt.data_access import GENFilesDownloader, GENDownloaderConfig

        # Use default configuration
        downloader = GENFilesDownloader()
        result = downloader.download_all()

        # Or with custom configuration
        config = GENDownloaderConfig(
            bsw_version=BSWVersion.BSW54,
            bern_dir=Path("/path/to/BERN54/GLOBAL/CONFIG"),
        )
        downloader = GENFilesDownloader(config)
        result = downloader.download_all()

    Example:
        >>> downloader = GENFilesDownloader()
        >>> result = downloader.download_all()
        >>> print(f"Downloaded {result.downloaded}/{result.total_files} files")
    """

    def __init__(self, config: GENDownloaderConfig | None = None):
        """Initialize GEN files downloader.

        Args:
            config: Downloader configuration. Uses defaults if not provided.
        """
        self.config = config or GENDownloaderConfig()
        self._ftp: FTPClient | None = None
        self._current_year = datetime.now().year

        # Build remote directory paths based on BSW version
        bsw_user = f"BSWUSER{self.config.bsw_version.value}"
        self._config_dir = f"/{bsw_user}/CONFIG"
        self._ref_dir = f"/{bsw_user}/REF"

    def _get_ftp_host(self) -> str:
        """Get FTP host from config or XML file."""
        if self.config.ftp_config_path and self.config.ftp_config_path.exists():
            try:
                ftp_configs = load_ftp_config(self.config.ftp_config_path)
                for cfg in ftp_configs:
                    if cfg.id == "CODE":
                        return cfg.host
            except Exception as e:
                logger.warning("Failed to load FTP config", error=str(e))

        return self.config.ftp_host

    def _connect(self) -> bool:
        """Connect to FTP server with retry logic.

        Returns:
            True if connection successful
        """
        host = self._get_ftp_host()

        for attempt in range(1, self.config.max_retries + 1):
            try:
                self._ftp = FTPClient(
                    host=host,
                    username="anonymous",
                    password="",
                    timeout=self.config.timeout,
                    passive=True,
                )
                self._ftp.connect()
                logger.info("Connected to CODE FTP server", host=host)
                return True

            except Exception as e:
                logger.warning(
                    "FTP connection attempt failed",
                    attempt=attempt,
                    max_retries=self.config.max_retries,
                    error=str(e),
                )
                if attempt < self.config.max_retries:
                    import time
                    time.sleep(5)  # Wait before retry

        return False

    def _disconnect(self) -> None:
        """Disconnect from FTP server."""
        if self._ftp:
            try:
                self._ftp.disconnect()
            except Exception:
                pass
            self._ftp = None

    def _validate_directories(self) -> list[str]:
        """Validate that destination directories exist.

        Returns:
            List of error messages (empty if all valid)
        """
        errors = []

        if self.config.bern_dir and not self.config.bern_dir.exists():
            errors.append(f"Bernese CONFIG directory does not exist: {self.config.bern_dir}")

        if self.config.ignss_info_dir and not self.config.ignss_info_dir.exists():
            errors.append(f"i-GNSS info directory does not exist: {self.config.ignss_info_dir}")

        return errors

    def _get_filename(self, spec: GENFileSpec) -> str:
        """Get actual filename, handling year suffix.

        Args:
            spec: File specification

        Returns:
            Actual filename to download
        """
        if spec.year_suffix:
            return f"{spec.filename}_{self._current_year}.CRX"
        return spec.filename

    def _download_file(
        self,
        spec: GENFileSpec,
        result: GENDownloadResult,
    ) -> bool:
        """Download a single GEN file.

        Args:
            spec: File specification
            result: Result object to update

        Returns:
            True if download successful
        """
        if not self._ftp:
            return False

        filename = self._get_filename(spec)
        remote_dir = self._config_dir if spec.remote_dir == "CONFIG" else self._ref_dir
        remote_path = f"{remote_dir}/{filename}"

        # Download to temp location first
        temp_path = Path(filename)

        try:
            success = self._ftp.download(remote_path, temp_path)

            if not success:
                result.failed += 1
                result.errors.append(f"Failed to download: {filename}")
                return False

            # Move to Bernese CONFIG directory
            if self.config.bern_dir:
                dest_path = self.config.bern_dir / filename
                shutil.move(str(temp_path), str(dest_path))
                logger.info("Moved file to Bernese CONFIG", file=filename)

                # Copy to info directory if needed
                if spec.copy_to_info and self.config.ignss_info_dir:
                    info_path = self.config.ignss_info_dir / filename
                    shutil.copy2(str(dest_path), str(info_path))
                    result.copied_to_info += 1
                    logger.info("Copied file to info directory", file=filename)

            result.downloaded += 1
            return True

        except Exception as e:
            result.failed += 1
            result.errors.append(f"Error downloading {filename}: {e}")
            logger.error("File download error", file=filename, error=str(e))

            # Clean up temp file if it exists
            if temp_path.exists():
                temp_path.unlink()

            return False

    def download_all(self) -> GENDownloadResult:
        """Download all GEN files.

        Returns:
            Download result with statistics
        """
        result = GENDownloadResult()

        # Validate directories
        dir_errors = self._validate_directories()
        if dir_errors:
            result.errors.extend(dir_errors)
            ignss_print(MessageType.FATAL, "Directory validation failed")
            for err in dir_errors:
                ignss_print(MessageType.LIST, err)
            return result

        # Connect to FTP
        if not self._connect():
            result.errors.append("Failed to connect to FTP server after retries")
            ignss_print(MessageType.FATAL, "Cannot connect to CODE FTP server")
            return result

        try:
            ignss_print(MessageType.INFO, "Starting GEN files download")

            # Download CONFIG files
            all_files = DEFAULT_CONFIG_FILES + [SAT_CRX_FILE] + DEFAULT_REF_FILES
            result.total_files = len(all_files)

            for spec in all_files:
                self._download_file(spec, result)

            # Print summary
            if result.failed == 0:
                ignss_print(
                    MessageType.INFO,
                    f"All {result.downloaded} GEN files downloaded successfully",
                )
            else:
                ignss_print(
                    MessageType.WARNING,
                    f"Downloaded {result.downloaded}/{result.total_files} files "
                    f"({result.failed} failed)",
                )
                for err in result.errors:
                    ignss_print(MessageType.LIST, err)

        finally:
            self._disconnect()

        return result

    def download_config_files(self) -> GENDownloadResult:
        """Download only CONFIG directory files.

        Returns:
            Download result with statistics
        """
        result = GENDownloadResult()

        if not self._connect():
            result.errors.append("Failed to connect to FTP server")
            return result

        try:
            files = DEFAULT_CONFIG_FILES + [SAT_CRX_FILE]
            result.total_files = len(files)

            for spec in files:
                self._download_file(spec, result)

        finally:
            self._disconnect()

        return result

    def download_ref_files(self) -> GENDownloadResult:
        """Download only REF directory files (antenna files).

        Returns:
            Download result with statistics
        """
        result = GENDownloadResult()

        if not self._connect():
            result.errors.append("Failed to connect to FTP server")
            return result

        try:
            result.total_files = len(DEFAULT_REF_FILES)

            for spec in DEFAULT_REF_FILES:
                self._download_file(spec, result)

        finally:
            self._disconnect()

        return result


# =============================================================================
# Convenience Functions
# =============================================================================

def download_gen_files(
    bsw_version: str = "54",
    bern_dir: Path | str | None = None,
    info_dir: Path | str | None = None,
    verbose: bool = False,
) -> GENDownloadResult:
    """Download all BSW GEN files.

    Convenience function for quick GEN file downloads.

    Args:
        bsw_version: Bernese Software version ("52" or "54")
        bern_dir: Optional Bernese CONFIG directory override
        info_dir: Optional i-GNSS info directory override
        verbose: Print progress messages

    Returns:
        Download result

    Example:
        >>> result = download_gen_files(bsw_version="54")
        >>> print(f"Success rate: {result.success_rate:.1f}%")
    """
    version = BSWVersion(bsw_version)

    config = GENDownloaderConfig(
        bsw_version=version,
        bern_dir=Path(bern_dir) if bern_dir else None,
        ignss_info_dir=Path(info_dir) if info_dir else None,
    )

    downloader = GENFilesDownloader(config)
    return downloader.download_all()


def download_antenna_files(
    bsw_version: str = "54",
    bern_dir: Path | str | None = None,
) -> GENDownloadResult:
    """Download only antenna-related files (PCV, ATX).

    Args:
        bsw_version: Bernese Software version
        bern_dir: Optional Bernese CONFIG directory override

    Returns:
        Download result
    """
    version = BSWVersion(bsw_version)

    config = GENDownloaderConfig(
        bsw_version=version,
        bern_dir=Path(bern_dir) if bern_dir else None,
    )

    downloader = GENFilesDownloader(config)
    return downloader.download_ref_files()


# =============================================================================
# CLI Entry Point
# =============================================================================

def main() -> int:
    """CLI entry point for GEN files downloader.

    Returns:
        Exit code (0 for success, 1 for failure)
    """
    import argparse

    parser = argparse.ArgumentParser(
        description="Download BSW GEN files from CODE FTP server",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s                     # Download all GEN files
  %(prog)s --version 54        # Download for BSW54
  %(prog)s --config-only       # Download only CONFIG files
  %(prog)s --ref-only          # Download only REF files (antenna)
        """,
    )

    parser.add_argument(
        "--version", "-v",
        choices=["52", "54"],
        default="54",
        help="Bernese Software version (default: 54)",
    )
    parser.add_argument(
        "--bern-dir",
        type=Path,
        help="Bernese CONFIG directory",
    )
    parser.add_argument(
        "--info-dir",
        type=Path,
        help="i-GNSS info directory",
    )
    parser.add_argument(
        "--config-only",
        action="store_true",
        help="Download only CONFIG directory files",
    )
    parser.add_argument(
        "--ref-only",
        action="store_true",
        help="Download only REF directory files",
    )
    parser.add_argument(
        "--quiet", "-q",
        action="store_true",
        help="Suppress output messages",
    )

    args = parser.parse_args()

    # Build configuration
    config = GENDownloaderConfig(
        bsw_version=BSWVersion(args.version),
        bern_dir=args.bern_dir,
        ignss_info_dir=args.info_dir,
    )

    downloader = GENFilesDownloader(config)

    # Run appropriate download
    if args.config_only:
        result = downloader.download_config_files()
    elif args.ref_only:
        result = downloader.download_ref_files()
    else:
        result = downloader.download_all()

    # Print summary unless quiet
    if not args.quiet:
        print(f"\nDownload Summary:")
        print(f"  Total files: {result.total_files}")
        print(f"  Downloaded:  {result.downloaded}")
        print(f"  Failed:      {result.failed}")
        print(f"  Copied to info: {result.copied_to_info}")
        print(f"  Success rate: {result.success_rate:.1f}%")

        if result.errors:
            print(f"\nErrors:")
            for err in result.errors:
                print(f"  - {err}")

    return 0 if result.failed == 0 else 1


if __name__ == "__main__":
    import sys
    sys.exit(main())
