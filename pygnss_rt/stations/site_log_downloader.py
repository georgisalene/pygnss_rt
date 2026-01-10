"""
Site Log Downloader - Port of FTPSiteLog.pm from i-BSWSTA.

Downloads IGS-format site log files (.log) from FTP/SFTP servers
for station metadata maintenance.

Site logs contain station equipment history (receivers, antennas,
eccentricities) needed to generate Bernese .STA files.

Usage:
    from pygnss_rt.stations.site_log_downloader import SiteLogDownloader

    downloader = SiteLogDownloader()
    results = downloader.download(
        source="IGS",
        destination="/data/sitelogs",
        station_filter=["algo", "nrc1", "dubo"],
    )
"""

from __future__ import annotations

import ftplib
import logging
import re
import shutil
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass
class SiteLogSource:
    """Configuration for a site log source (FTP/SFTP server).

    Port of source configuration from call_autoSta_*.pl scripts.
    """

    name: str  # Source identifier
    host: str  # FTP/SFTP server hostname
    protocol: str = "ftp"  # ftp or sftp
    username: str = "anonymous"
    password: str = ""
    port: int = 21
    remote_dir: str = "/"  # Remote directory containing .log files
    passive: bool = True
    timeout: int = 60
    file_pattern: str = r".*\.log$"  # Regex for matching site log files


@dataclass
class SiteLogDownloadResult:
    """Result of a site log download operation."""

    source: str
    total_files: int = 0
    downloaded: int = 0
    skipped: int = 0  # Already existed
    failed: int = 0
    filtered_out: int = 0  # Excluded by filter
    files_downloaded: list[str] = field(default_factory=list)
    files_failed: list[str] = field(default_factory=list)
    duration_seconds: float = 0.0
    errors: list[str] = field(default_factory=list)


# Pre-defined sources matching i-BSWSTA scripts
IGS_SITE_LOG_SOURCE = SiteLogSource(
    name="IGS",
    host="files.igs.org",
    protocol="ftp",
    username="anonymous",
    password="anonymous@",
    remote_dir="/pub/station/log",
    file_pattern=r".*\.log$",
)

IGS_HISTORICAL_SOURCE = SiteLogSource(
    name="IGS_HISTORICAL",
    host="files.igs.org",
    protocol="ftp",
    username="anonymous",
    password="anonymous@",
    remote_dir="/pub/station/log_archive",
    file_pattern=r".*\.log$",
)

EUREF_SITE_LOG_SOURCE = SiteLogSource(
    name="EUREF",
    host="epncb.oma.be",
    protocol="ftp",
    username="anonymous",
    password="anonymous@",
    remote_dir="/pub/station/log",
    file_pattern=r".*\.log$",
)

OSGB_SITE_LOG_SOURCE = SiteLogSource(
    name="OSGB",
    host="gnss.ordnancesurvey.co.uk",
    protocol="sftp",
    username="",  # Requires authentication
    password="",
    port=22,
    remote_dir="/sitelogs",
    file_pattern=r".*\.log$",
)

# Default sources dictionary
DEFAULT_SITE_LOG_SOURCES: dict[str, SiteLogSource] = {
    "IGS": IGS_SITE_LOG_SOURCE,
    "IGS_HISTORICAL": IGS_HISTORICAL_SOURCE,
    "EUREF": EUREF_SITE_LOG_SOURCE,
    "OSGB": OSGB_SITE_LOG_SOURCE,
}


class SiteLogDownloader:
    """Downloads IGS site log files from FTP/SFTP servers.

    Port of FTPSiteLog.pm from i-BSWSTA.

    Features:
    - Download from multiple sources (IGS, EUREF, OSGB)
    - Filter by station list (include/exclude)
    - Remove duplicates (keep latest version)
    - Skip already downloaded files
    """

    def __init__(
        self,
        sources: dict[str, SiteLogSource] | None = None,
        bad_stations: list[str] | None = None,
        verbose: bool = False,
    ):
        """Initialize site log downloader.

        Args:
            sources: Dictionary of available sources (uses defaults if None)
            bad_stations: List of station IDs to always exclude
            verbose: Enable verbose logging
        """
        self.sources = sources or DEFAULT_SITE_LOG_SOURCES.copy()
        self.bad_stations = set(s.lower() for s in (bad_stations or []))
        self.verbose = verbose
        self._ftp: Optional[ftplib.FTP] = None

    def download(
        self,
        source: str | SiteLogSource,
        destination: str | Path,
        station_filter: list[str] | None = None,
        exclude_stations: list[str] | None = None,
        overwrite: bool = False,
        remove_duplicates: bool = True,
    ) -> SiteLogDownloadResult:
        """Download site logs from a source.

        Args:
            source: Source name or SiteLogSource config
            destination: Local directory for downloaded files
            station_filter: Only download these stations (None = all)
            exclude_stations: Exclude these stations
            overwrite: Overwrite existing files
            remove_duplicates: Keep only latest version per station

        Returns:
            SiteLogDownloadResult with statistics
        """
        start_time = datetime.now()

        # Resolve source config
        if isinstance(source, str):
            if source not in self.sources:
                raise ValueError(f"Unknown source: {source}. Available: {list(self.sources.keys())}")
            source_config = self.sources[source]
        else:
            source_config = source

        destination = Path(destination)
        destination.mkdir(parents=True, exist_ok=True)

        result = SiteLogDownloadResult(source=source_config.name)

        # Build station filter set
        include_stations = None
        if station_filter:
            include_stations = set(s.lower()[:4] for s in station_filter)

        exclude_set = set(s.lower() for s in (exclude_stations or []))
        exclude_set.update(self.bad_stations)

        try:
            if source_config.protocol == "ftp":
                self._download_ftp(
                    source_config,
                    destination,
                    include_stations,
                    exclude_set,
                    overwrite,
                    result,
                )
            elif source_config.protocol == "sftp":
                self._download_sftp(
                    source_config,
                    destination,
                    include_stations,
                    exclude_set,
                    overwrite,
                    result,
                )
            else:
                raise ValueError(f"Unknown protocol: {source_config.protocol}")

            # Remove duplicates if requested
            if remove_duplicates:
                removed = self._remove_duplicates(destination)
                if self.verbose and removed:
                    logger.info(f"Removed {removed} duplicate site logs")

        except Exception as e:
            result.errors.append(str(e))
            logger.error(f"Download failed: {e}")

        finally:
            self._disconnect()

        result.duration_seconds = (datetime.now() - start_time).total_seconds()

        logger.info(
            f"Site log download complete: {result.downloaded} downloaded, "
            f"{result.skipped} skipped, {result.failed} failed"
        )

        return result

    def _download_ftp(
        self,
        source: SiteLogSource,
        destination: Path,
        include_stations: set[str] | None,
        exclude_stations: set[str],
        overwrite: bool,
        result: SiteLogDownloadResult,
    ) -> None:
        """Download site logs via FTP.

        Port of FTPSiteLog::download from i-BSWSTA.
        """
        logger.info(f"Connecting to FTP: {source.host}:{source.port}")

        # Connect to FTP server
        self._ftp = ftplib.FTP()
        self._ftp.connect(source.host, source.port, source.timeout)
        self._ftp.login(source.username, source.password or "")

        if source.passive:
            self._ftp.set_pasv(True)

        # Change to remote directory
        self._ftp.cwd(source.remote_dir)

        # List files
        file_list = []
        self._ftp.retrlines("NLST", file_list.append)

        # Filter to .log files
        pattern = re.compile(source.file_pattern, re.IGNORECASE)
        log_files = [f for f in file_list if pattern.match(f)]

        result.total_files = len(log_files)

        if self.verbose:
            logger.info(f"Found {len(log_files)} site log files")

        # Download each file
        for filename in log_files:
            # Extract station ID (first 4 chars of filename)
            station_id = filename[:4].lower()

            # Apply filters
            if include_stations and station_id not in include_stations:
                result.filtered_out += 1
                continue

            if station_id in exclude_stations:
                result.filtered_out += 1
                continue

            local_path = destination / filename

            # Check if already exists
            if local_path.exists() and not overwrite:
                result.skipped += 1
                continue

            # Download file
            try:
                with open(local_path, "wb") as f:
                    self._ftp.retrbinary(f"RETR {filename}", f.write)

                result.downloaded += 1
                result.files_downloaded.append(filename)

                if self.verbose:
                    logger.debug(f"Downloaded: {filename}")

            except Exception as e:
                result.failed += 1
                result.files_failed.append(filename)
                result.errors.append(f"{filename}: {e}")

                if self.verbose:
                    logger.warning(f"Failed to download {filename}: {e}")

    def _download_sftp(
        self,
        source: SiteLogSource,
        destination: Path,
        include_stations: set[str] | None,
        exclude_stations: set[str],
        overwrite: bool,
        result: SiteLogDownloadResult,
    ) -> None:
        """Download site logs via SFTP.

        Uses paramiko for SFTP connections.
        """
        try:
            import paramiko
        except ImportError:
            raise ImportError("paramiko is required for SFTP downloads. Install with: pip install paramiko")

        logger.info(f"Connecting to SFTP: {source.host}:{source.port}")

        # Connect to SFTP server
        transport = paramiko.Transport((source.host, source.port))
        transport.connect(username=source.username, password=source.password)
        sftp = paramiko.SFTPClient.from_transport(transport)

        try:
            # Change to remote directory
            sftp.chdir(source.remote_dir)

            # List files
            file_list = sftp.listdir()

            # Filter to .log files
            pattern = re.compile(source.file_pattern, re.IGNORECASE)
            log_files = [f for f in file_list if pattern.match(f)]

            result.total_files = len(log_files)

            if self.verbose:
                logger.info(f"Found {len(log_files)} site log files")

            # Download each file
            for filename in log_files:
                station_id = filename[:4].lower()

                # Apply filters
                if include_stations and station_id not in include_stations:
                    result.filtered_out += 1
                    continue

                if station_id in exclude_stations:
                    result.filtered_out += 1
                    continue

                local_path = destination / filename

                # Check if already exists
                if local_path.exists() and not overwrite:
                    result.skipped += 1
                    continue

                # Download file
                try:
                    sftp.get(filename, str(local_path))
                    result.downloaded += 1
                    result.files_downloaded.append(filename)

                    if self.verbose:
                        logger.debug(f"Downloaded: {filename}")

                except Exception as e:
                    result.failed += 1
                    result.files_failed.append(filename)
                    result.errors.append(f"{filename}: {e}")

        finally:
            sftp.close()
            transport.close()

    def _disconnect(self) -> None:
        """Disconnect from FTP server."""
        if self._ftp:
            try:
                self._ftp.quit()
            except Exception:
                pass
            self._ftp = None

    def _remove_duplicates(self, directory: Path) -> int:
        """Remove duplicate site logs, keeping only the latest version.

        Site logs often have version suffixes like:
        - algo_20240101.log (newer)
        - algo_20230601.log (older)

        Port of duplicate removal logic from i-BSWSTA scripts.

        Args:
            directory: Directory containing site log files

        Returns:
            Number of duplicate files removed
        """
        log_files = list(directory.glob("*.log"))

        if not log_files:
            return 0

        # Group by station ID (first 4 chars)
        station_files: dict[str, list[Path]] = {}
        for filepath in log_files:
            station_id = filepath.stem[:4].lower()
            if station_id not in station_files:
                station_files[station_id] = []
            station_files[station_id].append(filepath)

        removed = 0

        for station_id, files in station_files.items():
            if len(files) <= 1:
                continue

            # Sort by modification time (newest first) then by name (for version suffixes)
            files_sorted = sorted(files, key=lambda p: (p.stat().st_mtime, p.stem), reverse=True)

            # Keep the first (newest), remove the rest
            for old_file in files_sorted[1:]:
                try:
                    old_file.unlink()
                    removed += 1
                    if self.verbose:
                        logger.debug(f"Removed duplicate: {old_file.name}")
                except Exception as e:
                    logger.warning(f"Failed to remove duplicate {old_file}: {e}")

        return removed

    def list_remote_files(self, source: str | SiteLogSource) -> list[str]:
        """List site log files on a remote server without downloading.

        Args:
            source: Source name or SiteLogSource config

        Returns:
            List of filenames
        """
        # Resolve source config
        if isinstance(source, str):
            if source not in self.sources:
                raise ValueError(f"Unknown source: {source}")
            source_config = self.sources[source]
        else:
            source_config = source

        try:
            if source_config.protocol == "ftp":
                self._ftp = ftplib.FTP()
                self._ftp.connect(source_config.host, source_config.port, source_config.timeout)
                self._ftp.login(source_config.username, source_config.password or "")
                self._ftp.cwd(source_config.remote_dir)

                file_list: list[str] = []
                self._ftp.retrlines("NLST", file_list.append)

                pattern = re.compile(source_config.file_pattern, re.IGNORECASE)
                return [f for f in file_list if pattern.match(f)]

            elif source_config.protocol == "sftp":
                import paramiko

                transport = paramiko.Transport((source_config.host, source_config.port))
                transport.connect(username=source_config.username, password=source_config.password)
                sftp = paramiko.SFTPClient.from_transport(transport)

                try:
                    sftp.chdir(source_config.remote_dir)
                    file_list = sftp.listdir()

                    pattern = re.compile(source_config.file_pattern, re.IGNORECASE)
                    return [f for f in file_list if pattern.match(f)]
                finally:
                    sftp.close()
                    transport.close()

        finally:
            self._disconnect()

        return []


def download_site_logs(
    source: str = "IGS",
    destination: str | Path = "/data/sitelogs",
    station_filter: list[str] | None = None,
    overwrite: bool = False,
    verbose: bool = False,
) -> SiteLogDownloadResult:
    """Convenience function to download site logs.

    Args:
        source: Source name (IGS, EUREF, etc.)
        destination: Local directory
        station_filter: Only download these stations
        overwrite: Overwrite existing files
        verbose: Enable verbose output

    Returns:
        SiteLogDownloadResult
    """
    downloader = SiteLogDownloader(verbose=verbose)
    return downloader.download(
        source=source,
        destination=destination,
        station_filter=station_filter,
        overwrite=overwrite,
    )


def download_and_parse_site_logs(
    source: str = "IGS",
    destination: str | Path = "/data/sitelogs",
    station_filter: list[str] | None = None,
    overwrite: bool = False,
    verbose: bool = False,
) -> tuple[SiteLogDownloadResult, dict]:
    """Download site logs and parse them.

    Combines download and parsing into a single operation.

    Args:
        source: Source name
        destination: Local directory
        station_filter: Only download these stations
        overwrite: Overwrite existing files
        verbose: Enable verbose output

    Returns:
        Tuple of (download_result, parsed_data_dict)
    """
    from pygnss_rt.stations.site_log_parser import parse_site_logs_directory

    # Download
    downloader = SiteLogDownloader(verbose=verbose)
    result = downloader.download(
        source=source,
        destination=destination,
        station_filter=station_filter,
        overwrite=overwrite,
    )

    # Parse
    destination = Path(destination)
    parsed = parse_site_logs_directory(destination)

    return result, parsed
