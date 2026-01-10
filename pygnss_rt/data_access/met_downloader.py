"""
Meteorological Data Downloader.

Downloads hourly and subhourly meteorological data files from FTP sources.
Port of Perl FTP::MET and FTP::SM packages from FTP.pm.

Meteorological data is used for ZTD to IWV conversion, providing:
- Surface temperature
- Surface pressure (MSL)
- Dew point temperature
- Relative humidity

Usage:
    from pygnss_rt.data_access.met_downloader import MeteorologicalDataDownloader

    downloader = MeteorologicalDataDownloader(download_dir="/data/met")
    results = downloader.download_hourly_met(
        year=2024,
        doy=260,
        hour=12,
    )
"""

from __future__ import annotations

import shutil
import subprocess
import time
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any

from pygnss_rt.data_access.ftp_client import FTPClient, SFTPClient, BaseClient
from pygnss_rt.data_access.ftp_config import FTPServerConfig, load_ftp_config
from pygnss_rt.utils.logging import get_logger

logger = get_logger(__name__)


class MetDataType(str, Enum):
    """Meteorological data types."""

    SYNOP = "synop"        # Synoptic observations
    METAR = "metar"        # Aviation weather
    BUOY = "buoy"          # Marine buoy data
    RADIOSONDE = "raob"    # Radiosonde soundings
    GNSS_MET = "gnss_met"  # GNSS-derived MET files


class TreeStructure(str, Enum):
    """Remote directory tree structures (from FTP::MET)."""

    FLAT = ""                    # No subdirectory
    YEAR_DOY_HOUR = "yyyy/doy/hn"  # year/doy/hour
    YEAR_DOY = "yyyy/doy"        # year/doy
    DOY = "doy"                  # doy only
    DOY_HOUR = "doy/hn"          # doy/hour
    YYDOY = "yydoy"              # 2-digit year + doy


@dataclass
class MetProviderConfig:
    """Configuration for a meteorological data provider."""

    name: str
    server: str
    protocol: str = "ftp"  # ftp, sftp, http, https
    username: str = "anonymous"
    password: str = ""
    port: int = 21
    base_path: str = ""
    tree_structure: TreeStructure = TreeStructure.YEAR_DOY
    filename_pattern: str = "synop_{yy}{doy:03d}{hour:02d}.dat"
    timeout: int = 60
    passive: bool = True
    priority: int = 10


@dataclass
class MetDownloadResult:
    """Result of a meteorological data download."""

    year: int
    doy: int
    hour: int | None
    success: bool
    local_path: Path | None = None
    file_size: int = 0
    provider_used: str = ""
    attempts: int = 0
    error: str = ""
    download_time: float = 0.0
    records_count: int = 0


# Default meteorological data providers (from FTP::MET configuration)
DEFAULT_MET_PROVIDERS: dict[str, MetProviderConfig] = {
    "NOAA_SYNOP": MetProviderConfig(
        name="NOAA_SYNOP",
        server="tgftp.nws.noaa.gov",
        protocol="ftp",
        username="anonymous",
        base_path="/SL.us008001/DF.an/DC.sflnd/DS.synop",
        tree_structure=TreeStructure.FLAT,
        filename_pattern="sflnd_synop.txt",
        priority=1,
    ),
    "OGIMET": MetProviderConfig(
        name="OGIMET",
        server="www.ogimet.com",
        protocol="http",
        base_path="/cgi-bin/getsynop",
        tree_structure=TreeStructure.FLAT,
        filename_pattern="synop_{yy}{doy:03d}{hour:02d}.txt",
        priority=2,
    ),
    "UK_MET_OFFICE": MetProviderConfig(
        name="UK_MET_OFFICE",
        server="datapoint.metoffice.gov.uk",
        protocol="http",
        base_path="/public/data/val/wxobs/all/datatype/synop",
        tree_structure=TreeStructure.FLAT,
        filename_pattern="synop.txt",
        priority=3,
    ),
    "LOCAL_MET": MetProviderConfig(
        name="LOCAL_MET",
        server="localhost",
        protocol="file",
        base_path="/data/met/synop",
        tree_structure=TreeStructure.YEAR_DOY_HOUR,
        filename_pattern="synop_{yy}{doy:03d}{hour:02d}.dat",
        priority=10,
    ),
}


class MeteorologicalDataDownloader:
    """Downloads meteorological data for ZTD to IWV conversion.

    Port of Perl FTP::MET and FTP::SM packages.

    Supports multiple providers with automatic fallback, retry logic,
    and different directory tree structures.
    """

    def __init__(
        self,
        download_dir: str | Path = "/data/met",
        providers: dict[str, MetProviderConfig] | None = None,
        ftp_config_path: Path | str | None = None,
        max_retries: int = 3,
        retry_delay: float = 5.0,
        verbose: bool = False,
    ):
        """Initialize meteorological data downloader.

        Args:
            download_dir: Base directory for downloads
            providers: Provider configurations (uses defaults if None)
            ftp_config_path: Path to FTP configuration XML
            max_retries: Maximum retry attempts per provider
            retry_delay: Delay between retries in seconds
            verbose: Enable verbose output
        """
        self.download_dir = Path(download_dir)
        self.providers = providers or DEFAULT_MET_PROVIDERS.copy()
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.verbose = verbose

        self._clients: dict[str, BaseClient] = {}

        # Load additional providers from FTP config if provided
        if ftp_config_path:
            self._load_ftp_config(Path(ftp_config_path))

    def _load_ftp_config(self, config_path: Path) -> None:
        """Load additional provider configuration from XML.

        Args:
            config_path: Path to FTP configuration XML
        """
        if not config_path.exists():
            return

        try:
            configs = load_ftp_config(config_path)
            for cfg in configs:
                if cfg.id.startswith("MET_") or "met" in cfg.id.lower():
                    # Convert FTPServerConfig to MetProviderConfig
                    self.providers[cfg.id] = MetProviderConfig(
                        name=cfg.id,
                        server=cfg.url,
                        username=cfg.username or "anonymous",
                        password=cfg.password or "",
                        base_path=cfg.root or "",
                        tree_structure=TreeStructure.YEAR_DOY,  # Default
                        priority=5,
                    )
        except Exception as e:
            logger.warning(f"Failed to load FTP config: {e}")

    def _get_client(self, provider: MetProviderConfig) -> BaseClient | None:
        """Get or create FTP/SFTP client for provider.

        Args:
            provider: Provider configuration

        Returns:
            Connected client or None for HTTP/file protocols
        """
        if provider.protocol in ("http", "https", "file"):
            return None

        if provider.name not in self._clients:
            if provider.protocol == "sftp":
                client = SFTPClient(
                    host=provider.server,
                    port=provider.port,
                    username=provider.username,
                    password=provider.password,
                    timeout=provider.timeout,
                )
            else:  # ftp
                client = FTPClient(
                    host=provider.server,
                    username=provider.username,
                    password=provider.password,
                    timeout=provider.timeout,
                    passive=provider.passive,
                )

            try:
                client.connect()
                self._clients[provider.name] = client
            except Exception as e:
                logger.warning(f"Failed to connect to {provider.name}: {e}")
                return None

        return self._clients.get(provider.name)

    def _build_remote_path(
        self,
        provider: MetProviderConfig,
        year: int,
        doy: int,
        hour: int,
    ) -> tuple[str, str]:
        """Build remote directory and filename.

        Port of the path building logic from FTP::MET.

        Args:
            provider: Provider configuration
            year: Year
            doy: Day of year
            hour: Hour (0-23)

        Returns:
            Tuple of (directory_path, filename)
        """
        yy = year % 100

        # Build directory based on tree structure
        tree = provider.tree_structure
        if tree == TreeStructure.FLAT:
            directory = provider.base_path
        elif tree == TreeStructure.YEAR_DOY_HOUR:
            directory = f"{provider.base_path}/{year}/{doy:03d}/{hour:02d}"
        elif tree == TreeStructure.YEAR_DOY:
            directory = f"{provider.base_path}/{year}/{doy:03d}"
        elif tree == TreeStructure.DOY:
            directory = f"{provider.base_path}/{doy:03d}"
        elif tree == TreeStructure.DOY_HOUR:
            directory = f"{provider.base_path}/{doy:03d}/{hour:02d}"
        elif tree == TreeStructure.YYDOY:
            directory = f"{provider.base_path}/{yy:02d}{doy:03d}"
        else:
            directory = provider.base_path

        # Build filename from pattern
        filename = provider.filename_pattern.format(
            year=year,
            yy=yy,
            doy=doy,
            hour=hour,
        )

        return directory, filename

    def _build_local_path(
        self,
        year: int,
        doy: int,
        hour: int,
        data_type: MetDataType = MetDataType.SYNOP,
    ) -> Path:
        """Build local file path.

        Args:
            year: Year
            doy: Day of year
            hour: Hour
            data_type: Type of met data

        Returns:
            Local file path
        """
        yy = year % 100
        filename = f"{data_type.value}_{yy:02d}{doy:03d}{hour:02d}.dat"

        return self.download_dir / str(year) / f"{doy:03d}" / filename

    def _download_http(
        self,
        url: str,
        local_path: Path,
        timeout: int = 60,
    ) -> bool:
        """Download file using HTTP/HTTPS.

        Args:
            url: Full URL to download
            local_path: Local destination path
            timeout: Timeout in seconds

        Returns:
            True if successful
        """
        local_path.parent.mkdir(parents=True, exist_ok=True)

        try:
            # Try with requests first
            import requests

            response = requests.get(url, timeout=timeout, stream=True)
            if response.status_code == 200:
                with open(local_path, "wb") as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)
                return local_path.exists() and local_path.stat().st_size > 0

        except ImportError:
            # Fall back to curl
            try:
                result = subprocess.run(
                    [
                        "curl", "-s", "-f", "-L",
                        "--connect-timeout", str(timeout),
                        "-o", str(local_path),
                        url,
                    ],
                    capture_output=True,
                    timeout=timeout + 30,
                )
                return result.returncode == 0 and local_path.exists()
            except subprocess.TimeoutExpired:
                return False
        except Exception as e:
            logger.debug(f"HTTP download failed: {e}")
            return False

        return False

    def _download_file(
        self,
        source_path: Path,
        local_path: Path,
    ) -> bool:
        """Copy file from local/mounted path.

        Args:
            source_path: Source file path
            local_path: Local destination path

        Returns:
            True if successful
        """
        if not source_path.exists():
            return False

        local_path.parent.mkdir(parents=True, exist_ok=True)

        try:
            shutil.copy2(source_path, local_path)
            return True
        except Exception as e:
            logger.debug(f"File copy failed: {e}")
            return False

    def download_hourly_met(
        self,
        year: int,
        doy: int,
        hour: int,
        providers: list[str] | None = None,
        data_type: MetDataType = MetDataType.SYNOP,
    ) -> MetDownloadResult:
        """Download hourly meteorological data.

        Port of FTP::MET download() method.

        Args:
            year: Year
            doy: Day of year
            hour: Hour (0-23)
            providers: Specific providers to use (all if None)
            data_type: Type of met data to download

        Returns:
            MetDownloadResult
        """
        result = MetDownloadResult(
            year=year,
            doy=doy,
            hour=hour,
            success=False,
        )

        # Build local path
        local_path = self._build_local_path(year, doy, hour, data_type)

        # Check if already exists
        if local_path.exists() and local_path.stat().st_size > 0:
            result.success = True
            result.local_path = local_path
            result.file_size = local_path.stat().st_size
            result.provider_used = "cached"
            return result

        # Determine provider order
        if providers:
            provider_order = providers
        else:
            provider_order = sorted(
                self.providers.keys(),
                key=lambda p: self.providers[p].priority,
            )

        start_time = time.time()

        for provider_name in provider_order:
            provider = self.providers.get(provider_name)
            if not provider:
                continue

            remote_dir, remote_file = self._build_remote_path(
                provider, year, doy, hour
            )
            remote_path = f"{remote_dir}/{remote_file}"

            for attempt in range(self.max_retries):
                result.attempts += 1

                try:
                    success = False

                    if provider.protocol in ("http", "https"):
                        url = f"{provider.protocol}://{provider.server}{remote_path}"
                        success = self._download_http(url, local_path, provider.timeout)

                    elif provider.protocol == "file":
                        source = Path(remote_path)
                        success = self._download_file(source, local_path)

                    else:  # FTP/SFTP
                        client = self._get_client(provider)
                        if client:
                            local_path.parent.mkdir(parents=True, exist_ok=True)
                            success = client.download(remote_path, local_path)

                    if success and local_path.exists():
                        result.success = True
                        result.local_path = local_path
                        result.file_size = local_path.stat().st_size
                        result.provider_used = provider_name
                        result.download_time = time.time() - start_time

                        # Count records in file
                        result.records_count = self._count_records(local_path)

                        if self.verbose:
                            print(
                                f"  Downloaded MET: {year}/{doy:03d}/{hour:02d} "
                                f"from {provider_name} ({result.records_count} records)"
                            )
                        return result

                except Exception as e:
                    result.error = str(e)
                    logger.debug(f"MET download attempt failed: {e}")

                # Retry delay
                if attempt < self.max_retries - 1:
                    time.sleep(self.retry_delay)

        result.download_time = time.time() - start_time
        if self.verbose:
            print(f"  FAILED MET: {year}/{doy:03d}/{hour:02d} - {result.error or 'Not found'}")

        return result

    def download_subhourly_met(
        self,
        year: int,
        doy: int,
        hour: int,
        minute: int = 0,
        providers: list[str] | None = None,
        data_type: MetDataType = MetDataType.SYNOP,
    ) -> MetDownloadResult:
        """Download subhourly (15-minute) meteorological data.

        Port of FTP::SM download_sm() method.

        Args:
            year: Year
            doy: Day of year
            hour: Hour (0-23)
            minute: Minute (0, 15, 30, 45)
            providers: Specific providers to use
            data_type: Type of met data

        Returns:
            MetDownloadResult
        """
        # Round minute to 15-minute boundary
        minute_block = (minute // 15) * 15

        result = MetDownloadResult(
            year=year,
            doy=doy,
            hour=hour,
            success=False,
        )

        # Build local path with minute in filename
        yy = year % 100
        filename = f"{data_type.value}_{yy:02d}{doy:03d}{hour:02d}{minute_block:02d}.dat"
        local_path = self.download_dir / str(year) / f"{doy:03d}" / filename

        # Check if already exists
        if local_path.exists() and local_path.stat().st_size > 0:
            result.success = True
            result.local_path = local_path
            result.file_size = local_path.stat().st_size
            result.provider_used = "cached"
            return result

        # Most providers only have hourly data, so try hourly as fallback
        hourly_result = self.download_hourly_met(year, doy, hour, providers, data_type)

        if hourly_result.success:
            # Copy hourly to subhourly location
            local_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(hourly_result.local_path, local_path)

            result.success = True
            result.local_path = local_path
            result.file_size = local_path.stat().st_size
            result.provider_used = hourly_result.provider_used
            result.records_count = hourly_result.records_count

        return result

    def download_met_range(
        self,
        year: int,
        start_doy: int,
        end_doy: int,
        hours: list[int] | None = None,
        providers: list[str] | None = None,
    ) -> list[MetDownloadResult]:
        """Download meteorological data for a date range.

        Args:
            year: Year
            start_doy: Start day of year
            end_doy: End day of year
            hours: Specific hours to download (all if None)
            providers: Specific providers to use

        Returns:
            List of MetDownloadResults
        """
        if hours is None:
            hours = list(range(0, 24, 6))  # 00, 06, 12, 18 UTC

        results = []

        for doy in range(start_doy, end_doy + 1):
            for hour in hours:
                result = self.download_hourly_met(year, doy, hour, providers)
                results.append(result)

        return results

    def _count_records(self, file_path: Path) -> int:
        """Count records in a meteorological data file.

        Args:
            file_path: Path to met data file

        Returns:
            Number of records
        """
        try:
            with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
                # Count non-comment, non-empty lines
                count = 0
                for line in f:
                    line = line.strip()
                    if line and not line.startswith("#") and not line.startswith("*"):
                        count += 1
                return count
        except Exception:
            return 0

    def get_download_summary(
        self,
        results: list[MetDownloadResult],
    ) -> dict[str, Any]:
        """Get summary of download batch.

        Args:
            results: List of download results

        Returns:
            Summary dictionary
        """
        successful = [r for r in results if r.success]
        failed = [r for r in results if not r.success]

        providers_used = {}
        for r in successful:
            prov = r.provider_used
            providers_used[prov] = providers_used.get(prov, 0) + 1

        return {
            "total": len(results),
            "successful": len(successful),
            "failed": len(failed),
            "success_rate": len(successful) / len(results) if results else 0,
            "total_size": sum(r.file_size for r in successful),
            "total_records": sum(r.records_count for r in successful),
            "total_time": sum(r.download_time for r in results),
            "providers_used": providers_used,
        }

    def close(self) -> None:
        """Close all connections."""
        for client in self._clients.values():
            try:
                client.disconnect()
            except Exception:
                pass
        self._clients.clear()

    def __enter__(self) -> "MeteorologicalDataDownloader":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()


def download_met_for_processing(
    year: int,
    doy: int,
    hour: int,
    download_dir: str | Path = "/data/met",
    verbose: bool = False,
) -> MetDownloadResult:
    """Convenience function to download meteorological data.

    Args:
        year: Year
        doy: Day of year
        hour: Hour
        download_dir: Download directory
        verbose: Enable verbose output

    Returns:
        MetDownloadResult
    """
    with MeteorologicalDataDownloader(download_dir=download_dir, verbose=verbose) as downloader:
        return downloader.download_hourly_met(year, doy, hour)


def download_met_for_day(
    year: int,
    doy: int,
    download_dir: str | Path = "/data/met",
    hours: list[int] | None = None,
    verbose: bool = False,
) -> dict[str, Any]:
    """Download all meteorological data for a day.

    Args:
        year: Year
        doy: Day of year
        download_dir: Download directory
        hours: Specific hours (default: 00, 06, 12, 18)
        verbose: Enable verbose output

    Returns:
        Download summary dictionary
    """
    if hours is None:
        hours = [0, 6, 12, 18]

    with MeteorologicalDataDownloader(download_dir=download_dir, verbose=verbose) as downloader:
        results = [
            downloader.download_hourly_met(year, doy, hour)
            for hour in hours
        ]
        return downloader.get_download_summary(results)
