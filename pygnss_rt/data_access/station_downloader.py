"""
Station RINEX Data Downloader.

Downloads hourly and daily RINEX observation files from multiple providers
(CDDIS, BKGE, OSGB, RGP, etc.) with retry logic and fallback support.

Replaces the Perl call_download_*.pl scripts:
- call_download_EUREF_stations.pl
- call_download_IGS_stations.pl
- call_download_OSGB_stations.pl
- call_download_RGP_stations.pl
- etc.

Usage:
    from pygnss_rt.data_access.station_downloader import StationDownloader

    downloader = StationDownloader(download_dir="/data/rinex")
    results = downloader.download_hourly_data(
        stations=["algo", "nrc1", "dubo"],
        year=2024,
        doy=260,
        hour=12,
    )
"""

from __future__ import annotations

import gzip
import os
import shutil
import subprocess
import time
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any
from concurrent.futures import ThreadPoolExecutor, as_completed

from pygnss_rt.data_access.ftp_client import FTPClient, SFTPClient, BaseClient
from pygnss_rt.data_access.ftp_config import FTPServerConfig


class RINEXType(str, Enum):
    """RINEX observation file types."""

    HOURLY = "hourly"       # 1-hour files
    DAILY = "daily"         # 24-hour files
    HIGHRATE = "highrate"   # High-rate (1Hz, 5Hz) files
    SUBHOURLY = "subhourly" # 15-minute files


class CompressionType(str, Enum):
    """File compression types."""

    NONE = ""
    GZIP = ".gz"
    COMPRESS = ".Z"
    HATANAKA = ".crx"  # Hatanaka compressed RINEX


@dataclass
class DownloadTask:
    """A single download task."""

    station_id: str
    year: int
    doy: int
    hour: int | None = None  # None for daily files
    rinex_type: RINEXType = RINEXType.HOURLY
    provider: str = ""
    remote_path: str = ""
    local_path: Path | None = None


@dataclass
class DownloadResult:
    """Result of a download attempt."""

    task: DownloadTask
    success: bool
    local_path: Path | None = None
    file_size: int = 0
    provider_used: str = ""
    attempts: int = 0
    error: str = ""
    download_time: float = 0.0


@dataclass
class ProviderConfig:
    """Configuration for a data provider."""

    name: str
    server: str
    protocol: str = "ftp"  # ftp, sftp, http, https
    username: str = "anonymous"
    password: str = ""
    port: int = 21
    base_path: str = ""
    path_template: str = ""  # Template with {year}, {doy}, {hour}, {station}
    filename_template: str = ""  # Template for filename
    timeout: int = 60
    passive: bool = True
    priority: int = 10  # Lower = higher priority
    supports_hourly: bool = True
    supports_daily: bool = True


# Default provider configurations
DEFAULT_PROVIDERS: dict[str, ProviderConfig] = {
    "CDDIS": ProviderConfig(
        name="CDDIS",
        server="cddis.nasa.gov",
        protocol="https",
        base_path="/archive/gnss/data",
        path_template="/hourly/{year}/{doy:03d}/{hour:02d}",
        filename_template="{station}{doy:03d}{hour_char}.{yy}o.gz",
        priority=1,
    ),
    "BKGE": ProviderConfig(
        name="BKGE",
        server="igs.bkg.bund.de",
        protocol="ftp",
        username="anonymous",
        base_path="/EUREF/obs",
        path_template="/{year}/{doy:03d}",
        filename_template="{station}{doy:03d}0.{yy}o.Z",
        priority=2,
        supports_hourly=False,
    ),
    "BKGE_HOURLY": ProviderConfig(
        name="BKGE_HOURLY",
        server="igs.bkg.bund.de",
        protocol="ftp",
        username="anonymous",
        base_path="/EUREF/highrate",
        path_template="/{year}/{doy:03d}",
        filename_template="{station}{doy:03d}{hour_char}.{yy}o.gz",
        priority=3,
    ),
    "OSGB": ProviderConfig(
        name="OSGB",
        server="ftp.ordnancesurvey.co.uk",
        protocol="ftp",
        username="anonymous",
        base_path="/gnss/hourly",
        path_template="/{year}/{doy:03d}/{hour:02d}",
        filename_template="{STATION}{doy:03d}{hour_char}.{yy}o.gz",
        priority=4,
    ),
    "RGP": ProviderConfig(
        name="RGP",
        server="rgpdata.ign.fr",
        protocol="ftp",
        username="anonymous",
        base_path="/pub/data",
        path_template="/{year}/{doy:03d}",
        filename_template="{station}{doy:03d}{hour_char}.{yy}o.gz",
        priority=5,
    ),
    "IGN_HOURLY": ProviderConfig(
        name="IGN_HOURLY",
        server="igs.ign.fr",
        protocol="ftp",
        username="anonymous",
        base_path="/pub/igs/data/hourly",
        path_template="/{year}/{doy:03d}/{hour:02d}",
        filename_template="{station}{doy:03d}{hour_char}.{yy}o.gz",
        priority=6,
    ),
    "SOPAC": ProviderConfig(
        name="SOPAC",
        server="garner.ucsd.edu",
        protocol="ftp",
        username="anonymous",
        base_path="/pub/rinex",
        path_template="/{year}/{doy:03d}",
        filename_template="{station}{doy:03d}0.{yy}o.Z",
        priority=7,
        supports_hourly=False,
    ),
}


class StationDownloader:
    """Downloads RINEX observation data for GNSS stations.

    Supports multiple providers with automatic fallback, retry logic,
    and parallel downloads.
    """

    def __init__(
        self,
        download_dir: str | Path = "/data/rinex",
        providers: dict[str, ProviderConfig] | None = None,
        max_retries: int = 3,
        retry_delay: float = 5.0,
        parallel_downloads: int = 4,
        verbose: bool = False,
    ):
        """Initialize station downloader.

        Args:
            download_dir: Base directory for downloads
            providers: Provider configurations (uses defaults if None)
            max_retries: Maximum retry attempts per provider
            retry_delay: Delay between retries in seconds
            parallel_downloads: Number of parallel download threads
            verbose: Enable verbose output
        """
        self.download_dir = Path(download_dir)
        self.providers = providers or DEFAULT_PROVIDERS.copy()
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.parallel_downloads = parallel_downloads
        self.verbose = verbose

        self._clients: dict[str, BaseClient] = {}

    def _get_client(self, provider: ProviderConfig) -> BaseClient:
        """Get or create FTP/SFTP client for provider."""
        if provider.name not in self._clients:
            if provider.protocol == "sftp":
                client = SFTPClient(
                    host=provider.server,
                    port=provider.port,
                    username=provider.username,
                    password=provider.password,
                    timeout=provider.timeout,
                )
            elif provider.protocol in ("ftp",):
                client = FTPClient(
                    host=provider.server,
                    username=provider.username,
                    password=provider.password,
                    timeout=provider.timeout,
                    passive=provider.passive,
                )
            else:
                # HTTP/HTTPS handled separately
                return None  # type: ignore

            client.connect()
            self._clients[provider.name] = client

        return self._clients[provider.name]

    def _build_remote_path(
        self,
        provider: ProviderConfig,
        station: str,
        year: int,
        doy: int,
        hour: int | None = None,
    ) -> tuple[str, str]:
        """Build remote directory and filename.

        Args:
            provider: Provider configuration
            station: Station ID (4-char)
            year: Year
            doy: Day of year
            hour: Hour (0-23) or None for daily

        Returns:
            Tuple of (directory_path, filename)
        """
        yy = year % 100
        hour_char = chr(ord('a') + (hour or 0)) if hour is not None else '0'

        # Format path
        path = provider.base_path + provider.path_template.format(
            year=year,
            yy=yy,
            doy=doy,
            hour=hour or 0,
            hour_char=hour_char,
            station=station.lower(),
            STATION=station.upper(),
        )

        # Format filename
        filename = provider.filename_template.format(
            year=year,
            yy=yy,
            doy=doy,
            hour=hour or 0,
            hour_char=hour_char,
            station=station.lower(),
            STATION=station.upper(),
        )

        return path, filename

    def _build_local_path(
        self,
        station: str,
        year: int,
        doy: int,
        hour: int | None = None,
        rinex_type: RINEXType = RINEXType.HOURLY,
    ) -> Path:
        """Build local file path.

        Args:
            station: Station ID
            year: Year
            doy: Day of year
            hour: Hour or None for daily
            rinex_type: RINEX type

        Returns:
            Local file path
        """
        yy = year % 100

        if rinex_type == RINEXType.DAILY:
            filename = f"{station.lower()}{doy:03d}0.{yy:02d}o"
            subdir = "daily"
        else:
            hour_char = chr(ord('a') + (hour or 0))
            filename = f"{station.lower()}{doy:03d}{hour_char}.{yy:02d}o"
            subdir = "hourly"

        return self.download_dir / subdir / str(year) / f"{doy:03d}" / filename

    def _download_with_curl(
        self,
        url: str,
        local_path: Path,
        timeout: int = 60,
    ) -> bool:
        """Download file using curl (for HTTPS with authentication).

        Args:
            url: Full URL to download
            local_path: Local destination path
            timeout: Timeout in seconds

        Returns:
            True if successful
        """
        local_path.parent.mkdir(parents=True, exist_ok=True)

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
        except Exception:
            return False

    def _download_single(
        self,
        task: DownloadTask,
        provider_order: list[str] | None = None,
    ) -> DownloadResult:
        """Download a single file with retry and fallback.

        Args:
            task: Download task
            provider_order: Ordered list of providers to try

        Returns:
            DownloadResult
        """
        if provider_order is None:
            # Sort providers by priority
            provider_order = sorted(
                self.providers.keys(),
                key=lambda p: self.providers[p].priority,
            )

        result = DownloadResult(
            task=task,
            success=False,
        )

        # Build local path
        local_path = task.local_path or self._build_local_path(
            task.station_id, task.year, task.doy, task.hour, task.rinex_type
        )

        # Check if already exists
        if local_path.exists():
            result.success = True
            result.local_path = local_path
            result.file_size = local_path.stat().st_size
            result.provider_used = "cached"
            return result

        start_time = time.time()

        for provider_name in provider_order:
            provider = self.providers.get(provider_name)
            if not provider:
                continue

            # Skip providers that don't support this type
            if task.rinex_type == RINEXType.HOURLY and not provider.supports_hourly:
                continue
            if task.rinex_type == RINEXType.DAILY and not provider.supports_daily:
                continue

            remote_dir, remote_file = self._build_remote_path(
                provider, task.station_id, task.year, task.doy, task.hour
            )
            remote_path = f"{remote_dir}/{remote_file}"

            for attempt in range(self.max_retries):
                result.attempts += 1

                try:
                    if provider.protocol in ("http", "https"):
                        # Use curl for HTTP/HTTPS
                        url = f"{provider.protocol}://{provider.server}{remote_path}"
                        success = self._download_with_curl(url, local_path, provider.timeout)
                    else:
                        # Use FTP/SFTP client
                        client = self._get_client(provider)
                        if client:
                            success = client.download(remote_path, local_path)
                        else:
                            success = False

                    if success and local_path.exists():
                        result.success = True
                        result.local_path = local_path
                        result.file_size = local_path.stat().st_size
                        result.provider_used = provider_name
                        result.download_time = time.time() - start_time

                        if self.verbose:
                            print(f"  Downloaded: {task.station_id} from {provider_name}")
                        return result

                except Exception as e:
                    result.error = str(e)

                # Retry delay
                if attempt < self.max_retries - 1:
                    time.sleep(self.retry_delay)

        result.download_time = time.time() - start_time
        if self.verbose:
            print(f"  FAILED: {task.station_id} - {result.error or 'Not found'}")

        return result

    def download_hourly_data(
        self,
        stations: list[str],
        year: int,
        doy: int,
        hour: int,
        providers: list[str] | None = None,
    ) -> list[DownloadResult]:
        """Download hourly RINEX data for multiple stations.

        Args:
            stations: List of station IDs
            year: Year
            doy: Day of year
            hour: Hour (0-23)
            providers: Specific providers to use (all if None)

        Returns:
            List of DownloadResults
        """
        tasks = [
            DownloadTask(
                station_id=sta,
                year=year,
                doy=doy,
                hour=hour,
                rinex_type=RINEXType.HOURLY,
            )
            for sta in stations
        ]

        return self._download_batch(tasks, providers)

    def download_daily_data(
        self,
        stations: list[str],
        year: int,
        doy: int,
        providers: list[str] | None = None,
    ) -> list[DownloadResult]:
        """Download daily RINEX data for multiple stations.

        Args:
            stations: List of station IDs
            year: Year
            doy: Day of year
            providers: Specific providers to use

        Returns:
            List of DownloadResults
        """
        tasks = [
            DownloadTask(
                station_id=sta,
                year=year,
                doy=doy,
                hour=None,
                rinex_type=RINEXType.DAILY,
            )
            for sta in stations
        ]

        return self._download_batch(tasks, providers)

    def _download_batch(
        self,
        tasks: list[DownloadTask],
        providers: list[str] | None = None,
    ) -> list[DownloadResult]:
        """Download multiple files in parallel.

        Args:
            tasks: List of download tasks
            providers: Specific providers to use

        Returns:
            List of DownloadResults
        """
        if not tasks:
            return []

        results = []

        if self.parallel_downloads > 1:
            with ThreadPoolExecutor(max_workers=self.parallel_downloads) as executor:
                futures = {
                    executor.submit(self._download_single, task, providers): task
                    for task in tasks
                }

                for future in as_completed(futures):
                    try:
                        result = future.result()
                        results.append(result)
                    except Exception as e:
                        task = futures[future]
                        results.append(DownloadResult(
                            task=task,
                            success=False,
                            error=str(e),
                        ))
        else:
            # Sequential download
            for task in tasks:
                result = self._download_single(task, providers)
                results.append(result)

        return results

    def decompress_file(self, file_path: Path) -> Path:
        """Decompress a downloaded file.

        Args:
            file_path: Path to compressed file

        Returns:
            Path to decompressed file
        """
        if file_path.suffix == ".gz":
            output_path = file_path.with_suffix("")
            with gzip.open(file_path, "rb") as f_in:
                with open(output_path, "wb") as f_out:
                    shutil.copyfileobj(f_in, f_out)
            return output_path

        elif file_path.suffix == ".Z":
            output_path = file_path.with_suffix("")
            try:
                subprocess.run(
                    ["uncompress", "-f", str(file_path)],
                    check=True,
                    capture_output=True,
                )
                return output_path
            except subprocess.CalledProcessError:
                # Try gzip as fallback
                subprocess.run(
                    ["gzip", "-d", "-f", str(file_path)],
                    check=True,
                    capture_output=True,
                )
                return output_path

        return file_path

    def get_download_summary(
        self,
        results: list[DownloadResult],
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
            "total_time": sum(r.download_time for r in results),
            "providers_used": providers_used,
            "failed_stations": [r.task.station_id for r in failed],
        }

    def close(self) -> None:
        """Close all connections."""
        for client in self._clients.values():
            try:
                client.disconnect()
            except Exception:
                pass
        self._clients.clear()

    def __enter__(self) -> "StationDownloader":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()


def download_stations_for_processing(
    stations: list[str],
    year: int,
    doy: int,
    hour: int | None = None,
    download_dir: str | Path = "/data/rinex",
    verbose: bool = False,
) -> dict[str, Any]:
    """Convenience function to download station data.

    Args:
        stations: List of station IDs
        year: Year
        doy: Day of year
        hour: Hour (None for daily)
        download_dir: Download directory
        verbose: Enable verbose output

    Returns:
        Download summary dictionary
    """
    with StationDownloader(download_dir=download_dir, verbose=verbose) as downloader:
        if hour is not None:
            results = downloader.download_hourly_data(stations, year, doy, hour)
        else:
            results = downloader.download_daily_data(stations, year, doy)

        return downloader.get_download_summary(results)
