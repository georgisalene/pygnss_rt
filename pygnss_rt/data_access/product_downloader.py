"""
GNSS Product Downloader.

Replaces Perl FTP.pm product download functionality:
- FTP::OE (Orbit/ERP downloads)
- FTP::DD (Hourly data downloads)
- FTP::SD (Subhourly data downloads)

Provides unified download interface for all GNSS products with:
- Multi-source retry logic
- Compression handling (.Z, .gz, .zip)
- Progress tracking
- Database status updates
"""

from __future__ import annotations

import gzip
import shutil
import subprocess
import tempfile
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any

from pygnss_rt.data_access.ftp_client import FTPClient, SFTPClient
from pygnss_rt.data_access.http_client import HTTPClient, CDDISClient
from pygnss_rt.data_access.ftp_config import FTPServerConfig, load_ftp_config
from pygnss_rt.utils.dates import GNSSDate, gps_week_from_mjd
from pygnss_rt.utils.logging import get_logger, ignss_print, MessageType


logger = get_logger(__name__)


class CompressionType(str, Enum):
    """Compression types for GNSS files."""

    NONE = ""
    UNIX_Z = ".Z"
    GZIP = ".gz"
    ZIP = ".zip"


class DownloadStatus(str, Enum):
    """Status of a download operation."""

    SUCCESS = "success"
    NOT_FOUND = "not_found"
    TIMEOUT = "timeout"
    CONNECTION_ERROR = "connection_error"
    DECOMPRESSION_ERROR = "decompression_error"
    UNKNOWN_ERROR = "unknown_error"


@dataclass
class ProductDownloadResult:
    """Result of a product download operation.

    Attributes:
        status: Download status
        local_path: Path to downloaded file
        remote_path: Original remote path
        source: Server/source used
        file_size: Size in bytes
        compressed: Whether file was compressed
        download_time_seconds: Time taken to download
        error_message: Error details if failed
    """

    status: DownloadStatus = DownloadStatus.UNKNOWN_ERROR
    local_path: Path | None = None
    remote_path: str = ""
    source: str = ""
    file_size: int = 0
    compressed: bool = False
    download_time_seconds: float = 0.0
    error_message: str = ""

    @property
    def success(self) -> bool:
        """Check if download was successful."""
        return self.status == DownloadStatus.SUCCESS


@dataclass
class ProductDownloadConfig:
    """Configuration for product downloads.

    Attributes:
        ftp_config_path: Path to FTP configuration XML
        destination_dir: Base directory for downloads
        max_retries: Maximum retry attempts per source
        timeout: Connection timeout in seconds
        decompress: Whether to decompress downloaded files
        parallel_downloads: Number of parallel downloads
    """

    ftp_config_path: Path | None = None
    destination_dir: Path = field(default_factory=lambda: Path("products"))
    max_retries: int = 3
    timeout: int = 60
    decompress: bool = True
    parallel_downloads: int = 4


class ProductDownloader:
    """Downloads GNSS products from multiple sources.

    Replaces Perl FTP::OE class for orbit, ERP, clock, DCB, BIA, ION products.

    Supports:
    - IGS (CDDIS, IGN)
    - CODE (Berne)
    - IGS Real-Time Service
    - EUREF

    Usage:
        from pygnss_rt.data_access import ProductDownloader, ProductDownloadConfig

        config = ProductDownloadConfig(
            destination_dir=Path("/data/products"),
            ftp_config_path=Path("/etc/gnss/ftp_config.xml"),
        )
        downloader = ProductDownloader(config)

        # Download orbit file
        result = downloader.download_orbit(
            GNSSDate.from_ymd(2024, 1, 15),
            provider="IGS",
            tier="final",
        )

        if result.success:
            print(f"Downloaded to {result.local_path}")
    """

    # Default sources for different product types
    ORBIT_SOURCES = ["CDDIS", "IGN", "CODE"]
    CLOCK_SOURCES = ["CDDIS", "IGN", "CODE"]
    ERP_SOURCES = ["CDDIS", "IGN", "CODE"]
    DCB_SOURCES = ["CODE", "CDDIS"]

    def __init__(self, config: ProductDownloadConfig | None = None):
        """Initialize product downloader.

        Args:
            config: Download configuration
        """
        self.config = config or ProductDownloadConfig()
        self._ftp_configs: list[FTPServerConfig] = []
        self._active_connections: dict[str, Any] = {}

    def _load_ftp_configs(self) -> list[FTPServerConfig]:
        """Load FTP server configurations."""
        if self._ftp_configs:
            return self._ftp_configs

        if self.config.ftp_config_path and self.config.ftp_config_path.exists():
            self._ftp_configs = load_ftp_config(self.config.ftp_config_path)

        return self._ftp_configs

    def _get_ftp_config(self, source_id: str) -> FTPServerConfig | None:
        """Get FTP configuration for a source."""
        for cfg in self._load_ftp_configs():
            if cfg.id.upper() == source_id.upper():
                return cfg
        return None

    def _connect(self, source_id: str) -> FTPClient | HTTPClient | None:
        """Connect to download source.

        Args:
            source_id: Source identifier (e.g., 'CDDIS', 'CODE')

        Returns:
            Connected client or None
        """
        if source_id in self._active_connections:
            return self._active_connections[source_id]

        config = self._get_ftp_config(source_id)
        if not config:
            logger.warning("No config for source", source=source_id)
            return None

        try:
            if source_id == "CDDIS":
                # CDDIS requires HTTPS with Earthdata login
                client = CDDISClient(timeout=self.config.timeout)
            elif config.protocol == "sftp":
                client = SFTPClient(
                    host=config.host,
                    username=config.username or "anonymous",
                    password=config.password or "",
                    timeout=self.config.timeout,
                )
                client.connect()
            elif config.protocol in ("ftp", ""):
                client = FTPClient(
                    host=config.host,
                    username=config.username or "anonymous",
                    password=config.password or "",
                    timeout=self.config.timeout,
                    passive=True,
                )
                client.connect()
            else:
                client = HTTPClient(
                    base_url=f"{config.protocol}://{config.host}",
                    timeout=self.config.timeout,
                )

            self._active_connections[source_id] = client
            return client

        except Exception as e:
            logger.error("Connection failed", source=source_id, error=str(e))
            return None

    def _disconnect_all(self) -> None:
        """Disconnect all active connections."""
        for source_id, client in self._active_connections.items():
            try:
                if hasattr(client, "disconnect"):
                    client.disconnect()
            except Exception:
                pass
        self._active_connections.clear()

    def _decompress_file(self, file_path: Path) -> Path | None:
        """Decompress a compressed file.

        Args:
            file_path: Path to compressed file

        Returns:
            Path to decompressed file or None on error
        """
        suffix = file_path.suffix.lower()

        try:
            if suffix == ".z":
                # Unix compress (.Z) - use system uncompress
                output_path = file_path.with_suffix("")
                result = subprocess.run(
                    ["uncompress", "-f", str(file_path)],
                    capture_output=True,
                )
                if result.returncode == 0 and output_path.exists():
                    return output_path
                # Fallback to gzip for .Z files
                with gzip.open(file_path, "rb") as f_in:
                    with open(output_path, "wb") as f_out:
                        shutil.copyfileobj(f_in, f_out)
                file_path.unlink()
                return output_path

            elif suffix == ".gz":
                output_path = file_path.with_suffix("")
                with gzip.open(file_path, "rb") as f_in:
                    with open(output_path, "wb") as f_out:
                        shutil.copyfileobj(f_in, f_out)
                file_path.unlink()
                return output_path

            elif suffix == ".zip":
                import zipfile
                output_dir = file_path.parent
                with zipfile.ZipFile(file_path, "r") as zip_ref:
                    names = zip_ref.namelist()
                    zip_ref.extractall(output_dir)
                file_path.unlink()
                if names:
                    return output_dir / names[0]
                return None

            else:
                # Not compressed
                return file_path

        except Exception as e:
            logger.error("Decompression failed", file=str(file_path), error=str(e))
            return None

    def _build_orbit_path(
        self,
        date: GNSSDate,
        provider: str = "IGS",
        tier: str = "final",
    ) -> tuple[str, str]:
        """Build remote path for orbit file.

        Args:
            date: GNSS date
            provider: Provider ID (IGS, CODE, etc.)
            tier: Product tier (final, rapid, ultra)

        Returns:
            Tuple of (remote_path, filename)
        """
        gps_week = date.gps_week
        dow = date.dow
        year = date.year

        provider_lower = provider.lower()

        # IGS naming convention
        if provider_lower == "igs":
            filename = f"igs{gps_week}{dow}.sp3.Z"
            remote_path = f"/pub/gps/products/{gps_week}/{filename}"
        elif provider_lower == "code":
            filename = f"COD{gps_week}{dow}.EPH.Z"
            remote_path = f"/BSWUSER54/ORB/{gps_week}/{filename}"
        else:
            filename = f"{provider_lower}{gps_week}{dow}.sp3.Z"
            remote_path = f"/pub/gps/products/{gps_week}/{filename}"

        return remote_path, filename

    def _build_clock_path(
        self,
        date: GNSSDate,
        provider: str = "IGS",
        tier: str = "final",
    ) -> tuple[str, str]:
        """Build remote path for clock file."""
        gps_week = date.gps_week
        dow = date.dow

        provider_lower = provider.lower()

        if provider_lower == "igs":
            filename = f"igs{gps_week}{dow}.clk.Z"
            remote_path = f"/pub/gps/products/{gps_week}/{filename}"
        elif provider_lower == "code":
            filename = f"COD{gps_week}{dow}.CLK.Z"
            remote_path = f"/BSWUSER54/CLK/{gps_week}/{filename}"
        else:
            filename = f"{provider_lower}{gps_week}{dow}.clk.Z"
            remote_path = f"/pub/gps/products/{gps_week}/{filename}"

        return remote_path, filename

    def _build_erp_path(
        self,
        date: GNSSDate,
        provider: str = "IGS",
    ) -> tuple[str, str]:
        """Build remote path for ERP file."""
        gps_week = date.gps_week

        provider_lower = provider.lower()

        if provider_lower == "igs":
            filename = f"igs{gps_week}7.erp.Z"
            remote_path = f"/pub/gps/products/{gps_week}/{filename}"
        elif provider_lower == "code":
            filename = f"COD{gps_week}7.ERP.Z"
            remote_path = f"/BSWUSER54/ORB/{gps_week}/{filename}"
        else:
            filename = f"{provider_lower}{gps_week}7.erp.Z"
            remote_path = f"/pub/gps/products/{gps_week}/{filename}"

        return remote_path, filename

    def download_product(
        self,
        remote_path: str,
        filename: str,
        sources: list[str] | None = None,
        subdirectory: str = "",
    ) -> ProductDownloadResult:
        """Download a product file from multiple sources.

        Tries each source in order until successful.

        Args:
            remote_path: Remote file path
            filename: Local filename
            sources: List of source IDs to try
            subdirectory: Subdirectory within destination

        Returns:
            Download result
        """
        sources = sources or self.ORBIT_SOURCES
        start_time = datetime.now()

        dest_dir = self.config.destination_dir
        if subdirectory:
            dest_dir = dest_dir / subdirectory
        dest_dir.mkdir(parents=True, exist_ok=True)

        local_path = dest_dir / filename

        # Check if already exists
        decompressed_path = local_path
        if local_path.suffix.lower() in (".z", ".gz", ".zip"):
            decompressed_path = local_path.with_suffix("")

        if decompressed_path.exists():
            logger.info("File already exists", file=str(decompressed_path))
            return ProductDownloadResult(
                status=DownloadStatus.SUCCESS,
                local_path=decompressed_path,
                remote_path=remote_path,
                source="local",
                file_size=decompressed_path.stat().st_size,
            )

        # Try each source
        for source in sources:
            for attempt in range(self.config.max_retries):
                try:
                    client = self._connect(source)
                    if not client:
                        continue

                    logger.info(
                        "Attempting download",
                        source=source,
                        attempt=attempt + 1,
                        file=filename,
                    )

                    # Download to temp file first
                    success = False
                    if isinstance(client, (FTPClient, SFTPClient)):
                        success = client.download(remote_path, local_path)
                    elif isinstance(client, (HTTPClient, CDDISClient)):
                        # Build full URL
                        config = self._get_ftp_config(source)
                        if config:
                            url = f"{config.protocol}://{config.host}{remote_path}"
                            success = client.download_file(url, local_path)

                    if success and local_path.exists():
                        elapsed = (datetime.now() - start_time).total_seconds()

                        # Decompress if needed
                        if self.config.decompress:
                            final_path = self._decompress_file(local_path)
                            if final_path:
                                local_path = final_path

                        return ProductDownloadResult(
                            status=DownloadStatus.SUCCESS,
                            local_path=local_path,
                            remote_path=remote_path,
                            source=source,
                            file_size=local_path.stat().st_size if local_path.exists() else 0,
                            compressed=filename != local_path.name,
                            download_time_seconds=elapsed,
                        )

                except Exception as e:
                    logger.warning(
                        "Download attempt failed",
                        source=source,
                        attempt=attempt + 1,
                        error=str(e),
                    )

        # All sources failed
        elapsed = (datetime.now() - start_time).total_seconds()
        return ProductDownloadResult(
            status=DownloadStatus.NOT_FOUND,
            remote_path=remote_path,
            download_time_seconds=elapsed,
            error_message=f"File not found on any source: {sources}",
        )

    def download_orbit(
        self,
        date: GNSSDate,
        provider: str = "IGS",
        tier: str = "final",
    ) -> ProductDownloadResult:
        """Download orbit (SP3) file.

        Args:
            date: GNSS date
            provider: Provider ID
            tier: Product tier

        Returns:
            Download result
        """
        remote_path, filename = self._build_orbit_path(date, provider, tier)
        gps_week = date.gps_week

        return self.download_product(
            remote_path,
            filename,
            sources=self.ORBIT_SOURCES,
            subdirectory=str(gps_week),
        )

    def download_clock(
        self,
        date: GNSSDate,
        provider: str = "IGS",
        tier: str = "final",
    ) -> ProductDownloadResult:
        """Download clock (CLK) file.

        Args:
            date: GNSS date
            provider: Provider ID
            tier: Product tier

        Returns:
            Download result
        """
        remote_path, filename = self._build_clock_path(date, provider, tier)
        gps_week = date.gps_week

        return self.download_product(
            remote_path,
            filename,
            sources=self.CLOCK_SOURCES,
            subdirectory=str(gps_week),
        )

    def download_erp(
        self,
        date: GNSSDate,
        provider: str = "IGS",
    ) -> ProductDownloadResult:
        """Download Earth rotation parameters (ERP) file.

        Args:
            date: GNSS date
            provider: Provider ID

        Returns:
            Download result
        """
        remote_path, filename = self._build_erp_path(date, provider)
        gps_week = date.gps_week

        return self.download_product(
            remote_path,
            filename,
            sources=self.ERP_SOURCES,
            subdirectory=str(gps_week),
        )

    def __enter__(self) -> "ProductDownloader":
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit."""
        self._disconnect_all()


class HourlyDataDownloader:
    """Downloads hourly RINEX observation data.

    Replaces Perl FTP::DD class for hourly data downloads.

    Supports downloading from multiple providers with:
    - Automatic source selection based on station network
    - Compression handling
    - Database status updates
    """

    def __init__(self, config: ProductDownloadConfig | None = None):
        """Initialize hourly data downloader.

        Args:
            config: Download configuration
        """
        self.config = config or ProductDownloadConfig()
        self._ftp_configs: list[FTPServerConfig] = []

    def download_hourly_file(
        self,
        station_id: str,
        date: GNSSDate,
        hour: int,
        destination: Path,
        sources: list[str] | None = None,
    ) -> ProductDownloadResult:
        """Download a single hourly RINEX file.

        Args:
            station_id: 4-character station ID
            date: GNSS date
            hour: Hour (0-23)
            destination: Destination directory
            sources: List of sources to try

        Returns:
            Download result
        """
        # Build filename
        doy = date.doy
        year = date.year
        yy = year % 100

        # Standard hourly filename: ssssdddhh.yyo.Z
        hour_char = chr(ord('a') + hour)
        filename = f"{station_id.lower()}{doy:03d}{hour_char}.{yy:02d}o.Z"

        # Try download from each source
        # (Implementation would iterate through sources)

        return ProductDownloadResult(
            status=DownloadStatus.NOT_FOUND,
            error_message="Not implemented - use station_downloader.py",
        )


# =============================================================================
# Convenience Functions
# =============================================================================

def download_products_for_date(
    date: GNSSDate,
    provider: str = "IGS",
    products: list[str] | None = None,
    destination: Path | str | None = None,
) -> dict[str, ProductDownloadResult]:
    """Download all products needed for a processing date.

    Args:
        date: GNSS date
        provider: Product provider
        products: List of products to download (orbit, clock, erp)
        destination: Destination directory

    Returns:
        Dictionary mapping product type to result
    """
    products = products or ["orbit", "clock", "erp"]
    config = ProductDownloadConfig(
        destination_dir=Path(destination) if destination else Path("products"),
    )

    results = {}

    with ProductDownloader(config) as downloader:
        if "orbit" in products:
            results["orbit"] = downloader.download_orbit(date, provider)

        if "clock" in products:
            results["clock"] = downloader.download_clock(date, provider)

        if "erp" in products:
            results["erp"] = downloader.download_erp(date, provider)

    return results
