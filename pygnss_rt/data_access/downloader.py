"""
Unified data downloader for GNSS products.

Coordinates downloads from multiple sources using FTP, SFTP, and HTTP.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from pygnss_rt.core.exceptions import ProductNotAvailableError
from pygnss_rt.data_access.ftp_client import FTPClient, SFTPClient
from pygnss_rt.data_access.http_client import HTTPClient, CDDISClient
from pygnss_rt.data_access.ftp_config import FTPServerConfig, DEFAULT_SERVERS
from pygnss_rt.database.models import ProductTier, ProductType
from pygnss_rt.utils.dates import GNSSDate
from pygnss_rt.utils.logging import get_logger

if TYPE_CHECKING:
    from pygnss_rt.database.connection import DatabaseManager


logger = get_logger(__name__)


@dataclass
class DownloadResult:
    """Result of a download operation."""

    success: bool
    local_path: Path | None = None
    file_size: int | None = None
    error: str | None = None


class DataDownloader:
    """Manages downloads from various GNSS data sources."""

    def __init__(
        self,
        download_dir: Path | str = Path("downloads"),
        servers: dict[str, FTPServerConfig] | None = None,
        db: "DatabaseManager | None" = None,
    ):
        """Initialize downloader.

        Args:
            download_dir: Base directory for downloads
            servers: FTP server configurations
            db: Database manager for tracking downloads
        """
        self.download_dir = Path(download_dir)
        self.servers = servers or DEFAULT_SERVERS.copy()
        self.db = db

        # Active clients
        self._ftp_clients: dict[str, FTPClient | SFTPClient] = {}
        self._http_clients: dict[str, HTTPClient] = {}

    def _get_ftp_client(self, server_name: str) -> FTPClient | SFTPClient:
        """Get or create FTP/SFTP client for server."""
        if server_name not in self._ftp_clients:
            config = self.servers.get(server_name)
            if not config:
                raise ValueError(f"Unknown server: {server_name}")

            if config.protocol == "sftp":
                client = SFTPClient(
                    host=config.url,
                    username=config.username,
                    password=config.password,
                    timeout=config.timeout,
                )
            else:
                client = FTPClient(
                    host=config.url,
                    username=config.username,
                    password=config.password,
                    timeout=config.timeout,
                    passive=config.passive,
                )

            client.connect()
            self._ftp_clients[server_name] = client

        return self._ftp_clients[server_name]

    def _get_http_client(self, server_name: str) -> HTTPClient:
        """Get or create HTTP client for server."""
        if server_name not in self._http_clients:
            config = self.servers.get(server_name)
            if not config:
                raise ValueError(f"Unknown server: {server_name}")

            if server_name == "CDDIS":
                client = CDDISClient(timeout=config.timeout)
            else:
                base_url = f"{config.protocol}://{config.url}"
                client = HTTPClient(base_url=base_url, timeout=config.timeout)

            self._http_clients[server_name] = client

        return self._http_clients[server_name]

    def download_product(
        self,
        product_type: ProductType,
        provider: str,
        tier: ProductTier,
        date: GNSSDate,
    ) -> DownloadResult:
        """Download a GNSS product.

        Args:
            product_type: Type of product (orbit, erp, clock, dcb)
            provider: Product provider (IGS, CODE, etc.)
            tier: Product tier (final, rapid, ultra)
            date: Date for the product

        Returns:
            DownloadResult with status and local path
        """
        # Build filename based on product type and provider
        filename = self._build_filename(product_type, provider, tier, date)

        # Determine local path
        local_dir = self.download_dir / product_type.value / provider / tier.value
        local_path = local_dir / filename

        # Skip if already downloaded
        if local_path.exists():
            logger.info("Product already downloaded", path=str(local_path))
            return DownloadResult(
                success=True,
                local_path=local_path,
                file_size=local_path.stat().st_size,
            )

        # Try download
        server_config = self.servers.get(provider)
        if not server_config:
            # Try CDDIS as fallback for IGS products
            server_config = self.servers.get("CDDIS")
            if not server_config:
                return DownloadResult(
                    success=False,
                    error=f"No server configured for {provider}",
                )

        try:
            if server_config.protocol in ("http", "https"):
                success = self._download_http(
                    server_config, product_type, date, filename, local_path
                )
            else:
                success = self._download_ftp(
                    server_config, product_type, date, filename, local_path
                )

            if success:
                return DownloadResult(
                    success=True,
                    local_path=local_path,
                    file_size=local_path.stat().st_size if local_path.exists() else None,
                )
            else:
                return DownloadResult(
                    success=False,
                    error="Download failed - file not found",
                )

        except Exception as e:
            logger.error(
                "Download failed",
                product=product_type.value,
                provider=provider,
                error=str(e),
            )
            return DownloadResult(success=False, error=str(e))

    def _build_filename(
        self,
        product_type: ProductType,
        provider: str,
        tier: ProductTier,
        date: GNSSDate,
    ) -> str:
        """Build product filename."""
        week = date.gps_week
        dow = date.day_of_week

        if product_type == ProductType.ORBIT:
            if provider == "IGS":
                if tier == ProductTier.FINAL:
                    return f"igs{week:04d}{dow}.sp3.Z"
                elif tier == ProductTier.RAPID:
                    return f"igr{week:04d}{dow}.sp3.Z"
                else:
                    return f"igu{week:04d}{dow}_00.sp3.Z"
            elif provider == "CODE":
                return f"COD{week:04d}{dow}.EPH.Z"

        elif product_type == ProductType.ERP:
            if provider == "IGS":
                return f"igs{week:04d}7.erp.Z"
            elif provider == "CODE":
                return f"COD{week:04d}7.ERP.Z"

        elif product_type == ProductType.CLOCK:
            if provider == "IGS":
                if tier == ProductTier.FINAL:
                    return f"igs{week:04d}{dow}.clk.Z"
                else:
                    return f"igr{week:04d}{dow}.clk.Z"

        elif product_type == ProductType.DCB:
            if provider == "CODE":
                month_abbr = date.datetime.strftime("%b").upper()
                return f"P1C1{date.year % 100:02d}{month_abbr}.DCB.Z"

        # Default pattern
        return f"{provider.lower()}{week:04d}{dow}.{product_type.value}.Z"

    def _download_ftp(
        self,
        config: FTPServerConfig,
        product_type: ProductType,
        date: GNSSDate,
        filename: str,
        local_path: Path,
    ) -> bool:
        """Download via FTP."""
        client = self._get_ftp_client(config.name)

        # Build remote path
        path_template = config.base_paths.get(product_type.value, "/{week}")
        remote_dir = path_template.format(
            week=date.gps_week,
            year=date.year,
            doy=date.doy,
            yy=date.year % 100,
        )
        remote_path = f"{remote_dir}/{filename}"

        return client.download(remote_path, local_path)

    def _download_http(
        self,
        config: FTPServerConfig,
        product_type: ProductType,
        date: GNSSDate,
        filename: str,
        local_path: Path,
    ) -> bool:
        """Download via HTTP."""
        client = self._get_http_client(config.name)

        if isinstance(client, CDDISClient):
            return client.download_product(
                product_type.value,
                date.year,
                date.doy,
                date.gps_week,
                filename,
                local_path,
            )
        else:
            path_template = config.base_paths.get(product_type.value, "/{week}")
            remote_path = path_template.format(
                week=date.gps_week,
                year=date.year,
                doy=date.doy,
                yy=date.year % 100,
            )
            url = f"{remote_path}/{filename}"
            return client.download(url, local_path)

    def close(self) -> None:
        """Close all connections."""
        for client in self._ftp_clients.values():
            try:
                client.disconnect()
            except Exception:
                pass
        self._ftp_clients.clear()

        for client in self._http_clients.values():
            try:
                client.close()
            except Exception:
                pass
        self._http_clients.clear()

    def __enter__(self) -> "DataDownloader":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()
