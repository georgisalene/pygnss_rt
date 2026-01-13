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
        self._ftp_configs: dict[str, FTPServerConfig] = {}
        self._active_connections: dict[str, Any] = {}

    def _load_ftp_configs(self) -> dict[str, FTPServerConfig]:
        """Load FTP server configurations."""
        if self._ftp_configs:
            return self._ftp_configs

        # Use default server configurations
        from pygnss_rt.data_access.ftp_config import DEFAULT_SERVERS
        self._ftp_configs = DEFAULT_SERVERS.copy()

        # Optionally load from XML if provided
        if self.config.ftp_config_path and self.config.ftp_config_path.exists():
            try:
                from pygnss_rt.data_access.ftp_config import load_ftp_config_xml
                manager = load_ftp_config_xml(self.config.ftp_config_path)
                # Merge with defaults (XML overrides defaults)
                for name in manager.list_servers():
                    server = manager.get_server(name)
                    if server:
                        self._ftp_configs[name] = FTPServerConfig(
                            name=name,
                            url=server.url,
                            protocol=server.protocol,
                            username=server.username,
                            password=server.password,
                            timeout=server.timeout,
                        )
            except Exception as e:
                logger.warning("Could not load FTP config XML", error=str(e))

        return self._ftp_configs

    def _get_ftp_config(self, source_id: str) -> FTPServerConfig | None:
        """Get FTP configuration for a source."""
        configs = self._load_ftp_configs()
        return configs.get(source_id.upper()) or configs.get(source_id)

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

        # Get host from url field
        host = config.url

        try:
            if source_id == "CDDIS":
                # CDDIS requires HTTPS with Earthdata login
                client = CDDISClient(timeout=self.config.timeout)
            elif config.protocol == "sftp":
                client = SFTPClient(
                    host=host,
                    username=config.username or "anonymous",
                    password=config.password or "",
                    timeout=self.config.timeout,
                )
                client.connect()
            elif config.protocol in ("ftp", ""):
                client = FTPClient(
                    host=host,
                    username=config.username or "anonymous",
                    password=config.password or "",
                    timeout=self.config.timeout,
                    passive=True,
                )
                client.connect()
            else:
                client = HTTPClient(
                    base_url=f"{config.protocol}://{host}",
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

    def _build_orbit_paths(
        self,
        date: GNSSDate,
        provider: str = "IGS",
        tier: str = "final",
    ) -> dict[str, tuple[str, str]]:
        """Build remote paths for orbit file for each server.

        Args:
            date: GNSS date
            provider: Provider ID (IGS, CODE, etc.)
            tier: Product tier (final, rapid, ultra)

        Returns:
            Dict mapping server_id to (remote_path, filename)
        """
        gps_week = date.gps_week
        dow = date.day_of_week
        year = date.year
        doy = date.doy

        paths = {}

        # IGS long-format naming convention (since 2022)
        if tier == "final":
            filename = f"IGS0OPSFIN_{year}{doy:03d}0000_01D_15M_ORB.SP3.gz"
        elif tier == "rapid":
            filename = f"IGS0OPSRAP_{year}{doy:03d}0000_01D_15M_ORB.SP3.gz"
        else:  # ultra
            filename = f"IGS0OPSULT_{year}{doy:03d}0000_02D_15M_ORB.SP3.gz"

        # CDDIS path (HTTPS)
        paths["CDDIS"] = (f"/archive/gnss/products/{gps_week}/{filename}", filename)

        # IGN path (similar to IGS FTP)
        paths["IGN"] = (f"/pub/igs/products/{gps_week}/{filename}", filename)

        # CODE FTP path (use their own products)
        code_filename = f"COD{gps_week}{dow}.EPH.Z"
        paths["CODE"] = (f"/CODE/{year}/{code_filename}", code_filename)

        return paths

    def _build_orbit_path(
        self,
        date: GNSSDate,
        provider: str = "IGS",
        tier: str = "final",
    ) -> tuple[str, str]:
        """Build remote path for orbit file (legacy interface).

        Args:
            date: GNSS date
            provider: Provider ID (IGS, CODE, etc.)
            tier: Product tier (final, rapid, ultra)

        Returns:
            Tuple of (remote_path, filename) for CDDIS
        """
        paths = self._build_orbit_paths(date, provider, tier)
        return paths.get("CDDIS", paths.get("IGN", list(paths.values())[0]))

    def _build_clock_paths(
        self,
        date: GNSSDate,
        provider: str = "IGS",
        tier: str = "final",
    ) -> dict[str, tuple[str, str]]:
        """Build remote paths for clock file for each server.

        Args:
            date: GNSS date
            provider: Provider ID (IGS, CODE, etc.)
            tier: Product tier (final, rapid, ultra)

        Returns:
            Dict mapping server_id to (remote_path, filename)
        """
        gps_week = date.gps_week
        dow = date.day_of_week
        year = date.year
        doy = date.doy

        paths = {}

        # IGS long-format naming convention
        if tier == "final":
            filename = f"IGS0OPSFIN_{year}{doy:03d}0000_01D_30S_CLK.CLK.gz"
        elif tier == "rapid":
            filename = f"IGS0OPSRAP_{year}{doy:03d}0000_01D_30S_CLK.CLK.gz"
        else:  # ultra
            filename = f"IGS0OPSULT_{year}{doy:03d}0000_01D_30S_CLK.CLK.gz"

        # CDDIS path (HTTPS)
        paths["CDDIS"] = (f"/archive/gnss/products/{gps_week}/{filename}", filename)

        # IGN path
        paths["IGN"] = (f"/pub/igs/products/{gps_week}/{filename}", filename)

        # CODE FTP path (use their own products)
        code_filename = f"COD{gps_week}{dow}.CLK.Z"
        paths["CODE"] = (f"/CODE/{year}/{code_filename}", code_filename)

        return paths

    def _build_clock_path(
        self,
        date: GNSSDate,
        provider: str = "IGS",
        tier: str = "final",
    ) -> tuple[str, str]:
        """Build remote path for clock file (legacy interface)."""
        paths = self._build_clock_paths(date, provider, tier)
        return paths.get("CDDIS", paths.get("IGN", list(paths.values())[0]))

    def _build_erp_paths(
        self,
        date: GNSSDate,
        provider: str = "IGS",
    ) -> dict[str, tuple[str, str]]:
        """Build remote paths for ERP file for each server.

        IGS now uses daily ERP files from CODE (COD0OPSFIN) for final products.

        Args:
            date: GNSS date
            provider: Provider ID (IGS, CODE, etc.)

        Returns:
            Dict mapping server_id to (remote_path, filename)
        """
        gps_week = date.gps_week
        dow = date.day_of_week
        year = date.year
        doy = date.doy

        paths = {}

        # IGS long-format naming: daily ERP from CODE (COD0OPSFIN)
        # Format: COD0OPSFIN_YYYYDOY0000_01D_01D_ERP.ERP.gz
        filename = f"COD0OPSFIN_{year}{doy:03d}0000_01D_01D_ERP.ERP.gz"

        # CDDIS path (HTTPS)
        paths["CDDIS"] = (f"/archive/gnss/products/{gps_week}/{filename}", filename)

        # IGN path
        paths["IGN"] = (f"/pub/igs/products/{gps_week}/{filename}", filename)

        # CODE FTP path (their short format daily product)
        code_filename = f"COD{gps_week}{dow}.ERP.Z"
        paths["CODE"] = (f"/CODE/{year}/{code_filename}", code_filename)

        return paths

    def _build_erp_path(
        self,
        date: GNSSDate,
        provider: str = "IGS",
    ) -> tuple[str, str]:
        """Build remote path for ERP file (legacy interface)."""
        paths = self._build_erp_paths(date, provider)
        return paths.get("CDDIS", paths.get("IGN", list(paths.values())[0]))

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
                            url = f"{config.protocol}://{config.url}{remote_path}"
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

    def _download_with_server_paths(
        self,
        server_paths: dict[str, tuple[str, str]],
        sources: list[str] | None = None,
        subdirectory: str = "",
    ) -> ProductDownloadResult:
        """Download a product file using server-specific paths.

        Args:
            server_paths: Dict mapping server_id to (remote_path, filename)
            sources: List of source IDs to try
            subdirectory: Subdirectory within destination

        Returns:
            Download result
        """
        sources = sources or self.ORBIT_SOURCES
        start_time = datetime.now()

        # Get first filename for local storage
        first_entry = list(server_paths.values())[0] if server_paths else ("", "")
        filename = first_entry[1]

        dest_dir = self.config.destination_dir
        if subdirectory:
            dest_dir = dest_dir / subdirectory
        dest_dir.mkdir(parents=True, exist_ok=True)

        local_path = dest_dir / filename

        # Check if already exists (any variant)
        for _, (_, fn) in server_paths.items():
            decompressed_name = fn
            for ext in [".gz", ".Z", ".zip"]:
                if decompressed_name.endswith(ext):
                    decompressed_name = decompressed_name[:-len(ext)]
            decompressed_path = dest_dir / decompressed_name
            if decompressed_path.exists():
                logger.info("File already exists", file=str(decompressed_path))
                return ProductDownloadResult(
                    status=DownloadStatus.SUCCESS,
                    local_path=decompressed_path,
                    remote_path="",
                    source="local",
                    file_size=decompressed_path.stat().st_size,
                )

        # Try each source with its specific path
        for source in sources:
            if source not in server_paths:
                continue

            remote_path, source_filename = server_paths[source]
            local_path = dest_dir / source_filename

            for attempt in range(self.config.max_retries):
                try:
                    client = self._connect(source)
                    if not client:
                        continue

                    logger.info(
                        "Attempting download",
                        source=source,
                        attempt=attempt + 1,
                        file=source_filename,
                    )

                    # Download
                    success = False
                    if isinstance(client, (FTPClient, SFTPClient)):
                        success = client.download(remote_path, local_path)
                    elif isinstance(client, (HTTPClient, CDDISClient)):
                        config = self._get_ftp_config(source)
                        if config:
                            url = f"{config.protocol}://{config.url}{remote_path}"
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
                            compressed=source_filename != local_path.name,
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
            remote_path="",
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
        gps_week = date.gps_week
        paths = self._build_orbit_paths(date, provider, tier)

        return self._download_with_server_paths(
            paths,
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
        gps_week = date.gps_week
        paths = self._build_clock_paths(date, provider, tier)

        return self._download_with_server_paths(
            paths,
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
        gps_week = date.gps_week
        paths = self._build_erp_paths(date, provider)

        return self._download_with_server_paths(
            paths,
            sources=self.ERP_SOURCES,
            subdirectory=str(gps_week),
        )

    def _build_bia_paths(
        self,
        date: GNSSDate,
        provider: str = "CODE",
    ) -> dict[str, tuple[str, str]]:
        """Build remote paths for BIA/OSB (Observable-Specific Signal Biases) file.

        For PPP-AR, we need the satellite signal biases (OSB) which come from
        CODE in the long filename format.

        Args:
            date: GNSS date
            provider: Provider ID (CODE recommended for PPP-AR)

        Returns:
            Dict mapping server_id to (remote_path, filename)
        """
        gps_week = date.gps_week
        dow = date.day_of_week
        year = date.year
        doy = date.doy

        paths = {}

        # CODE long-format OSB (for PPP-AR ambiguity resolution)
        # Format: COD0OPSFIN_YYYYDOY0000_01D_01D_OSB.BIA.gz
        bia_filename = f"COD0OPSFIN_{year}{doy:03d}0000_01D_01D_OSB.BIA.gz"
        paths["CDDIS"] = (f"/archive/gnss/products/{gps_week}/{bia_filename}", bia_filename)
        paths["IGN"] = (f"/pub/igs/products/{gps_week}/{bia_filename}", bia_filename)

        # Also try shorter CODE format
        code_filename = f"COD{gps_week}{dow}.BIA.Z"
        paths["CODE"] = (f"/CODE/{year}/{code_filename}", code_filename)

        return paths

    def download_bia(
        self,
        date: GNSSDate,
        provider: str = "CODE",
    ) -> ProductDownloadResult:
        """Download BIA/OSB (Observable-Specific Signal Biases) file.

        Required for PPP-AR (Precise Point Positioning with Ambiguity Resolution).

        Args:
            date: GNSS date
            provider: Provider ID (CODE recommended)

        Returns:
            Download result
        """
        gps_week = date.gps_week
        paths = self._build_bia_paths(date, provider)

        return self._download_with_server_paths(
            paths,
            sources=["CDDIS", "IGN", "CODE"],
            subdirectory=str(gps_week),
        )

    def _build_ion_paths(
        self,
        date: GNSSDate,
        provider: str = "CODE",
    ) -> dict[str, tuple[str, str]]:
        """Build remote paths for ION/GIM (Global Ionosphere Map) file.

        Downloads Bernese ION format (.ION) for Higher-Order Ionosphere (HOI)
        corrections from CODE FTP (ftp.aiub.unibe.ch).

        The Bernese ION format is required by GPSEST for HOI corrections.
        File naming: COD0OPSFIN_YYYYDDD0000_01D_01H_GIM.ION.gz

        Note: ION files are ONLY available from CODE FTP, not CDDIS.

        Args:
            date: GNSS date
            provider: Provider ID

        Returns:
            Dict mapping server_id to (remote_path, filename)
        """
        year = date.year
        doy = date.doy

        paths = {}

        # CODE Bernese ION format (for Higher-Order Ionosphere corrections)
        # Format: COD0OPSFIN_YYYYDDD0000_01D_01H_GIM.ION.gz
        # Only available from CODE FTP: ftp.aiub.unibe.ch/CODE/YYYY/
        ion_filename = f"COD0OPSFIN_{year}{doy:03d}0000_01D_01H_GIM.ION.gz"
        paths["CODE"] = (f"/CODE/{year}/{ion_filename}", ion_filename)

        return paths

    def download_ion(
        self,
        date: GNSSDate,
        provider: str = "CODE",
    ) -> ProductDownloadResult:
        """Download ION/GIM (Global Ionosphere Map) file.

        Bernese ION format file for Higher-Order Ionosphere (HOI) corrections.
        Goes to campaign ATM directory as HOI_YYYYDDD0.ION.

        Note: ION files are only available from CODE FTP (ftp.aiub.unibe.ch).

        Args:
            date: GNSS date
            provider: Provider ID (only CODE supported)

        Returns:
            Download result
        """
        gps_week = date.gps_week
        paths = self._build_ion_paths(date, provider)

        return self._download_with_server_paths(
            paths,
            sources=["CODE"],  # ION only from CODE
            subdirectory=str(gps_week),
        )

    def _build_vmf_paths(
        self,
        date: GNSSDate,
    ) -> dict[str, tuple[str, str]]:
        """Build remote paths for VMF3 (Vienna Mapping Functions) file.

        VMF3 provides troposphere mapping functions and zenith delays.

        Args:
            date: GNSS date

        Returns:
            Dict mapping server_id to (remote_path, filename)
        """
        year = date.year
        doy = date.doy

        paths = {}

        # VMF3 files from TU Vienna
        # Format: VMFG_YYYYDOY.H00 through VMFG_YYYYDOY.H23
        # For daily processing, we need all 24 files or a combined daily file
        vmf_filename = f"VMFG_{year}{doy:03d}.GRD"

        # TU Vienna VMF3 server
        paths["VMF"] = (f"/{year}/{vmf_filename}", vmf_filename)

        # CDDIS also hosts VMF3 in products/troposphere
        cddis_filename = f"VMF3_{year}{doy:03d}.GRD.gz"
        paths["CDDIS"] = (f"/archive/gnss/products/troposphere/vmf3/{year}/{doy:03d}/{cddis_filename}", cddis_filename)

        return paths

    def download_vmf(
        self,
        date: GNSSDate,
    ) -> ProductDownloadResult:
        """Download VMF3 (Vienna Mapping Functions) grid file.

        VMF3 provides troposphere mapping functions for GNSS processing.
        Goes to campaign GRD directory.

        Args:
            date: GNSS date

        Returns:
            Download result
        """
        gps_week = date.gps_week
        paths = self._build_vmf_paths(date)

        return self._download_with_server_paths(
            paths,
            sources=["CDDIS", "VMF"],
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
