"""
FTP server configuration management.

Loads FTP server definitions from XML configuration (ftpConfig.xml).
Replaces Perl FTPCONF.pm module.
"""

from __future__ import annotations

import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

import yaml

from pygnss_rt.utils.logging import get_logger


logger = get_logger(__name__)


@dataclass
class ProductConfig:
    """Configuration for a specific product type/tier."""

    root: str = ""
    remote_tree_type: str = ""
    prefix: str = ""
    body: str = ""
    extension: str = ""
    compression: str = ""
    format: str = ""
    suffix: str = ""
    file_pattern: str = ""


@dataclass
class DataConfig:
    """Configuration for data (RINEX) downloads."""

    root: str = ""
    remote_tree_type: str = ""
    compression: str = ""
    file_pattern: str = ""
    username: str = ""
    password: str = ""


@dataclass
class FTPServerConfig:
    """Configuration for an FTP server."""

    name: str
    url: str
    protocol: str = "ftp"
    username: str = "anonymous"
    password: str = ""
    passive: bool = True
    timeout: int = 60
    max_retries: int = 3
    base_paths: dict[str, str] = field(default_factory=dict)

    # Extended configuration from XML
    data_configs: dict[str, DataConfig] = field(default_factory=dict)  # daily, hourly, subhourly
    product_configs: dict[str, dict[str, ProductConfig]] = field(default_factory=dict)  # eph/erp/clk -> tier -> config

    @classmethod
    def from_dict(cls, name: str, data: dict[str, Any]) -> FTPServerConfig:
        """Create from dictionary (YAML format)."""
        return cls(
            name=name,
            url=data.get("url", ""),
            protocol=data.get("protocol", "ftp"),
            username=data.get("username", "anonymous"),
            password=data.get("password", ""),
            passive=data.get("passive", True),
            timeout=data.get("timeout", 60),
            max_retries=data.get("max_retries", 3),
            base_paths=data.get("paths", {}),
        )


class FTPConfigManager:
    """Manages FTP server configurations from XML file.

    Provides a Python interface to the ftpConfig.xml file,
    equivalent to the Perl FTPCONF.pm module.
    """

    def __init__(self, config_path: Path | str | None = None):
        """Initialize configuration manager.

        Args:
            config_path: Path to ftpConfig.xml file
        """
        self._servers: dict[str, FTPServerConfig] = {}
        self._xml_tree: ET.ElementTree | None = None
        self._xml_root: ET.Element | None = None

        if config_path:
            self.load(config_path)

    def load(self, config_path: Path | str) -> int:
        """Load FTP configuration from XML file.

        Args:
            config_path: Path to ftpConfig.xml

        Returns:
            Number of servers loaded
        """
        path = Path(config_path)
        if not path.exists():
            raise FileNotFoundError(f"FTP config file not found: {path}")

        logger.info("Loading FTP config", path=str(path))

        self._xml_tree = ET.parse(path)
        self._xml_root = self._xml_tree.getroot()

        # Parse each server configuration
        for server_elem in self._xml_root:
            if server_elem.tag == "configuration":
                continue  # Skip root attributes

            server_name = server_elem.tag
            try:
                config = self._parse_server(server_name, server_elem)
                self._servers[server_name] = config
            except Exception as e:
                logger.warning(
                    "Failed to parse server config",
                    server=server_name,
                    error=str(e),
                )

        logger.info("Loaded FTP servers", count=len(self._servers))
        return len(self._servers)

    def _parse_server(self, name: str, elem: ET.Element) -> FTPServerConfig:
        """Parse a single server configuration element."""
        # Get URL and protocol
        url_elem = elem.find("url")
        url = ""
        protocol = "ftp"

        if url_elem is not None:
            url = url_elem.text or ""
            protocol = url_elem.get("protocol", "ftp")

        config = FTPServerConfig(
            name=name,
            url=url,
            protocol=protocol,
        )

        # Parse data configurations
        data_elem = elem.find("data")
        if data_elem is not None:
            for data_type in ["daily", "hourly", "subhourly", "met", "highrate"]:
                type_elem = data_elem.find(data_type)
                if type_elem is not None:
                    config.data_configs[data_type] = self._parse_data_config(type_elem)

        # Parse product configurations
        products_elem = elem.find("products")
        if products_elem is not None:
            for category in ["eph", "erp", "clk", "dcb", "bia", "ion", "iep"]:
                cat_elem = products_elem.find(category)
                if cat_elem is not None:
                    config.product_configs[category] = {}
                    for tier_elem in cat_elem:
                        tier_name = tier_elem.tag
                        config.product_configs[category][tier_name] = self._parse_product_config(tier_elem)

        # Also check for BSWUSER50/BSWUSER54 (Bernese user directories)
        for bsw in ["BSWUSER50", "BSWUSER54"]:
            bsw_elem = elem.find(bsw)
            if bsw_elem is not None:
                root = bsw_elem.findtext("root", "")
                config.base_paths[bsw.lower()] = root

        return config

    def _parse_data_config(self, elem: ET.Element) -> DataConfig:
        """Parse a data configuration element (daily/hourly/etc)."""
        return DataConfig(
            root=elem.findtext("root", ""),
            remote_tree_type=elem.findtext("remote_tree_type", ""),
            compression=elem.findtext("compression", ""),
            file_pattern=elem.findtext("filePattern", ""),
            username=elem.findtext("username", ""),
            password=elem.findtext("password", ""),
        )

    def _parse_product_config(self, elem: ET.Element) -> ProductConfig:
        """Parse a product configuration element."""
        return ProductConfig(
            root=elem.findtext("root", ""),
            remote_tree_type=elem.findtext("remote_tree_type", ""),
            prefix=elem.findtext("prefix", ""),
            body=elem.findtext("body", ""),
            extension=elem.findtext("extension", ""),
            compression=elem.findtext("compression", ""),
            format=elem.findtext("format", ""),
            suffix=elem.findtext("sufix", ""),  # Note: XML uses 'sufix' not 'suffix'
            file_pattern=elem.findtext("filePattern", ""),
        )

    def get_server(self, server_name: str) -> FTPServerConfig | None:
        """Get server configuration by name."""
        return self._servers.get(server_name)

    def get_url(self, server_name: str) -> str:
        """Get URL for a server."""
        server = self._servers.get(server_name)
        return server.url if server else ""

    def get_protocol(self, server_name: str) -> str:
        """Get protocol for a server."""
        server = self._servers.get(server_name)
        return server.protocol if server else "ftp"

    def get_data_root(
        self,
        server_name: str,
        data_type: str,  # daily, hourly, subhourly
    ) -> str:
        """Get root path for data downloads.

        Args:
            server_name: Server ID
            data_type: Data type (daily, hourly, subhourly)

        Returns:
            Root path for data
        """
        server = self._servers.get(server_name)
        if not server:
            return ""

        data_config = server.data_configs.get(data_type)
        return data_config.root if data_config else ""

    def get_data_remote_tree_type(
        self,
        server_name: str,
        data_type: str,
    ) -> str:
        """Get remote tree type for data downloads.

        Tree type patterns:
        - yyyy/doy
        - yyyy/doy/yyd
        - doy/hn
        - etc.
        """
        server = self._servers.get(server_name)
        if not server:
            return ""

        data_config = server.data_configs.get(data_type)
        return data_config.remote_tree_type if data_config else ""

    def get_data_compression(
        self,
        server_name: str,
        data_type: str,
    ) -> str:
        """Get compression extension for data files."""
        server = self._servers.get(server_name)
        if not server:
            return ""

        data_config = server.data_configs.get(data_type)
        return data_config.compression if data_config else ""

    def get_data_credentials(
        self,
        server_name: str,
        data_type: str,
    ) -> tuple[str, str]:
        """Get username and password for data downloads.

        Returns:
            Tuple of (username, password)
        """
        server = self._servers.get(server_name)
        if not server:
            return "", ""

        data_config = server.data_configs.get(data_type)
        if data_config:
            return data_config.username, data_config.password
        return "", ""

    def get_product_root(
        self,
        server_name: str,
        category: str,  # eph, erp, clk, dcb, bia
        tier: str,  # final, rapid, ultra, predicted
    ) -> str:
        """Get root path for product downloads."""
        server = self._servers.get(server_name)
        if not server:
            return ""

        cat_config = server.product_configs.get(category, {})
        product_config = cat_config.get(tier)
        return product_config.root if product_config else ""

    def get_product_remote_tree_type(
        self,
        server_name: str,
        category: str,
        tier: str,
    ) -> str:
        """Get remote tree type for product downloads."""
        server = self._servers.get(server_name)
        if not server:
            return ""

        cat_config = server.product_configs.get(category, {})
        product_config = cat_config.get(tier)
        return product_config.remote_tree_type if product_config else ""

    def get_product_prefix(
        self,
        server_name: str,
        category: str,
        tier: str,
    ) -> str:
        """Get filename prefix for products."""
        server = self._servers.get(server_name)
        if not server:
            return ""

        cat_config = server.product_configs.get(category, {})
        product_config = cat_config.get(tier)
        return product_config.prefix if product_config else ""

    def get_product_body(
        self,
        server_name: str,
        category: str,
        tier: str,
    ) -> str:
        """Get filename body pattern for products."""
        server = self._servers.get(server_name)
        if not server:
            return ""

        cat_config = server.product_configs.get(category, {})
        product_config = cat_config.get(tier)
        return product_config.body if product_config else ""

    def get_product_extension(
        self,
        server_name: str,
        category: str,
        tier: str,
    ) -> str:
        """Get file extension for products."""
        server = self._servers.get(server_name)
        if not server:
            return ""

        cat_config = server.product_configs.get(category, {})
        product_config = cat_config.get(tier)
        return product_config.extension if product_config else ""

    def get_product_compression(
        self,
        server_name: str,
        category: str,
        tier: str,
    ) -> str:
        """Get compression extension for products."""
        server = self._servers.get(server_name)
        if not server:
            return ""

        cat_config = server.product_configs.get(category, {})
        product_config = cat_config.get(tier)
        return product_config.compression if product_config else ""

    def get_product_suffix(
        self,
        server_name: str,
        category: str,
        tier: str,
    ) -> str:
        """Get filename suffix for products (IGS long format)."""
        server = self._servers.get(server_name)
        if not server:
            return ""

        cat_config = server.product_configs.get(category, {})
        product_config = cat_config.get(tier)
        return product_config.suffix if product_config else ""

    def get_product_format(
        self,
        server_name: str,
        category: str,
        tier: str,
    ) -> str:
        """Get product format (SP3, ERP, IERS, etc)."""
        server = self._servers.get(server_name)
        if not server:
            return ""

        cat_config = server.product_configs.get(category, {})
        product_config = cat_config.get(tier)
        return product_config.format if product_config else ""

    def get_product_config(
        self,
        server_name: str,
        category: str,
        tier: str,
    ) -> ProductConfig | None:
        """Get full product configuration."""
        server = self._servers.get(server_name)
        if not server:
            return None

        cat_config = server.product_configs.get(category, {})
        return cat_config.get(tier)

    def build_remote_path(
        self,
        server_name: str,
        path_type: str,  # 'data' or 'products'
        category: str,  # For data: daily/hourly/subhourly. For products: eph/erp/clk
        tier: str | None = None,  # For products: final/rapid/ultra
        year: int | None = None,
        doy: int | None = None,
        gps_week: int | None = None,
        hour: int | None = None,
        station: str | None = None,
    ) -> str:
        """Build the remote path for a file.

        Expands tree type patterns to actual paths.

        Args:
            server_name: Server ID
            path_type: 'data' or 'products'
            category: Data type or product category
            tier: Product tier (for products only)
            year: Year
            doy: Day of year
            gps_week: GPS week
            hour: Hour (0-23)
            station: Station code (for station-specific paths)

        Returns:
            Expanded remote path
        """
        server = self._servers.get(server_name)
        if not server:
            return ""

        if path_type == "data":
            data_config = server.data_configs.get(category)
            if not data_config:
                return ""
            root = data_config.root
            tree_type = data_config.remote_tree_type
        else:
            cat_config = server.product_configs.get(category, {})
            product_config = cat_config.get(tier) if tier else None
            if not product_config:
                return ""
            root = product_config.root
            tree_type = product_config.remote_tree_type

        # Expand tree type pattern
        path = self._expand_tree_type(
            tree_type,
            year=year,
            doy=doy,
            gps_week=gps_week,
            hour=hour,
            station=station,
        )

        if root and path:
            return f"{root}/{path}"
        elif root:
            return root
        return path

    def _expand_tree_type(
        self,
        tree_type: str,
        year: int | None = None,
        doy: int | None = None,
        gps_week: int | None = None,
        hour: int | None = None,
        station: str | None = None,
    ) -> str:
        """Expand tree type pattern to actual path.

        Pattern elements:
        - yyyy: 4-digit year
        - yy: 2-digit year
        - doy: 3-digit day of year
        - gpsweek: GPS week number
        - hn: Numeric hour (00-23)
        - ha: Alphabetic hour (a-x)
        - ssss: Station code
        - yyd: Year/type suffix for RINEX
        """
        if not tree_type:
            return ""

        from pygnss_rt.utils.format import hour_to_alpha

        result = tree_type

        if year is not None:
            result = result.replace("yyyy", f"{year:04d}")
            result = result.replace("yy", f"{year % 100:02d}")

        if doy is not None:
            result = result.replace("doy", f"{doy:03d}")

        if gps_week is not None:
            result = result.replace("gpsweek", f"{gps_week:04d}")

        if hour is not None:
            result = result.replace("hn", f"{hour:02d}")
            try:
                result = result.replace("ha", hour_to_alpha(hour))
            except ValueError:
                pass

        if station is not None:
            result = result.replace("ssss", station.lower())
            result = result.replace("site", station.lower())

        # Handle special suffixes
        if year is not None:
            result = result.replace("yyd", f"{year % 100:02d}d")
            result = result.replace("yyn", f"{year % 100:02d}n")

        return result

    def build_filename(
        self,
        server_name: str,
        category: str,
        tier: str,
        year: int | None = None,
        doy: int | None = None,
        gps_week: int | None = None,
        day_of_week: int | None = None,
        hour: int | None = None,
    ) -> str:
        """Build product filename from configuration.

        Args:
            server_name: Server ID
            category: Product category (eph, erp, clk, etc.)
            tier: Product tier (final, rapid, ultra)
            year, doy, gps_week, day_of_week, hour: Date/time components

        Returns:
            Formatted filename
        """
        config = self.get_product_config(server_name, category, tier)
        if not config:
            return ""

        # Build filename from components
        filename = ""

        # Prefix
        if config.prefix:
            filename += config.prefix

        # Body - expand patterns
        body = config.body
        if body:
            if year is not None:
                body = body.replace("yyyy", f"{year:04d}")
                body = body.replace("yy", f"{year % 100:02d}")
            if doy is not None:
                body = body.replace("ddd", f"{doy:03d}")
            if gps_week is not None:
                body = body.replace("wwww", f"{gps_week:04d}")
            if day_of_week is not None:
                body = body.replace("d", str(day_of_week), 1)  # Replace first 'd' only for day of week
            if hour is not None:
                body = body.replace("hh", f"{hour:02d}")
                body = body.replace("mm", "00")  # Default minute
                body = body.replace("_hn", f"_{hour:02d}")
            else:
                # Default for daily products
                body = body.replace("hhmm", "0000")
                body = body.replace("hh", "00")
                body = body.replace("mm", "00")
            filename += body

        # Suffix (for IGS long format)
        if config.suffix:
            filename += config.suffix

        # Extension
        if config.extension:
            filename += config.extension

        # Compression
        if config.compression:
            filename += config.compression

        return filename

    def list_servers(self) -> list[str]:
        """List all configured server names."""
        return list(self._servers.keys())

    def __len__(self) -> int:
        return len(self._servers)

    def __contains__(self, server_name: str) -> bool:
        return server_name in self._servers


def load_ftp_config(config_path: Path | str) -> dict[str, FTPServerConfig]:
    """Load FTP server configurations from YAML file.

    Args:
        config_path: Path to YAML configuration file

    Returns:
        Dictionary mapping server names to configurations
    """
    path = Path(config_path)
    if not path.exists():
        return {}

    with open(path) as f:
        data = yaml.safe_load(f)

    servers: dict[str, FTPServerConfig] = {}

    for name, server_data in data.get("servers", {}).items():
        servers[name] = FTPServerConfig.from_dict(name, server_data)

    return servers


def load_ftp_config_xml(config_path: Path | str) -> FTPConfigManager:
    """Load FTP configuration from XML file.

    Args:
        config_path: Path to ftpConfig.xml

    Returns:
        FTPConfigManager instance
    """
    manager = FTPConfigManager(config_path)
    return manager


# Default server configurations (fallback)
DEFAULT_SERVERS = {
    "CDDIS": FTPServerConfig(
        name="CDDIS",
        url="cddis.nasa.gov",
        protocol="https",
        timeout=120,
        base_paths={
            "orbit": "/archive/gnss/products/{week}",
            "clock": "/archive/gnss/products/{week}",
            "erp": "/archive/gnss/products/{week}",
            "rinex": "/archive/gnss/data/daily/{year}/{doy}/{yy}d",
        },
    ),
    "IGS": FTPServerConfig(
        name="IGS",
        url="ftp.igs.org",
        protocol="ftp",
        base_paths={
            "orbit": "/pub/product/{week}",
            "clock": "/pub/product/{week}",
            "erp": "/pub/product/{week}",
        },
    ),
    "CODE": FTPServerConfig(
        name="CODE",
        url="ftp.aiub.unibe.ch",
        protocol="ftp",
        base_paths={
            "orbit": "/CODE/{year}",
            "dcb": "/CODE/{year}",
            "erp": "/CODE/{year}",
            "clk": "/CODE/{year}",
        },
    ),
    "BKGE": FTPServerConfig(
        name="BKGE",
        url="igs-ftp.bkg.bund.de",
        protocol="ftp",
        base_paths={
            "daily": "/EUREF/obs/{year}/{doy}",
            "hourly": "/EUREF/nrt/{doy}",
        },
    ),
}
