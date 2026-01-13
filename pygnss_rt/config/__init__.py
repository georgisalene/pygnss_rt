"""
Configuration management for pygnss_rt.

Author: Addisu Hunegnaw
Date: January 2026

Provides YAML-based configuration loading for FTP servers, network profiles,
and other settings. Replaces the Perl XML configuration files.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml


# Default config directory (relative to this module)
CONFIG_DIR = Path(__file__).parent


@dataclass
class ServerConfig:
    """Configuration for a single FTP/HTTPS server."""

    name: str
    description: str
    protocol: str  # ftp, https, sftp
    host: str
    port: int = 0  # 0 means default for protocol
    auth_required: bool = False
    username: str = ""
    password: str = ""
    timeout: int = 120
    passive: bool = True  # For FTP


@dataclass
class ProductPathConfig:
    """Configuration for a product path on a server."""

    path: str  # Path template with placeholders
    filename: str  # Filename template


@dataclass
class ProductServerConfig(ServerConfig):
    """Configuration for a product download server."""

    products: dict[str, dict[str, ProductPathConfig]] = field(default_factory=dict)


@dataclass
class StationPathConfig:
    """Configuration for station data paths."""

    path: str
    filename: str = ""
    filename_pattern: str = ""  # For CDDIS RINEX3 pattern matching


@dataclass
class StationServerConfig(ServerConfig):
    """Configuration for a station data server."""

    rinex_version: int = 2
    compression: str = ".Z"
    daily: StationPathConfig | None = None
    hourly: StationPathConfig | None = None


class FTPConfig:
    """Manages FTP server configuration.

    Loads configuration from YAML file and provides access to server configs.

    Usage:
        config = FTPConfig()  # Uses default config file
        config = FTPConfig("/path/to/custom/config.yaml")

        # Get product server
        cddis = config.get_product_server("CDDIS")

        # Get station server
        bkge = config.get_station_server("BKGE_IGS")

        # Get provider priority
        orbit_providers = config.get_provider_priority("products", "orbit")
    """

    def __init__(self, config_path: str | Path | None = None):
        """Initialize FTP configuration.

        Args:
            config_path: Path to YAML config file. If None, uses default.
        """
        if config_path is None:
            config_path = CONFIG_DIR / "ftp_servers.yaml"
        else:
            config_path = Path(config_path)

        self._config_path = config_path
        self._config: dict[str, Any] = {}
        self._product_servers: dict[str, ProductServerConfig] = {}
        self._station_servers: dict[str, StationServerConfig] = {}
        self._auxiliary_servers: dict[str, ServerConfig] = {}

        self._load_config()

    def _load_config(self) -> None:
        """Load configuration from YAML file."""
        if not self._config_path.exists():
            raise FileNotFoundError(f"Config file not found: {self._config_path}")

        with open(self._config_path) as f:
            self._config = yaml.safe_load(f)

        self._parse_product_servers()
        self._parse_station_servers()
        self._parse_auxiliary_servers()

    def _parse_product_servers(self) -> None:
        """Parse product server configurations."""
        servers = self._config.get("product_servers", {})
        for name, cfg in servers.items():
            products = {}
            for product_type, tiers in cfg.get("products", {}).items():
                products[product_type] = {}
                for tier, paths in tiers.items():
                    products[product_type][tier] = ProductPathConfig(
                        path=paths.get("path", ""),
                        filename=paths.get("filename", ""),
                    )

            self._product_servers[name] = ProductServerConfig(
                name=name,
                description=cfg.get("description", ""),
                protocol=cfg.get("protocol", "ftp"),
                host=cfg.get("host", ""),
                auth_required=cfg.get("auth_required", False),
                products=products,
            )

    def _parse_station_servers(self) -> None:
        """Parse station server configurations."""
        servers = self._config.get("station_servers", {})
        for name, cfg in servers.items():
            daily = None
            hourly = None

            if "daily" in cfg:
                daily_cfg = cfg["daily"]
                daily = StationPathConfig(
                    path=daily_cfg.get("path", ""),
                    filename=daily_cfg.get("filename", ""),
                    filename_pattern=daily_cfg.get("filename_pattern", ""),
                )

            if "hourly" in cfg:
                hourly_cfg = cfg["hourly"]
                hourly = StationPathConfig(
                    path=hourly_cfg.get("path", ""),
                    filename=hourly_cfg.get("filename", ""),
                    filename_pattern=hourly_cfg.get("filename_pattern", ""),
                )

            self._station_servers[name] = StationServerConfig(
                name=name,
                description=cfg.get("description", ""),
                protocol=cfg.get("protocol", "ftp"),
                host=cfg.get("host", ""),
                auth_required=cfg.get("auth_required", False),
                rinex_version=cfg.get("rinex_version", 2),
                compression=cfg.get("compression", ".Z"),
                daily=daily,
                hourly=hourly,
            )

    def _parse_auxiliary_servers(self) -> None:
        """Parse auxiliary server configurations."""
        servers = self._config.get("auxiliary_servers", {})
        for name, cfg in servers.items():
            self._auxiliary_servers[name] = ServerConfig(
                name=name,
                description=cfg.get("description", ""),
                protocol=cfg.get("protocol", "https"),
                host=cfg.get("host", ""),
                auth_required=cfg.get("auth_required", False),
            )

    def get_product_server(self, name: str) -> ProductServerConfig | None:
        """Get product server configuration by name."""
        return self._product_servers.get(name)

    def get_station_server(self, name: str) -> StationServerConfig | None:
        """Get station server configuration by name."""
        return self._station_servers.get(name)

    def get_auxiliary_server(self, name: str) -> ServerConfig | None:
        """Get auxiliary server configuration by name."""
        return self._auxiliary_servers.get(name)

    def get_provider_priority(
        self, category: str, data_type: str
    ) -> list[str]:
        """Get ordered list of providers for a data type.

        Args:
            category: "products" or "stations"
            data_type: e.g., "orbit", "clock", "igs", "euref"

        Returns:
            List of server names in priority order
        """
        priority = self._config.get("provider_priority", {})
        return priority.get(category, {}).get(data_type, [])

    def list_product_servers(self) -> list[str]:
        """List all product server names."""
        return list(self._product_servers.keys())

    def list_station_servers(self) -> list[str]:
        """List all station server names."""
        return list(self._station_servers.keys())

    def format_path(
        self,
        template: str,
        year: int,
        doy: int,
        hour: int = 0,
        gps_week: int = 0,
        station: str = "",
    ) -> str:
        """Format a path template with date/station values.

        Supported placeholders:
        - {year}: 4-digit year
        - {yy}: 2-digit year
        - {doy:03d}: 3-digit day of year
        - {hour:02d}: 2-digit hour
        - {hour_char}: Hour as character (a-x)
        - {gps_week}: GPS week number
        - {station}: Lowercase 4-char station ID
        - {STATION}: Uppercase 4-char station ID

        Args:
            template: Path or filename template
            year: Year (4-digit)
            doy: Day of year
            hour: Hour (0-23)
            gps_week: GPS week number
            station: Station ID

        Returns:
            Formatted string
        """
        yy = year % 100
        hour_char = chr(ord('a') + hour) if hour < 24 else 'x'

        return template.format(
            year=year,
            yy=yy,
            doy=doy,
            hour=hour,
            hour_char=hour_char,
            gps_week=gps_week,
            station=station.lower(),
            STATION=station.upper(),
        )


# Global singleton instance
_config_instance: FTPConfig | None = None


def get_ftp_config(config_path: str | Path | None = None) -> FTPConfig:
    """Get FTP configuration singleton.

    Args:
        config_path: Optional path to config file. Only used on first call.

    Returns:
        FTPConfig instance
    """
    global _config_instance
    if _config_instance is None:
        _config_instance = FTPConfig(config_path)
    return _config_instance


def reset_ftp_config() -> None:
    """Reset the FTP config singleton (useful for testing)."""
    global _config_instance
    _config_instance = None
