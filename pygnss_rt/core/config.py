"""
Configuration management for PyGNSS-RT.

Uses Pydantic for validation and supports YAML configuration files
with environment variable expansion.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, Field, field_validator
from pydantic_settings import BaseSettings


def expand_env_vars(value: Any) -> Any:
    """Recursively expand environment variables in strings."""
    if isinstance(value, str):
        return os.path.expandvars(value)
    elif isinstance(value, dict):
        return {k: expand_env_vars(v) for k, v in value.items()}
    elif isinstance(value, list):
        return [expand_env_vars(v) for v in value]
    return value


class DatabaseConfig(BaseModel):
    """Database configuration."""

    path: Path = Field(default=Path("data/pygnss_rt.duckdb"))
    read_only: bool = False


class BSWConfig(BaseModel):
    """Bernese GNSS Software configuration."""

    bsw_root: Path = Field(default=Path("/opt/BERN54"))
    temp_dir: Path = Field(default=Path("/tmp/bsw"))
    user_dir: Path | None = None
    exec_dir: Path | None = None
    queue_dir: Path | None = None
    campaign_root: Path = Field(default=Path("campaigns"))
    loadgps_setvar: Path | None = None

    def model_post_init(self, __context: Any) -> None:
        """Set derived paths after initialization."""
        if self.user_dir is None:
            self.user_dir = self.bsw_root / "GPS"
        if self.exec_dir is None:
            self.exec_dir = self.bsw_root / "GPS" / "EXE"
        if self.queue_dir is None:
            self.queue_dir = self.bsw_root / "GPS" / "BPE"
        if self.loadgps_setvar is None:
            self.loadgps_setvar = self.bsw_root / "LOADGPS.setvar"


class DataConfig(BaseModel):
    """Data directory configuration."""

    oedc_dir: Path = Field(default=Path("data/oedc"))
    rinex_dir: Path = Field(default=Path("data/rinex"))
    station_data_dir: Path = Field(default=Path("station_data"))


class ProcessingConfig(BaseModel):
    """Processing configuration."""

    proc_type: str = "hourly"
    orbit_product: str = "IGS"
    orbit_type: str = "final"
    erp_product: str = "IGS"
    erp_type: str = "final"
    clock_product: str = "IGS"
    clock_type: str = "final"
    dcb_product: str = "CODE"
    dcb_type: str = "final"
    use_clockprep: bool = False
    use_teqc: bool = False
    use_cc2noncc: bool = False
    latency_hours: int = 3


class FTPServerConfig(BaseModel):
    """Single FTP server configuration."""

    url: str
    protocol: str = "ftp"
    username: str = "anonymous"
    password: str = ""
    passive: bool = True
    timeout: int = 60
    max_retries: int = 3


class FTPConfig(BaseModel):
    """FTP servers configuration."""

    servers: dict[str, FTPServerConfig] = Field(default_factory=dict)


class EmailConfig(BaseModel):
    """Email notification configuration."""

    enabled: bool = False
    smtp_server: str = "localhost"
    smtp_port: int = 25
    sender: str = "pygnss-rt@localhost"
    recipients: list[str] = Field(default_factory=list)


class LoggingConfig(BaseModel):
    """Logging configuration."""

    level: str = "INFO"
    log_dir: Path = Field(default=Path("logs"))
    log_to_file: bool = True
    log_to_console: bool = True
    json_format: bool = False


class Settings(BaseSettings):
    """Main settings container."""

    database: DatabaseConfig = Field(default_factory=DatabaseConfig)
    bsw: BSWConfig = Field(default_factory=BSWConfig)
    data: DataConfig = Field(default_factory=DataConfig)
    processing: ProcessingConfig = Field(default_factory=ProcessingConfig)
    ftp: FTPConfig = Field(default_factory=FTPConfig)
    email: EmailConfig = Field(default_factory=EmailConfig)
    logging: LoggingConfig = Field(default_factory=LoggingConfig)

    class Config:
        env_prefix = "PYGNSS_"
        env_nested_delimiter = "__"


def load_settings(config_path: Path | str | None = None) -> Settings:
    """Load settings from YAML file.

    Args:
        config_path: Path to YAML configuration file.
                    If None, tries default locations.

    Returns:
        Settings instance.
    """
    search_paths = []

    if config_path:
        search_paths.append(Path(config_path))
    else:
        # Default search locations
        search_paths.extend([
            Path("config/settings.local.yaml"),
            Path("config/settings.yaml"),
            Path.home() / ".pygnss_rt" / "settings.yaml",
        ])

    config_data: dict[str, Any] = {}

    for path in search_paths:
        if path.exists():
            with open(path) as f:
                raw_data = yaml.safe_load(f)
                if raw_data:
                    config_data = expand_env_vars(raw_data)
            break

    return Settings(**config_data)


def load_config(config_path: Path | str | None = None) -> dict[str, Any]:
    """Load raw configuration dictionary from YAML file.

    Simpler alternative to load_settings() that returns raw dict
    for cases where Pydantic validation is not needed.

    Args:
        config_path: Path to YAML configuration file.

    Returns:
        Configuration dictionary.
    """
    search_paths = []

    if config_path:
        search_paths.append(Path(config_path))
    else:
        search_paths.extend([
            Path("config/settings.local.yaml"),
            Path("config/settings.yaml"),
            Path.home() / ".pygnss_rt" / "settings.yaml",
        ])

    for path in search_paths:
        if path.exists():
            with open(path) as f:
                raw_data = yaml.safe_load(f)
                if raw_data:
                    return expand_env_vars(raw_data)

    return {}
