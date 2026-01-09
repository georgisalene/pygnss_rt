"""Data access layer for FTP, SFTP, and HTTP downloads."""

from pygnss_rt.data_access.ftp_client import FTPClient, SFTPClient
from pygnss_rt.data_access.http_client import HTTPClient, CDDISClient
from pygnss_rt.data_access.ftp_config import FTPServerConfig, load_ftp_config
from pygnss_rt.data_access.downloader import DataDownloader, DownloadResult
from pygnss_rt.data_access.station_downloader import (
    StationDownloader,
    ProviderConfig,
    DownloadTask,
    DownloadResult as StationDownloadResult,
    RINEXType,
    DEFAULT_PROVIDERS,
    download_stations_for_processing,
)

__all__ = [
    "FTPClient",
    "SFTPClient",
    "HTTPClient",
    "CDDISClient",
    "FTPServerConfig",
    "load_ftp_config",
    "DataDownloader",
    "DownloadResult",
    # Station downloader
    "StationDownloader",
    "ProviderConfig",
    "DownloadTask",
    "StationDownloadResult",
    "RINEXType",
    "DEFAULT_PROVIDERS",
    "download_stations_for_processing",
]
