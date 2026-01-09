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
from pygnss_rt.data_access.download_callers import (
    # Base classes
    BaseDownloadCaller,
    DownloadJobConfig,
    DownloadJobStats,
    DataType,
    # Network-specific callers
    IGSDownloadCaller,
    IGSSubhourlyDownloadCaller,
    EUREFDownloadCaller,
    OSGBDownloadCaller,
    RGPDownloadCaller,
    NRCANDownloadCaller,
    IrishDownloadCaller,
    IcelandicDownloadCaller,
    ScientificDownloadCaller,
    SupersiteDownloadCaller,
    # Convenience functions
    run_download_job,
    run_all_download_jobs,
    DOWNLOAD_CALLERS,
)
from pygnss_rt.data_access.gen_files_downloader import (
    # Main class
    GENFilesDownloader,
    GENDownloaderConfig,
    GENDownloadResult,
    GENFileSpec,
    BSWVersion,
    # Default file lists
    DEFAULT_CONFIG_FILES,
    DEFAULT_REF_FILES,
    # Convenience functions
    download_gen_files,
    download_antenna_files,
)
from pygnss_rt.data_access.product_downloader import (
    # Main classes (replaces FTP::OE, FTP::DD from FTP.pm)
    ProductDownloader,
    ProductDownloadConfig,
    ProductDownloadResult,
    HourlyDataDownloader,
    # Enums
    CompressionType,
    DownloadStatus,
    # Convenience functions
    download_products_for_date,
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
    # Download callers (replaces call_download_*.pl)
    "BaseDownloadCaller",
    "DownloadJobConfig",
    "DownloadJobStats",
    "DataType",
    "IGSDownloadCaller",
    "IGSSubhourlyDownloadCaller",
    "EUREFDownloadCaller",
    "OSGBDownloadCaller",
    "RGPDownloadCaller",
    "NRCANDownloadCaller",
    "IrishDownloadCaller",
    "IcelandicDownloadCaller",
    "ScientificDownloadCaller",
    "SupersiteDownloadCaller",
    "run_download_job",
    "run_all_download_jobs",
    "DOWNLOAD_CALLERS",
    # GEN files downloader (replaces genFilesDownloader*.pm)
    "GENFilesDownloader",
    "GENDownloaderConfig",
    "GENDownloadResult",
    "GENFileSpec",
    "BSWVersion",
    "DEFAULT_CONFIG_FILES",
    "DEFAULT_REF_FILES",
    "download_gen_files",
    "download_antenna_files",
    # Product downloader (replaces FTP::OE, FTP::DD from FTP.pm)
    "ProductDownloader",
    "ProductDownloadConfig",
    "ProductDownloadResult",
    "HourlyDataDownloader",
    "CompressionType",
    "DownloadStatus",
    "download_products_for_date",
]
