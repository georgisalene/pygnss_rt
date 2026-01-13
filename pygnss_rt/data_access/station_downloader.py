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
import netrc
import os
import shutil
import subprocess
import tempfile
import time
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from pygnss_rt.data_access.ftp_client import FTPClient, SFTPClient, BaseClient
from pygnss_rt.data_access.ftp_config import FTPServerConfig

# Import structured logging
try:
    import structlog
    logger = structlog.get_logger(__name__)
except ImportError:
    import logging
    logger = logging.getLogger(__name__)


class CDDISSession:
    """Manages authenticated session to CDDIS via NASA Earthdata Login.

    CDDIS requires authentication via NASA Earthdata Login (URS).
    This class handles the OAuth flow by:
    1. Reading credentials from ~/.netrc (machine urs.earthdata.nasa.gov)
    2. Creating a session with proper cookies for authentication
    3. Following redirects through the Earthdata Login portal

    Replicates the behavior of the old Perl FTP.pm edl_preauth_cddis() function.
    """

    _instance: "CDDISSession | None" = None

    def __init__(self):
        self.session: requests.Session | None = None
        self._authenticated = False
        self._auth_url = "https://urs.earthdata.nasa.gov"
        self._cddis_url = "https://cddis.nasa.gov"

    @classmethod
    def get_session(cls) -> requests.Session:
        """Get or create authenticated CDDIS session (singleton)."""
        if cls._instance is None:
            cls._instance = cls()
        if not cls._instance._authenticated:
            cls._instance._authenticate()
        return cls._instance.session

    def _get_credentials(self) -> tuple[str, str]:
        """Get credentials from ~/.netrc file."""
        netrc_path = Path.home() / ".netrc"
        if not netrc_path.exists():
            raise FileNotFoundError(
                "~/.netrc file not found. Required for CDDIS authentication."
            )

        try:
            nrc = netrc.netrc(str(netrc_path))
            # Try Earthdata URS first
            auth = nrc.authenticators("urs.earthdata.nasa.gov")
            if not auth:
                # Fall back to cddis.nasa.gov
                auth = nrc.authenticators("cddis.nasa.gov")
            if not auth:
                raise ValueError(
                    "No credentials found for urs.earthdata.nasa.gov or cddis.nasa.gov in ~/.netrc"
                )
            return auth[0], auth[2]  # login, password
        except Exception as e:
            raise ValueError(f"Failed to parse ~/.netrc: {e}")

    def _authenticate(self) -> None:
        """Authenticate to CDDIS via Earthdata Login."""
        username, password = self._get_credentials()

        # Create session with retry logic
        self.session = requests.Session()

        # Configure retry strategy
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

        # Set headers like the old Perl implementation
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (i-GNSS Python downloader)",
        })

        # Pre-authenticate by accessing CDDIS archive (triggers EDL redirect)
        # The session will follow redirects and authenticate automatically
        self.session.auth = (username, password)

        try:
            # Hit the CDDIS archive to trigger Earthdata Login
            # This is the same approach as edl_preauth_cddis() in FTP.pm
            response = self.session.get(
                f"{self._cddis_url}/archive/",
                timeout=60,
                allow_redirects=True,
            )

            # Check if we got redirected to login page (auth failed)
            if "urs.earthdata.nasa.gov" in response.url and response.status_code == 200:
                # We're stuck at login page - try explicit auth
                response = self.session.get(
                    f"{self._cddis_url}/archive/",
                    timeout=60,
                    allow_redirects=True,
                )

            if response.status_code == 200 and "Earthdata Login" not in response.text:
                self._authenticated = True
            else:
                raise RuntimeError("CDDIS authentication failed - received login page")

        except requests.RequestException as e:
            raise RuntimeError(f"Failed to authenticate to CDDIS: {e}")

    @classmethod
    def reset(cls) -> None:
        """Reset the session (force re-authentication)."""
        if cls._instance:
            if cls._instance.session:
                cls._instance.session.close()
            cls._instance = None


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
        # CDDIS uses RINEX3 naming: SSSS00CCC_R_YYYYDDDHHMM_01H_30S_MO.crx.gz
        # For hourly: /archive/gnss/data/hourly/YYYY/DDD/HH/
        path_template="/hourly/{year}/{doy:03d}/{hour:02d}",
        # Use pattern matching for RINEX3 (station + wildcard for country code)
        filename_template="{STATION}00*_R_{year}{doy:03d}{hour:02d}00_01H_30S_MO.crx.gz",
        priority=1,
        timeout=120,  # CDDIS can be slow
    ),
    "CDDIS_DAILY": ProviderConfig(
        name="CDDIS_DAILY",
        server="cddis.nasa.gov",
        protocol="https",
        base_path="/archive/gnss/data",
        # Daily: /archive/gnss/data/daily/YYYY/DDD/YYd/
        path_template="/daily/{year}/{doy:03d}/{yy}d",
        # Pattern for daily RINEX3
        filename_template="{STATION}00*_R_{year}{doy:03d}0000_01D_30S_MO.crx.gz",
        priority=2,
        supports_hourly=False,
        timeout=120,
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
    # BKGE_IGS - BKG's IGS mirror for global IGS stations
    "BKGE_IGS": ProviderConfig(
        name="BKGE_IGS",
        server="igs-ftp.bkg.bund.de",
        protocol="ftp",
        username="anonymous",
        base_path="/IGS/obs",
        # Path: /IGS/obs/{year}/{doy}/ - RINEX 2 compressed (.Z)
        path_template="/{year}/{doy:03d}",
        filename_template="{station}{doy:03d}0.{yy}o.Z",
        priority=3,  # After CDDIS but before regional providers
        supports_hourly=False,
    ),
    "BKGE_IGS_HOURLY": ProviderConfig(
        name="BKGE_IGS_HOURLY",
        server="igs-ftp.bkg.bund.de",
        protocol="ftp",
        username="anonymous",
        base_path="/IGS/nrt",
        # Path: /IGS/nrt/{doy}/{hour}/
        path_template="/{doy:03d}/{hour:02d}",
        filename_template="{station}{doy:03d}{hour_char}.{yy}o.gz",
        priority=3,
        supports_daily=False,
    ),
    # IGN - IGS mirror at IGN France
    "IGN_IGS": ProviderConfig(
        name="IGN_IGS",
        server="igs.ign.fr",
        protocol="ftp",
        username="anonymous",
        base_path="/pub/igs/data/daily",
        # Path: /pub/igs/data/daily/{year}/{doy}/{yy}d/
        path_template="/{year}/{doy:03d}/{yy}d",
        filename_template="{station}{doy:03d}0.{yy}d.Z",
        priority=4,
        supports_hourly=False,
    ),
}


def load_providers_from_yaml(config_path: str | Path | None = None) -> dict[str, ProviderConfig]:
    """Load station provider configurations from YAML file.

    Args:
        config_path: Path to YAML config file. Uses default if None.

    Returns:
        Dictionary of provider name to ProviderConfig
    """
    from pathlib import Path
    import yaml

    if config_path is None:
        config_path = Path(__file__).parent.parent / "config" / "ftp_servers.yaml"
    else:
        config_path = Path(config_path)

    if not config_path.exists():
        logger.warning("YAML config not found, using defaults", path=str(config_path))
        return DEFAULT_PROVIDERS.copy()

    try:
        with open(config_path) as f:
            config = yaml.safe_load(f)

        providers: dict[str, ProviderConfig] = {}
        station_servers = config.get("station_servers", {})
        priority = 1

        for name, cfg in station_servers.items():
            host = cfg.get("host", "")
            protocol = cfg.get("protocol", "ftp")
            rinex_version = cfg.get("rinex_version", 2)

            # Create provider for daily data
            daily = cfg.get("daily")
            if daily:
                prov_name = name if "daily" not in name.lower() else name
                providers[prov_name] = ProviderConfig(
                    name=prov_name,
                    server=host,
                    protocol=protocol,
                    username="anonymous",
                    base_path="",  # Embedded in path template
                    path_template=daily.get("path", ""),
                    filename_template=daily.get("filename", "") or daily.get("filename_pattern", ""),
                    priority=priority,
                    supports_hourly=False,
                    supports_daily=True,
                    timeout=120 if protocol == "https" else 60,
                )
                priority += 1

            # Create provider for hourly data
            hourly = cfg.get("hourly")
            if hourly:
                prov_name = f"{name}_HOURLY" if "hourly" not in name.lower() else name
                providers[prov_name] = ProviderConfig(
                    name=prov_name,
                    server=host,
                    protocol=protocol,
                    username="anonymous",
                    base_path="",
                    path_template=hourly.get("path", ""),
                    filename_template=hourly.get("filename", "") or hourly.get("filename_pattern", ""),
                    priority=priority,
                    supports_hourly=True,
                    supports_daily=False,
                    timeout=120 if protocol == "https" else 60,
                )
                priority += 1

        if providers:
            logger.info("Loaded station providers from YAML", path=str(config_path), count=len(providers))
            return providers

        logger.warning("No station providers in YAML, using defaults")
        return DEFAULT_PROVIDERS.copy()

    except Exception as e:
        logger.warning("Failed to load YAML config", error=str(e))
        return DEFAULT_PROVIDERS.copy()


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
        flat_structure: bool = False,
        use_yaml_config: bool = True,
        config_path: str | Path | None = None,
    ):
        """Initialize station downloader.

        Args:
            download_dir: Base directory for downloads
            providers: Provider configurations (uses defaults if None)
            max_retries: Maximum retry attempts per provider
            retry_delay: Delay between retries in seconds
            parallel_downloads: Number of parallel download threads
            verbose: Enable verbose output
            flat_structure: If True, save files directly to download_dir
                            without subdirectories. Use for BSW campaigns.
            use_yaml_config: If True and providers is None, load from YAML
            config_path: Path to YAML config (uses default if None)
        """
        self.download_dir = Path(download_dir)

        # Load providers: explicit > YAML > defaults
        if providers is not None:
            self.providers = providers
        elif use_yaml_config:
            self.providers = load_providers_from_yaml(config_path)
        else:
            self.providers = DEFAULT_PROVIDERS.copy()

        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.parallel_downloads = parallel_downloads
        self.verbose = verbose
        self.flat_structure = flat_structure

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
        country_code: str = "",
    ) -> Path:
        """Build local file path.

        For BSW 5.4 with flat_structure=True, uses Bernese long format:
            SSSS00CCC_YYYYDDDS.RXO (e.g., WTZR00DEU_20252710.RXO)

        For non-BSW (flat_structure=False), uses traditional RINEX 2 format:
            ssssddds.yyo (e.g., wtzr2710.25o)

        Args:
            station: Station ID (4-char)
            year: Year
            doy: Day of year
            hour: Hour or None for daily
            rinex_type: RINEX type
            country_code: 3-char country code (for Bernese long format)

        Returns:
            Local file path
        """
        yy = year % 100

        # Bernese 5.4 long format for BSW campaigns
        if self.flat_structure:
            # Format: SSSS00CCC_YYYYDDDS.RXO
            # Where: SSSS = 4-char station code (uppercase)
            #        00 = monument/receiver markers
            #        CCC = 3-char country code (or 'XXX' if unknown)
            #        YYYY = 4-digit year
            #        DDD = day of year
            #        S = session character (0 for daily, a-x for hourly)
            station_upper = station.upper()[:4]
            ccode = country_code.upper()[:3] if country_code else "XXX"

            if rinex_type == RINEXType.DAILY:
                session_char = "0"
            else:
                session_char = chr(ord('a') + (hour or 0))

            # Bernese 5.4 format: WTZR00DEU_20252710.RXO (underscore before date)
            filename = f"{station_upper}00{ccode}_{year}{doy:03d}{session_char}.RXO"
            return self.download_dir / filename

        # Traditional RINEX 2 short format for non-BSW usage
        if rinex_type == RINEXType.DAILY:
            filename = f"{station.lower()}{doy:03d}0.{yy:02d}o"
            subdir = "daily"
        else:
            hour_char = chr(ord('a') + (hour or 0))
            filename = f"{station.lower()}{doy:03d}{hour_char}.{yy:02d}o"
            subdir = "hourly"

        return self.download_dir / subdir / str(year) / f"{doy:03d}" / filename

    def _decompress_rinex(
        self,
        compressed_path: Path,
        target_path: Path,
    ) -> bool:
        """Decompress and convert RINEX file to standard format.

        Handles:
        - .gz compression (gzip)
        - .Z compression (Unix compress)
        - .crx files (Hatanaka compressed RINEX)

        Args:
            compressed_path: Path to compressed file
            target_path: Desired output path

        Returns:
            True if successful
        """
        try:
            current_file = compressed_path

            # Step 1: Handle gzip compression (.gz)
            if str(current_file).endswith('.gz'):
                decompressed = current_file.with_suffix('')
                with gzip.open(current_file, 'rb') as f_in:
                    with open(decompressed, 'wb') as f_out:
                        shutil.copyfileobj(f_in, f_out)
                current_file.unlink()  # Remove .gz file
                current_file = decompressed

            # Step 1b: Handle Unix compress (.Z) - common on FTP servers
            if str(current_file).endswith('.Z'):
                decompressed = Path(str(current_file)[:-2])  # Remove .Z suffix
                # Use system uncompress or gzip -d
                try:
                    result = subprocess.run(
                        ['gzip', '-d', '-f', str(current_file)],
                        capture_output=True,
                        timeout=60,
                    )
                    if result.returncode == 0 and decompressed.exists():
                        current_file = decompressed
                    else:
                        if self.verbose:
                            print(f"  WARNING: Failed to decompress .Z file: {result.stderr.decode()}")
                except Exception as e:
                    if self.verbose:
                        print(f"  WARNING: .Z decompression error: {e}")

            # Step 2: Handle Hatanaka compression (.crx)
            if str(current_file).endswith('.crx'):
                # Use crx2rnx to convert Hatanaka to RINEX
                crx2rnx_paths = [
                    Path.home() / ".local/bin/crx2rnx",
                    Path("/usr/local/bin/crx2rnx"),
                    Path("/usr/bin/crx2rnx"),
                ]
                crx2rnx = None
                for path in crx2rnx_paths:
                    if path.exists():
                        crx2rnx = str(path)
                        break

                if not crx2rnx:
                    if self.verbose:
                        print(f"  WARNING: crx2rnx not found, keeping Hatanaka format")
                    # Move to target without conversion
                    shutil.move(str(current_file), str(target_path))
                    return target_path.exists()

                # Run crx2rnx
                result = subprocess.run(
                    [crx2rnx, str(current_file)],
                    capture_output=True,
                    timeout=60,
                )

                if result.returncode == 0:
                    # crx2rnx creates .rnx file
                    rnx_file = current_file.with_suffix('.rnx')
                    if rnx_file.exists():
                        current_file.unlink()  # Remove .crx
                        current_file = rnx_file
                    else:
                        if self.verbose:
                            print(f"  WARNING: crx2rnx didn't create .rnx file")

            # Step 3: Move/rename to target path
            if current_file != target_path:
                target_path.parent.mkdir(parents=True, exist_ok=True)
                shutil.move(str(current_file), str(target_path))

            return target_path.exists()

        except Exception as e:
            if self.verbose:
                print(f"  Decompression error: {e}")
            return False

    def _search_cddis_directory(
        self,
        station: str,
        year: int,
        doy: int,
        hour: int | None,
        rinex_type: RINEXType,
    ) -> tuple[str | None, str]:
        """Search CDDIS directory for matching station file.

        CDDIS uses RINEX 3 naming with country codes that vary per station.
        This method searches the directory listing for a matching file.

        Args:
            station: 4-char station ID
            year: Year
            doy: Day of year
            hour: Hour (None for daily)
            rinex_type: RINEX type

        Returns:
            Tuple of (Full URL to the matching file, 3-char country code)
            Returns (None, "") if not found
        """
        import re

        try:
            session = CDDISSession.get_session()

            # Build directory URL
            if rinex_type == RINEXType.DAILY:
                yy = year % 100
                dir_url = f"https://cddis.nasa.gov/archive/gnss/data/daily/{year}/{doy:03d}/{yy:02d}d/"
            else:
                dir_url = f"https://cddis.nasa.gov/archive/gnss/data/hourly/{year}/{doy:03d}/{hour:02d}/"

            # Get directory listing
            response = session.get(dir_url, timeout=60)
            if response.status_code != 200:
                return None, ""

            # Find matching files (station name is case-insensitive in RINEX3)
            station_upper = station.upper()

            # Pattern: SSSS00CCC_R_... where SSSS is station, CCC is country code
            # Match both .crx.gz and .rnx.gz variants
            # Capture the country code (3 chars after 00)
            pattern = rf'href="({station_upper}00(\w{{3}})_R_[^"]+\.(?:crx|rnx)\.gz)\s*"'
            matches = re.findall(pattern, response.text, re.IGNORECASE)

            if matches:
                # Prefer 30S sampling rate, then any
                for filename, country_code in matches:
                    if "_30S_" in filename:
                        return f"{dir_url}{filename.strip()}", country_code.upper()
                # Fall back to first match
                return f"{dir_url}{matches[0][0].strip()}", matches[0][1].upper()

            if self.verbose:
                print(f"  No match found for {station} in {dir_url}")
            return None, ""

        except Exception as e:
            if self.verbose:
                print(f"  Directory search error: {e}")
            return None, ""

    def _download_with_https(
        self,
        url: str,
        local_path: Path,
        timeout: int = 60,
        provider_name: str = "",
    ) -> bool:
        """Download file using HTTPS with proper authentication.

        For CDDIS, uses the authenticated CDDISSession.
        For other providers, uses simple requests.

        Args:
            url: Full URL to download
            local_path: Local destination path
            timeout: Timeout in seconds
            provider_name: Provider name (for special handling)

        Returns:
            True if successful
        """
        local_path.parent.mkdir(parents=True, exist_ok=True)

        try:
            # Use CDDISSession for CDDIS downloads (requires Earthdata auth)
            if provider_name == "CDDIS" or "cddis" in url.lower():
                session = CDDISSession.get_session()
            else:
                # Create a simple session for other providers
                session = requests.Session()
                session.headers.update({
                    "User-Agent": "Mozilla/5.0 (i-GNSS Python downloader)",
                })

            # Download the file
            response = session.get(
                url,
                timeout=timeout,
                allow_redirects=True,
                stream=True,
            )

            # Check if we got redirected to login page (auth failed for CDDIS)
            if response.status_code == 200:
                # Verify it's not an HTML login page
                content_type = response.headers.get("Content-Type", "")
                if "text/html" in content_type.lower():
                    # Check first few bytes for HTML
                    first_chunk = next(response.iter_content(chunk_size=512), b"")
                    if b"<!DOCTYPE" in first_chunk or b"<html" in first_chunk:
                        if self.verbose:
                            print(f"  WARNING: Got HTML instead of data for {url}")
                        return False
                    # Not HTML, write the chunk and continue
                    with open(local_path, "wb") as f:
                        f.write(first_chunk)
                        for chunk in response.iter_content(chunk_size=8192):
                            f.write(chunk)
                else:
                    # Not HTML content type, stream directly
                    with open(local_path, "wb") as f:
                        for chunk in response.iter_content(chunk_size=8192):
                            f.write(chunk)

                # Verify file size (should be > 1KB for RINEX)
                if local_path.exists():
                    size = local_path.stat().st_size
                    if size > 1000:  # RINEX files should be > 1KB
                        return True
                    else:
                        # Too small, likely an error page
                        local_path.unlink(missing_ok=True)
                        return False
            else:
                if self.verbose:
                    print(f"  HTTP {response.status_code} for {url}")
                return False

        except requests.RequestException as e:
            if self.verbose:
                print(f"  Download error: {e}")
            return False
        except Exception as e:
            if self.verbose:
                print(f"  Unexpected error: {e}")
            return False

        return False

    def _download_with_curl(
        self,
        url: str,
        local_path: Path,
        timeout: int = 60,
    ) -> bool:
        """Download file using curl with .netrc authentication.

        Uses curl with --netrc flag to read credentials from ~/.netrc.
        This is the fallback method if requests doesn't work.

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
                    "--netrc",  # Use ~/.netrc for authentication
                    "-b", "",   # Enable cookie engine
                    "-c", "",   # Cookie jar
                    "--connect-timeout", str(timeout),
                    "-o", str(local_path),
                    url,
                ],
                capture_output=True,
                timeout=timeout + 30,
            )

            if result.returncode == 0 and local_path.exists():
                # Verify it's not an HTML error page
                size = local_path.stat().st_size
                if size > 1000:
                    return True
                # Check content for HTML
                with open(local_path, "rb") as f:
                    header = f.read(256)
                    if b"<!DOCTYPE" in header or b"<html" in header:
                        local_path.unlink(missing_ok=True)
                        return False
                return True
            return False
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

        # Check if already exists and is valid (>100KB for daily RINEX)
        if local_path.exists():
            file_size = local_path.stat().st_size
            # Daily RINEX files should be at least 100KB
            # Hourly files at least 50KB
            min_size = 100000 if task.rinex_type == RINEXType.DAILY else 50000
            if file_size >= min_size:
                result.success = True
                result.local_path = local_path
                result.file_size = file_size
                result.provider_used = "cached"
                return result
            else:
                # File too small - likely corrupted or HTML error page
                # Delete and re-download
                if self.verbose:
                    print(f"  {task.station_id}: cached file too small ({file_size} bytes), re-downloading")
                local_path.unlink(missing_ok=True)

        start_time = time.time()
        country_code = ""  # Will be populated from CDDIS search

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
                        # For CDDIS, search directory for matching RINEX3 files
                        if provider_name.startswith("CDDIS"):
                            url, country_code = self._search_cddis_directory(
                                task.station_id, task.year, task.doy,
                                task.hour, task.rinex_type
                            )
                            if not url:
                                if self.verbose:
                                    print(f"  {task.station_id}: not found on {provider_name}")
                                break  # Skip remaining retries for this provider

                            # For CDDIS, download to temp file (compressed RINEX3)
                            # Extract original filename from URL
                            download_filename = url.split("/")[-1]
                            download_path = local_path.parent / download_filename
                        else:
                            url = f"{provider.protocol}://{provider.server}{remote_path}"
                            download_path = local_path

                        success = self._download_with_https(
                            url, download_path, provider.timeout, provider_name
                        )
                        # Update local_path for decompression step
                        if success and provider_name.startswith("CDDIS"):
                            local_path = download_path
                    else:
                        # Use FTP/SFTP client
                        client = self._get_client(provider)
                        if client:
                            success = client.download(remote_path, local_path)
                        else:
                            success = False

                    if success and local_path.exists():
                        # Decompress and convert to Bernese format
                        downloaded_file = local_path
                        # Build target Bernese 5.4 long format filename for BSW
                        target_rinex = self._build_local_path(
                            task.station_id, task.year, task.doy,
                            task.hour, task.rinex_type, country_code
                        )

                        # Check if file needs decompression (ends in .gz, .Z, .crx)
                        needs_decompress = any(
                            str(downloaded_file).endswith(ext)
                            for ext in ['.gz', '.Z', '.crx']
                        )

                        if needs_decompress or downloaded_file != target_rinex:
                            if self.verbose:
                                print(f"  Processing: {local_path.name} -> {target_rinex.name}")

                            if self._decompress_rinex(downloaded_file, target_rinex):
                                local_path = target_rinex
                            else:
                                if self.verbose:
                                    print(f"  WARNING: Processing failed for {task.station_id}")
                                # Still mark as success if file exists
                                if not target_rinex.exists() and downloaded_file.exists():
                                    local_path = downloaded_file

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
