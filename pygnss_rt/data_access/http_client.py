"""
HTTP/HTTPS client for GNSS data downloads.

Provides Curl-like functionality using the requests library.
Specifically supports NASA CDDIS archive which requires authentication.
"""

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import BinaryIO
from urllib.parse import urljoin

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from pygnss_rt.core.exceptions import HTTPError
from pygnss_rt.utils.logging import get_logger


logger = get_logger(__name__)


class HTTPClient:
    """HTTP/HTTPS client with retry support."""

    def __init__(
        self,
        base_url: str | None = None,
        timeout: int = 60,
        max_retries: int = 3,
        verify_ssl: bool = True,
    ):
        """Initialize HTTP client.

        Args:
            base_url: Base URL for requests
            timeout: Request timeout in seconds
            max_retries: Maximum retry attempts
            verify_ssl: Verify SSL certificates
        """
        self.base_url = base_url
        self.timeout = timeout
        self.max_retries = max_retries
        self.verify_ssl = verify_ssl

        # Configure session with retries
        self.session = requests.Session()
        retry_strategy = Retry(
            total=max_retries,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

    def download(
        self,
        url: str,
        local_path: Path,
        expected_checksum: str | None = None,
    ) -> bool:
        """Download a file from URL.

        Args:
            url: Full URL or path (if base_url set)
            local_path: Local destination path
            expected_checksum: Expected MD5 checksum

        Returns:
            True if successful
        """
        full_url = urljoin(self.base_url, url) if self.base_url else url

        try:
            response = self.session.get(
                full_url,
                timeout=self.timeout,
                verify=self.verify_ssl,
                stream=True,
            )
            response.raise_for_status()

            # Create directory if needed
            local_path.parent.mkdir(parents=True, exist_ok=True)

            # Download with checksum verification
            md5 = hashlib.md5()
            with open(local_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
                    md5.update(chunk)

            # Verify checksum if provided
            if expected_checksum:
                actual_checksum = md5.hexdigest()
                if actual_checksum != expected_checksum:
                    local_path.unlink()
                    raise HTTPError(
                        full_url,
                        None,
                        f"Checksum mismatch: {actual_checksum} != {expected_checksum}",
                    )

            logger.info(
                "Downloaded file via HTTP",
                url=full_url,
                local=str(local_path),
                size=local_path.stat().st_size,
            )
            return True

        except requests.exceptions.HTTPError as e:
            status = e.response.status_code if e.response else None
            if status == 404:
                logger.warning("File not found", url=full_url)
                return False
            raise HTTPError(full_url, status, str(e)) from e

        except requests.exceptions.RequestException as e:
            raise HTTPError(full_url, None, str(e)) from e

    def get_file_info(self, url: str) -> dict | None:
        """Get file information without downloading.

        Args:
            url: Full URL or path (if base_url set)

        Returns:
            Dictionary with size and last-modified, or None if not found
        """
        full_url = urljoin(self.base_url, url) if self.base_url else url

        try:
            response = self.session.head(
                full_url,
                timeout=self.timeout,
                verify=self.verify_ssl,
            )
            response.raise_for_status()

            return {
                "size": int(response.headers.get("content-length", 0)),
                "last_modified": response.headers.get("last-modified"),
            }
        except requests.exceptions.HTTPError:
            return None

    def close(self) -> None:
        """Close the session."""
        self.session.close()

    def __enter__(self) -> "HTTPClient":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()


class CDDISClient(HTTPClient):
    """HTTP client for NASA CDDIS archive.

    CDDIS requires authentication via Earthdata Login.
    Uses .netrc file or provided credentials.
    """

    CDDIS_BASE_URL = "https://cddis.nasa.gov"

    def __init__(
        self,
        username: str | None = None,
        password: str | None = None,
        **kwargs,
    ):
        """Initialize CDDIS client.

        Args:
            username: Earthdata username (or use .netrc)
            password: Earthdata password (or use .netrc)
        """
        super().__init__(base_url=self.CDDIS_BASE_URL, **kwargs)

        # Set up authentication
        if username and password:
            self.session.auth = (username, password)

    def get_product_url(
        self,
        product_type: str,
        year: int,
        doy: int,
        gps_week: int,
        filename: str,
    ) -> str:
        """Build URL for GNSS product.

        Args:
            product_type: Type of product (orbit, clock, etc.)
            year: Year
            doy: Day of year
            gps_week: GPS week number
            filename: Product filename

        Returns:
            Full URL to product
        """
        if product_type in ("orbit", "clock", "erp"):
            return f"/archive/gnss/products/{gps_week}/{filename}"
        elif product_type == "rinex":
            yy = year % 100
            return f"/archive/gnss/data/daily/{year}/{doy:03d}/{yy:02d}d/{filename}"
        else:
            return f"/archive/gnss/products/{gps_week}/{filename}"

    def download_product(
        self,
        product_type: str,
        year: int,
        doy: int,
        gps_week: int,
        filename: str,
        local_path: Path,
    ) -> bool:
        """Download GNSS product from CDDIS.

        Args:
            product_type: Type of product
            year: Year
            doy: Day of year
            gps_week: GPS week number
            filename: Product filename
            local_path: Local destination

        Returns:
            True if successful
        """
        url = self.get_product_url(product_type, year, doy, gps_week, filename)
        return self.download(url, local_path)
