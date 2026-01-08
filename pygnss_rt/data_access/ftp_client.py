"""
FTP and SFTP clients for GNSS data downloads.

Replaces Perl FTP.pm module.
"""

from __future__ import annotations

import ftplib
import socket
from abc import ABC, abstractmethod
from pathlib import Path
from typing import BinaryIO

import paramiko

from pygnss_rt.core.exceptions import FTPError
from pygnss_rt.utils.logging import get_logger


logger = get_logger(__name__)


class BaseClient(ABC):
    """Abstract base class for FTP/SFTP clients."""

    def __init__(
        self,
        host: str,
        username: str = "anonymous",
        password: str = "",
        timeout: int = 60,
        passive: bool = True,
    ):
        """Initialize client.

        Args:
            host: Server hostname
            username: Login username
            password: Login password
            timeout: Connection timeout in seconds
            passive: Use passive mode (FTP only)
        """
        self.host = host
        self.username = username
        self.password = password
        self.timeout = timeout
        self.passive = passive
        self._connected = False

    @abstractmethod
    def connect(self) -> None:
        """Connect to server."""
        pass

    @abstractmethod
    def disconnect(self) -> None:
        """Disconnect from server."""
        pass

    @abstractmethod
    def download(self, remote_path: str, local_path: Path) -> bool:
        """Download a file.

        Args:
            remote_path: Remote file path
            local_path: Local destination path

        Returns:
            True if successful
        """
        pass

    @abstractmethod
    def list_files(self, remote_dir: str) -> list[str]:
        """List files in remote directory.

        Args:
            remote_dir: Remote directory path

        Returns:
            List of filenames
        """
        pass

    @abstractmethod
    def file_exists(self, remote_path: str) -> bool:
        """Check if remote file exists.

        Args:
            remote_path: Remote file path

        Returns:
            True if file exists
        """
        pass

    def __enter__(self) -> "BaseClient":
        """Context manager entry."""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit."""
        self.disconnect()


class FTPClient(BaseClient):
    """FTP client using ftplib."""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._ftp: ftplib.FTP | None = None

    def connect(self) -> None:
        """Connect to FTP server."""
        try:
            self._ftp = ftplib.FTP()
            self._ftp.connect(self.host, timeout=self.timeout)
            self._ftp.login(self.username, self.password)

            if self.passive:
                self._ftp.set_pasv(True)

            self._connected = True
            logger.info("Connected to FTP server", host=self.host)

        except socket.timeout as e:
            raise FTPError(self.host, "connect", f"Connection timeout: {e}") from e
        except ftplib.error_perm as e:
            raise FTPError(self.host, "connect", f"Permission error: {e}") from e
        except Exception as e:
            raise FTPError(self.host, "connect", str(e)) from e

    def disconnect(self) -> None:
        """Disconnect from FTP server."""
        if self._ftp:
            try:
                self._ftp.quit()
            except Exception:
                pass
            self._ftp = None
            self._connected = False

    def download(self, remote_path: str, local_path: Path) -> bool:
        """Download file from FTP server."""
        if not self._ftp:
            raise FTPError(self.host, "download", "Not connected")

        try:
            # Create local directory if needed
            local_path.parent.mkdir(parents=True, exist_ok=True)

            with open(local_path, "wb") as f:
                self._ftp.retrbinary(f"RETR {remote_path}", f.write)

            logger.info(
                "Downloaded file",
                remote=remote_path,
                local=str(local_path),
            )
            return True

        except ftplib.error_perm as e:
            logger.warning(
                "FTP download failed",
                remote=remote_path,
                error=str(e),
            )
            return False
        except Exception as e:
            raise FTPError(self.host, "download", str(e)) from e

    def list_files(self, remote_dir: str) -> list[str]:
        """List files in remote directory."""
        if not self._ftp:
            raise FTPError(self.host, "list", "Not connected")

        try:
            return self._ftp.nlst(remote_dir)
        except ftplib.error_perm:
            return []

    def file_exists(self, remote_path: str) -> bool:
        """Check if remote file exists."""
        if not self._ftp:
            return False

        try:
            self._ftp.size(remote_path)
            return True
        except ftplib.error_perm:
            return False


class SFTPClient(BaseClient):
    """SFTP client using paramiko."""

    def __init__(self, port: int = 22, **kwargs):
        super().__init__(**kwargs)
        self.port = port
        self._transport: paramiko.Transport | None = None
        self._sftp: paramiko.SFTPClient | None = None

    def connect(self) -> None:
        """Connect to SFTP server."""
        try:
            self._transport = paramiko.Transport((self.host, self.port))
            self._transport.connect(
                username=self.username,
                password=self.password if self.password else None,
            )
            self._sftp = paramiko.SFTPClient.from_transport(self._transport)
            self._connected = True
            logger.info("Connected to SFTP server", host=self.host)

        except socket.timeout as e:
            raise FTPError(self.host, "connect", f"Connection timeout: {e}") from e
        except paramiko.AuthenticationException as e:
            raise FTPError(self.host, "connect", f"Authentication failed: {e}") from e
        except Exception as e:
            raise FTPError(self.host, "connect", str(e)) from e

    def disconnect(self) -> None:
        """Disconnect from SFTP server."""
        if self._sftp:
            self._sftp.close()
            self._sftp = None
        if self._transport:
            self._transport.close()
            self._transport = None
        self._connected = False

    def download(self, remote_path: str, local_path: Path) -> bool:
        """Download file from SFTP server."""
        if not self._sftp:
            raise FTPError(self.host, "download", "Not connected")

        try:
            local_path.parent.mkdir(parents=True, exist_ok=True)
            self._sftp.get(remote_path, str(local_path))
            logger.info(
                "Downloaded file via SFTP",
                remote=remote_path,
                local=str(local_path),
            )
            return True

        except FileNotFoundError:
            logger.warning("Remote file not found", remote=remote_path)
            return False
        except Exception as e:
            raise FTPError(self.host, "download", str(e)) from e

    def list_files(self, remote_dir: str) -> list[str]:
        """List files in remote directory."""
        if not self._sftp:
            raise FTPError(self.host, "list", "Not connected")

        try:
            return self._sftp.listdir(remote_dir)
        except FileNotFoundError:
            return []

    def file_exists(self, remote_path: str) -> bool:
        """Check if remote file exists."""
        if not self._sftp:
            return False

        try:
            self._sftp.stat(remote_path)
            return True
        except FileNotFoundError:
            return False
