"""
Custom exceptions for PyGNSS-RT.

Provides a hierarchy of exceptions for different error conditions.
"""

from __future__ import annotations


class PyGNSSError(Exception):
    """Base exception for all PyGNSS-RT errors."""

    pass


class ConfigurationError(PyGNSSError):
    """Configuration-related errors."""

    pass


class DatabaseError(PyGNSSError):
    """Database-related errors."""

    pass


class ProductNotAvailableError(PyGNSSError):
    """GNSS product not available."""

    def __init__(
        self,
        product_type: str,
        provider: str,
        date: str,
        message: str | None = None,
    ):
        self.product_type = product_type
        self.provider = provider
        self.date = date
        super().__init__(
            message or f"{product_type} from {provider} not available for {date}"
        )


class FTPError(PyGNSSError):
    """FTP/SFTP transfer errors."""

    def __init__(self, server: str, operation: str, message: str):
        self.server = server
        self.operation = operation
        super().__init__(f"{operation} failed on {server}: {message}")


class HTTPError(PyGNSSError):
    """HTTP/HTTPS transfer errors."""

    def __init__(self, url: str, status_code: int | None, message: str):
        self.url = url
        self.status_code = status_code
        super().__init__(f"HTTP error for {url}: {message}")


class BSWError(PyGNSSError):
    """Bernese GNSS Software errors."""

    def __init__(self, program: str, message: str, return_code: int | None = None):
        self.program = program
        self.return_code = return_code
        super().__init__(f"BSW {program} failed: {message}")


class ProcessingError(PyGNSSError):
    """Processing pipeline errors."""

    def __init__(self, stage: str, message: str, mjd: float | None = None):
        self.stage = stage
        self.mjd = mjd
        super().__init__(f"Processing failed at {stage}: {message}")


class StationError(PyGNSSError):
    """Station configuration or data errors."""

    def __init__(self, station_id: str, message: str):
        self.station_id = station_id
        super().__init__(f"Station {station_id}: {message}")


class DataValidationError(PyGNSSError):
    """Data validation errors."""

    pass
