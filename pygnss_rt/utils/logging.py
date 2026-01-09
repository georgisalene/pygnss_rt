"""
Logging utilities for PyGNSS-RT.

Uses structlog for structured logging with optional JSON output.
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path
from typing import Any

import structlog


def setup_logging(
    level: str = "INFO",
    log_dir: Path | str | None = None,
    log_to_file: bool = True,
    log_to_console: bool = True,
    json_format: bool = False,
) -> None:
    """Configure logging for the application.

    Args:
        level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_dir: Directory for log files
        log_to_file: Whether to log to file
        log_to_console: Whether to log to console
        json_format: Use JSON format for logs
    """
    # Convert level string to logging constant
    log_level = getattr(logging, level.upper(), logging.INFO)

    # Configure standard logging
    handlers: list[logging.Handler] = []

    if log_to_console:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(log_level)
        handlers.append(console_handler)

    if log_to_file and log_dir:
        log_path = Path(log_dir)
        log_path.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(log_path / "pygnss_rt.log")
        file_handler.setLevel(log_level)
        handlers.append(file_handler)

    logging.basicConfig(
        level=log_level,
        handlers=handlers,
        format="%(message)s",
    )

    # Configure structlog
    processors: list[Any] = [
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.UnicodeDecoder(),
    ]

    if json_format:
        processors.append(structlog.processors.JSONRenderer())
    else:
        processors.append(structlog.dev.ConsoleRenderer())

    structlog.configure(
        processors=processors,
        wrapper_class=structlog.stdlib.BoundLogger,
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )


def get_logger(name: str) -> structlog.stdlib.BoundLogger:
    """Get a logger instance.

    Args:
        name: Logger name (typically __name__)

    Returns:
        Configured logger instance
    """
    return structlog.get_logger(name)


# =============================================================================
# i-GNSS Message Printer (replaces Perl PRINT.pm)
# =============================================================================

from enum import Enum


class MessageType(str, Enum):
    """Message types for i-GNSS output.

    Replaces the type parameter in Perl PRINT.pm:
    - REMINDER: General reminder message
    - FATAL: Fatal error message
    - ABORT: Warning that causes abort
    - WARNING: Warning message
    - INFO: Informational message
    - SILENT: Quiet message (minimal formatting)
    - LIST: List item
    - SEVERE: Severe warning with banner
    """

    REMINDER = "REMINDER"
    FATAL = "FATAL"
    ABORT = "ABORT"
    WARNING = "WARNING"
    INFO = "INFO"
    SILENT = "SILENT"
    LIST = "LIST"
    SEVERE = "SEVERE"


class IGNSSPrinter:
    """Formatted message printer for i-GNSS.

    Replaces Perl PRINT.pm module.

    Provides formatted output for different message types
    consistent with the original i-GNSS Perl output.

    Usage:
        from pygnss_rt.utils.logging import IGNSSPrinter, MessageType

        printer = IGNSSPrinter()
        printer.info("Processing started")
        printer.warning("Missing data for station")
        printer.fatal("Cannot connect to database")

        # Or with explicit type
        printer.print_message(MessageType.SEVERE, "Critical error!")

        # Print banner
        printer.print_banner()
    """

    # Message type prefixes
    PREFIXES = {
        MessageType.REMINDER: "i-GNSS REMINDER",
        MessageType.FATAL: "i-GNSS FATAL",
        MessageType.ABORT: "i-GNSS WARNING/ABORT",
        MessageType.WARNING: "i-GNSS WARNING",
        MessageType.INFO: "i-GNSS INFO",
        MessageType.SILENT: "",
        MessageType.LIST: "",
        MessageType.SEVERE: "I-GNSS SEVERE WARN.",
    }

    BANNER = """
____________________________________________________________

   #    ### #   #  ###  ###
       #    ##  # #    #
   # - # ## # # # #### ####      @Orliac,E.,J.,2006 also Hunegnaw 2024
   #   #  # #  ##    #    #             IESSG
   #    ### #   # #### ####   University of Nottingham and UL
___________________________________________________________

"""

    def __init__(self, output_func: callable | None = None):
        """Initialize i-GNSS printer.

        Args:
            output_func: Function to use for output (default: print)
        """
        self._output = output_func or print

    def print_message(
        self,
        msg_type: MessageType | str,
        message: str,
        newline: bool = True,
    ) -> None:
        """Print formatted message.

        Args:
            msg_type: Message type (from MessageType enum or string)
            message: Message text
            newline: Whether to add trailing newline
        """
        # Convert string to enum if needed
        if isinstance(msg_type, str):
            msg_type = MessageType(msg_type.upper())

        cr = "\n" if newline else ""

        if msg_type == MessageType.REMINDER:
            self._output(f"\n{'i-GNSS REMINDER':<20}: {message}{cr}")

        elif msg_type == MessageType.FATAL:
            self._output(f"\n{'i-GNSS FATAL':<20}: {message}{cr}")

        elif msg_type == MessageType.ABORT:
            self._output(f"\n{'i-GNSS WARNING/ABORT':<20}: {message}{cr}")

        elif msg_type == MessageType.WARNING:
            self._output(f"\n{'i-GNSS WARNING':<20}: {message}{cr}")

        elif msg_type == MessageType.INFO:
            self._output(f"\n{'i-GNSS INFO':<20}: {message}{cr}")

        elif msg_type == MessageType.SILENT:
            self._output(f"{'':<20}  ({message}){cr}")

        elif msg_type == MessageType.LIST:
            self._output(f"{'':<20}  - {message}{cr}")

        elif msg_type == MessageType.SEVERE:
            band = "*" * 63
            self._output(
                f"\n\n{'I-GNSS SEVERE WARN.':<20}: {band}\n"
                f"{'':<20}  {message}\n"
                f"{'':<20}  {band}\n\n"
            )

    def reminder(self, message: str, newline: bool = True) -> None:
        """Print reminder message."""
        self.print_message(MessageType.REMINDER, message, newline)

    def fatal(self, message: str, newline: bool = True) -> None:
        """Print fatal error message."""
        self.print_message(MessageType.FATAL, message, newline)

    def abort(self, message: str, newline: bool = True) -> None:
        """Print abort warning message."""
        self.print_message(MessageType.ABORT, message, newline)

    def warning(self, message: str, newline: bool = True) -> None:
        """Print warning message."""
        self.print_message(MessageType.WARNING, message, newline)

    def info(self, message: str, newline: bool = True) -> None:
        """Print info message."""
        self.print_message(MessageType.INFO, message, newline)

    def silent(self, message: str, newline: bool = True) -> None:
        """Print silent/quiet message."""
        self.print_message(MessageType.SILENT, message, newline)

    def list_item(self, message: str, newline: bool = True) -> None:
        """Print list item."""
        self.print_message(MessageType.LIST, message, newline)

    def severe(self, message: str) -> None:
        """Print severe warning with banner."""
        self.print_message(MessageType.SEVERE, message)

    def print_banner(self) -> None:
        """Print i-GNSS ASCII banner."""
        self._output(self.BANNER)


# Module-level convenience instance
_default_printer = IGNSSPrinter()


def ignss_print(
    msg_type: MessageType | str,
    message: str,
    newline: bool = True,
) -> None:
    """Print i-GNSS formatted message.

    Convenience function for quick access to i-GNSS printing.

    Args:
        msg_type: Message type
        message: Message text
        newline: Whether to add trailing newline

    Example:
        from pygnss_rt.utils.logging import ignss_print, MessageType

        ignss_print(MessageType.INFO, "Starting processing")
        ignss_print("warning", "Missing data")
    """
    _default_printer.print_message(msg_type, message, newline)


def ignss_banner() -> None:
    """Print i-GNSS ASCII banner."""
    _default_printer.print_banner()
