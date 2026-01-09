"""
Monitoring and alerting module for PyGNSS-RT.

Replaces Perl TIVOLI2.pm module with modern Python alerting capabilities.

Provides:
- Structured event logging with severity levels
- Email notifications for critical events
- Processing status tracking
- Alert aggregation and deduplication
- Integration with external monitoring systems

Usage:
    from pygnss_rt.utils.monitoring import AlertManager, AlertLevel, ProcessingAlert

    # Initialize alert manager
    alerts = AlertManager(
        log_file="/path/to/alerts.log",
        email_config=EmailConfig(smtp_server="smtp.example.com", ...)
    )

    # Log processing events
    alerts.log_event(
        ProcessingAlert(
            code="E001",
            level=AlertLevel.FATAL,
            campaign="IG2024001",
            message="CPU file is locked",
        )
    )

    # Send email for critical alerts
    alerts.send_email_alert(
        subject="i-GNSS Processing Failed",
        body="Campaign IG2024001 failed due to CPU lock",
        recipients=["alerts@example.com"],
    )
"""

from __future__ import annotations

import json
import smtplib
import socket
from dataclasses import dataclass, field
from datetime import datetime, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from enum import Enum
from pathlib import Path
from typing import Any, Callable

from pygnss_rt.utils.logging import get_logger

logger = get_logger(__name__)


# =============================================================================
# Alert Enums and Constants
# =============================================================================

class AlertLevel(str, Enum):
    """Alert severity levels (matches TIVOLI2.pm)."""

    FATAL = "FATAL"  # Processing cannot continue
    CRITICAL = "CRITICAL"  # Severe issue requiring attention
    MINOR = "MINOR"  # Issue that may affect results
    WARNING = "WARNING"  # Potential issue to monitor
    HARMLESS = "HARMLESS"  # Informational only
    INFO = "INFO"  # General information


class AlertType(str, Enum):
    """Alert type categories."""

    ERROR = "ERROR"  # Processing error
    MISSING = "MISSING"  # Missing data or products
    CLEARING = "CLEARING"  # Alert cleared/resolved
    SYSTEM = "SYSTEM"  # System-level alert
    DOWNLOAD = "DOWNLOAD"  # Download-related alert
    PROCESSING = "PROCESSING"  # Processing-related alert


# Standard alert codes (from TIVOLI2.pm)
ALERT_CODES = {
    # Error codes
    "E001": {
        "type": AlertType.ERROR,
        "level": AlertLevel.FATAL,
        "description": "CPU file is locked",
        "action": "Check if another process is running or clear the lock file",
    },
    "E002": {
        "type": AlertType.ERROR,
        "level": AlertLevel.FATAL,
        "description": "Campaign processing failed",
        "action": "Check BPE directory for error logs",
    },
    "E003": {
        "type": AlertType.ERROR,
        "level": AlertLevel.CRITICAL,
        "description": "BSW execution error",
        "action": "Check Bernese log files for details",
    },
    "E004": {
        "type": AlertType.ERROR,
        "level": AlertLevel.CRITICAL,
        "description": "Database connection failed",
        "action": "Verify database is running and credentials are correct",
    },
    # Missing data codes
    "M001": {
        "type": AlertType.MISSING,
        "level": AlertLevel.FATAL,
        "description": "Missing mandatory product(s)",
        "action": "Check product download status and availability",
    },
    "M002": {
        "type": AlertType.MISSING,
        "level": AlertLevel.HARMLESS,
        "description": "Low percentage of available hourly RINEX files",
        "action": "Monitor data availability from stations",
    },
    "M003": {
        "type": AlertType.MISSING,
        "level": AlertLevel.FATAL,
        "description": "No hourly RINEX file available",
        "action": "Check station data downloads and FTP connectivity",
    },
    "M004": {
        "type": AlertType.MISSING,
        "level": AlertLevel.HARMLESS,
        "description": "Low percentage of available daily RINEX files",
        "action": "Monitor data availability from stations",
    },
    "M005": {
        "type": AlertType.MISSING,
        "level": AlertLevel.FATAL,
        "description": "No daily RINEX file available",
        "action": "Check station data downloads and FTP connectivity",
    },
    "M006": {
        "type": AlertType.MISSING,
        "level": AlertLevel.HARMLESS,
        "description": "Missing hourly meteorological file",
        "action": "ZTD to IWV conversion will be skipped",
    },
    # Download codes
    "D001": {
        "type": AlertType.DOWNLOAD,
        "level": AlertLevel.WARNING,
        "description": "Product download failed",
        "action": "Retry download or check FTP server availability",
    },
    "D002": {
        "type": AlertType.DOWNLOAD,
        "level": AlertLevel.WARNING,
        "description": "Station data download failed",
        "action": "Check data provider connectivity",
    },
    # System codes
    "S001": {
        "type": AlertType.SYSTEM,
        "level": AlertLevel.CRITICAL,
        "description": "Disk space low",
        "action": "Free up disk space or expand storage",
    },
    "S002": {
        "type": AlertType.SYSTEM,
        "level": AlertLevel.WARNING,
        "description": "High memory usage",
        "action": "Monitor system resources",
    },
    # Processing codes
    "P001": {
        "type": AlertType.PROCESSING,
        "level": AlertLevel.INFO,
        "description": "Processing started",
        "action": None,
    },
    "P002": {
        "type": AlertType.PROCESSING,
        "level": AlertLevel.INFO,
        "description": "Processing completed successfully",
        "action": None,
    },
    "P003": {
        "type": AlertType.PROCESSING,
        "level": AlertLevel.WARNING,
        "description": "Processing completed with warnings",
        "action": "Review processing logs for issues",
    },
}


# =============================================================================
# Data Classes
# =============================================================================

@dataclass
class ProcessingAlert:
    """A processing alert/event."""

    code: str
    level: AlertLevel
    campaign: str
    message: str
    alert_type: AlertType | None = None
    script_name: str | None = None
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    details: dict[str, Any] = field(default_factory=dict)
    action: str | None = None

    def __post_init__(self):
        """Fill in defaults from ALERT_CODES if available."""
        if self.code in ALERT_CODES:
            code_info = ALERT_CODES[self.code]
            if self.alert_type is None:
                self.alert_type = code_info["type"]
            if self.action is None:
                self.action = code_info.get("action")

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "code": self.code,
            "level": self.level.value,
            "type": self.alert_type.value if self.alert_type else None,
            "campaign": self.campaign,
            "message": self.message,
            "script_name": self.script_name,
            "timestamp": self.timestamp.isoformat(),
            "details": self.details,
            "action": self.action,
        }

    def to_log_line(self) -> str:
        """Format as log line (TIVOLI2 format)."""
        ts = self.timestamp.strftime("%d/%m/%Y %H:%M:%S")
        script = self.script_name or "pygnss-rt"
        alert_type = self.alert_type.value if self.alert_type else "UNKNOWN"
        return (
            f"{ts} {script} {alert_type} {self.code} {self.level.value} "
            f"{self.message}"
        )


@dataclass
class EmailConfig:
    """Email configuration for alerts."""

    smtp_server: str
    smtp_port: int = 587
    use_tls: bool = True
    username: str | None = None
    password: str | None = None
    from_address: str = "pygnss-rt@localhost"
    default_recipients: list[str] = field(default_factory=list)
    enabled: bool = True

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "EmailConfig":
        """Create from dictionary."""
        return cls(
            smtp_server=data.get("smtp_server", "localhost"),
            smtp_port=data.get("smtp_port", 587),
            use_tls=data.get("use_tls", True),
            username=data.get("username"),
            password=data.get("password"),
            from_address=data.get("from_address", "pygnss-rt@localhost"),
            default_recipients=data.get("default_recipients", []),
            enabled=data.get("enabled", True),
        )


@dataclass
class AlertStats:
    """Statistics about alerts."""

    total_alerts: int = 0
    by_level: dict[str, int] = field(default_factory=dict)
    by_type: dict[str, int] = field(default_factory=dict)
    by_campaign: dict[str, int] = field(default_factory=dict)
    emails_sent: int = 0
    last_alert: datetime | None = None


# =============================================================================
# Alert Manager
# =============================================================================

class AlertManager:
    """
    Central alert management for PyGNSS-RT processing.

    Handles:
    - Logging alerts to file
    - Sending email notifications
    - Tracking alert statistics
    - Alert deduplication
    - Webhook notifications (optional)
    """

    def __init__(
        self,
        log_file: Path | str | None = None,
        email_config: EmailConfig | None = None,
        script_name: str = "pygnss-rt",
        enable_console: bool = True,
    ):
        """Initialize alert manager.

        Args:
            log_file: Path to alert log file
            email_config: Email configuration for notifications
            script_name: Name of calling script
            enable_console: Also print alerts to console
        """
        self.log_file = Path(log_file) if log_file else None
        self.email_config = email_config
        self.script_name = script_name
        self.enable_console = enable_console

        self._stats = AlertStats()
        self._recent_alerts: list[ProcessingAlert] = []
        self._alert_hooks: list[Callable[[ProcessingAlert], None]] = []
        self._hostname = socket.gethostname()

        # Ensure log file directory exists
        if self.log_file:
            self.log_file.parent.mkdir(parents=True, exist_ok=True)

    def log_event(
        self,
        alert: ProcessingAlert,
        send_email: bool | None = None,
    ) -> None:
        """Log a processing alert/event.

        Args:
            alert: The alert to log
            send_email: Override email sending (None = auto based on level)
        """
        # Set script name if not provided
        if alert.script_name is None:
            alert.script_name = self.script_name

        # Update statistics
        self._update_stats(alert)
        self._recent_alerts.append(alert)

        # Keep only last 1000 alerts in memory
        if len(self._recent_alerts) > 1000:
            self._recent_alerts = self._recent_alerts[-1000:]

        # Log to file
        if self.log_file:
            self._write_to_log(alert)

        # Log to console
        if self.enable_console:
            self._log_to_console(alert)

        # Send email if needed
        if send_email is None:
            send_email = alert.level in (AlertLevel.FATAL, AlertLevel.CRITICAL)

        if send_email and self.email_config and self.email_config.enabled:
            self._send_email_for_alert(alert)

        # Call registered hooks
        for hook in self._alert_hooks:
            try:
                hook(alert)
            except Exception as e:
                logger.warning(f"Alert hook failed: {e}")

    def log_error(
        self,
        code: str,
        campaign: str,
        message: str,
        **details: Any,
    ) -> None:
        """Convenience method to log an error alert.

        Args:
            code: Alert code (e.g., "E001")
            campaign: Campaign identifier
            message: Error message
            **details: Additional details
        """
        level = AlertLevel.FATAL
        if code in ALERT_CODES:
            level = ALERT_CODES[code]["level"]

        alert = ProcessingAlert(
            code=code,
            level=level,
            campaign=campaign,
            message=message,
            details=details,
        )
        self.log_event(alert)

    def log_warning(
        self,
        code: str,
        campaign: str,
        message: str,
        **details: Any,
    ) -> None:
        """Convenience method to log a warning alert."""
        alert = ProcessingAlert(
            code=code,
            level=AlertLevel.WARNING,
            campaign=campaign,
            message=message,
            details=details,
        )
        self.log_event(alert)

    def log_info(
        self,
        campaign: str,
        message: str,
        **details: Any,
    ) -> None:
        """Convenience method to log an info event."""
        alert = ProcessingAlert(
            code="P001",
            level=AlertLevel.INFO,
            campaign=campaign,
            message=message,
            details=details,
        )
        self.log_event(alert, send_email=False)

    def log_success(
        self,
        campaign: str,
        message: str = "Processing completed successfully",
        **details: Any,
    ) -> None:
        """Convenience method to log successful completion."""
        alert = ProcessingAlert(
            code="P002",
            level=AlertLevel.INFO,
            campaign=campaign,
            message=message,
            alert_type=AlertType.PROCESSING,
            details=details,
        )
        self.log_event(alert, send_email=False)

    def send_email_alert(
        self,
        subject: str,
        body: str,
        recipients: list[str] | None = None,
        html_body: str | None = None,
    ) -> bool:
        """Send an email alert.

        Args:
            subject: Email subject
            body: Plain text body
            recipients: List of recipients (or use defaults)
            html_body: Optional HTML body

        Returns:
            True if email was sent successfully
        """
        if not self.email_config or not self.email_config.enabled:
            logger.warning("Email not configured or disabled")
            return False

        recipients = recipients or self.email_config.default_recipients
        if not recipients:
            logger.warning("No email recipients configured")
            return False

        try:
            # Create message
            msg = MIMEMultipart("alternative")
            msg["Subject"] = f"[i-GNSS] {subject}"
            msg["From"] = self.email_config.from_address
            msg["To"] = ", ".join(recipients)

            # Add plain text body
            msg.attach(MIMEText(body, "plain"))

            # Add HTML body if provided
            if html_body:
                msg.attach(MIMEText(html_body, "html"))

            # Send email
            with smtplib.SMTP(
                self.email_config.smtp_server,
                self.email_config.smtp_port,
            ) as server:
                if self.email_config.use_tls:
                    server.starttls()
                if self.email_config.username and self.email_config.password:
                    server.login(
                        self.email_config.username,
                        self.email_config.password,
                    )
                server.send_message(msg)

            self._stats.emails_sent += 1
            logger.info(f"Alert email sent to {recipients}")
            return True

        except Exception as e:
            logger.error(f"Failed to send email alert: {e}")
            return False

    def register_hook(
        self,
        callback: Callable[[ProcessingAlert], None],
    ) -> None:
        """Register a callback to be called for each alert.

        Args:
            callback: Function that takes a ProcessingAlert
        """
        self._alert_hooks.append(callback)

    def get_stats(self) -> AlertStats:
        """Get alert statistics."""
        return self._stats

    def get_recent_alerts(
        self,
        level: AlertLevel | None = None,
        campaign: str | None = None,
        limit: int = 100,
    ) -> list[ProcessingAlert]:
        """Get recent alerts with optional filtering.

        Args:
            level: Filter by level
            campaign: Filter by campaign
            limit: Maximum number to return

        Returns:
            List of matching alerts
        """
        alerts = self._recent_alerts

        if level:
            alerts = [a for a in alerts if a.level == level]
        if campaign:
            alerts = [a for a in alerts if a.campaign == campaign]

        return alerts[-limit:]

    def clear_stats(self) -> None:
        """Reset statistics."""
        self._stats = AlertStats()
        self._recent_alerts = []

    def _update_stats(self, alert: ProcessingAlert) -> None:
        """Update statistics for an alert."""
        self._stats.total_alerts += 1
        self._stats.last_alert = alert.timestamp

        # By level
        level_key = alert.level.value
        self._stats.by_level[level_key] = self._stats.by_level.get(level_key, 0) + 1

        # By type
        if alert.alert_type:
            type_key = alert.alert_type.value
            self._stats.by_type[type_key] = self._stats.by_type.get(type_key, 0) + 1

        # By campaign
        camp_key = alert.campaign
        self._stats.by_campaign[camp_key] = self._stats.by_campaign.get(camp_key, 0) + 1

    def _write_to_log(self, alert: ProcessingAlert) -> None:
        """Write alert to log file."""
        if not self.log_file:
            return

        try:
            with open(self.log_file, "a", encoding="utf-8") as f:
                f.write(alert.to_log_line() + "\n")
        except Exception as e:
            logger.error(f"Failed to write to alert log: {e}")

    def _log_to_console(self, alert: ProcessingAlert) -> None:
        """Log alert to console via structlog."""
        log_method = logger.info
        if alert.level == AlertLevel.FATAL:
            log_method = logger.critical
        elif alert.level == AlertLevel.CRITICAL:
            log_method = logger.error
        elif alert.level in (AlertLevel.WARNING, AlertLevel.MINOR):
            log_method = logger.warning

        log_method(
            alert.message,
            code=alert.code,
            campaign=alert.campaign,
            level=alert.level.value,
        )

    def _send_email_for_alert(self, alert: ProcessingAlert) -> None:
        """Send email for a specific alert."""
        subject = f"{alert.level.value}: {alert.campaign} - {alert.code}"

        body = f"""
i-GNSS Processing Alert
=======================

Timestamp: {alert.timestamp.strftime('%Y-%m-%d %H:%M:%S UTC')}
Host: {self._hostname}
Script: {alert.script_name}

Campaign: {alert.campaign}
Alert Code: {alert.code}
Severity: {alert.level.value}
Type: {alert.alert_type.value if alert.alert_type else 'Unknown'}

Message:
{alert.message}

"""
        if alert.action:
            body += f"""
Recommended Action:
{alert.action}

"""

        if alert.details:
            body += f"""
Details:
{json.dumps(alert.details, indent=2)}

"""

        body += """
---
This is an automated alert from the i-GNSS processing system.
"""

        self.send_email_alert(subject, body)


# =============================================================================
# Convenience Functions
# =============================================================================

# Global alert manager instance
_global_alert_manager: AlertManager | None = None


def get_alert_manager() -> AlertManager:
    """Get or create the global alert manager."""
    global _global_alert_manager
    if _global_alert_manager is None:
        _global_alert_manager = AlertManager()
    return _global_alert_manager


def configure_alerts(
    log_file: Path | str | None = None,
    email_config: EmailConfig | dict[str, Any] | None = None,
    script_name: str = "pygnss-rt",
) -> AlertManager:
    """Configure the global alert manager.

    Args:
        log_file: Path to alert log file
        email_config: Email configuration (dict or EmailConfig)
        script_name: Name of calling script

    Returns:
        Configured AlertManager
    """
    global _global_alert_manager

    if isinstance(email_config, dict):
        email_config = EmailConfig.from_dict(email_config)

    _global_alert_manager = AlertManager(
        log_file=log_file,
        email_config=email_config,
        script_name=script_name,
    )

    return _global_alert_manager


def alert(
    code: str,
    campaign: str,
    message: str,
    level: AlertLevel | None = None,
    send_email: bool | None = None,
    **details: Any,
) -> None:
    """Convenience function to log an alert.

    Args:
        code: Alert code
        campaign: Campaign identifier
        message: Alert message
        level: Override severity level
        send_email: Override email sending
        **details: Additional details
    """
    manager = get_alert_manager()

    if level is None and code in ALERT_CODES:
        level = ALERT_CODES[code]["level"]
    elif level is None:
        level = AlertLevel.WARNING

    alert_obj = ProcessingAlert(
        code=code,
        level=level,
        campaign=campaign,
        message=message,
        details=details,
    )

    manager.log_event(alert_obj, send_email=send_email)


def alert_error(campaign: str, message: str, code: str = "E002", **details: Any) -> None:
    """Log an error alert."""
    alert(code, campaign, message, level=AlertLevel.FATAL, **details)


def alert_warning(campaign: str, message: str, code: str = "P003", **details: Any) -> None:
    """Log a warning alert."""
    alert(code, campaign, message, level=AlertLevel.WARNING, **details)


def alert_success(campaign: str, message: str = "Processing completed", **details: Any) -> None:
    """Log a success notification."""
    alert("P002", campaign, message, level=AlertLevel.INFO, send_email=False, **details)
