"""
Subhourly (15-minute) Meteorological Data (SM) database operations.

Replaces Perl call_SM_maintain.pl and SM-related functions from DB.pm.

Manages 15-minute meteorological data tracking for high-frequency
tropospheric processing. Tracks the status of subhourly MET files.

Usage:
    from pygnss_rt.database.subhourly_met import SubhourlyMetManager

    sm_manager = SubhourlyMetManager(db)
    sm_manager.maintain()  # Add new entries for current 15-min period
    waiting = sm_manager.get_waiting_list()  # Get files to download
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import TYPE_CHECKING, Any

from pygnss_rt.core.exceptions import DatabaseError
from pygnss_rt.utils.dates import GNSSDate
from pygnss_rt.utils.logging import get_logger

if TYPE_CHECKING:
    from pygnss_rt.database.connection import DatabaseManager


logger = get_logger(__name__)


class SMStatus(str, Enum):
    """Status values for subhourly met records."""

    WAITING = "Waiting"
    ON_TIME = "On Time"
    TOO_LATE = "Too Late"
    DOWNLOADED = "Downloaded"
    PROCESSED = "Processed"
    FAILED = "Failed"
    MISSING = "Missing"


@dataclass
class SMEntry:
    """Subhourly meteorological data entry."""

    year: int
    doy: int
    hour: int
    quarter: int  # 0, 1, 2, 3 (for 00, 15, 30, 45 minutes)
    mjd: float
    status: SMStatus = SMStatus.WAITING
    met_file: str | None = None
    provider: str | None = None
    download_time: datetime | None = None
    file_size: int | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None

    @property
    def minute(self) -> int:
        """Get minute value (0, 15, 30, or 45)."""
        return self.quarter * 15

    @property
    def yydoy(self) -> str:
        """Get 2-digit year + 3-digit DOY string."""
        return f"{self.year % 100:02d}{self.doy:03d}"

    @property
    def time_str(self) -> str:
        """Get time string (e.g., '12:15')."""
        return f"{self.hour:02d}:{self.minute:02d}"

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for database insertion."""
        return {
            "year": self.year,
            "doy": self.doy,
            "hour": self.hour,
            "quarter": self.quarter,
            "mjd": self.mjd,
            "status": self.status.value,
            "met_file": self.met_file,
            "provider": self.provider,
            "file_size": self.file_size,
        }


@dataclass
class SMStatistics:
    """Statistics for subhourly met data."""

    total: int = 0
    waiting: int = 0
    downloaded: int = 0
    processed: int = 0
    failed: int = 0
    too_late: int = 0
    missing: int = 0

    @property
    def success_rate(self) -> float:
        """Calculate success rate as percentage."""
        if self.total == 0:
            return 0.0
        return (self.downloaded + self.processed) / self.total * 100


class SubhourlyMetManager:
    """Manages subhourly (15-minute) meteorological data tracking.

    Tracks the status of 15-minute MET files for high-frequency
    tropospheric processing.

    Replaces Perl SM table management from DB.pm:
    - add_met_table_sm()
    - maintain_met_sm()
    - fill_met_gap_sm()
    - set_met_too_late_files_sm()
    - get_met_list_sm()
    - update_met_table_sm()
    """

    TABLE_NAME = "subhourly_met"

    def __init__(self, db: "DatabaseManager"):
        """Initialize SM manager.

        Args:
            db: Database manager instance
        """
        self.db = db

    def table_exists(self) -> bool:
        """Check if SM table exists."""
        row = self.db.fetchone(
            f"SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '{self.TABLE_NAME}'"
        )
        return row is not None and row[0] > 0

    def create_table(self) -> None:
        """Create the subhourly met tracking table."""
        self.db.execute(f"""
            CREATE TABLE IF NOT EXISTS {self.TABLE_NAME} (
                year INTEGER NOT NULL,
                doy INTEGER NOT NULL,
                hour INTEGER NOT NULL,
                quarter INTEGER NOT NULL,
                mjd DOUBLE NOT NULL,
                status VARCHAR(30) DEFAULT '{SMStatus.WAITING.value}',
                met_file VARCHAR,
                provider VARCHAR,
                file_size BIGINT,
                download_time TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (year, doy, hour, quarter)
            )
        """)

        # Create indexes for efficient queries
        self.db.execute(
            f"CREATE INDEX IF NOT EXISTS idx_sm_mjd ON {self.TABLE_NAME}(mjd)"
        )
        self.db.execute(
            f"CREATE INDEX IF NOT EXISTS idx_sm_status ON {self.TABLE_NAME}(status)"
        )
        self.db.execute(
            f"CREATE INDEX IF NOT EXISTS idx_sm_date ON {self.TABLE_NAME}(year, doy)"
        )

    def ensure_table(self) -> None:
        """Ensure the SM table exists, creating if needed."""
        if not self.table_exists():
            self.create_table()

    def add_entry(
        self,
        year: int,
        doy: int,
        hour: int,
        quarter: int,
    ) -> bool:
        """Add a single subhourly entry.

        Args:
            year: Year
            doy: Day of year
            hour: Hour (0-23)
            quarter: Quarter (0-3 for 00, 15, 30, 45 minutes)

        Returns:
            True if entry was added, False if already exists
        """
        # Calculate MJD for this time
        gnss_date = GNSSDate.from_year_doy(year, doy)
        mjd = gnss_date.mjd + (hour + quarter * 0.25) / 24.0

        # Check if exists
        existing = self.db.fetchone(
            f"""
            SELECT 1 FROM {self.TABLE_NAME}
            WHERE year = ? AND doy = ? AND hour = ? AND quarter = ?
            """,
            (year, doy, hour, quarter),
        )

        if existing:
            return False

        self.db.execute(
            f"""
            INSERT INTO {self.TABLE_NAME} (year, doy, hour, quarter, mjd, status)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (year, doy, hour, quarter, mjd, SMStatus.WAITING.value),
        )
        return True

    def maintain(
        self,
        reference_date: GNSSDate | None = None,
    ) -> int:
        """Maintain the SM table by adding entry for current 15-min period.

        Called every 15 minutes by cron.

        Replaces Perl maintain_met_sm().

        Args:
            reference_date: Reference date/time (defaults to now)

        Returns:
            Number of new entries added
        """
        if reference_date is None:
            reference_date = GNSSDate.now()

        # Get current time details
        dt = reference_date.datetime
        hour = dt.hour
        quarter = dt.minute // 15

        if self.add_entry(reference_date.year, reference_date.doy, hour, quarter):
            logger.info(
                "Added subhourly met entry",
                year=reference_date.year,
                doy=reference_date.doy,
                hour=hour,
                quarter=quarter,
            )
            return 1
        return 0

    def fill_gap(
        self,
        late_day: int = 0,
        late_15min: int = 2,
        reference_date: GNSSDate | None = None,
    ) -> int:
        """Fill gaps in the SM table from interruptions.

        If cron was interrupted, this fills in missing 15-minute entries.

        Replaces Perl fill_met_gap_sm().

        Args:
            late_day: Days considered late (default 0)
            late_15min: 15-min periods considered late (default 2)
            reference_date: Reference date/time (defaults to now)

        Returns:
            Number of entries added
        """
        if reference_date is None:
            reference_date = GNSSDate.now()

        # Calculate cutoff MJD (latency in fractional days)
        latency_days = late_day + (late_15min * 0.25) / 24.0
        cutoff_mjd = reference_date.mjd - latency_days

        # Get current min/max in table
        row = self.db.fetchone(
            f"SELECT MIN(mjd), MAX(mjd) FROM {self.TABLE_NAME}"
        )

        if row is None or row[0] is None:
            # Empty table - start from cutoff
            start_mjd = cutoff_mjd - 1.0
        else:
            start_mjd = row[0]

        end_mjd = min(row[1] if row and row[1] else cutoff_mjd, cutoff_mjd)

        added = 0
        current_mjd = start_mjd

        # Step through in 15-minute increments (1/96 of a day)
        step = 1.0 / 96.0

        while current_mjd <= end_mjd:
            current_date = GNSSDate.from_mjd(current_mjd)
            dt = current_date.datetime
            hour = dt.hour
            quarter = dt.minute // 15

            if self.add_entry(current_date.year, current_date.doy, hour, quarter):
                added += 1

            current_mjd += step

        if added > 0:
            logger.info("Filled subhourly met gaps", entries_added=added)

        return added

    def set_too_late_files(
        self,
        late_day: int = 0,
        late_15min: int = 1,
        reference_date: GNSSDate | None = None,
    ) -> int:
        """Mark old waiting files as 'Too Late'.

        Files still in 'Waiting' status past the latency threshold
        are marked as 'Too Late'.

        Replaces Perl set_met_too_late_files_sm().

        Args:
            late_day: Days threshold for "too late"
            late_15min: 15-minute periods threshold
            reference_date: Reference date/time (defaults to now)

        Returns:
            Number of entries updated
        """
        if reference_date is None:
            reference_date = GNSSDate.now()

        # Calculate cutoff MJD
        latency_days = late_day + (late_15min * 0.25) / 24.0
        cutoff_mjd = reference_date.mjd - latency_days

        # Count entries to update
        count_row = self.db.fetchone(
            f"""
            SELECT COUNT(*) FROM {self.TABLE_NAME}
            WHERE status = ? AND mjd < ?
            """,
            (SMStatus.WAITING.value, cutoff_mjd),
        )
        count = count_row[0] if count_row else 0

        # Update entries
        self.db.execute(
            f"""
            UPDATE {self.TABLE_NAME}
            SET status = ?, updated_at = CURRENT_TIMESTAMP
            WHERE status = ? AND mjd < ?
            """,
            (SMStatus.TOO_LATE.value, SMStatus.WAITING.value, cutoff_mjd),
        )

        if count > 0:
            logger.info("Marked subhourly met files as too late", count=count)

        return count

    def get_waiting_list(
        self,
        limit: int | None = None,
    ) -> list[SMEntry]:
        """Get list of files waiting for download.

        Replaces Perl get_met_list_sm({status=>'Waiting'}).

        Args:
            limit: Optional limit on number of results

        Returns:
            List of SMEntry objects
        """
        query = f"""
            SELECT year, doy, hour, quarter, mjd, status,
                   met_file, provider, file_size, download_time,
                   created_at, updated_at
            FROM {self.TABLE_NAME}
            WHERE status = ?
            ORDER BY mjd
        """

        if limit:
            query += f" LIMIT {limit}"

        rows = self.db.fetchall(query, (SMStatus.WAITING.value,))

        return [
            SMEntry(
                year=row[0],
                doy=row[1],
                hour=row[2],
                quarter=row[3],
                mjd=row[4],
                status=SMStatus(row[5]),
                met_file=row[6],
                provider=row[7],
                file_size=row[8],
                download_time=row[9],
                created_at=row[10],
                updated_at=row[11],
            )
            for row in rows
        ]

    def update_downloaded(
        self,
        year: int,
        doy: int,
        hour: int,
        quarter: int,
        met_file: str,
        provider: str | None = None,
        file_size: int | None = None,
        late_day: int = 0,
        late_15min: int = 1,
    ) -> bool:
        """Mark an entry as downloaded.

        Also determines if download was "On Time" or late.

        Replaces Perl update_met_table_sm().

        Args:
            year: Year
            doy: Day of year
            hour: Hour
            quarter: Quarter (0-3)
            met_file: Downloaded MET filename
            provider: Download provider
            file_size: File size in bytes
            late_day: Days threshold for "on time"
            late_15min: 15-min periods threshold

        Returns:
            True if entry was updated
        """
        # Calculate entry MJD
        gnss_date = GNSSDate.from_year_doy(year, doy)
        entry_mjd = gnss_date.mjd + (hour + quarter * 0.25) / 24.0

        # Calculate cutoff for "on time"
        now = GNSSDate.now()
        latency_days = late_day + (late_15min * 0.25) / 24.0
        cutoff_mjd = now.mjd - latency_days

        # Determine status
        if entry_mjd >= cutoff_mjd:
            status = SMStatus.ON_TIME
        else:
            status = SMStatus.DOWNLOADED

        self.db.execute(
            f"""
            UPDATE {self.TABLE_NAME}
            SET status = ?,
                met_file = ?,
                provider = ?,
                file_size = ?,
                download_time = CURRENT_TIMESTAMP,
                updated_at = CURRENT_TIMESTAMP
            WHERE year = ? AND doy = ? AND hour = ? AND quarter = ?
            """,
            (
                status.value,
                met_file,
                provider,
                file_size,
                year,
                doy,
                hour,
                quarter,
            ),
        )

        logger.info(
            "Updated subhourly met download",
            year=year,
            doy=doy,
            hour=hour,
            quarter=quarter,
            status=status.value,
        )

        return True

    def update_failed(
        self,
        year: int,
        doy: int,
        hour: int,
        quarter: int,
    ) -> bool:
        """Mark an entry as failed.

        Args:
            year: Year
            doy: Day of year
            hour: Hour
            quarter: Quarter

        Returns:
            True if entry was updated
        """
        self.db.execute(
            f"""
            UPDATE {self.TABLE_NAME}
            SET status = ?, updated_at = CURRENT_TIMESTAMP
            WHERE year = ? AND doy = ? AND hour = ? AND quarter = ?
            """,
            (SMStatus.FAILED.value, year, doy, hour, quarter),
        )
        return True

    def get_statistics(
        self,
        start_date: GNSSDate | None = None,
        end_date: GNSSDate | None = None,
    ) -> SMStatistics:
        """Get statistics for subhourly met data.

        Args:
            start_date: Optional start date filter
            end_date: Optional end date filter

        Returns:
            SMStatistics object
        """
        conditions = []
        params: list[Any] = []

        if start_date:
            conditions.append("mjd >= ?")
            params.append(start_date.mjd)
        if end_date:
            conditions.append("mjd <= ?")
            params.append(end_date.mjd)

        where_clause = "WHERE " + " AND ".join(conditions) if conditions else ""

        query = f"""
            SELECT status, COUNT(*) as count
            FROM {self.TABLE_NAME}
            {where_clause}
            GROUP BY status
        """

        rows = self.db.fetchall(query, tuple(params) if params else None)

        stats = SMStatistics()
        for row in rows:
            status, count = row[0], row[1]
            stats.total += count

            if status == SMStatus.WAITING.value:
                stats.waiting = count
            elif status in (SMStatus.DOWNLOADED.value, SMStatus.ON_TIME.value):
                stats.downloaded += count
            elif status == SMStatus.PROCESSED.value:
                stats.processed = count
            elif status == SMStatus.FAILED.value:
                stats.failed = count
            elif status == SMStatus.TOO_LATE.value:
                stats.too_late = count
            elif status == SMStatus.MISSING.value:
                stats.missing = count

        return stats

    def cleanup_old_entries(self, days_to_keep: int = 30) -> int:
        """Remove old entries to prevent table bloat.

        Args:
            days_to_keep: Number of days of data to retain

        Returns:
            Number of rows deleted
        """
        cutoff_mjd = GNSSDate.now().mjd - days_to_keep

        count_row = self.db.fetchone(
            f"SELECT COUNT(*) FROM {self.TABLE_NAME} WHERE mjd < ?",
            (cutoff_mjd,),
        )
        count = count_row[0] if count_row else 0

        self.db.execute(
            f"DELETE FROM {self.TABLE_NAME} WHERE mjd < ?",
            (cutoff_mjd,),
        )

        if count > 0:
            logger.info("Cleaned up old subhourly met entries", deleted=count)

        return count

    def build_met_filename(
        self,
        year: int,
        doy: int,
        hour: int,
        quarter: int,
        extension: str = ".met",
    ) -> str:
        """Build standard MET filename for subhourly data.

        Args:
            year: Year
            doy: Day of year
            hour: Hour (0-23)
            quarter: Quarter (0-3)
            extension: File extension

        Returns:
            Filename string (e.g., 'met24260_1215.met')
        """
        yy = year % 100
        minute = quarter * 15
        return f"met{yy:02d}{doy:03d}_{hour:02d}{minute:02d}{extension}"


def run_sm_maintenance(
    db: "DatabaseManager",
    met_dir: str | Path = "/data/subhourlymetData",
    late_day: int = 0,
    late_15min: int = 1,
) -> dict[str, Any]:
    """Run full subhourly MET maintenance cycle.

    Convenience function that performs all SM maintenance steps:
    1. Create table if needed
    2. Maintain (add current entry)
    3. Fill gaps
    4. Set too late files
    5. Return waiting list

    Replaces the main logic of call_SM_maintain.pl.

    Args:
        db: Database manager
        met_dir: Directory for MET files
        late_day: Days latency threshold
        late_15min: 15-minute periods latency threshold

    Returns:
        Dictionary with maintenance results
    """
    sm_manager = SubhourlyMetManager(db)

    # Ensure table exists
    sm_manager.ensure_table()

    # Maintain table (add current entry)
    added = sm_manager.maintain()

    # Fill gaps
    gap_filled = sm_manager.fill_gap(late_day=late_day, late_15min=late_15min + 1)

    # Set too late files
    too_late_count = sm_manager.set_too_late_files(
        late_day=late_day,
        late_15min=late_15min,
    )

    # Get waiting list
    waiting = sm_manager.get_waiting_list()

    # Get statistics
    stats = sm_manager.get_statistics()

    return {
        "entries_added": added,
        "gaps_filled": gap_filled,
        "marked_too_late": too_late_count,
        "waiting_count": len(waiting),
        "waiting_files": waiting,
        "statistics": stats,
    }
