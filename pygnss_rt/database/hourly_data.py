"""
Hourly Data (HD) database operations.

Replaces Perl call_HD_maintain.pl and HD-related functions from DB.pm.

Manages hourly RINEX observation data tracking for near-real-time processing.
HD data tracks the status of hourly RINEX files needed for NRDDP TRO processing.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import TYPE_CHECKING, Any

from pygnss_rt.core.exceptions import DatabaseError
from pygnss_rt.utils.dates import GNSSDate, hour_to_alpha, alpha_to_hour

if TYPE_CHECKING:
    from pygnss_rt.database.connection import DatabaseManager


class HDStatus(str, Enum):
    """Status values for hourly data records."""

    WAITING = "Waiting"
    ON_TIME = "On Time"
    TOO_LATE = "Too Late"
    DOWNLOADED = "Downloaded"
    PROCESSED = "Processed"
    FAILED = "Failed"
    MISSING = "Missing"


@dataclass
class HDEntry:
    """Hourly data entry."""

    station_id: str
    year: int
    doy: int
    hour: int  # 0-23
    mjd: float
    status: HDStatus = HDStatus.WAITING
    rinex_file: str | None = None
    provider: str | None = None
    download_time: datetime | None = None
    file_size: int | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None

    @property
    def hour_alpha(self) -> str:
        """Get hour as alpha character (a-x)."""
        return hour_to_alpha(self.hour)

    @property
    def yydoy(self) -> str:
        """Get 2-digit year + 3-digit DOY string."""
        return f"{self.year % 100:02d}{self.doy:03d}"

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for database insertion."""
        return {
            "station_id": self.station_id,
            "year": self.year,
            "doy": self.doy,
            "hour": self.hour,
            "mjd": self.mjd,
            "status": self.status.value,
            "rinex_file": self.rinex_file,
            "provider": self.provider,
            "file_size": self.file_size,
        }


@dataclass
class HDStatistics:
    """Statistics for hourly data."""

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


class HourlyDataManager:
    """Manages hourly RINEX observation data tracking.

    Tracks the status of hourly RINEX files across multiple stations.
    Files progress through states: Waiting -> Downloaded -> Processed

    Replaces Perl HD table management from DB.pm:
    - add_hd_table()
    - maintain_hd()
    - fill_hd_gap()
    - set_hd_too_late_files()
    - get_hd_list()
    - update_hd_table()
    """

    TABLE_NAME = "hourly_data"

    def __init__(self, db: "DatabaseManager"):
        """Initialize HD manager.

        Args:
            db: Database manager instance
        """
        self.db = db

    def table_exists(self) -> bool:
        """Check if HD table exists."""
        row = self.db.fetchone(
            f"SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '{self.TABLE_NAME}'"
        )
        return row is not None and row[0] > 0

    def create_table(self) -> None:
        """Create the hourly data tracking table.

        Extended schema compared to original Perl implementation:
        - Adds provider tracking
        - Adds file size tracking
        - Adds download timestamp
        """
        self.db.execute(f"""
            CREATE TABLE IF NOT EXISTS {self.TABLE_NAME} (
                station_id VARCHAR NOT NULL,
                year INTEGER NOT NULL,
                doy INTEGER NOT NULL,
                hour INTEGER NOT NULL,
                mjd DOUBLE NOT NULL,
                status VARCHAR(30) DEFAULT '{HDStatus.WAITING.value}',
                rinex_file VARCHAR,
                provider VARCHAR,
                file_size BIGINT,
                download_time TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (station_id, year, doy, hour)
            )
        """)

        # Create indexes for efficient queries
        self.db.execute(
            f"CREATE INDEX IF NOT EXISTS idx_hd_mjd ON {self.TABLE_NAME}(mjd)"
        )
        self.db.execute(
            f"CREATE INDEX IF NOT EXISTS idx_hd_status ON {self.TABLE_NAME}(status)"
        )
        self.db.execute(
            f"CREATE INDEX IF NOT EXISTS idx_hd_station ON {self.TABLE_NAME}(station_id)"
        )
        self.db.execute(
            f"CREATE INDEX IF NOT EXISTS idx_hd_date ON {self.TABLE_NAME}(year, doy)"
        )

    def ensure_table(self) -> None:
        """Ensure the HD table exists, creating if needed."""
        if not self.table_exists():
            self.create_table()

    def add_station_hour(
        self,
        station_id: str,
        date: GNSSDate,
        hour: int,
    ) -> bool:
        """Add a single station-hour entry.

        Args:
            station_id: 4-character station ID
            date: GNSS date
            hour: Hour (0-23)

        Returns:
            True if entry was added, False if already exists
        """
        station_id = station_id.lower()
        mjd = date.mjd + hour / 24.0

        # Check if exists
        existing = self.db.fetchone(
            f"""
            SELECT 1 FROM {self.TABLE_NAME}
            WHERE station_id = ? AND year = ? AND doy = ? AND hour = ?
            """,
            (station_id, date.year, date.doy, hour),
        )

        if existing:
            return False

        self.db.execute(
            f"""
            INSERT INTO {self.TABLE_NAME} (station_id, year, doy, hour, mjd, status)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (station_id, date.year, date.doy, hour, mjd, HDStatus.WAITING.value),
        )
        return True

    def maintain(
        self,
        station_ids: list[str],
        reference_date: GNSSDate | None = None,
    ) -> int:
        """Maintain the HD table by adding new hourly entries for all stations.

        Adds new rows for the current hour if they don't exist.
        This is typically called every hour by cron.

        Replaces Perl maintain_hd().

        Args:
            station_ids: List of station IDs to maintain
            reference_date: Reference date/time (defaults to now)

        Returns:
            Number of new rows added
        """
        if reference_date is None:
            reference_date = GNSSDate.now()

        added = 0
        hour = reference_date.hour if hasattr(reference_date, 'hour') else 0

        for station_id in station_ids:
            if self.add_station_hour(station_id, reference_date, hour):
                added += 1

        return added

    def fill_gap(
        self,
        station_ids: list[str],
        late_day: int = 3,
        late_hour: int = 1,
        reference_date: GNSSDate | None = None,
    ) -> int:
        """Fill gaps in the HD table from interruptions.

        If cron was interrupted, this fills in missing hourly entries
        between the last entry and (now - latency).

        Replaces Perl fill_hd_gap().

        Args:
            station_ids: List of station IDs to fill
            late_day: Days considered late (default 3)
            late_hour: Hours considered late (default 1)
            reference_date: Reference date/time (defaults to now)

        Returns:
            Number of rows added
        """
        if reference_date is None:
            reference_date = GNSSDate.now()

        # Calculate cutoff MJD (current time minus latency)
        latency_hours = late_day * 24 + late_hour
        cutoff_date = reference_date.add_hours(-latency_hours)
        cutoff_mjd = cutoff_date.mjd

        added = 0

        for station_id in station_ids:
            station_id = station_id.lower()

            # Get the latest entry for this station
            row = self.db.fetchone(
                f"SELECT MAX(mjd) FROM {self.TABLE_NAME} WHERE station_id = ?",
                (station_id,),
            )
            last_mjd = row[0] if row and row[0] else None

            if last_mjd is None:
                # No entries for this station, start from cutoff minus one day
                last_mjd = cutoff_mjd - 1.0

            # Fill in hourly entries from last_mjd to cutoff_mjd
            current_mjd = last_mjd + (1 / 24.0)

            while current_mjd <= cutoff_mjd:
                current_date = GNSSDate.from_mjd(current_mjd)
                hour = int((current_mjd % 1) * 24) % 24

                if self.add_station_hour(station_id, current_date, hour):
                    added += 1

                current_mjd += 1 / 24.0

        return added

    def set_too_late_files(
        self,
        late_day: int = 3,
        late_hour: int = 0,
        reference_date: GNSSDate | None = None,
    ) -> int:
        """Mark old waiting files as 'Too Late'.

        Files still in 'Waiting' status past the latency threshold
        are marked as 'Too Late' since the data is no longer useful
        for near-real-time processing.

        Replaces Perl set_hd_too_late_files().

        Args:
            late_day: Days threshold for "too late"
            late_hour: Hours threshold for "too late"
            reference_date: Reference date/time (defaults to now)

        Returns:
            Number of rows updated
        """
        if reference_date is None:
            reference_date = GNSSDate.now()

        latency_hours = late_day * 24 + late_hour
        cutoff_date = reference_date.add_hours(-latency_hours)
        cutoff_mjd = cutoff_date.mjd

        # Count rows to be updated
        count_row = self.db.fetchone(
            f"""
            SELECT COUNT(*) FROM {self.TABLE_NAME}
            WHERE status = ? AND mjd < ?
            """,
            (HDStatus.WAITING.value, cutoff_mjd),
        )
        count = count_row[0] if count_row else 0

        # Update all waiting entries older than cutoff
        self.db.execute(
            f"""
            UPDATE {self.TABLE_NAME}
            SET status = ?, updated_at = CURRENT_TIMESTAMP
            WHERE status = ? AND mjd < ?
            """,
            (HDStatus.TOO_LATE.value, HDStatus.WAITING.value, cutoff_mjd),
        )

        return count

    def get_waiting_list(
        self,
        station_ids: list[str] | None = None,
        limit: int | None = None,
    ) -> list[HDEntry]:
        """Get list of files waiting for download.

        Returns entries with 'Waiting' status that need to be downloaded.

        Replaces Perl get_hd_list({status=>'Waiting'}).

        Args:
            station_ids: Optional list of stations to filter
            limit: Optional limit on number of results

        Returns:
            List of HDEntry objects
        """
        query = f"""
            SELECT station_id, year, doy, hour, mjd, status,
                   rinex_file, provider, file_size, download_time,
                   created_at, updated_at
            FROM {self.TABLE_NAME}
            WHERE status = ?
        """
        params: list[Any] = [HDStatus.WAITING.value]

        if station_ids:
            placeholders = ",".join("?" * len(station_ids))
            query += f" AND station_id IN ({placeholders})"
            params.extend([s.lower() for s in station_ids])

        query += " ORDER BY mjd"

        if limit:
            query += f" LIMIT {limit}"

        rows = self.db.fetchall(query, tuple(params))

        return [
            HDEntry(
                station_id=row[0],
                year=row[1],
                doy=row[2],
                hour=row[3],
                mjd=row[4],
                status=HDStatus(row[5]),
                rinex_file=row[6],
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
        station_id: str,
        year: int,
        doy: int,
        hour: int,
        rinex_file: str,
        provider: str | None = None,
        file_size: int | None = None,
    ) -> bool:
        """Mark an entry as downloaded.

        Args:
            station_id: Station ID
            year: Year
            doy: Day of year
            hour: Hour
            rinex_file: Downloaded RINEX filename
            provider: Download provider
            file_size: File size in bytes

        Returns:
            True if entry was updated
        """
        station_id = station_id.lower()

        result = self.db.execute(
            f"""
            UPDATE {self.TABLE_NAME}
            SET status = ?,
                rinex_file = ?,
                provider = ?,
                file_size = ?,
                download_time = CURRENT_TIMESTAMP,
                updated_at = CURRENT_TIMESTAMP
            WHERE station_id = ? AND year = ? AND doy = ? AND hour = ?
            """,
            (
                HDStatus.DOWNLOADED.value,
                rinex_file,
                provider,
                file_size,
                station_id,
                year,
                doy,
                hour,
            ),
        )
        return True

    def update_processed(
        self,
        station_id: str,
        year: int,
        doy: int,
        hour: int,
    ) -> bool:
        """Mark an entry as processed.

        Args:
            station_id: Station ID
            year: Year
            doy: Day of year
            hour: Hour

        Returns:
            True if entry was updated
        """
        station_id = station_id.lower()

        self.db.execute(
            f"""
            UPDATE {self.TABLE_NAME}
            SET status = ?, updated_at = CURRENT_TIMESTAMP
            WHERE station_id = ? AND year = ? AND doy = ? AND hour = ?
            """,
            (HDStatus.PROCESSED.value, station_id, year, doy, hour),
        )
        return True

    def update_failed(
        self,
        station_id: str,
        year: int,
        doy: int,
        hour: int,
    ) -> bool:
        """Mark an entry as failed.

        Args:
            station_id: Station ID
            year: Year
            doy: Day of year
            hour: Hour

        Returns:
            True if entry was updated
        """
        station_id = station_id.lower()

        self.db.execute(
            f"""
            UPDATE {self.TABLE_NAME}
            SET status = ?, updated_at = CURRENT_TIMESTAMP
            WHERE station_id = ? AND year = ? AND doy = ? AND hour = ?
            """,
            (HDStatus.FAILED.value, station_id, year, doy, hour),
        )
        return True

    def get_statistics(
        self,
        start_date: GNSSDate | None = None,
        end_date: GNSSDate | None = None,
    ) -> HDStatistics:
        """Get statistics for hourly data.

        Args:
            start_date: Optional start date filter
            end_date: Optional end date filter

        Returns:
            HDStatistics object
        """
        where_clause = ""
        params: list[Any] = []

        if start_date or end_date:
            conditions = []
            if start_date:
                conditions.append("mjd >= ?")
                params.append(start_date.mjd)
            if end_date:
                conditions.append("mjd <= ?")
                params.append(end_date.mjd)
            where_clause = "WHERE " + " AND ".join(conditions)

        query = f"""
            SELECT status, COUNT(*) as count
            FROM {self.TABLE_NAME}
            {where_clause}
            GROUP BY status
        """

        rows = self.db.fetchall(query, tuple(params) if params else None)

        stats = HDStatistics()
        for row in rows:
            status, count = row[0], row[1]
            stats.total += count

            if status == HDStatus.WAITING.value:
                stats.waiting = count
            elif status == HDStatus.DOWNLOADED.value:
                stats.downloaded = count
            elif status == HDStatus.PROCESSED.value:
                stats.processed = count
            elif status == HDStatus.FAILED.value:
                stats.failed = count
            elif status == HDStatus.TOO_LATE.value:
                stats.too_late = count
            elif status == HDStatus.MISSING.value:
                stats.missing = count

        return stats

    def get_station_statistics(
        self,
        station_id: str,
        start_date: GNSSDate | None = None,
        end_date: GNSSDate | None = None,
    ) -> HDStatistics:
        """Get statistics for a specific station.

        Args:
            station_id: Station ID
            start_date: Optional start date filter
            end_date: Optional end date filter

        Returns:
            HDStatistics object
        """
        station_id = station_id.lower()
        conditions = ["station_id = ?"]
        params: list[Any] = [station_id]

        if start_date:
            conditions.append("mjd >= ?")
            params.append(start_date.mjd)
        if end_date:
            conditions.append("mjd <= ?")
            params.append(end_date.mjd)

        where_clause = "WHERE " + " AND ".join(conditions)

        query = f"""
            SELECT status, COUNT(*) as count
            FROM {self.TABLE_NAME}
            {where_clause}
            GROUP BY status
        """

        rows = self.db.fetchall(query, tuple(params))

        stats = HDStatistics()
        for row in rows:
            status, count = row[0], row[1]
            stats.total += count

            if status == HDStatus.WAITING.value:
                stats.waiting = count
            elif status == HDStatus.DOWNLOADED.value:
                stats.downloaded = count
            elif status == HDStatus.PROCESSED.value:
                stats.processed = count
            elif status == HDStatus.FAILED.value:
                stats.failed = count
            elif status == HDStatus.TOO_LATE.value:
                stats.too_late = count
            elif status == HDStatus.MISSING.value:
                stats.missing = count

        return stats

    def get_entries_for_processing(
        self,
        date: GNSSDate,
        hour: int | None = None,
        station_ids: list[str] | None = None,
    ) -> list[HDEntry]:
        """Get entries ready for processing.

        Returns downloaded entries that have not yet been processed.

        Args:
            date: Date to process
            hour: Optional specific hour
            station_ids: Optional list of stations to filter

        Returns:
            List of HDEntry objects
        """
        conditions = ["status = ?", "year = ?", "doy = ?"]
        params: list[Any] = [HDStatus.DOWNLOADED.value, date.year, date.doy]

        if hour is not None:
            conditions.append("hour = ?")
            params.append(hour)

        if station_ids:
            placeholders = ",".join("?" * len(station_ids))
            conditions.append(f"station_id IN ({placeholders})")
            params.extend([s.lower() for s in station_ids])

        where_clause = "WHERE " + " AND ".join(conditions)

        query = f"""
            SELECT station_id, year, doy, hour, mjd, status,
                   rinex_file, provider, file_size, download_time,
                   created_at, updated_at
            FROM {self.TABLE_NAME}
            {where_clause}
            ORDER BY station_id, hour
        """

        rows = self.db.fetchall(query, tuple(params))

        return [
            HDEntry(
                station_id=row[0],
                year=row[1],
                doy=row[2],
                hour=row[3],
                mjd=row[4],
                status=HDStatus(row[5]),
                rinex_file=row[6],
                provider=row[7],
                file_size=row[8],
                download_time=row[9],
                created_at=row[10],
                updated_at=row[11],
            )
            for row in rows
        ]

    def cleanup_old_entries(self, days_to_keep: int = 90) -> int:
        """Remove old entries to prevent table bloat.

        Args:
            days_to_keep: Number of days of data to retain

        Returns:
            Number of rows deleted
        """
        cutoff_date = GNSSDate.now().add_days(-days_to_keep)
        cutoff_mjd = cutoff_date.mjd

        # Count rows to delete
        count_row = self.db.fetchone(
            f"SELECT COUNT(*) FROM {self.TABLE_NAME} WHERE mjd < ?",
            (cutoff_mjd,),
        )
        count = count_row[0] if count_row else 0

        self.db.execute(
            f"DELETE FROM {self.TABLE_NAME} WHERE mjd < ?",
            (cutoff_mjd,),
        )

        return count

    def bulk_insert(self, entries: list[HDEntry]) -> int:
        """Bulk insert multiple entries efficiently.

        Args:
            entries: List of HDEntry objects

        Returns:
            Number of entries inserted
        """
        if not entries:
            return 0

        inserted = 0

        # Use transaction for efficiency
        with self.db.transaction():
            for entry in entries:
                try:
                    self.db.execute(
                        f"""
                        INSERT INTO {self.TABLE_NAME}
                        (station_id, year, doy, hour, mjd, status, rinex_file, provider, file_size)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            entry.station_id.lower(),
                            entry.year,
                            entry.doy,
                            entry.hour,
                            entry.mjd,
                            entry.status.value,
                            entry.rinex_file,
                            entry.provider,
                            entry.file_size,
                        ),
                    )
                    inserted += 1
                except Exception:
                    # Skip duplicates
                    pass

        return inserted
