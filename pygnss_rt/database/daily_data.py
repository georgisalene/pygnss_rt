"""
Daily Data (SD) database operations.

Replaces Perl call_SD_maintain.pl and SD-related functions from DB.pm.

Manages daily RINEX observation data tracking for daily PPP processing.
SD data tracks the status of daily RINEX files across multiple networks.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import TYPE_CHECKING, Any

from pygnss_rt.core.exceptions import DatabaseError
from pygnss_rt.utils.dates import GNSSDate

if TYPE_CHECKING:
    from pygnss_rt.database.connection import DatabaseManager


class SDStatus(str, Enum):
    """Status values for daily data records."""

    WAITING = "Waiting"
    ON_TIME = "On Time"
    TOO_LATE = "Too Late"
    DOWNLOADED = "Downloaded"
    PROCESSED = "Processed"
    FAILED = "Failed"
    MISSING = "Missing"
    LATE_1 = "1 day late"
    LATE_2 = "2 days late"
    LATE_3 = "3 days late"


@dataclass
class SDEntry:
    """Daily data entry."""

    station_id: str
    network: str
    year: int
    doy: int
    mjd: float
    status: SDStatus = SDStatus.WAITING
    rinex_file: str | None = None
    provider: str | None = None
    download_time: datetime | None = None
    file_size: int | None = None
    processing_run_id: int | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None

    @property
    def yydoy(self) -> str:
        """Get 2-digit year + 3-digit DOY string."""
        return f"{self.year % 100:02d}{self.doy:03d}"

    @property
    def gps_week(self) -> int:
        """Calculate GPS week from MJD."""
        return int((self.mjd - 44244) / 7)

    @property
    def day_of_week(self) -> int:
        """Calculate day of GPS week (0-6)."""
        return int(self.mjd - 44244) % 7

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for database insertion."""
        return {
            "station_id": self.station_id,
            "network": self.network,
            "year": self.year,
            "doy": self.doy,
            "mjd": self.mjd,
            "status": self.status.value,
            "rinex_file": self.rinex_file,
            "provider": self.provider,
            "file_size": self.file_size,
            "processing_run_id": self.processing_run_id,
        }


@dataclass
class SDStatistics:
    """Statistics for daily data."""

    total: int = 0
    waiting: int = 0
    downloaded: int = 0
    processed: int = 0
    failed: int = 0
    too_late: int = 0
    missing: int = 0
    late_counts: dict[int, int] = field(default_factory=dict)

    @property
    def success_rate(self) -> float:
        """Calculate success rate as percentage."""
        if self.total == 0:
            return 0.0
        return (self.downloaded + self.processed) / self.total * 100


class DailyDataManager:
    """Manages daily RINEX observation data tracking.

    Tracks the status of daily RINEX files across multiple stations and networks.
    Files progress through states: Waiting -> Downloaded -> Processed

    Replaces Perl SD table management from DB.pm:
    - add_sd_table()
    - maintain_sd()
    - fill_sd_gap()
    - set_sd_too_late_files()
    - get_sd_list()
    - update_sd_table()
    """

    TABLE_NAME = "daily_data"

    def __init__(self, db: "DatabaseManager"):
        """Initialize SD manager.

        Args:
            db: Database manager instance
        """
        self.db = db

    def table_exists(self) -> bool:
        """Check if SD table exists."""
        row = self.db.fetchone(
            f"SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '{self.TABLE_NAME}'"
        )
        return row is not None and row[0] > 0

    def create_table(self) -> None:
        """Create the daily data tracking table.

        Extended schema compared to original Perl implementation:
        - Adds network tracking
        - Adds provider tracking
        - Adds processing run linkage
        """
        self.db.execute(f"""
            CREATE TABLE IF NOT EXISTS {self.TABLE_NAME} (
                station_id VARCHAR NOT NULL,
                network VARCHAR NOT NULL,
                year INTEGER NOT NULL,
                doy INTEGER NOT NULL,
                mjd DOUBLE NOT NULL,
                status VARCHAR(30) DEFAULT '{SDStatus.WAITING.value}',
                rinex_file VARCHAR,
                provider VARCHAR,
                file_size BIGINT,
                download_time TIMESTAMP,
                processing_run_id INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (station_id, network, year, doy)
            )
        """)

        # Create indexes for efficient queries
        self.db.execute(
            f"CREATE INDEX IF NOT EXISTS idx_sd_mjd ON {self.TABLE_NAME}(mjd)"
        )
        self.db.execute(
            f"CREATE INDEX IF NOT EXISTS idx_sd_status ON {self.TABLE_NAME}(status)"
        )
        self.db.execute(
            f"CREATE INDEX IF NOT EXISTS idx_sd_station ON {self.TABLE_NAME}(station_id)"
        )
        self.db.execute(
            f"CREATE INDEX IF NOT EXISTS idx_sd_network ON {self.TABLE_NAME}(network)"
        )
        self.db.execute(
            f"CREATE INDEX IF NOT EXISTS idx_sd_date ON {self.TABLE_NAME}(year, doy)"
        )

    def ensure_table(self) -> None:
        """Ensure the SD table exists, creating if needed."""
        if not self.table_exists():
            self.create_table()

    def add_station_day(
        self,
        station_id: str,
        network: str,
        date: GNSSDate,
    ) -> bool:
        """Add a single station-day entry.

        Args:
            station_id: 4-character station ID
            network: Network identifier
            date: GNSS date

        Returns:
            True if entry was added, False if already exists
        """
        station_id = station_id.lower()

        # Check if exists
        existing = self.db.fetchone(
            f"""
            SELECT 1 FROM {self.TABLE_NAME}
            WHERE station_id = ? AND network = ? AND year = ? AND doy = ?
            """,
            (station_id, network, date.year, date.doy),
        )

        if existing:
            return False

        self.db.execute(
            f"""
            INSERT INTO {self.TABLE_NAME} (station_id, network, year, doy, mjd, status)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (station_id, network, date.year, date.doy, date.mjd, SDStatus.WAITING.value),
        )
        return True

    def maintain(
        self,
        station_ids: list[str],
        network: str,
        reference_date: GNSSDate | None = None,
    ) -> int:
        """Maintain the SD table by adding new daily entries for all stations.

        Adds new rows for the current day if they don't exist.
        This is typically called daily by cron.

        Replaces Perl maintain_sd().

        Args:
            station_ids: List of station IDs to maintain
            network: Network identifier
            reference_date: Reference date (defaults to today)

        Returns:
            Number of new rows added
        """
        if reference_date is None:
            reference_date = GNSSDate.now()

        added = 0

        for station_id in station_ids:
            if self.add_station_day(station_id, network, reference_date):
                added += 1

        return added

    def fill_gap(
        self,
        station_ids: list[str],
        network: str,
        late_days: int = 30,
        reference_date: GNSSDate | None = None,
    ) -> int:
        """Fill gaps in the SD table from interruptions.

        If cron was interrupted, this fills in missing daily entries
        between the last entry and (now - latency).

        Replaces Perl fill_sd_gap().

        Args:
            station_ids: List of station IDs to fill
            network: Network identifier
            late_days: Days to go back for gap filling (default 30)
            reference_date: Reference date (defaults to today)

        Returns:
            Number of rows added
        """
        if reference_date is None:
            reference_date = GNSSDate.now()

        # Calculate cutoff date
        cutoff_date = reference_date.add_days(-late_days)

        added = 0

        for station_id in station_ids:
            station_id = station_id.lower()

            # Get the latest entry for this station/network
            row = self.db.fetchone(
                f"""
                SELECT MAX(mjd) FROM {self.TABLE_NAME}
                WHERE station_id = ? AND network = ?
                """,
                (station_id, network),
            )
            last_mjd = row[0] if row and row[0] else None

            if last_mjd is None:
                # No entries for this station, start from cutoff
                last_mjd = cutoff_date.mjd - 1

            # Fill in daily entries from last_mjd to current date
            current_mjd = last_mjd + 1

            while current_mjd <= reference_date.mjd:
                current_date = GNSSDate.from_mjd(current_mjd)
                if self.add_station_day(station_id, network, current_date):
                    added += 1
                current_mjd += 1

        return added

    def set_too_late_files(
        self,
        late_days: int = 30,
        reference_date: GNSSDate | None = None,
    ) -> int:
        """Mark old waiting files as 'Too Late'.

        Files still in 'Waiting' status past the latency threshold
        are marked as 'Too Late' since the data is no longer useful.

        Replaces Perl set_sd_too_late_files().

        Args:
            late_days: Days threshold for "too late"
            reference_date: Reference date (defaults to today)

        Returns:
            Number of rows updated
        """
        if reference_date is None:
            reference_date = GNSSDate.now()

        cutoff_date = reference_date.add_days(-late_days)
        cutoff_mjd = cutoff_date.mjd

        # Count rows to be updated
        count_row = self.db.fetchone(
            f"""
            SELECT COUNT(*) FROM {self.TABLE_NAME}
            WHERE status = ? AND mjd < ?
            """,
            (SDStatus.WAITING.value, cutoff_mjd),
        )
        count = count_row[0] if count_row else 0

        # Update all waiting entries older than cutoff
        self.db.execute(
            f"""
            UPDATE {self.TABLE_NAME}
            SET status = ?, updated_at = CURRENT_TIMESTAMP
            WHERE status = ? AND mjd < ?
            """,
            (SDStatus.TOO_LATE.value, SDStatus.WAITING.value, cutoff_mjd),
        )

        return count

    def update_late_status(
        self,
        reference_date: GNSSDate | None = None,
    ) -> int:
        """Update waiting entries with "X days late" status.

        Updates entries that are waiting but past their expected
        download time to show how many days late they are.

        Args:
            reference_date: Reference date (defaults to today)

        Returns:
            Number of rows updated
        """
        if reference_date is None:
            reference_date = GNSSDate.now()

        current_mjd = reference_date.mjd
        updated = 0

        # Update entries 1, 2, 3 days late
        for days_late in [1, 2, 3]:
            late_status = f"{days_late} day{'s' if days_late > 1 else ''} late"
            min_mjd = current_mjd - days_late - 1
            max_mjd = current_mjd - days_late

            # Count rows to update
            count_row = self.db.fetchone(
                f"""
                SELECT COUNT(*) FROM {self.TABLE_NAME}
                WHERE status = ? AND mjd > ? AND mjd <= ?
                """,
                (SDStatus.WAITING.value, min_mjd, max_mjd),
            )
            count = count_row[0] if count_row else 0

            self.db.execute(
                f"""
                UPDATE {self.TABLE_NAME}
                SET status = ?, updated_at = CURRENT_TIMESTAMP
                WHERE status = ? AND mjd > ? AND mjd <= ?
                """,
                (late_status, SDStatus.WAITING.value, min_mjd, max_mjd),
            )
            updated += count

        return updated

    def get_waiting_list(
        self,
        network: str | None = None,
        station_ids: list[str] | None = None,
        limit: int | None = None,
    ) -> list[SDEntry]:
        """Get list of files waiting for download.

        Returns entries with 'Waiting' status that need to be downloaded.

        Replaces Perl get_sd_list({status=>'Waiting'}).

        Args:
            network: Optional network filter
            station_ids: Optional list of stations to filter
            limit: Optional limit on number of results

        Returns:
            List of SDEntry objects
        """
        conditions = ["status = ?"]
        params: list[Any] = [SDStatus.WAITING.value]

        if network:
            conditions.append("network = ?")
            params.append(network)

        if station_ids:
            placeholders = ",".join("?" * len(station_ids))
            conditions.append(f"station_id IN ({placeholders})")
            params.extend([s.lower() for s in station_ids])

        where_clause = "WHERE " + " AND ".join(conditions)

        query = f"""
            SELECT station_id, network, year, doy, mjd, status,
                   rinex_file, provider, file_size, download_time,
                   processing_run_id, created_at, updated_at
            FROM {self.TABLE_NAME}
            {where_clause}
            ORDER BY mjd
        """

        if limit:
            query += f" LIMIT {limit}"

        rows = self.db.fetchall(query, tuple(params))

        return [
            SDEntry(
                station_id=row[0],
                network=row[1],
                year=row[2],
                doy=row[3],
                mjd=row[4],
                status=SDStatus(row[5]) if row[5] in [s.value for s in SDStatus] else SDStatus.WAITING,
                rinex_file=row[6],
                provider=row[7],
                file_size=row[8],
                download_time=row[9],
                processing_run_id=row[10],
                created_at=row[11],
                updated_at=row[12],
            )
            for row in rows
        ]

    def get_entries_by_network(
        self,
        network: str,
        date: GNSSDate,
    ) -> list[SDEntry]:
        """Get all entries for a network on a specific date.

        Args:
            network: Network identifier
            date: Date to query

        Returns:
            List of SDEntry objects
        """
        rows = self.db.fetchall(
            f"""
            SELECT station_id, network, year, doy, mjd, status,
                   rinex_file, provider, file_size, download_time,
                   processing_run_id, created_at, updated_at
            FROM {self.TABLE_NAME}
            WHERE network = ? AND year = ? AND doy = ?
            ORDER BY station_id
            """,
            (network, date.year, date.doy),
        )

        return [
            SDEntry(
                station_id=row[0],
                network=row[1],
                year=row[2],
                doy=row[3],
                mjd=row[4],
                status=SDStatus(row[5]) if row[5] in [s.value for s in SDStatus] else SDStatus.WAITING,
                rinex_file=row[6],
                provider=row[7],
                file_size=row[8],
                download_time=row[9],
                processing_run_id=row[10],
                created_at=row[11],
                updated_at=row[12],
            )
            for row in rows
        ]

    def update_downloaded(
        self,
        station_id: str,
        network: str,
        year: int,
        doy: int,
        rinex_file: str,
        provider: str | None = None,
        file_size: int | None = None,
    ) -> bool:
        """Mark an entry as downloaded.

        Args:
            station_id: Station ID
            network: Network identifier
            year: Year
            doy: Day of year
            rinex_file: Downloaded RINEX filename
            provider: Download provider
            file_size: File size in bytes

        Returns:
            True if entry was updated
        """
        station_id = station_id.lower()

        self.db.execute(
            f"""
            UPDATE {self.TABLE_NAME}
            SET status = ?,
                rinex_file = ?,
                provider = ?,
                file_size = ?,
                download_time = CURRENT_TIMESTAMP,
                updated_at = CURRENT_TIMESTAMP
            WHERE station_id = ? AND network = ? AND year = ? AND doy = ?
            """,
            (
                SDStatus.DOWNLOADED.value,
                rinex_file,
                provider,
                file_size,
                station_id,
                network,
                year,
                doy,
            ),
        )
        return True

    def update_processed(
        self,
        station_id: str,
        network: str,
        year: int,
        doy: int,
        processing_run_id: int | None = None,
    ) -> bool:
        """Mark an entry as processed.

        Args:
            station_id: Station ID
            network: Network identifier
            year: Year
            doy: Day of year
            processing_run_id: Optional processing run ID

        Returns:
            True if entry was updated
        """
        station_id = station_id.lower()

        self.db.execute(
            f"""
            UPDATE {self.TABLE_NAME}
            SET status = ?,
                processing_run_id = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE station_id = ? AND network = ? AND year = ? AND doy = ?
            """,
            (SDStatus.PROCESSED.value, processing_run_id, station_id, network, year, doy),
        )
        return True

    def update_failed(
        self,
        station_id: str,
        network: str,
        year: int,
        doy: int,
    ) -> bool:
        """Mark an entry as failed.

        Args:
            station_id: Station ID
            network: Network identifier
            year: Year
            doy: Day of year

        Returns:
            True if entry was updated
        """
        station_id = station_id.lower()

        self.db.execute(
            f"""
            UPDATE {self.TABLE_NAME}
            SET status = ?, updated_at = CURRENT_TIMESTAMP
            WHERE station_id = ? AND network = ? AND year = ? AND doy = ?
            """,
            (SDStatus.FAILED.value, station_id, network, year, doy),
        )
        return True

    def get_statistics(
        self,
        network: str | None = None,
        start_date: GNSSDate | None = None,
        end_date: GNSSDate | None = None,
    ) -> SDStatistics:
        """Get statistics for daily data.

        Args:
            network: Optional network filter
            start_date: Optional start date filter
            end_date: Optional end date filter

        Returns:
            SDStatistics object
        """
        conditions = []
        params: list[Any] = []

        if network:
            conditions.append("network = ?")
            params.append(network)
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

        stats = SDStatistics()
        for row in rows:
            status, count = row[0], row[1]
            stats.total += count

            if status == SDStatus.WAITING.value:
                stats.waiting = count
            elif status == SDStatus.DOWNLOADED.value:
                stats.downloaded = count
            elif status == SDStatus.PROCESSED.value:
                stats.processed = count
            elif status == SDStatus.FAILED.value:
                stats.failed = count
            elif status == SDStatus.TOO_LATE.value:
                stats.too_late = count
            elif status == SDStatus.MISSING.value:
                stats.missing = count
            elif "day" in status and "late" in status:
                # Parse "X days late" status
                try:
                    days = int(status.split()[0])
                    stats.late_counts[days] = count
                except ValueError:
                    pass

        return stats

    def get_network_summary(self, date: GNSSDate) -> dict[str, SDStatistics]:
        """Get statistics summary by network for a specific date.

        Args:
            date: Date to query

        Returns:
            Dict mapping network to SDStatistics
        """
        rows = self.db.fetchall(
            f"""
            SELECT network, status, COUNT(*) as count
            FROM {self.TABLE_NAME}
            WHERE year = ? AND doy = ?
            GROUP BY network, status
            """,
            (date.year, date.doy),
        )

        summary: dict[str, SDStatistics] = {}

        for row in rows:
            network, status, count = row[0], row[1], row[2]

            if network not in summary:
                summary[network] = SDStatistics()

            stats = summary[network]
            stats.total += count

            if status == SDStatus.WAITING.value:
                stats.waiting = count
            elif status == SDStatus.DOWNLOADED.value:
                stats.downloaded = count
            elif status == SDStatus.PROCESSED.value:
                stats.processed = count
            elif status == SDStatus.FAILED.value:
                stats.failed = count
            elif status == SDStatus.TOO_LATE.value:
                stats.too_late = count

        return summary

    def cleanup_old_entries(self, days_to_keep: int = 180) -> int:
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

    def get_entries_for_processing(
        self,
        network: str,
        date: GNSSDate,
    ) -> list[SDEntry]:
        """Get downloaded entries ready for processing.

        Args:
            network: Network identifier
            date: Date to process

        Returns:
            List of SDEntry objects with Downloaded status
        """
        rows = self.db.fetchall(
            f"""
            SELECT station_id, network, year, doy, mjd, status,
                   rinex_file, provider, file_size, download_time,
                   processing_run_id, created_at, updated_at
            FROM {self.TABLE_NAME}
            WHERE network = ? AND year = ? AND doy = ? AND status = ?
            ORDER BY station_id
            """,
            (network, date.year, date.doy, SDStatus.DOWNLOADED.value),
        )

        return [
            SDEntry(
                station_id=row[0],
                network=row[1],
                year=row[2],
                doy=row[3],
                mjd=row[4],
                status=SDStatus.DOWNLOADED,
                rinex_file=row[6],
                provider=row[7],
                file_size=row[8],
                download_time=row[9],
                processing_run_id=row[10],
                created_at=row[11],
                updated_at=row[12],
            )
            for row in rows
        ]

    def bulk_insert(self, entries: list[SDEntry]) -> int:
        """Bulk insert multiple entries efficiently.

        Args:
            entries: List of SDEntry objects

        Returns:
            Number of entries inserted
        """
        if not entries:
            return 0

        inserted = 0

        with self.db.transaction():
            for entry in entries:
                try:
                    self.db.execute(
                        f"""
                        INSERT INTO {self.TABLE_NAME}
                        (station_id, network, year, doy, mjd, status, rinex_file, provider, file_size)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            entry.station_id.lower(),
                            entry.network,
                            entry.year,
                            entry.doy,
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
