"""
Meteorological data database operations.

Replaces Perl call_MET_maintain.pl and MET-related functions from DB.pm.

Manages hourly meteorological data tracking for ZTD to IWV conversion.
MET data provides temperature and pressure values needed to convert
Zenith Total Delay to Integrated Water Vapor.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING

from pygnss_rt.core.exceptions import DatabaseError
from pygnss_rt.utils.dates import GNSSDate, hour_to_alpha, alpha_to_hour

if TYPE_CHECKING:
    from pygnss_rt.database.connection import DatabaseManager


# Status constants (matching Perl implementation)
STATUS_WAITING = "Waiting"
STATUS_ON_TIME = "On Time"
STATUS_TOO_LATE = "Too Late"


class MetManager:
    """Manages hourly meteorological data records in the database.

    Tracks the status of hourly MET files needed for ZTD to IWV conversion.
    Files progress through states: Waiting -> On Time/X hours late -> Too Late

    Replaces Perl MET table management from DB.pm:
    - add_met_table()
    - maintain_met()
    - fill_met_gap()
    - set_met_too_late_files()
    - get_met_list()
    - update_met_table()
    """

    TABLE_NAME = "hourly_met"

    def __init__(self, db: "DatabaseManager"):
        """Initialize MET manager.

        Args:
            db: Database manager instance
        """
        self.db = db

    def table_exists(self) -> bool:
        """Check if MET table exists."""
        row = self.db.fetchone(
            f"SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '{self.TABLE_NAME}'"
        )
        return row is not None and row[0] > 0

    def create_table(self) -> None:
        """Create the hourly MET tracking table.

        Matches Perl add_met_table() structure:
        - year: 4-char year
        - doy: Day of year (1-366)
        - hour: Alpha character a-x (0-23)
        - mjd: Modified Julian Date
        - status: Waiting, On Time, X hours late, Too Late
        """
        self.db.execute(f"""
            CREATE TABLE IF NOT EXISTS {self.TABLE_NAME} (
                year VARCHAR(4) NOT NULL,
                doy INTEGER NOT NULL,
                hour VARCHAR(1) NOT NULL,
                mjd DOUBLE NOT NULL,
                status VARCHAR(30) DEFAULT '{STATUS_WAITING}',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (year, doy, hour)
            )
        """)

        # Create index on MJD for efficient date range queries
        self.db.execute(
            f"CREATE INDEX IF NOT EXISTS idx_met_mjd ON {self.TABLE_NAME}(mjd)"
        )
        self.db.execute(
            f"CREATE INDEX IF NOT EXISTS idx_met_status ON {self.TABLE_NAME}(status)"
        )

    def ensure_table(self) -> None:
        """Ensure the MET table exists, creating if needed."""
        if not self.table_exists():
            self.create_table()

    def maintain(self, reference_date: GNSSDate | None = None) -> int:
        """Maintain the MET table by adding new hourly entries.

        Adds a new row for the current hour if it doesn't exist.
        This is typically called every hour by cron.

        Replaces Perl maintain_met().

        Args:
            reference_date: Reference date/time (defaults to now)

        Returns:
            Number of new rows added
        """
        if reference_date is None:
            reference_date = GNSSDate.now()

        year = str(reference_date.year)
        doy = reference_date.doy
        hour_alpha = reference_date.hour_alpha
        mjd = reference_date.mjd

        # Check if entry already exists
        existing = self.db.fetchone(
            f"SELECT 1 FROM {self.TABLE_NAME} WHERE year = ? AND doy = ? AND hour = ?",
            (year, doy, hour_alpha),
        )

        if existing:
            return 0

        # Insert new entry
        self.db.execute(
            f"""
            INSERT INTO {self.TABLE_NAME} (year, doy, hour, mjd, status)
            VALUES (?, ?, ?, ?, ?)
            """,
            (year, doy, hour_alpha, mjd, STATUS_WAITING),
        )

        return 1

    def fill_gap(
        self,
        late_day: int = 3,
        late_hour: int = 1,
        reference_date: GNSSDate | None = None,
    ) -> int:
        """Fill gaps in the MET table from interruptions.

        If cron was interrupted, this fills in missing hourly entries
        between the last entry and (now - latency).

        Replaces Perl fill_met_gap().

        Args:
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

        # Get the latest entry in the table
        row = self.db.fetchone(
            f"SELECT MAX(mjd) FROM {self.TABLE_NAME}"
        )
        last_mjd = row[0] if row and row[0] else None

        if last_mjd is None:
            # Table is empty, start from cutoff
            last_mjd = cutoff_mjd - (24 / 24.0)  # Go back one day

        added = 0

        # Fill in hourly entries from last_mjd to cutoff_mjd
        current_mjd = last_mjd + (1 / 24.0)  # Start one hour after last entry

        while current_mjd <= cutoff_mjd:
            current_date = GNSSDate.from_mjd(current_mjd)
            year = str(current_date.year)
            doy = current_date.doy
            hour_alpha = current_date.hour_alpha

            # Check if exists
            existing = self.db.fetchone(
                f"SELECT 1 FROM {self.TABLE_NAME} WHERE year = ? AND doy = ? AND hour = ?",
                (year, doy, hour_alpha),
            )

            if not existing:
                self.db.execute(
                    f"""
                    INSERT INTO {self.TABLE_NAME} (year, doy, hour, mjd, status)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (year, doy, hour_alpha, current_mjd, STATUS_WAITING),
                )
                added += 1

            current_mjd += 1 / 24.0  # Next hour

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

        Replaces Perl set_met_too_late_files().

        Args:
            late_day: Days threshold for "too late"
            late_hour: Hours threshold for "too late"
            reference_date: Reference date/time (defaults to now)

        Returns:
            Number of rows updated
        """
        if reference_date is None:
            reference_date = GNSSDate.now()

        # Calculate cutoff MJD
        latency_hours = late_day * 24 + late_hour
        cutoff_date = reference_date.add_hours(-latency_hours)
        cutoff_mjd = cutoff_date.mjd

        # Update all waiting entries older than cutoff
        result = self.db.execute(
            f"""
            UPDATE {self.TABLE_NAME}
            SET status = ?, updated_at = CURRENT_TIMESTAMP
            WHERE status = ? AND mjd < ?
            """,
            (STATUS_TOO_LATE, STATUS_WAITING, cutoff_mjd),
        )

        # DuckDB returns rowcount via fetchone after UPDATE
        return result.fetchone()[0] if result else 0

    def get_waiting_list(self, compression: str = "") -> list[dict]:
        """Get list of files waiting for download.

        Returns entries with 'Waiting' status that need to be downloaded.

        Replaces Perl get_met_list({status=>'Waiting'}).

        Args:
            compression: File compression suffix (e.g., '.Z', '.gz')

        Returns:
            List of dicts with file information for downloading
        """
        rows = self.db.fetchall(
            f"""
            SELECT year, doy, hour, mjd
            FROM {self.TABLE_NAME}
            WHERE status = ?
            ORDER BY mjd
            """,
            (STATUS_WAITING,),
        )

        result = []
        for row in rows:
            year, doy, hour_alpha, mjd = row
            hour_int = alpha_to_hour(hour_alpha)

            # Build filename in standard format: SSSSDDDHH.MET
            # Where SSSS=station (filled in later), DDD=doy, HH=hour
            filename = f"{int(doy):03d}{hour_int:02d}.met{compression}"

            result.append({
                "year": year,
                "doy": doy,
                "hour": hour_alpha,
                "hour_int": hour_int,
                "mjd": mjd,
                "filename": filename,
                "yydoy": f"{int(year) % 100:02d}{int(doy):03d}",
            })

        return result

    def update_status(
        self,
        downloaded: list[dict],
        late_day: int = 3,
        late_hour: int = 0,
        reference_date: GNSSDate | None = None,
    ) -> int:
        """Update status after download.

        Marks successfully downloaded files as 'On Time' or 'X hours late'
        based on how long they waited.

        Replaces Perl update_met_table().

        Args:
            downloaded: List of successfully downloaded file dicts
            late_day: Days threshold for late calculation
            late_hour: Hours threshold for late calculation
            reference_date: Reference date/time (defaults to now)

        Returns:
            Number of rows updated
        """
        if reference_date is None:
            reference_date = GNSSDate.now()

        current_mjd = reference_date.mjd
        updated = 0

        for item in downloaded:
            year = item.get("year")
            doy = item.get("doy")
            hour = item.get("hour")
            file_mjd = item.get("mjd", 0)

            if not all([year, doy, hour]):
                continue

            # Calculate how late the file is
            hours_late = int((current_mjd - file_mjd) * 24)

            if hours_late <= 0:
                status = STATUS_ON_TIME
            else:
                status = f"{hours_late} hours late"

            self.db.execute(
                f"""
                UPDATE {self.TABLE_NAME}
                SET status = ?, updated_at = CURRENT_TIMESTAMP
                WHERE year = ? AND doy = ? AND hour = ? AND status = ?
                """,
                (status, str(year), doy, hour, STATUS_WAITING),
            )
            updated += 1

        return updated

    def get_status_summary(self) -> dict:
        """Get summary of MET table status.

        Returns:
            Dict with counts for each status
        """
        rows = self.db.fetchall(
            f"""
            SELECT status, COUNT(*) as count
            FROM {self.TABLE_NAME}
            GROUP BY status
            ORDER BY status
            """
        )

        return {row[0]: row[1] for row in rows}

    def get_entries_by_date_range(
        self,
        start_date: GNSSDate,
        end_date: GNSSDate,
        status: str | None = None,
    ) -> list[dict]:
        """Get MET entries for a date range.

        Args:
            start_date: Start date
            end_date: End date
            status: Optional status filter

        Returns:
            List of entry dicts
        """
        query = f"""
            SELECT year, doy, hour, mjd, status, created_at, updated_at
            FROM {self.TABLE_NAME}
            WHERE mjd >= ? AND mjd <= ?
        """
        params: list = [start_date.mjd, end_date.mjd]

        if status:
            query += " AND status = ?"
            params.append(status)

        query += " ORDER BY mjd"

        rows = self.db.fetchall(query, tuple(params))

        return [
            {
                "year": row[0],
                "doy": row[1],
                "hour": row[2],
                "mjd": row[3],
                "status": row[4],
                "created_at": row[5],
                "updated_at": row[6],
            }
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

        result = self.db.execute(
            f"DELETE FROM {self.TABLE_NAME} WHERE mjd < ?",
            (cutoff_mjd,),
        )

        return result.fetchone()[0] if result else 0
