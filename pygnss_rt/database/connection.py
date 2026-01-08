"""
DuckDB database connection and schema management.

Replaces Perl DB.pm module.
"""

from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from typing import Any, Generator

import duckdb

from pygnss_rt.core.exceptions import DatabaseError


class DatabaseManager:
    """Manages DuckDB database connections and schema."""

    def __init__(self, db_path: Path | str, read_only: bool = False):
        """Initialize database manager.

        Args:
            db_path: Path to DuckDB database file
            read_only: Open in read-only mode
        """
        self.db_path = Path(db_path)
        self.read_only = read_only
        self._conn: duckdb.DuckDBPyConnection | None = None

    @property
    def conn(self) -> duckdb.DuckDBPyConnection:
        """Get database connection, creating if needed."""
        if self._conn is None:
            self._connect()
        return self._conn  # type: ignore

    def _connect(self) -> None:
        """Establish database connection."""
        try:
            # Create parent directory if needed
            if not self.read_only:
                self.db_path.parent.mkdir(parents=True, exist_ok=True)

            self._conn = duckdb.connect(
                str(self.db_path),
                read_only=self.read_only,
            )
        except Exception as e:
            raise DatabaseError(f"Failed to connect to database: {e}") from e

    def close(self) -> None:
        """Close database connection."""
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    def create_schema(self) -> None:
        """Create database schema."""
        # Products table - tracks downloaded GNSS products
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS products (
                id INTEGER PRIMARY KEY,
                product_type VARCHAR NOT NULL,
                provider VARCHAR NOT NULL,
                tier VARCHAR NOT NULL,
                mjd DOUBLE NOT NULL,
                gps_week INTEGER,
                day_of_week INTEGER,
                filename VARCHAR NOT NULL,
                local_path VARCHAR,
                file_size BIGINT,
                checksum VARCHAR,
                download_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(product_type, provider, tier, mjd)
            )
        """)

        # Hourly data tracking
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS hourly_data (
                id INTEGER PRIMARY KEY,
                station_id VARCHAR NOT NULL,
                mjd DOUBLE NOT NULL,
                hour INTEGER NOT NULL,
                rinex_file VARCHAR,
                status VARCHAR DEFAULT 'pending',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(station_id, mjd, hour)
            )
        """)

        # Stations table
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS stations (
                id INTEGER PRIMARY KEY,
                station_id VARCHAR NOT NULL UNIQUE,
                name VARCHAR,
                network VARCHAR,
                latitude DOUBLE,
                longitude DOUBLE,
                height DOUBLE,
                use_nrt BOOLEAN DEFAULT TRUE,
                active BOOLEAN DEFAULT TRUE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Processing runs
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS processing_runs (
                id INTEGER PRIMARY KEY,
                run_type VARCHAR NOT NULL,
                start_mjd DOUBLE NOT NULL,
                end_mjd DOUBLE NOT NULL,
                status VARCHAR NOT NULL,
                start_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                end_time TIMESTAMP,
                stations_processed INTEGER,
                errors TEXT
            )
        """)

        # ZTD results
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS ztd_results (
                id INTEGER PRIMARY KEY,
                station_id VARCHAR NOT NULL,
                mjd DOUBLE NOT NULL,
                ztd DOUBLE NOT NULL,
                ztd_sigma DOUBLE,
                zhd DOUBLE,
                zwd DOUBLE,
                iwv DOUBLE,
                iwv_sigma DOUBLE,
                temperature DOUBLE,
                pressure DOUBLE,
                processing_run_id INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(station_id, mjd)
            )
        """)

        # Create indexes
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_products_mjd ON products(mjd)"
        )
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_hourly_mjd ON hourly_data(mjd)"
        )
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_ztd_mjd ON ztd_results(mjd)"
        )
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_ztd_station ON ztd_results(station_id)"
        )

    def execute(self, query: str, params: tuple[Any, ...] | None = None) -> Any:
        """Execute a query.

        Args:
            query: SQL query
            params: Query parameters

        Returns:
            Query result
        """
        if params:
            return self.conn.execute(query, params)
        return self.conn.execute(query)

    def fetchone(self, query: str, params: tuple[Any, ...] | None = None) -> Any:
        """Execute query and fetch one row."""
        result = self.execute(query, params)
        return result.fetchone()

    def fetchall(self, query: str, params: tuple[Any, ...] | None = None) -> list[Any]:
        """Execute query and fetch all rows."""
        result = self.execute(query, params)
        return result.fetchall()

    @contextmanager
    def transaction(self) -> Generator[None, None, None]:
        """Context manager for transactions."""
        try:
            self.conn.execute("BEGIN TRANSACTION")
            yield
            self.conn.execute("COMMIT")
        except Exception:
            self.conn.execute("ROLLBACK")
            raise


def init_db(
    db_path: Path | str,
    create_schema: bool = True,
) -> DatabaseManager:
    """Initialize database.

    Args:
        db_path: Path to database file
        create_schema: Whether to create schema

    Returns:
        DatabaseManager instance
    """
    db = DatabaseManager(db_path)
    if create_schema:
        db.create_schema()
    return db
