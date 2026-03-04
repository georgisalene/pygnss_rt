"""
Database Schema and Connection Management

This module contains the base QualityDatabaseSchema class with connection
management and table creation methods.
"""

import os
from pathlib import Path

try:
    import duckdb
except ImportError:
    raise ImportError("Please install duckdb: pip install duckdb")


class QualityDatabaseSchema:
    """Base database connection and schema management"""

    def __init__(self, db_path: str = None):
        """
        Initialize database connection.

        Args:
            db_path: Path to DuckDB file. Defaults to pygnss_rt database.
        """
        if db_path is None:
            # Use existing pygnss_rt database
            db_path = os.environ.get(
                "PYGNSS_DB_PATH",
                str(Path(__file__).parent.parent / "data" / "pygnss_rt.duckdb")
            )

        self.db_path = db_path
        self._conn = None
        self._read_only = False

    def connect(self, read_only: bool = False):
        """Establish database connection

        Args:
            read_only: If True, open in read-only mode for concurrent access
        """
        if self._conn is None:
            # Create parent directory if needed
            Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
            self._read_only = read_only
            self._conn = duckdb.connect(self.db_path, read_only=read_only)
        return self._conn

    @property
    def conn(self):
        """Get the database connection (creates if needed)"""
        return self.connect()

    def close(self):
        """Close database connection"""
        if self._conn:
            self._conn.close()
            self._conn = None

    def create_tables(self):
        """Create quality monitoring tables if they don't exist"""
        conn = self.connect()

        # PPP Solutions table
        conn.execute("""
            CREATE TABLE IF NOT EXISTS ppp_solutions (
                station_id VARCHAR NOT NULL,
                year INTEGER NOT NULL,
                doy INTEGER NOT NULL,
                mjd DOUBLE NOT NULL,
                x DOUBLE NOT NULL,
                y DOUBLE NOT NULL,
                z DOUBLE NOT NULL,
                x_rms DOUBLE NOT NULL,
                y_rms DOUBLE NOT NULL,
                z_rms DOUBLE NOT NULL,
                lat DOUBLE,
                lon DOUBLE,
                height DOUBLE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY(station_id, year, doy)
            )
        """)

        # Processing Statistics table
        conn.execute("""
            CREATE TABLE IF NOT EXISTS processing_stats (
                station_id VARCHAR NOT NULL,
                year INTEGER NOT NULL,
                doy INTEGER NOT NULL,
                mjd DOUBLE NOT NULL,
                rms_unit_weight DOUBLE NOT NULL,
                chi2_dof DOUBLE,
                num_observations INTEGER NOT NULL,
                num_parameters INTEGER,
                dof INTEGER,
                num_satellites INTEGER,
                ambiguity_fixed INTEGER,
                ambiguity_total INTEGER,
                ambiguity_rate DOUBLE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY(station_id, year, doy)
            )
        """)

        # Hourly ZTD table
        conn.execute("""
            CREATE TABLE IF NOT EXISTS ztd_hourly (
                station_id VARCHAR NOT NULL,
                year INTEGER NOT NULL,
                doy INTEGER NOT NULL,
                hour INTEGER NOT NULL,
                mjd DOUBLE NOT NULL,
                ztd DOUBLE NOT NULL,
                ztd_rms DOUBLE NOT NULL,
                grad_n DOUBLE,
                grad_n_rms DOUBLE,
                grad_e DOUBLE,
                grad_e_rms DOUBLE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY(station_id, year, doy, hour)
            )
        """)

        # Ambiguity Resolution table
        conn.execute("""
            CREATE TABLE IF NOT EXISTS ambiguity_resolution (
                station_id VARCHAR NOT NULL,
                year INTEGER NOT NULL,
                doy INTEGER NOT NULL,
                receiver VARCHAR,
                wl_gps DOUBLE,
                wl_gal DOUBLE,
                wl_combined DOUBLE,
                nl_gps DOUBLE,
                nl_gal DOUBLE,
                nl_combined DOUBLE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY(station_id, year, doy)
            )
        """)

        # Satellite Tracking table
        conn.execute("""
            CREATE TABLE IF NOT EXISTS satellite_tracking (
                year INTEGER NOT NULL,
                doy INTEGER NOT NULL,
                prn INTEGER NOT NULL,
                constellation VARCHAR NOT NULL,
                obs_percent DOUBLE NOT NULL,
                obs_count INTEGER NOT NULL,
                rms DOUBLE NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY(year, doy, prn)
            )
        """)

        # Satellite Ambiguity PRN table (per-satellite ambiguity resolution)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS satellite_ambiguity_prn (
                year INTEGER NOT NULL,
                doy INTEGER NOT NULL,
                prn INTEGER NOT NULL,
                constellation VARCHAR NOT NULL,
                amb_total INTEGER NOT NULL,
                l1l2_solved INTEGER NOT NULL,
                l1l2_cluster DOUBLE,
                l1l2_rel DOUBLE NOT NULL,
                l5_solved INTEGER NOT NULL,
                l5_cluster DOUBLE,
                l5_rel DOUBLE NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY(year, doy, prn)
            )
        """)

        # Station Residuals table (per-station, per-satellite residuals)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS station_residuals (
                year INTEGER NOT NULL,
                doy INTEGER NOT NULL,
                station_id VARCHAR NOT NULL,
                prn INTEGER NOT NULL,
                constellation VARCHAR NOT NULL,
                rms DOUBLE NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY(year, doy, station_id, prn)
            )
        """)

        # Data Availability table (satellite observation statistics)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS data_availability (
                year INTEGER NOT NULL,
                doy INTEGER NOT NULL,
                prn INTEGER NOT NULL,
                constellation VARCHAR NOT NULL,
                obs_pct_before DOUBLE NOT NULL,
                obs_pct_after DOUBLE NOT NULL,
                obs_count_before INTEGER NOT NULL,
                obs_count_after INTEGER NOT NULL,
                obs_rejected INTEGER NOT NULL,
                rejection_rate DOUBLE NOT NULL,
                rms_before DOUBLE NOT NULL,
                rms_after DOUBLE NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY(year, doy, prn)
            )
        """)

        # Receiver Clock table (from RINEX clock files)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS receiver_clocks (
                station_id VARCHAR NOT NULL,
                year INTEGER NOT NULL,
                doy INTEGER NOT NULL,
                epoch DOUBLE NOT NULL,
                clock_offset DOUBLE NOT NULL,
                clock_sigma DOUBLE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY(station_id, year, doy, epoch)
            )
        """)

        # Create indexes
        conn.execute("CREATE INDEX IF NOT EXISTS idx_ppp_station ON ppp_solutions(station_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_ppp_mjd ON ppp_solutions(mjd)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_stats_station ON processing_stats(station_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_ztd_station ON ztd_hourly(station_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_ztd_mjd ON ztd_hourly(mjd)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_amb_station ON ambiguity_resolution(station_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_sat_constellation ON satellite_tracking(constellation)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_sat_amb_prn_constellation ON satellite_ambiguity_prn(constellation)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_station_res_station ON station_residuals(station_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_station_res_constellation ON station_residuals(constellation)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_data_avail_constellation ON data_availability(constellation)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_receiver_clocks_station ON receiver_clocks(station_id)")

        # Station Data Completeness table (station-level data availability)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS station_data_completeness (
                station_id VARCHAR NOT NULL,
                year INTEGER NOT NULL,
                doy INTEGER NOT NULL,
                expected_obs INTEGER NOT NULL,
                actual_obs INTEGER NOT NULL,
                rejected_obs INTEGER NOT NULL,
                completeness_pct DOUBLE NOT NULL,
                rejection_pct DOUBLE NOT NULL,
                first_epoch DOUBLE,
                last_epoch DOUBLE,
                gap_hours DOUBLE,
                gap_count INTEGER,
                has_full_day BOOLEAN DEFAULT TRUE,
                quality_flag VARCHAR DEFAULT 'GOOD',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY(station_id, year, doy)
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_sdc_station ON station_data_completeness(station_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_sdc_quality ON station_data_completeness(quality_flag)")

        # Observation Quality table (RINEX QC metrics)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS observation_quality (
                station_id VARCHAR NOT NULL,
                year INTEGER NOT NULL,
                doy INTEGER NOT NULL,
                mp1 DOUBLE,
                mp2 DOUBLE,
                snr_l1 DOUBLE,
                snr_l2 DOUBLE,
                cycle_slips INTEGER DEFAULT 0,
                cycle_slip_rate DOUBLE,
                completeness_pct DOUBLE DEFAULT 0,
                total_epochs INTEGER DEFAULT 0,
                total_observations INTEGER DEFAULT 0,
                mean_sats_per_epoch DOUBLE DEFAULT 0,
                gps_sats INTEGER DEFAULT 0,
                glo_sats INTEGER DEFAULT 0,
                gal_sats INTEGER DEFAULT 0,
                quality_level VARCHAR DEFAULT 'ACCEPTABLE',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY(station_id, year, doy)
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_obs_quality_station ON observation_quality(station_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_obs_quality_level ON observation_quality(quality_level)")

        print(f"Quality monitoring tables created in {self.db_path}")
