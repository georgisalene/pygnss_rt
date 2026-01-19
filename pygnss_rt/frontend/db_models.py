"""
DuckDB Database Models for Quality Monitoring

Integrates with existing pygnss_rt DuckDB database.

Tables added:
- ppp_solutions: Daily PPP coordinate solutions with quality metrics
- processing_stats: Processing statistics (RMS, observations, DOF)
- ztd_hourly: Hourly ZTD estimates
"""

import os
from datetime import datetime
from typing import Optional
from dataclasses import dataclass
from pathlib import Path

try:
    import duckdb
except ImportError:
    raise ImportError("Please install duckdb: pip install duckdb")


@dataclass
class PPPSolution:
    """PPP coordinate solution for a station/day"""
    station_id: str
    year: int
    doy: int
    mjd: float
    x: float  # meters
    y: float  # meters
    z: float  # meters
    x_rms: float  # meters
    y_rms: float  # meters
    z_rms: float  # meters
    lat: Optional[float] = None  # degrees
    lon: Optional[float] = None  # degrees
    height: Optional[float] = None  # meters
    created_at: Optional[datetime] = None


@dataclass
class ProcessingStats:
    """Processing statistics from GPSEST output"""
    station_id: str
    year: int
    doy: int
    mjd: float
    rms_unit_weight: float  # meters (a posteriori RMS)
    chi2_dof: float  # Chi^2 / DOF
    num_observations: int
    num_parameters: int
    dof: int  # Degrees of freedom
    num_satellites: Optional[int] = None
    ambiguity_fixed: Optional[int] = None  # Number of fixed ambiguities
    ambiguity_total: Optional[int] = None  # Total ambiguities
    ambiguity_rate: Optional[float] = None  # Fix rate percentage
    created_at: Optional[datetime] = None


@dataclass
class ZTDHourly:
    """Hourly ZTD estimate"""
    station_id: str
    year: int
    doy: int
    hour: int
    mjd: float
    ztd: float  # meters (Zenith Total Delay)
    ztd_rms: float  # meters (TROTOT STDDEV)
    grad_n: Optional[float] = None  # North gradient (TGNTOT)
    grad_n_rms: Optional[float] = None  # North gradient STDDEV
    grad_e: Optional[float] = None  # East gradient (TGETOT)
    grad_e_rms: Optional[float] = None  # East gradient STDDEV
    created_at: Optional[datetime] = None


@dataclass
class AmbiguityResolution:
    """Ambiguity resolution statistics by constellation"""
    station_id: str
    year: int
    doy: int
    receiver: Optional[str] = None
    # Widelane (WL) resolution rates (%)
    wl_gps: Optional[float] = None
    wl_gal: Optional[float] = None
    wl_combined: Optional[float] = None
    # Narrowlane (NL) resolution rates (%)
    nl_gps: Optional[float] = None
    nl_gal: Optional[float] = None
    nl_combined: Optional[float] = None
    created_at: Optional[datetime] = None


@dataclass
class SatelliteTracking:
    """Satellite tracking statistics from CHK_*.SUM files"""
    year: int
    doy: int
    prn: int  # PRN number (1-32=GPS, 101-128=GLONASS, 201-236=Galileo)
    constellation: str  # 'GPS', 'GLONASS', 'GALILEO'
    obs_percent: float  # Percentage of observations
    obs_count: int  # Number of observations
    rms: float  # RMS in mm
    created_at: Optional[datetime] = None


@dataclass
class SatelliteAmbiguityPRN:
    """Satellite-wise ambiguity resolution statistics from AMB_*.SUM files"""
    year: int
    doy: int
    prn: int  # PRN number (1-32=GPS, 201-236=Galileo)
    constellation: str  # 'GPS' or 'GAL'
    amb_total: int  # Total ambiguities
    l1l2_solved: int  # L1 & L2 ambiguities solved
    l1l2_cluster: float  # L1 & L2 #/clu (cluster size)
    l1l2_rel: float  # L1 & L2 resolution rate (%)
    l5_solved: int  # L5 ambiguities solved
    l5_cluster: float  # L5 #/clu (cluster size)
    l5_rel: float  # L5 resolution rate (%)
    created_at: Optional[datetime] = None


@dataclass
class StationResidual:
    """Per-station, per-satellite observation residuals from EDL_*.SUM"""
    year: int
    doy: int
    station_id: str
    prn: int  # PRN number (1-32=GPS, 101-128=GLONASS, 201-236=Galileo)
    constellation: str  # 'GPS', 'GLONASS', or 'GALILEO'
    rms: float  # Residual RMS in mm
    created_at: Optional[datetime] = None


@dataclass
class DataAvailability:
    """Satellite data availability statistics from CHK_*.SUM"""
    year: int
    doy: int
    prn: int  # PRN number
    constellation: str  # 'GPS', 'GLONASS', or 'GALILEO'
    obs_pct_before: float  # % observations before outlier rejection
    obs_pct_after: float  # % observations after outlier rejection
    obs_count_before: int  # # observations before
    obs_count_after: int  # # observations after
    obs_rejected: int  # # rejected observations
    rejection_rate: float  # % of observations rejected
    rms_before: float  # RMS in mm before
    rms_after: float  # RMS in mm after
    created_at: Optional[datetime] = None


@dataclass
class ReceiverClock:
    """Receiver clock estimates from RINEX clock files"""
    station_id: str
    year: int
    doy: int
    epoch: float  # Seconds of day (0-86400)
    clock_offset: float  # Clock offset in seconds
    clock_sigma: Optional[float] = None  # Clock sigma in seconds
    created_at: Optional[datetime] = None


class QualityDatabase:
    """DuckDB database connection and operations for quality monitoring"""

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

    def connect(self):
        """Establish database connection"""
        if self._conn is None:
            # Create parent directory if needed
            Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
            self._conn = duckdb.connect(self.db_path)
        return self._conn

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

        print(f"Quality monitoring tables created in {self.db_path}")

    def insert_solution(self, sol: PPPSolution):
        """Insert or update a PPP solution"""
        conn = self.connect()

        # Use INSERT OR REPLACE for upsert behavior
        conn.execute("""
            INSERT OR REPLACE INTO ppp_solutions
                (station_id, year, doy, mjd, x, y, z, x_rms, y_rms, z_rms, lat, lon, height)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, [sol.station_id, sol.year, sol.doy, sol.mjd,
              sol.x, sol.y, sol.z, sol.x_rms, sol.y_rms, sol.z_rms,
              sol.lat, sol.lon, sol.height])

    def insert_stats(self, stats: ProcessingStats):
        """Insert or update processing statistics"""
        conn = self.connect()

        # Use INSERT OR REPLACE for upsert behavior
        conn.execute("""
            INSERT OR REPLACE INTO processing_stats
                (station_id, year, doy, mjd, rms_unit_weight, chi2_dof,
                 num_observations, num_parameters, dof, num_satellites,
                 ambiguity_fixed, ambiguity_total, ambiguity_rate)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, [stats.station_id, stats.year, stats.doy, stats.mjd,
              stats.rms_unit_weight, stats.chi2_dof, stats.num_observations,
              stats.num_parameters, stats.dof, stats.num_satellites,
              stats.ambiguity_fixed, stats.ambiguity_total, stats.ambiguity_rate])

    def insert_ztd(self, ztd: ZTDHourly):
        """Insert or update hourly ZTD"""
        conn = self.connect()

        # Use INSERT OR REPLACE for upsert behavior
        conn.execute("""
            INSERT OR REPLACE INTO ztd_hourly
                (station_id, year, doy, hour, mjd, ztd, ztd_rms, grad_n, grad_n_rms, grad_e, grad_e_rms)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, [ztd.station_id, ztd.year, ztd.doy, ztd.hour, ztd.mjd,
              ztd.ztd, ztd.ztd_rms, ztd.grad_n, ztd.grad_n_rms, ztd.grad_e, ztd.grad_e_rms])

    def insert_ambiguity(self, amb: 'AmbiguityResolution'):
        """Insert or update ambiguity resolution stats"""
        conn = self.connect()

        conn.execute("""
            INSERT OR REPLACE INTO ambiguity_resolution
                (station_id, year, doy, receiver, wl_gps, wl_gal, wl_combined,
                 nl_gps, nl_gal, nl_combined)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, [amb.station_id, amb.year, amb.doy, amb.receiver,
              amb.wl_gps, amb.wl_gal, amb.wl_combined,
              amb.nl_gps, amb.nl_gal, amb.nl_combined])

    def get_solutions(self, station_id: Optional[str] = None,
                      year: Optional[int] = None,
                      start_doy: Optional[int] = None,
                      end_doy: Optional[int] = None,
                      limit: int = 365) -> list[dict]:
        """Query PPP solutions with optional filters"""
        conn = self.connect()

        query = "SELECT * FROM ppp_solutions WHERE 1=1"
        params = []

        if station_id:
            query += " AND station_id = ?"
            params.append(station_id)
        if year:
            query += " AND year = ?"
            params.append(year)
        if start_doy:
            query += " AND doy >= ?"
            params.append(start_doy)
        if end_doy:
            query += " AND doy <= ?"
            params.append(end_doy)

        query += " ORDER BY year DESC, doy DESC LIMIT ?"
        params.append(limit)

        result = conn.execute(query, params).fetchdf()
        return result.to_dict('records')

    def get_stats(self, station_id: Optional[str] = None,
                  year: Optional[int] = None,
                  limit: int = 365) -> list[dict]:
        """Query processing statistics"""
        conn = self.connect()

        query = "SELECT * FROM processing_stats WHERE 1=1"
        params = []

        if station_id:
            query += " AND station_id = ?"
            params.append(station_id)
        if year:
            query += " AND year = ?"
            params.append(year)

        query += " ORDER BY year DESC, doy DESC LIMIT ?"
        params.append(limit)

        result = conn.execute(query, params).fetchdf()
        return result.to_dict('records')

    def get_ztd(self, station_id: str, year: int, doy: int) -> list[dict]:
        """Get hourly ZTD for a specific station and day"""
        conn = self.connect()

        result = conn.execute("""
            SELECT * FROM ztd_hourly
            WHERE station_id = ? AND year = ? AND doy = ?
            ORDER BY hour
        """, [station_id, year, doy]).fetchdf()

        return result.to_dict('records')

    def get_coordinate_repeatability(self, station_id: str,
                                     year: int,
                                     start_doy: int,
                                     end_doy: int) -> dict:
        """Calculate coordinate repeatability (std dev) over a period"""
        conn = self.connect()

        result = conn.execute("""
            SELECT
                station_id,
                COUNT(*) as num_days,
                AVG(x) as mean_x, STDDEV(x) as std_x,
                AVG(y) as mean_y, STDDEV(y) as std_y,
                AVG(z) as mean_z, STDDEV(z) as std_z,
                AVG(x_rms) as mean_x_rms,
                AVG(y_rms) as mean_y_rms,
                AVG(z_rms) as mean_z_rms
            FROM ppp_solutions
            WHERE station_id = ? AND year = ? AND doy BETWEEN ? AND ?
            GROUP BY station_id
        """, [station_id, year, start_doy, end_doy]).fetchdf()

        if len(result) > 0:
            return result.to_dict('records')[0]
        return {}

    def get_all_stations(self) -> list[str]:
        """Get list of all stations with solutions"""
        conn = self.connect()

        result = conn.execute(
            "SELECT DISTINCT station_id FROM ppp_solutions ORDER BY station_id"
        ).fetchdf()

        return result['station_id'].tolist() if len(result) > 0 else []

    def get_latest_processing(self, limit: int = 50) -> list[dict]:
        """Get most recently processed solutions"""
        conn = self.connect()

        result = conn.execute("""
            SELECT s.*, p.rms_unit_weight, p.num_observations
            FROM ppp_solutions s
            LEFT JOIN processing_stats p
                ON s.station_id = p.station_id AND s.year = p.year AND s.doy = p.doy
            ORDER BY s.created_at DESC
            LIMIT ?
        """, [limit]).fetchdf()

        return result.to_dict('records')

    def get_ambiguity(self, station_id: Optional[str] = None,
                      year: Optional[int] = None,
                      start_doy: Optional[int] = None,
                      end_doy: Optional[int] = None) -> list[dict]:
        """Query ambiguity resolution statistics"""
        conn = self.connect()

        query = "SELECT * FROM ambiguity_resolution WHERE 1=1"
        params = []

        if station_id:
            query += " AND station_id = ?"
            params.append(station_id)
        if year:
            query += " AND year = ?"
            params.append(year)
        if start_doy:
            query += " AND doy >= ?"
            params.append(start_doy)
        if end_doy:
            query += " AND doy <= ?"
            params.append(end_doy)

        query += " ORDER BY station_id, year, doy"

        result = conn.execute(query, params).fetchdf()
        return result.to_dict('records')

    def insert_satellite_tracking(self, sat: 'SatelliteTracking'):
        """Insert or update satellite tracking stats"""
        conn = self.connect()

        conn.execute("""
            INSERT OR REPLACE INTO satellite_tracking
                (year, doy, prn, constellation, obs_percent, obs_count, rms)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, [sat.year, sat.doy, sat.prn, sat.constellation,
              sat.obs_percent, sat.obs_count, sat.rms])

    def get_satellite_tracking(self, year: Optional[int] = None,
                               doy: Optional[int] = None,
                               constellation: Optional[str] = None) -> list[dict]:
        """Query satellite tracking statistics"""
        conn = self.connect()

        query = "SELECT * FROM satellite_tracking WHERE 1=1"
        params = []

        if year:
            query += " AND year = ?"
            params.append(year)
        if doy:
            query += " AND doy = ?"
            params.append(doy)
        if constellation:
            query += " AND constellation = ?"
            params.append(constellation)

        query += " ORDER BY year, doy, prn"

        result = conn.execute(query, params).fetchdf()
        return result.to_dict('records')

    def insert_satellite_ambiguity_prn(self, sat: 'SatelliteAmbiguityPRN'):
        """Insert or update satellite-wise ambiguity resolution stats"""
        conn = self.connect()

        conn.execute("""
            INSERT OR REPLACE INTO satellite_ambiguity_prn
                (year, doy, prn, constellation, amb_total,
                 l1l2_solved, l1l2_cluster, l1l2_rel,
                 l5_solved, l5_cluster, l5_rel)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, [sat.year, sat.doy, sat.prn, sat.constellation, sat.amb_total,
              sat.l1l2_solved, sat.l1l2_cluster, sat.l1l2_rel,
              sat.l5_solved, sat.l5_cluster, sat.l5_rel])

    def get_satellite_ambiguity_prn(self, year: Optional[int] = None,
                                    doy: Optional[int] = None,
                                    constellation: Optional[str] = None) -> list[dict]:
        """Query satellite-wise ambiguity resolution statistics"""
        conn = self.connect()

        query = "SELECT * FROM satellite_ambiguity_prn WHERE 1=1"
        params = []

        if year:
            query += " AND year = ?"
            params.append(year)
        if doy:
            query += " AND doy = ?"
            params.append(doy)
        if constellation:
            query += " AND constellation = ?"
            params.append(constellation)

        query += " ORDER BY year, doy, prn"

        result = conn.execute(query, params).fetchdf()
        return result.to_dict('records')

    def insert_station_residual(self, res: 'StationResidual'):
        """Insert or update station residual data"""
        conn = self.connect()

        conn.execute("""
            INSERT OR REPLACE INTO station_residuals
                (year, doy, station_id, prn, constellation, rms)
            VALUES (?, ?, ?, ?, ?, ?)
        """, [res.year, res.doy, res.station_id, res.prn,
              res.constellation, res.rms])

    def get_station_residuals(self, year: Optional[int] = None,
                              doy: Optional[int] = None,
                              station_id: Optional[str] = None,
                              constellation: Optional[str] = None) -> list[dict]:
        """Query station residuals"""
        conn = self.connect()

        query = "SELECT * FROM station_residuals WHERE 1=1"
        params = []

        if year:
            query += " AND year = ?"
            params.append(year)
        if doy:
            query += " AND doy = ?"
            params.append(doy)
        if station_id:
            query += " AND station_id = ?"
            params.append(station_id)
        if constellation:
            query += " AND constellation = ?"
            params.append(constellation)

        query += " ORDER BY year, doy, station_id, prn"

        result = conn.execute(query, params).fetchdf()
        return result.to_dict('records')

    def insert_data_availability(self, da: 'DataAvailability'):
        """Insert or update data availability statistics"""
        conn = self.connect()

        conn.execute("""
            INSERT OR REPLACE INTO data_availability
                (year, doy, prn, constellation, obs_pct_before, obs_pct_after,
                 obs_count_before, obs_count_after, obs_rejected, rejection_rate,
                 rms_before, rms_after)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, [da.year, da.doy, da.prn, da.constellation,
              da.obs_pct_before, da.obs_pct_after,
              da.obs_count_before, da.obs_count_after,
              da.obs_rejected, da.rejection_rate,
              da.rms_before, da.rms_after])

    def get_data_availability(self, year: Optional[int] = None,
                              doy: Optional[int] = None,
                              constellation: Optional[str] = None) -> list[dict]:
        """Query data availability statistics"""
        conn = self.connect()

        query = "SELECT * FROM data_availability WHERE 1=1"
        params = []

        if year:
            query += " AND year = ?"
            params.append(year)
        if doy:
            query += " AND doy = ?"
            params.append(doy)
        if constellation:
            query += " AND constellation = ?"
            params.append(constellation)

        query += " ORDER BY year, doy, prn"

        result = conn.execute(query, params).fetchdf()
        return result.to_dict('records')

    def insert_receiver_clock(self, clk: 'ReceiverClock'):
        """Insert or update receiver clock estimate"""
        conn = self.connect()

        conn.execute("""
            INSERT OR REPLACE INTO receiver_clocks
                (station_id, year, doy, epoch, clock_offset, clock_sigma)
            VALUES (?, ?, ?, ?, ?, ?)
        """, [clk.station_id, clk.year, clk.doy, clk.epoch,
              clk.clock_offset, clk.clock_sigma])

    def get_receiver_clocks(self, year: Optional[int] = None,
                            doy: Optional[int] = None,
                            station_id: Optional[str] = None) -> list[dict]:
        """Query receiver clock estimates"""
        conn = self.connect()

        query = "SELECT * FROM receiver_clocks WHERE 1=1"
        params = []

        if year:
            query += " AND year = ?"
            params.append(year)
        if doy:
            query += " AND doy = ?"
            params.append(doy)
        if station_id:
            query += " AND station_id = ?"
            params.append(station_id)

        query += " ORDER BY station_id, year, doy, epoch"

        result = conn.execute(query, params).fetchdf()
        return result.to_dict('records')

    def get_tropospheric_gradients(self, year: Optional[int] = None,
                                   doy: Optional[int] = None,
                                   station_id: Optional[str] = None) -> list[dict]:
        """Query tropospheric gradients from ztd_hourly table"""
        conn = self.connect()

        query = """
            SELECT station_id, year, doy, hour, ztd, ztd_rms,
                   grad_n, grad_n_rms, grad_e, grad_e_rms,
                   SQRT(grad_n*grad_n + grad_e*grad_e) as grad_magnitude,
                   DEGREES(ATAN2(grad_e, grad_n)) as grad_azimuth
            FROM ztd_hourly
            WHERE grad_n IS NOT NULL AND grad_e IS NOT NULL
        """
        params = []

        if year:
            query += " AND year = ?"
            params.append(year)
        if doy:
            query += " AND doy = ?"
            params.append(doy)
        if station_id:
            query += " AND station_id = ?"
            params.append(station_id)

        query += " ORDER BY station_id, year, doy, hour"

        result = conn.execute(query, params).fetchdf()
        return result.to_dict('records')

    def get_clock_stations(self, year: int, doy: int) -> list[str]:
        """Get list of stations with clock data for a specific day"""
        conn = self.connect()

        result = conn.execute("""
            SELECT DISTINCT station_id
            FROM receiver_clocks
            WHERE year = ? AND doy = ?
            ORDER BY station_id
        """, [year, doy]).fetchdf()

        return result['station_id'].tolist() if len(result) > 0 else []

    def get_gradient_stations(self, year: int, doy: int) -> list[str]:
        """Get list of stations with gradient data for a specific day"""
        conn = self.connect()

        result = conn.execute("""
            SELECT DISTINCT station_id
            FROM ztd_hourly
            WHERE year = ? AND doy = ? AND grad_n IS NOT NULL
            ORDER BY station_id
        """, [year, doy]).fetchdf()

        return result['station_id'].tolist() if len(result) > 0 else []
