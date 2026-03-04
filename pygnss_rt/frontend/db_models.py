"""
DuckDB Database Models for Quality Monitoring

Integrates with existing pygnss_rt DuckDB database.

Tables added:
- ppp_solutions: Daily PPP coordinate solutions with quality metrics
- processing_stats: Processing statistics (RMS, observations, DOF)
- ztd_hourly: Hourly ZTD estimates
"""

from datetime import datetime
from typing import Optional
from dataclasses import dataclass


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


@dataclass
class StationDataCompleteness:
    """Station-level data completeness and availability statistics"""
    station_id: str
    year: int
    doy: int
    # Expected observations based on 24h, 30s sampling, ~30 satellites
    expected_obs: int  # Expected observations (theoretical max)
    actual_obs: int  # Actual observations used in processing
    rejected_obs: int  # Observations rejected during screening
    # Completeness metrics
    completeness_pct: float  # actual_obs / expected_obs * 100
    rejection_pct: float  # rejected_obs / (actual_obs + rejected_obs) * 100
    # Data gaps
    first_epoch: Optional[float] = None  # First epoch with data (hours since 00:00)
    last_epoch: Optional[float] = None  # Last epoch with data (hours since 00:00)
    gap_hours: Optional[float] = None  # Total hours of missing data
    gap_count: Optional[int] = None  # Number of distinct data gaps
    # Quality indicators
    has_full_day: bool = True  # True if data spans full 24 hours
    quality_flag: str = 'GOOD'  # 'GOOD', 'PARTIAL', 'POOR'
    created_at: Optional[datetime] = None


@dataclass
class ObservationQuality:
    """RINEX observation quality metrics from QC analysis"""
    station_id: str
    year: int
    doy: int
    # Multipath indicators (meters)
    mp1: Optional[float] = None  # L1 multipath
    mp2: Optional[float] = None  # L2 multipath
    # Signal-to-noise ratio (dB-Hz)
    snr_l1: Optional[float] = None  # Mean SNR on L1
    snr_l2: Optional[float] = None  # Mean SNR on L2
    # Cycle slips
    cycle_slips: int = 0  # Total cycle slips
    cycle_slip_rate: Optional[float] = None  # Slips per 1000 observations
    # Data completeness
    completeness_pct: float = 0.0  # Percentage of expected observations
    total_epochs: int = 0
    total_observations: int = 0
    # Satellite tracking
    mean_sats_per_epoch: float = 0.0
    gps_sats: int = 0
    glo_sats: int = 0
    gal_sats: int = 0
    # Quality assessment
    quality_level: str = 'ACCEPTABLE'  # 'EXCELLENT', 'GOOD', 'ACCEPTABLE', 'POOR', 'UNUSABLE'
    created_at: Optional[datetime] = None


from .db_analytics import QualityDatabaseAnalytics


class QualityDatabase(QualityDatabaseAnalytics):
    """
    DuckDB database connection and operations for quality monitoring.

    This class inherits from the mixin chain:
    QualityDatabaseSchema -> QualityDatabaseInsert -> QualityDatabaseQuery -> QualityDatabaseAnalytics

    Provides complete database functionality through inheritance:
    - Schema management and connection handling (from QualityDatabaseSchema)
    - Insert operations for all data types (from QualityDatabaseInsert)
    - Query operations for retrieving data (from QualityDatabaseQuery)
    - Analytics and computation methods (from QualityDatabaseAnalytics)
    """
    pass
