"""
ML-Based Station Quality Assessment

This module provides comprehensive machine learning-based quality assessment
for GNSS stations by analyzing multiple data sources:

1. Helmert residuals (NEU) - Coordinate quality
2. ZTD and gradients (TRP) - Troposphere estimates
3. Baseline RMS (RMS) - Phase residuals
4. Ambiguity resolution (AMB.SUM) - AR success rates
5. Observation counts - Data completeness
6. Baseline connectivity - Network geometry

The module uses:
- Isolation Forest for multivariate anomaly detection
- Z-score analysis for individual metric outliers
- Feature importance for diagnosing WHY a station is bad
"""

import os
import re
import gzip
import math
import numpy as np
from dataclasses import dataclass, field
from typing import Optional, Dict, List, Tuple
from collections import defaultdict

# Try to import sklearn for ML
try:
    from sklearn.ensemble import IsolationForest
    from sklearn.preprocessing import StandardScaler
    from sklearn.neighbors import LocalOutlierFactor
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False


@dataclass
class StationMetrics:
    """Comprehensive metrics for a single station"""
    station_id: str
    year: int
    doy: int

    # Helmert residuals (mm)
    hlm_north: float = 0.0
    hlm_east: float = 0.0
    hlm_up: float = 0.0
    hlm_3d: float = 0.0
    is_reference: bool = False

    # ZTD metrics
    ztd_mean: float = 0.0          # Mean ZTD (m)
    ztd_std: float = 0.0           # ZTD standard deviation (m)
    ztd_sigma_mean: float = 0.0    # Mean formal error (m)
    gradient_n_mean: float = 0.0   # Mean N gradient (m)
    gradient_e_mean: float = 0.0   # Mean E gradient (m)
    gradient_magnitude: float = 0.0 # Mean gradient magnitude

    # Baseline metrics (averages across all baselines involving this station)
    baseline_rms_mean: float = 0.0      # Mean phase RMS (mm)
    baseline_rms_max: float = 0.0       # Max phase RMS (mm)
    num_baselines: int = 0              # Number of baselines
    obs_count_mean: float = 0.0         # Mean observation count
    obs_count_min: int = 0              # Min observation count
    num_satellites_mean: float = 0.0    # Mean satellites

    # Ambiguity resolution
    ar_wl_success: float = 0.0    # Widelane AR success rate (%)
    ar_nl_success: float = 0.0    # Narrowlane AR success rate (%)
    ar_combined: float = 0.0      # Combined AR success rate (%)

    # Computed quality scores
    anomaly_score: float = 0.0    # Isolation Forest anomaly score (-1 to 1)
    quality_score: float = 100.0  # Overall quality score (0-100)
    quality_grade: str = "A"      # A, B, C, D, F

    # Diagnostic flags
    flags: List[str] = field(default_factory=list)

    def compute_derived_metrics(self):
        """Compute derived metrics from raw data"""
        # 3D Helmert residual
        self.hlm_3d = math.sqrt(self.hlm_north**2 + self.hlm_east**2 + self.hlm_up**2)

        # Gradient magnitude
        self.gradient_magnitude = math.sqrt(self.gradient_n_mean**2 + self.gradient_e_mean**2)


@dataclass
class QualityDiagnosis:
    """Diagnosis explaining why a station might be problematic"""
    station_id: str
    severity: str  # "critical", "warning", "info"
    category: str  # "coordinate", "troposphere", "ambiguity", "observations", "baseline"
    message: str
    metric_name: str
    metric_value: float
    threshold: float
    recommendation: str


@dataclass
class NetworkQualityReport:
    """Complete quality report for a network/day"""
    year: int
    doy: int
    stations: Dict[str, StationMetrics] = field(default_factory=dict)
    diagnoses: List[QualityDiagnosis] = field(default_factory=list)

    # Network-level statistics
    num_stations: int = 0
    num_good_stations: int = 0
    num_warning_stations: int = 0
    num_bad_stations: int = 0

    # Feature importance from ML model
    feature_importance: Dict[str, float] = field(default_factory=dict)


def parse_hlm_metrics(hlm_path: str, year: int, doy: int) -> Dict[str, StationMetrics]:
    """Parse HLM file and extract per-station metrics"""
    metrics = {}

    if not os.path.exists(hlm_path):
        return metrics

    if hlm_path.endswith('.gz'):
        open_func = lambda f: gzip.open(f, 'rt')
    else:
        open_func = lambda f: open(f, 'r')

    with open_func(hlm_path) as f:
        in_table = False
        for line in f:
            if '| NUM |' in line and 'RESIDUALS' in line:
                in_table = True
                continue

            if in_table and line.startswith(' |') and re.match(r'\s*\|\s+\d+\s+\|', line):
                try:
                    parts = line.split('|')
                    if len(parts) >= 5:
                        station = parts[2].strip()[:4].upper()
                        residuals = parts[4].strip().split()
                        is_ref = 'V' not in parts[-2] if len(parts) > 5 else True

                        if len(residuals) >= 3:
                            m = StationMetrics(
                                station_id=station,
                                year=year,
                                doy=doy,
                                hlm_north=float(residuals[0]),
                                hlm_east=float(residuals[1]),
                                hlm_up=float(residuals[2]),
                                is_reference=is_ref
                            )
                            m.compute_derived_metrics()
                            metrics[station] = m
                except (ValueError, IndexError):
                    continue

            # Stop at statistics section
            if in_table and ('RMS / COMPONENT' in line or 'IQR' in line):
                break

    return metrics


def parse_trp_metrics(trp_path: str, existing_metrics: Dict[str, StationMetrics]) -> Dict[str, StationMetrics]:
    """Parse TRP file and add troposphere metrics to existing station metrics"""

    if not os.path.exists(trp_path):
        return existing_metrics

    if trp_path.endswith('.gz'):
        open_func = lambda f: gzip.open(f, 'rt')
    else:
        open_func = lambda f: open(f, 'r')

    # Collect ZTD values per station
    station_ztd = defaultdict(list)
    station_sigma = defaultdict(list)
    station_grad_n = defaultdict(list)
    station_grad_e = defaultdict(list)

    with open_func(trp_path) as f:
        for line in f:
            if line.startswith(' ') and len(line) > 80:
                parts = line.split()
                # TRP format after split:
                # 0:STATION 1:FLAG 2-7:DATE 8:MOD_U 9:CORR_U 10:SIGMA_U 11:TOTAL_U 12:CORR_N 13:SIGMA_N 14:CORR_E 15:SIGMA_E
                if len(parts) >= 16:
                    try:
                        station = parts[0][:4].upper()
                        # Skip header/comment lines
                        if station in ['STAT', 'TROP', '----', 'SOUR', 'A', 'MAPP', 'TABU', 'MINI']:
                            continue

                        total_ztd = float(parts[11])  # TOTAL_U
                        sigma_u = float(parts[10])    # SIGMA_U
                        grad_n = float(parts[12])     # CORR_N
                        grad_e = float(parts[14])     # CORR_E

                        station_ztd[station].append(total_ztd)
                        station_sigma[station].append(sigma_u)
                        station_grad_n[station].append(grad_n)
                        station_grad_e[station].append(grad_e)
                    except (ValueError, IndexError):
                        continue

    # Compute statistics and update metrics
    for station, ztd_values in station_ztd.items():
        if station in existing_metrics:
            m = existing_metrics[station]
        else:
            m = StationMetrics(station_id=station, year=0, doy=0)
            existing_metrics[station] = m

        if ztd_values:
            m.ztd_mean = np.mean(ztd_values)
            m.ztd_std = np.std(ztd_values)
        if station in station_sigma:
            m.ztd_sigma_mean = np.mean(station_sigma[station])
        if station in station_grad_n:
            m.gradient_n_mean = np.mean(station_grad_n[station])
        if station in station_grad_e:
            m.gradient_e_mean = np.mean(station_grad_e[station])
        m.gradient_magnitude = math.sqrt(m.gradient_n_mean**2 + m.gradient_e_mean**2)

    return existing_metrics


def parse_rms_metrics(rms_path: str, existing_metrics: Dict[str, StationMetrics]) -> Dict[str, StationMetrics]:
    """Parse RMS file and add baseline residual metrics"""

    if not os.path.exists(rms_path):
        return existing_metrics

    if rms_path.endswith('.gz'):
        open_func = lambda f: gzip.open(f, 'rt')
    else:
        open_func = lambda f: open(f, 'r')

    # Collect per-station baseline statistics
    station_rms = defaultdict(list)
    station_obs = defaultdict(list)
    station_sats = defaultdict(list)

    with open_func(rms_path) as f:
        in_data = False
        past_separator = False
        for line in f:
            if 'Num   Station 1' in line:
                in_data = True
                past_separator = False
                continue

            if in_data and not past_separator:
                # Skip the separator line after header
                if line.strip().startswith('---'):
                    past_separator = True
                continue

            if in_data and past_separator:
                # Stop at end markers
                if line.strip().startswith(('NO EDIT', '>>>')):
                    break
                # Also stop at another separator (end of data section)
                if line.strip().startswith('---') and len(line.strip()) > 50:
                    break

                if line.strip():
                    parts = line.split()
                    if len(parts) >= 8:
                        try:
                            num = int(parts[0])
                            sta1 = parts[1][:4].upper()
                            sta2 = parts[2][:4].upper()
                            total_rms = float(parts[3])
                            num_obs = int(parts[6])
                            num_sat = int(parts[7])

                            # Add to both stations
                            for sta in [sta1, sta2]:
                                station_rms[sta].append(total_rms)
                                station_obs[sta].append(num_obs)
                                station_sats[sta].append(num_sat)
                        except (ValueError, IndexError):
                            continue

    # Update metrics
    for station, rms_values in station_rms.items():
        if station in existing_metrics:
            m = existing_metrics[station]
        else:
            m = StationMetrics(station_id=station, year=0, doy=0)
            existing_metrics[station] = m

        m.baseline_rms_mean = np.mean(rms_values)
        m.baseline_rms_max = np.max(rms_values)
        m.num_baselines = len(rms_values)

        if station in station_obs:
            m.obs_count_mean = np.mean(station_obs[station])
            m.obs_count_min = int(np.min(station_obs[station]))
        if station in station_sats:
            m.num_satellites_mean = np.mean(station_sats[station])

    return existing_metrics


def parse_amb_metrics(amb_path: str, existing_metrics: Dict[str, StationMetrics]) -> Dict[str, StationMetrics]:
    """Parse AMB.SUM file and add ambiguity resolution metrics"""

    if not os.path.exists(amb_path):
        return existing_metrics

    if amb_path.endswith('.gz'):
        open_func = lambda f: gzip.open(f, 'rt')
    else:
        open_func = lambda f: open(f, 'r')

    # Collect AR statistics per station
    station_wl = defaultdict(list)  # Widelane success rates
    station_nl = defaultdict(list)  # Narrowlane success rates

    with open_func(amb_path) as f:
        for line in f:
            # Parse WL and NL lines
            # Format: FILE STA1 STA2 LENGTH #Before #After RES% ...
            if '#AR_WL' in line or '#AR_NL' in line:
                parts = line.split()
                if len(parts) >= 12:
                    try:
                        sta1 = parts[1][:4].upper()
                        sta2 = parts[2][:4].upper()

                        # Validate station names (should be 4 alphanumeric chars)
                        if not (sta1[0].isalnum() and len(sta1) >= 3):
                            continue
                        if not (sta2[0].isalnum() and len(sta2) >= 3):
                            continue

                        # Find the success rate (column with % value, usually around 89-100)
                        # It's after the "Res (%)" column which shows resolution percentage
                        success_rate = float(parts[9])  # Resolution % column

                        if '#AR_WL' in line:
                            station_wl[sta1].append(success_rate)
                            station_wl[sta2].append(success_rate)
                        else:
                            station_nl[sta1].append(success_rate)
                            station_nl[sta2].append(success_rate)
                    except (ValueError, IndexError):
                        continue

    # Update metrics
    all_stations = set(station_wl.keys()) | set(station_nl.keys())
    for station in all_stations:
        if station in existing_metrics:
            m = existing_metrics[station]
        else:
            m = StationMetrics(station_id=station, year=0, doy=0)
            existing_metrics[station] = m

        if station in station_wl:
            m.ar_wl_success = np.mean(station_wl[station])
        if station in station_nl:
            m.ar_nl_success = np.mean(station_nl[station])

        # Combined AR success (product of WL and NL)
        if m.ar_wl_success > 0 and m.ar_nl_success > 0:
            m.ar_combined = (m.ar_wl_success / 100) * (m.ar_nl_success / 100) * 100
        elif m.ar_wl_success > 0:
            m.ar_combined = m.ar_wl_success
        elif m.ar_nl_success > 0:
            m.ar_combined = m.ar_nl_success

    return existing_metrics


def build_feature_matrix(metrics: Dict[str, StationMetrics]) -> Tuple[np.ndarray, List[str], List[str]]:
    """Build feature matrix for ML analysis"""

    feature_names = [
        'hlm_north', 'hlm_east', 'hlm_up', 'hlm_3d',
        'ztd_std', 'ztd_sigma_mean', 'gradient_magnitude',
        'baseline_rms_mean', 'baseline_rms_max', 'num_baselines',
        'obs_count_mean', 'num_satellites_mean',
        'ar_wl_success', 'ar_nl_success', 'ar_combined'
    ]

    stations = list(metrics.keys())
    X = []

    for station in stations:
        m = metrics[station]
        features = [
            abs(m.hlm_north),
            abs(m.hlm_east),
            abs(m.hlm_up),
            m.hlm_3d,
            m.ztd_std * 1000,  # Convert to mm for comparable scale
            m.ztd_sigma_mean * 1000,
            m.gradient_magnitude * 1000,
            m.baseline_rms_mean,
            m.baseline_rms_max,
            m.num_baselines,
            m.obs_count_mean / 1000,  # Scale down
            m.num_satellites_mean,
            100 - m.ar_wl_success,  # Invert so higher = worse
            100 - m.ar_nl_success,
            100 - m.ar_combined
        ]
        X.append(features)

    return np.array(X), stations, feature_names


def run_anomaly_detection(metrics: Dict[str, StationMetrics]) -> Dict[str, StationMetrics]:
    """Run Isolation Forest anomaly detection on station metrics"""

    if not SKLEARN_AVAILABLE or len(metrics) < 5:
        return metrics

    X, stations, feature_names = build_feature_matrix(metrics)

    # Handle missing values (replace with column mean)
    X = np.nan_to_num(X, nan=0.0)
    col_means = np.nanmean(X, axis=0)
    for i in range(X.shape[1]):
        X[np.isnan(X[:, i]), i] = col_means[i]

    # Standardize features
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    # Run Isolation Forest
    # contamination = expected fraction of outliers (5-10% typical)
    iso_forest = IsolationForest(
        contamination=0.1,
        random_state=42,
        n_estimators=100
    )

    # Get anomaly scores (-1 = anomaly, 1 = normal)
    predictions = iso_forest.fit_predict(X_scaled)
    scores = iso_forest.decision_function(X_scaled)

    # Update metrics with anomaly scores
    for i, station in enumerate(stations):
        # Convert score to 0-1 range (0 = most anomalous, 1 = normal)
        normalized_score = (scores[i] + 0.5) / 1.0  # Approximate normalization
        normalized_score = max(0, min(1, normalized_score))
        metrics[station].anomaly_score = normalized_score

    return metrics


def compute_quality_scores(metrics: Dict[str, StationMetrics]) -> Dict[str, StationMetrics]:
    """Compute overall quality scores based on multiple factors"""

    for station, m in metrics.items():
        score = 100.0

        # Helmert residual penalties
        if m.hlm_3d > 10:
            score -= 30
        elif m.hlm_3d > 6:
            score -= 15
        elif m.hlm_3d > 3:
            score -= 5

        # Vertical component (typically worse, so different thresholds)
        if abs(m.hlm_up) > 10:
            score -= 15
        elif abs(m.hlm_up) > 7:
            score -= 8

        # ZTD variability penalty
        if m.ztd_std > 0.02:  # 20mm
            score -= 15
        elif m.ztd_std > 0.01:  # 10mm
            score -= 5

        # Large gradients penalty (values in meters, 0.001m = 1mm)
        if m.gradient_magnitude > 0.003:  # 3mm - unusually large
            score -= 10
        elif m.gradient_magnitude > 0.002:  # 2mm
            score -= 5

        # Baseline RMS penalty
        if m.baseline_rms_mean > 2.0:
            score -= 15
        elif m.baseline_rms_mean > 1.5:
            score -= 5

        # Ambiguity resolution penalty
        if m.ar_combined < 70:
            score -= 20
        elif m.ar_combined < 85:
            score -= 10
        elif m.ar_combined < 95:
            score -= 3

        # Low observation count penalty
        if m.obs_count_min > 0 and m.obs_count_min < 5000:
            score -= 10
        elif m.obs_count_min > 0 and m.obs_count_min < 8000:
            score -= 5

        # Few baselines penalty (connectivity issues)
        if m.num_baselines == 1:
            score -= 10
        elif m.num_baselines == 0:
            score -= 20

        # Anomaly score factor (if available)
        if m.anomaly_score > 0:
            if m.anomaly_score < 0.3:
                score -= 15
            elif m.anomaly_score < 0.5:
                score -= 8

        # Clamp score
        score = max(0, min(100, score))
        m.quality_score = score

        # Assign grade
        if score >= 90:
            m.quality_grade = "A"
        elif score >= 75:
            m.quality_grade = "B"
        elif score >= 60:
            m.quality_grade = "C"
        elif score >= 40:
            m.quality_grade = "D"
        else:
            m.quality_grade = "F"

    return metrics


def generate_diagnoses(metrics: Dict[str, StationMetrics]) -> List[QualityDiagnosis]:
    """Generate diagnostic messages explaining quality issues"""

    diagnoses = []

    for station, m in metrics.items():
        # Check each metric against thresholds

        # Coordinate issues
        if m.hlm_3d > 10:
            diagnoses.append(QualityDiagnosis(
                station_id=station,
                severity="critical",
                category="coordinate",
                message=f"Very large 3D residual ({m.hlm_3d:.2f}mm)",
                metric_name="hlm_3d",
                metric_value=m.hlm_3d,
                threshold=10.0,
                recommendation="Check antenna setup, monument stability, or data quality"
            ))
        elif m.hlm_3d > 6:
            diagnoses.append(QualityDiagnosis(
                station_id=station,
                severity="warning",
                category="coordinate",
                message=f"Large 3D residual ({m.hlm_3d:.2f}mm)",
                metric_name="hlm_3d",
                metric_value=m.hlm_3d,
                threshold=6.0,
                recommendation="Review station for potential multipath or antenna issues"
            ))

        # Vertical component often problematic
        if abs(m.hlm_up) > 8:
            diagnoses.append(QualityDiagnosis(
                station_id=station,
                severity="warning",
                category="coordinate",
                message=f"Large vertical residual ({m.hlm_up:.2f}mm)",
                metric_name="hlm_up",
                metric_value=m.hlm_up,
                threshold=8.0,
                recommendation="Check troposphere modeling or antenna height"
            ))

        # ZTD variability
        if m.ztd_std > 0.015:
            diagnoses.append(QualityDiagnosis(
                station_id=station,
                severity="warning",
                category="troposphere",
                message=f"High ZTD variability ({m.ztd_std*1000:.1f}mm std)",
                metric_name="ztd_std",
                metric_value=m.ztd_std * 1000,
                threshold=15.0,
                recommendation="Weather-related or potential antenna/radome issue"
            ))

        # Large gradients (values in meters, typical range is 0.0001-0.001m)
        if m.gradient_magnitude > 0.002:  # > 2mm is unusual
            diagnoses.append(QualityDiagnosis(
                station_id=station,
                severity="warning",
                category="troposphere",
                message=f"Large troposphere gradient ({m.gradient_magnitude*1000:.2f}mm)",
                metric_name="gradient_magnitude",
                metric_value=m.gradient_magnitude * 1000,
                threshold=2.0,
                recommendation="May indicate local troposphere effects, terrain, or multipath"
            ))
        elif m.gradient_magnitude > 0.001:  # > 1mm
            diagnoses.append(QualityDiagnosis(
                station_id=station,
                severity="info",
                category="troposphere",
                message=f"Elevated troposphere gradient ({m.gradient_magnitude*1000:.2f}mm)",
                metric_name="gradient_magnitude",
                metric_value=m.gradient_magnitude * 1000,
                threshold=1.0,
                recommendation="Monitor for persistent patterns"
            ))

        # Baseline RMS
        if m.baseline_rms_max > 2.5:
            diagnoses.append(QualityDiagnosis(
                station_id=station,
                severity="warning",
                category="baseline",
                message=f"High baseline RMS (max {m.baseline_rms_max:.2f}mm)",
                metric_name="baseline_rms_max",
                metric_value=m.baseline_rms_max,
                threshold=2.5,
                recommendation="Check for cycle slips or multipath on this station"
            ))

        # Ambiguity resolution
        if m.ar_combined > 0 and m.ar_combined < 80:
            diagnoses.append(QualityDiagnosis(
                station_id=station,
                severity="critical",
                category="ambiguity",
                message=f"Poor ambiguity resolution ({m.ar_combined:.1f}%)",
                metric_name="ar_combined",
                metric_value=m.ar_combined,
                threshold=80.0,
                recommendation="Check receiver/antenna compatibility, multipath, or ionosphere"
            ))
        elif m.ar_combined > 0 and m.ar_combined < 90:
            diagnoses.append(QualityDiagnosis(
                station_id=station,
                severity="warning",
                category="ambiguity",
                message=f"Below-average AR success ({m.ar_combined:.1f}%)",
                metric_name="ar_combined",
                metric_value=m.ar_combined,
                threshold=90.0,
                recommendation="Review baseline lengths and receiver combinations"
            ))

        # Low observations
        if m.obs_count_min > 0 and m.obs_count_min < 6000:
            diagnoses.append(QualityDiagnosis(
                station_id=station,
                severity="warning",
                category="observations",
                message=f"Low observation count (min {m.obs_count_min})",
                metric_name="obs_count_min",
                metric_value=m.obs_count_min,
                threshold=6000,
                recommendation="Check for data gaps, outages, or tracking issues"
            ))

        # Poor connectivity
        if m.num_baselines == 1:
            diagnoses.append(QualityDiagnosis(
                station_id=station,
                severity="warning",
                category="baseline",
                message="Only 1 baseline connection",
                metric_name="num_baselines",
                metric_value=m.num_baselines,
                threshold=2,
                recommendation="Station weakly connected - consider adding nearby stations"
            ))
        elif m.num_baselines == 0:
            diagnoses.append(QualityDiagnosis(
                station_id=station,
                severity="critical",
                category="baseline",
                message="No baseline connections found",
                metric_name="num_baselines",
                metric_value=0,
                threshold=1,
                recommendation="Station may be isolated or data missing"
            ))

        # Store flags in metrics
        m.flags = [d.message for d in diagnoses if d.station_id == station]

    return diagnoses


def assess_network_quality(base_dir: str, year: int, doy: int) -> NetworkQualityReport:
    """
    Main function to assess network quality for a given day.

    Args:
        base_dir: Base directory for results (e.g., /path/to/GPSRESULTS/campaign)
        year: Year
        doy: Day of year

    Returns:
        NetworkQualityReport with all station metrics and diagnoses
    """

    report = NetworkQualityReport(year=year, doy=doy)

    # Build file paths
    hlm_path = f"{base_dir}/{year}/{doy}/OUT/HLM_{year}{doy:03d}0.OUT.gz"
    trp_path = f"{base_dir}/{year}/{doy}/ATM/F10_{year}{doy:03d}0.TRP.gz"
    rms_path = f"{base_dir}/{year}/{doy}/OUT/RMS_{year}{doy:03d}0.OUT.gz"
    amb_path = f"{base_dir}/{year}/{doy}/OUT/AMB_{year}{doy:03d}0.SUM.gz"

    # Parse all data sources
    metrics = {}

    # 1. HLM residuals (primary source)
    if os.path.exists(hlm_path):
        metrics = parse_hlm_metrics(hlm_path, year, doy)

    # 2. Troposphere estimates
    if os.path.exists(trp_path):
        metrics = parse_trp_metrics(trp_path, metrics)

    # 3. Baseline RMS
    if os.path.exists(rms_path):
        metrics = parse_rms_metrics(rms_path, metrics)

    # 4. Ambiguity resolution
    if os.path.exists(amb_path):
        metrics = parse_amb_metrics(amb_path, metrics)

    if not metrics:
        return report

    # Run ML anomaly detection
    metrics = run_anomaly_detection(metrics)

    # Compute quality scores
    metrics = compute_quality_scores(metrics)

    # Generate diagnoses
    diagnoses = generate_diagnoses(metrics)

    # Build report
    report.stations = metrics
    report.diagnoses = diagnoses
    report.num_stations = len(metrics)
    report.num_good_stations = sum(1 for m in metrics.values() if m.quality_grade in ['A', 'B'])
    report.num_warning_stations = sum(1 for m in metrics.values() if m.quality_grade == 'C')
    report.num_bad_stations = sum(1 for m in metrics.values() if m.quality_grade in ['D', 'F'])

    return report


def get_worst_stations(report: NetworkQualityReport, n: int = 5) -> List[Tuple[str, StationMetrics]]:
    """Get the N worst stations by quality score"""
    sorted_stations = sorted(
        report.stations.items(),
        key=lambda x: x[1].quality_score
    )
    return sorted_stations[:n]


def get_station_diagnoses(report: NetworkQualityReport, station_id: str) -> List[QualityDiagnosis]:
    """Get all diagnoses for a specific station"""
    return [d for d in report.diagnoses if d.station_id == station_id.upper()]


def format_quality_summary(report: NetworkQualityReport) -> str:
    """Format a text summary of the quality report"""
    lines = [
        f"Network Quality Report - {report.year} DOY {report.doy}",
        "=" * 50,
        f"Total stations: {report.num_stations}",
        f"Good (A/B): {report.num_good_stations}",
        f"Warning (C): {report.num_warning_stations}",
        f"Bad (D/F): {report.num_bad_stations}",
        "",
        "Worst Stations:",
        "-" * 30
    ]

    worst = get_worst_stations(report, 5)
    for station, m in worst:
        lines.append(f"  {station}: Score={m.quality_score:.0f} Grade={m.quality_grade}")
        for flag in m.flags[:3]:
            lines.append(f"    - {flag}")

    return "\n".join(lines)


if __name__ == "__main__":
    # Test with example data
    import sys

    if len(sys.argv) >= 4:
        base_dir = sys.argv[1]
        year = int(sys.argv[2])
        doy = int(sys.argv[3])
    else:
        base_dir = "/home/ahunegnaw/GPSRESULTS/ahunegnaw/OCT2025_MGX"
        year = 2025
        doy = 302

    print(f"Assessing network quality for {year} DOY {doy}")
    print(f"Base directory: {base_dir}")
    print()

    report = assess_network_quality(base_dir, year, doy)
    print(format_quality_summary(report))

    print("\n" + "=" * 50)
    print("All Diagnoses:")
    print("-" * 50)
    for d in sorted(report.diagnoses, key=lambda x: (x.severity != 'critical', x.severity != 'warning', x.station_id)):
        severity_icon = "🔴" if d.severity == "critical" else "🟡" if d.severity == "warning" else "🔵"
        print(f"{severity_icon} {d.station_id}: {d.message}")
        print(f"   Recommendation: {d.recommendation}")
