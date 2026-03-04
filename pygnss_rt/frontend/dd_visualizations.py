"""
Double Difference (DD) Results Visualization Module

This module provides comprehensive visualizations for DD GNSS processing results:
1. Baseline Repeatability Time Series (HLM)
2. Satellite Health Monitor (CHK.SUM)
3. Ambiguity Resolution Success Dashboard (AMB.SUM)
4. Troposphere Gradient Animation (TRP)
5. Baseline Network Graph (RMS + BSL)
6. Coordinate Time Series (CRD)
"""

import os
import re
import gzip
import math
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from typing import Optional, Dict, List, Tuple, Any
from collections import defaultdict


# =============================================================================
# 1. BASELINE REPEATABILITY TIME SERIES (HLM)
# =============================================================================

@dataclass
class StationRepeatability:
    """Station coordinate repeatability over multiple days"""
    station_id: str
    doys: List[int] = field(default_factory=list)
    north_mm: List[float] = field(default_factory=list)
    east_mm: List[float] = field(default_factory=list)
    up_mm: List[float] = field(default_factory=list)
    rms_3d: List[float] = field(default_factory=list)
    is_reference: List[bool] = field(default_factory=list)

    def get_wrms(self) -> Tuple[float, float, float]:
        """Calculate Weighted RMS for N, E, U components"""
        if not self.north_mm:
            return 0.0, 0.0, 0.0
        n = np.array(self.north_mm)
        e = np.array(self.east_mm)
        u = np.array(self.up_mm)
        return np.sqrt(np.mean(n**2)), np.sqrt(np.mean(e**2)), np.sqrt(np.mean(u**2))

    def detect_jumps(self, threshold_mm: float = 10.0) -> List[Tuple[int, str, float]]:
        """Detect coordinate jumps between consecutive days"""
        jumps = []
        for i in range(1, len(self.doys)):
            dn = abs(self.north_mm[i] - self.north_mm[i-1])
            de = abs(self.east_mm[i] - self.east_mm[i-1])
            du = abs(self.up_mm[i] - self.up_mm[i-1])

            if dn > threshold_mm:
                jumps.append((self.doys[i], 'North', dn))
            if de > threshold_mm:
                jumps.append((self.doys[i], 'East', de))
            if du > threshold_mm:
                jumps.append((self.doys[i], 'Up', du))
        return jumps


def load_hlm_timeseries(base_dir: str, year: int, doy_start: int, doy_end: int) -> Dict[str, StationRepeatability]:
    """
    Load HLM residuals for a range of DOYs and build time series per station.

    Args:
        base_dir: Base directory for DD results
        year: Year
        doy_start: Starting DOY
        doy_end: Ending DOY

    Returns:
        Dictionary mapping station_id to StationRepeatability
    """
    stations = {}

    for doy in range(doy_start, doy_end + 1):
        hlm_path = None
        for pattern in [
            f"{base_dir}/{year}/{doy:03d}/OUT/HLM_{year}{doy:03d}0.OUT.gz",
            f"{base_dir}/{year}/{doy}/OUT/HLM_{year}{doy:03d}0.OUT.gz",
        ]:
            if os.path.exists(pattern):
                hlm_path = pattern
                break

        if not hlm_path:
            continue

        # Parse HLM file
        try:
            if hlm_path.endswith('.gz'):
                f = gzip.open(hlm_path, 'rt')
            else:
                f = open(hlm_path, 'r')

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
                                n = float(residuals[0])
                                e = float(residuals[1])
                                u = float(residuals[2])
                                rms_3d = math.sqrt(n**2 + e**2 + u**2)

                                if station not in stations:
                                    stations[station] = StationRepeatability(station_id=station)

                                stations[station].doys.append(doy)
                                stations[station].north_mm.append(n)
                                stations[station].east_mm.append(e)
                                stations[station].up_mm.append(u)
                                stations[station].rms_3d.append(rms_3d)
                                stations[station].is_reference.append(is_ref)
                    except (ValueError, IndexError):
                        continue

                if in_table and ('RMS / COMPONENT' in line or 'IQR' in line):
                    break
            f.close()
        except Exception as e:
            continue

    return stations


def get_repeatability_summary(stations: Dict[str, StationRepeatability]) -> pd.DataFrame:
    """Generate summary statistics for all stations"""
    data = []
    for sta_id, sta in stations.items():
        if len(sta.doys) < 2:
            continue
        wrms_n, wrms_e, wrms_u = sta.get_wrms()
        wrms_3d = math.sqrt(wrms_n**2 + wrms_e**2 + wrms_u**2)
        jumps = sta.detect_jumps()

        data.append({
            'Station': sta_id,
            'Days': len(sta.doys),
            'WRMS_N (mm)': wrms_n,
            'WRMS_E (mm)': wrms_e,
            'WRMS_U (mm)': wrms_u,
            'WRMS_3D (mm)': wrms_3d,
            'Jumps': len(jumps),
            'Mean 3D (mm)': np.mean(sta.rms_3d),
            'Max 3D (mm)': np.max(sta.rms_3d)
        })

    df = pd.DataFrame(data)
    if not df.empty and 'WRMS_3D (mm)' in df.columns:
        return df.sort_values('WRMS_3D (mm)')
    return df


# =============================================================================
# 2. SATELLITE HEALTH MONITOR (CHK.SUM)
# =============================================================================

@dataclass
class SatelliteStats:
    """Statistics for a single satellite"""
    prn: int
    constellation: str  # 'G' for GPS, 'E' for Galileo, 'R' for GLONASS
    obs_percent_before: float = 0.0
    obs_percent_after: float = 0.0
    obs_count_before: int = 0
    obs_count_after: int = 0
    obs_deleted: int = 0
    rms_before: float = 0.0
    rms_after: float = 0.0


def parse_chk_summary(chk_path: str) -> List[SatelliteStats]:
    """
    Parse CHK summary file for satellite-level statistics.

    Args:
        chk_path: Path to CHK_*.SUM.gz file

    Returns:
        List of SatelliteStats objects
    """
    satellites = []

    if not os.path.exists(chk_path):
        return satellites

    try:
        if chk_path.endswith('.gz'):
            f = gzip.open(chk_path, 'rt')
        else:
            f = open(chk_path, 'r')

        in_table = False
        for line in f:
            if 'PRN |' in line and '% Observations' in line:
                in_table = True
                continue

            if in_table and line.startswith('----'):
                continue

            if in_table and line.strip():
                parts = line.split('|')
                if len(parts) >= 4:
                    try:
                        prn_str = parts[0].strip()
                        if not prn_str:
                            continue

                        prn = int(prn_str)

                        # Determine constellation
                        if prn >= 100:
                            constellation = 'E'  # Galileo (101-136)
                        elif prn >= 40:
                            constellation = 'R'  # GLONASS
                        else:
                            constellation = 'G'  # GPS

                        obs_parts = parts[1].strip().split()
                        count_parts = parts[2].strip().split()
                        rms_parts = parts[3].strip().split()

                        sat = SatelliteStats(
                            prn=prn,
                            constellation=constellation,
                            obs_percent_before=float(obs_parts[0]) if len(obs_parts) > 0 else 0,
                            obs_percent_after=float(obs_parts[1]) if len(obs_parts) > 1 else 0,
                            obs_count_before=int(count_parts[0]) if len(count_parts) > 0 else 0,
                            obs_count_after=int(count_parts[1]) if len(count_parts) > 1 else 0,
                            obs_deleted=int(count_parts[2]) if len(count_parts) > 2 else 0,
                            rms_before=float(rms_parts[0]) if len(rms_parts) > 0 else 0,
                            rms_after=float(rms_parts[1]) if len(rms_parts) > 1 else 0
                        )
                        satellites.append(sat)
                    except (ValueError, IndexError):
                        continue
        f.close()
    except Exception:
        pass

    return satellites


def get_chk_path(base_dir: str, year: int, doy: int) -> Optional[str]:
    """Get path to CHK summary file"""
    patterns = [
        f"{base_dir}/{year}/{doy:03d}/OUT/CHK_{year}{doy:03d}0.SUM.gz",
        f"{base_dir}/{year}/{doy}/OUT/CHK_{year}{doy:03d}0.SUM.gz",
    ]
    for p in patterns:
        if os.path.exists(p):
            return p
    return None


def load_satellite_timeseries(base_dir: str, year: int, doy_start: int, doy_end: int) -> Dict[int, Dict[str, List]]:
    """
    Load satellite statistics for a range of DOYs.

    Returns:
        Dictionary mapping PRN to {'doys': [], 'rms': [], 'obs_count': []}
    """
    satellites = defaultdict(lambda: {'doys': [], 'rms': [], 'obs_count': [], 'constellation': ''})

    for doy in range(doy_start, doy_end + 1):
        chk_path = get_chk_path(base_dir, year, doy)
        if not chk_path:
            continue

        stats = parse_chk_summary(chk_path)
        for sat in stats:
            satellites[sat.prn]['doys'].append(doy)
            satellites[sat.prn]['rms'].append(sat.rms_after)
            satellites[sat.prn]['obs_count'].append(sat.obs_count_after)
            satellites[sat.prn]['constellation'] = sat.constellation

    return dict(satellites)


# =============================================================================
# 3. AMBIGUITY RESOLUTION SUCCESS DASHBOARD (AMB.SUM)
# =============================================================================

@dataclass
class BaselineAmbiguity:
    """Ambiguity resolution statistics for a single baseline"""
    station1: str
    station2: str
    wl_fixed: int = 0
    wl_total: int = 0
    nl_fixed: int = 0
    nl_total: int = 0
    l5_fixed: int = 0
    l5_total: int = 0

    @property
    def wl_rate(self) -> float:
        return (self.wl_fixed / self.wl_total * 100) if self.wl_total > 0 else 0

    @property
    def nl_rate(self) -> float:
        return (self.nl_fixed / self.nl_total * 100) if self.nl_total > 0 else 0

    @property
    def l5_rate(self) -> float:
        return (self.l5_fixed / self.l5_total * 100) if self.l5_total > 0 else 0


def parse_amb_summary(amb_path: str) -> Tuple[List[BaselineAmbiguity], Dict[str, Any]]:
    """
    Parse AMB.SUM file for ambiguity resolution statistics.

    Format: File Sta1 Sta2 Length Before(#Amb,mm) After(#Amb,mm) Res(%) Sys Max/RMS ...

    Returns:
        Tuple of (list of BaselineAmbiguity, network_summary dict)
    """
    baselines = []
    baseline_dict = {}  # Track unique baselines
    network_summary = {
        'wl_fixed': 0, 'wl_total': 0,
        'nl_fixed': 0, 'nl_total': 0,
        'wl_rate': 0.0, 'nl_rate': 0.0
    }

    if not os.path.exists(amb_path):
        return baselines, network_summary

    try:
        if amb_path.endswith('.gz'):
            f = gzip.open(amb_path, 'rt')
        else:
            f = open(amb_path, 'r')

        in_table = False
        past_separator = False

        for line in f:
            # Look for the data section
            if 'File' in line and 'Sta1' in line and 'Sta2' in line:
                in_table = True
                continue

            if in_table and not past_separator:
                if line.strip().startswith('---'):
                    past_separator = True
                continue

            if past_separator and line.strip():
                # Check for summary/total line
                if line.strip().startswith('Tot:') or line.strip().startswith('---'):
                    continue

                parts = line.split()
                if len(parts) >= 8:
                    try:
                        # Format: WLI_2025 STA1 STA2 Length Before After Res% Sys...
                        # Find station names (4-char alphanumeric starting at position 1)
                        sta1 = parts[1].upper()[:4]
                        sta2 = parts[2].upper()[:4]

                        # Validate station names
                        if not (sta1.isalnum() and sta2.isalnum() and len(sta1) >= 3 and len(sta2) >= 3):
                            continue

                        # Parse numeric values
                        # parts[3] = length, parts[4] = before_amb, parts[5] = before_mm
                        # parts[6] = after_amb, parts[7] = after_mm, parts[8] = res%
                        before_amb = int(parts[4])
                        after_amb = int(parts[6])
                        res_pct = float(parts[8])

                        # Look for system (G, E, G E combined)
                        sys_str = ""
                        for i in range(9, min(12, len(parts))):
                            if parts[i] in ['G', 'E', 'R']:
                                sys_str += parts[i]
                            elif sys_str:
                                break

                        # Only use combined G E rows for unique baselines
                        if 'G' in sys_str and 'E' in sys_str:
                            key = tuple(sorted([sta1, sta2]))
                            if key not in baseline_dict:
                                fixed = before_amb - after_amb
                                baseline_dict[key] = BaselineAmbiguity(
                                    station1=sta1,
                                    station2=sta2,
                                    wl_fixed=fixed,
                                    wl_total=before_amb,
                                    nl_fixed=0,
                                    nl_total=0
                                )
                                network_summary['wl_fixed'] += fixed
                                network_summary['wl_total'] += before_amb
                    except (ValueError, IndexError):
                        continue
        f.close()

        # Calculate rates
        if network_summary['wl_total'] > 0:
            network_summary['wl_rate'] = network_summary['wl_fixed'] / network_summary['wl_total'] * 100

        baselines = list(baseline_dict.values())

    except Exception:
        pass

    return baselines, network_summary


def get_amb_path(base_dir: str, year: int, doy: int) -> Optional[str]:
    """Get path to AMB.SUM file"""
    patterns = [
        f"{base_dir}/{year}/{doy:03d}/OUT/AMB_{year}{doy:03d}0.SUM.gz",
        f"{base_dir}/{year}/{doy}/OUT/AMB_{year}{doy:03d}0.SUM.gz",
    ]
    for p in patterns:
        if os.path.exists(p):
            return p
    return None


def get_station_ar_rates(baselines: List[BaselineAmbiguity]) -> pd.DataFrame:
    """Calculate per-station AR success rates"""
    station_stats = defaultdict(lambda: {'wl_fixed': 0, 'wl_total': 0, 'nl_fixed': 0, 'nl_total': 0, 'baselines': 0})

    for bl in baselines:
        for sta in [bl.station1, bl.station2]:
            station_stats[sta]['wl_fixed'] += bl.wl_fixed
            station_stats[sta]['wl_total'] += bl.wl_total
            station_stats[sta]['nl_fixed'] += bl.nl_fixed
            station_stats[sta]['nl_total'] += bl.nl_total
            station_stats[sta]['baselines'] += 1

    data = []
    for sta, stats in station_stats.items():
        wl_rate = (stats['wl_fixed'] / stats['wl_total'] * 100) if stats['wl_total'] > 0 else 0
        nl_rate = (stats['nl_fixed'] / stats['nl_total'] * 100) if stats['nl_total'] > 0 else 0
        data.append({
            'Station': sta,
            'Baselines': stats['baselines'] // 2,  # Each baseline counted twice
            'WL Fixed': stats['wl_fixed'],
            'WL Total': stats['wl_total'],
            'WL Rate (%)': wl_rate,
            'NL Fixed': stats['nl_fixed'],
            'NL Total': stats['nl_total'],
            'NL Rate (%)': nl_rate
        })

    return pd.DataFrame(data).sort_values('NL Rate (%)')


def load_ar_timeseries(base_dir: str, year: int, doy_start: int, doy_end: int) -> pd.DataFrame:
    """Load AR statistics for multiple DOYs"""
    data = []

    for doy in range(doy_start, doy_end + 1):
        amb_path = get_amb_path(base_dir, year, doy)
        if not amb_path:
            continue

        baselines, summary = parse_amb_summary(amb_path)

        wl_rate = (summary['wl_fixed'] / summary['wl_total'] * 100) if summary['wl_total'] > 0 else 0
        nl_rate = (summary['nl_fixed'] / summary['nl_total'] * 100) if summary['nl_total'] > 0 else 0

        data.append({
            'DOY': doy,
            'Baselines': len(baselines),
            'WL Rate (%)': wl_rate,
            'NL Rate (%)': nl_rate,
            'WL Fixed': summary['wl_fixed'],
            'WL Total': summary['wl_total'],
            'NL Fixed': summary['nl_fixed'],
            'NL Total': summary['nl_total']
        })

    return pd.DataFrame(data)


# =============================================================================
# 4. TROPOSPHERE GRADIENT ANIMATION (TRP)
# =============================================================================

@dataclass
class TroposphereEpoch:
    """Troposphere estimate for a single epoch"""
    station: str
    epoch: datetime
    ztd_total: float  # meters
    ztd_sigma: float
    gradient_n: float  # meters
    gradient_e: float
    lat: float = 0.0
    lon: float = 0.0


def parse_trp_file(trp_path: str) -> List[TroposphereEpoch]:
    """
    Parse TRP file for troposphere estimates at all epochs.

    Args:
        trp_path: Path to TRP file

    Returns:
        List of TroposphereEpoch objects
    """
    epochs = []

    if not os.path.exists(trp_path):
        return epochs

    try:
        if trp_path.endswith('.gz'):
            f = gzip.open(trp_path, 'rt')
        else:
            f = open(trp_path, 'r')

        for line in f:
            if line.startswith(' ') and len(line) > 80:
                parts = line.split()
                # TRP format:
                # STATION FLAG YYYY MM DD HH MM SS MOD_U CORR_U SIGMA_U TOTAL_U CORR_N SIGMA_N CORR_E SIGMA_E
                if len(parts) >= 16:
                    try:
                        station = parts[0][:4].upper()
                        # Skip header lines
                        if station in ['STAT', 'TROP', '----', 'SOUR', 'MAPP', 'TABU', 'MINI']:
                            continue

                        year = int(parts[2])
                        month = int(parts[3])
                        day = int(parts[4])
                        hour = int(parts[5])
                        minute = int(parts[6])
                        second = int(float(parts[7]))

                        epoch = datetime(year, month, day, hour, minute, second)

                        total_ztd = float(parts[11])  # TOTAL_U in meters
                        sigma_u = float(parts[10])    # SIGMA_U
                        grad_n = float(parts[12])     # CORR_N
                        grad_e = float(parts[14])     # CORR_E

                        epochs.append(TroposphereEpoch(
                            station=station,
                            epoch=epoch,
                            ztd_total=total_ztd,
                            ztd_sigma=sigma_u,
                            gradient_n=grad_n,
                            gradient_e=grad_e
                        ))
                    except (ValueError, IndexError):
                        continue
        f.close()
    except Exception:
        pass

    return epochs


def get_trp_path(base_dir: str, year: int, doy: int) -> Optional[str]:
    """Get path to TRP file"""
    patterns = [
        f"{base_dir}/{year}/{doy:03d}/ATM/F10_{year}{doy:03d}0.TRP.gz",
        f"{base_dir}/{year}/{doy}/ATM/F10_{year}{doy:03d}0.TRP.gz",
    ]
    for p in patterns:
        if os.path.exists(p):
            return p
    return None


def get_gradient_frames(epochs: List[TroposphereEpoch], station_coords: Dict[str, Tuple[float, float]]) -> List[Dict]:
    """
    Group troposphere epochs by time for animation frames.

    Args:
        epochs: List of TroposphereEpoch
        station_coords: Dictionary mapping station to (lat, lon)

    Returns:
        List of frame dictionaries with station gradients
    """
    # Group by epoch hour
    frames = defaultdict(list)
    for e in epochs:
        hour_key = e.epoch.strftime('%H:%M')
        if e.station in station_coords:
            lat, lon = station_coords[e.station]
            frames[hour_key].append({
                'station': e.station,
                'lat': lat,
                'lon': lon,
                'ztd': e.ztd_total * 1000,  # Convert to mm
                'grad_n': e.gradient_n * 1000,  # mm
                'grad_e': e.gradient_e * 1000,
                'grad_mag': math.sqrt(e.gradient_n**2 + e.gradient_e**2) * 1000
            })

    # Sort by time and return as list
    return [{'time': k, 'stations': v} for k, v in sorted(frames.items())]


# =============================================================================
# 5. BASELINE NETWORK GRAPH (RMS + BSL)
# =============================================================================

@dataclass
class BaselineRMS:
    """RMS statistics for a single baseline"""
    station1: str
    station2: str
    rms_total: float  # mm
    median_resi: float
    sigma: float
    num_obs: int
    num_sat: int
    num_deleted: int


def parse_rms_file(rms_path: str) -> List[BaselineRMS]:
    """
    Parse RMS output file for baseline phase RMS statistics.

    Args:
        rms_path: Path to RMS_*.OUT.gz file

    Returns:
        List of BaselineRMS objects
    """
    baselines = []

    if not os.path.exists(rms_path):
        return baselines

    try:
        if rms_path.endswith('.gz'):
            f = gzip.open(rms_path, 'rt')
        else:
            f = open(rms_path, 'r')

        in_table = False
        past_separator = False

        for line in f:
            if 'Num   Station 1' in line and 'Station 2' in line:
                in_table = True
                continue

            if in_table and not past_separator:
                if line.strip().startswith('---'):
                    past_separator = True
                continue

            if past_separator and line.strip():
                parts = line.split()
                if len(parts) >= 10:
                    try:
                        # Format: Num Station1 Station2 TotalRMS med.Resi Sigma numObs nSat nDel ObsTyp...
                        num = int(parts[0])
                        sta1 = parts[1].upper()[:4]
                        sta2 = parts[2].upper()[:4]

                        # Validate station names
                        if not (sta1.isalnum() and sta2.isalnum()):
                            continue

                        rms_total = float(parts[3])
                        median_resi = float(parts[4])
                        sigma = float(parts[5])
                        num_obs = int(parts[6])
                        num_sat = int(parts[7])
                        num_del = int(parts[8])

                        baselines.append(BaselineRMS(
                            station1=sta1,
                            station2=sta2,
                            rms_total=rms_total,
                            median_resi=median_resi,
                            sigma=sigma,
                            num_obs=num_obs,
                            num_sat=num_sat,
                            num_deleted=num_del
                        ))
                    except (ValueError, IndexError):
                        continue
        f.close()
    except Exception:
        pass

    return baselines


def get_rms_path(base_dir: str, year: int, doy: int) -> Optional[str]:
    """Get path to RMS output file"""
    patterns = [
        f"{base_dir}/{year}/{doy:03d}/OUT/RMS_{year}{doy:03d}0.OUT.gz",
        f"{base_dir}/{year}/{doy}/OUT/RMS_{year}{doy:03d}0.OUT.gz",
    ]
    for p in patterns:
        if os.path.exists(p):
            return p
    return None


def build_network_graph_data(baselines: List[BaselineRMS],
                             station_coords: Dict[str, Tuple[float, float]]) -> Tuple[List[Dict], List[Dict]]:
    """
    Build node and edge data for network graph visualization.

    Returns:
        Tuple of (nodes, edges) where each is a list of dictionaries
    """
    # Build nodes from unique stations
    stations = set()
    for bl in baselines:
        stations.add(bl.station1)
        stations.add(bl.station2)

    nodes = []
    for sta in stations:
        if sta in station_coords:
            lat, lon = station_coords[sta]
            nodes.append({
                'id': sta,
                'lat': lat,
                'lon': lon
            })

    # Build edges
    edges = []
    for bl in baselines:
        if bl.station1 in station_coords and bl.station2 in station_coords:
            lat1, lon1 = station_coords[bl.station1]
            lat2, lon2 = station_coords[bl.station2]

            # Calculate baseline length (approximate)
            R = 6371  # km
            dlat = math.radians(lat2 - lat1)
            dlon = math.radians(lon2 - lon1)
            a = math.sin(dlat/2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon/2)**2
            length_km = R * 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))

            edges.append({
                'station1': bl.station1,
                'station2': bl.station2,
                'lat1': lat1,
                'lon1': lon1,
                'lat2': lat2,
                'lon2': lon2,
                'rms': bl.rms_total,
                'num_obs': bl.num_obs,
                'num_sat': bl.num_sat,
                'length_km': length_km
            })

    return nodes, edges


def get_station_connectivity(baselines: List[BaselineRMS]) -> pd.DataFrame:
    """Calculate connectivity statistics per station"""
    stats = defaultdict(lambda: {'connections': 0, 'total_rms': 0, 'total_obs': 0})

    for bl in baselines:
        for sta in [bl.station1, bl.station2]:
            stats[sta]['connections'] += 1
            stats[sta]['total_rms'] += bl.rms_total
            stats[sta]['total_obs'] += bl.num_obs

    data = []
    for sta, s in stats.items():
        data.append({
            'Station': sta,
            'Connections': s['connections'],
            'Mean RMS (mm)': s['total_rms'] / s['connections'] if s['connections'] > 0 else 0,
            'Total Obs': s['total_obs'],
            'Mean Obs': s['total_obs'] / s['connections'] if s['connections'] > 0 else 0
        })

    return pd.DataFrame(data).sort_values('Connections')


# =============================================================================
# 6. COORDINATE TIME SERIES (CRD)
# =============================================================================

@dataclass
class StationCoordinates:
    """XYZ coordinates for a station"""
    station: str
    x: float
    y: float
    z: float
    sigma_x: float = 0.0
    sigma_y: float = 0.0
    sigma_z: float = 0.0


def parse_crd_file(crd_path: str) -> Dict[str, StationCoordinates]:
    """
    Parse CRD file for station coordinates.

    Args:
        crd_path: Path to CRD file

    Returns:
        Dictionary mapping station_id to StationCoordinates
    """
    coords = {}

    if not os.path.exists(crd_path):
        return coords

    try:
        if crd_path.endswith('.gz'):
            f = gzip.open(crd_path, 'rt')
        else:
            f = open(crd_path, 'r')

        in_data = False
        for line in f:
            if 'NUM  STATION NAME' in line:
                in_data = True
                continue

            if not in_data or not line.strip():
                continue

            parts = line.split()
            if len(parts) >= 5:
                try:
                    station = parts[1][:4].upper()

                    # Find XYZ values (large floats)
                    xyz = []
                    for p in parts[2:]:
                        try:
                            val = float(p)
                            if abs(val) > 100000:
                                xyz.append(val)
                                if len(xyz) == 3:
                                    break
                        except ValueError:
                            continue

                    if len(xyz) == 3:
                        coords[station] = StationCoordinates(
                            station=station,
                            x=xyz[0],
                            y=xyz[1],
                            z=xyz[2]
                        )
                except (ValueError, IndexError):
                    continue
        f.close()
    except Exception:
        pass

    return coords


def get_crd_path(base_dir: str, year: int, doy: int, prefix: str = "F10") -> Optional[str]:
    """Get path to CRD file"""
    patterns = [
        f"{base_dir}/{year}/{doy:03d}/STA/{prefix}_{year}{doy:03d}0.CRD.gz",
        f"{base_dir}/{year}/{doy}/STA/{prefix}_{year}{doy:03d}0.CRD.gz",
    ]
    for p in patterns:
        if os.path.exists(p):
            return p
    return None


def xyz_to_enu(x: float, y: float, z: float,
               x_ref: float, y_ref: float, z_ref: float) -> Tuple[float, float, float]:
    """Convert XYZ difference to ENU (local coordinates)"""
    # Reference point latitude and longitude
    lon_ref = math.atan2(y_ref, x_ref)
    lat_ref = math.atan2(z_ref, math.sqrt(x_ref**2 + y_ref**2))

    # Differences
    dx = x - x_ref
    dy = y - y_ref
    dz = z - z_ref

    # Rotation matrix
    sin_lat = math.sin(lat_ref)
    cos_lat = math.cos(lat_ref)
    sin_lon = math.sin(lon_ref)
    cos_lon = math.cos(lon_ref)

    e = -sin_lon * dx + cos_lon * dy
    n = -sin_lat * cos_lon * dx - sin_lat * sin_lon * dy + cos_lat * dz
    u = cos_lat * cos_lon * dx + cos_lat * sin_lon * dy + sin_lat * dz

    return e * 1000, n * 1000, u * 1000  # Convert to mm


def load_coordinate_timeseries(base_dir: str, year: int, doy_start: int, doy_end: int,
                               prefix: str = "F10") -> Dict[str, Dict[str, List]]:
    """
    Load coordinates for multiple DOYs and compute ENU differences.

    Returns:
        Dictionary mapping station to {'doys': [], 'east': [], 'north': [], 'up': []}
    """
    # First pass: collect all coordinates
    all_coords = {}
    for doy in range(doy_start, doy_end + 1):
        crd_path = get_crd_path(base_dir, year, doy, prefix)
        if crd_path:
            coords = parse_crd_file(crd_path)
            all_coords[doy] = coords

    if not all_coords:
        return {}

    # Find reference (mean) coordinates per station
    station_xyz = defaultdict(lambda: {'x': [], 'y': [], 'z': []})
    for doy, coords in all_coords.items():
        for sta, c in coords.items():
            station_xyz[sta]['x'].append(c.x)
            station_xyz[sta]['y'].append(c.y)
            station_xyz[sta]['z'].append(c.z)

    # Compute mean reference coordinates
    ref_coords = {}
    for sta, xyz in station_xyz.items():
        if len(xyz['x']) >= 2:
            ref_coords[sta] = (
                np.mean(xyz['x']),
                np.mean(xyz['y']),
                np.mean(xyz['z'])
            )

    # Second pass: compute ENU differences
    timeseries = defaultdict(lambda: {'doys': [], 'east': [], 'north': [], 'up': []})

    for doy in sorted(all_coords.keys()):
        coords = all_coords[doy]
        for sta, c in coords.items():
            if sta in ref_coords:
                x_ref, y_ref, z_ref = ref_coords[sta]
                e, n, u = xyz_to_enu(c.x, c.y, c.z, x_ref, y_ref, z_ref)

                timeseries[sta]['doys'].append(doy)
                timeseries[sta]['east'].append(e)
                timeseries[sta]['north'].append(n)
                timeseries[sta]['up'].append(u)

    return dict(timeseries)


def estimate_velocity(doys: List[int], coords: List[float]) -> Tuple[float, float, float]:
    """
    Estimate linear velocity from coordinate time series.

    Returns:
        Tuple of (velocity mm/year, intercept mm, std mm)
    """
    if len(doys) < 3:
        return 0.0, 0.0, 0.0

    # Convert DOY to years
    x = np.array(doys) / 365.25
    y = np.array(coords)

    # Linear regression
    A = np.vstack([x, np.ones(len(x))]).T
    try:
        result = np.linalg.lstsq(A, y, rcond=None)
        slope, intercept = result[0]
        residuals = y - (slope * x + intercept)
        std = np.std(residuals)
        return slope * 365.25, intercept, std  # Convert to mm/year
    except:
        return 0.0, 0.0, 0.0


def get_coordinate_summary(timeseries: Dict[str, Dict[str, List]]) -> pd.DataFrame:
    """Generate summary statistics for coordinate time series"""
    data = []

    for sta, ts in timeseries.items():
        if len(ts['doys']) < 2:
            continue

        # Compute statistics
        e_std = np.std(ts['east'])
        n_std = np.std(ts['north'])
        u_std = np.std(ts['up'])

        # Estimate velocities
        ve, _, _ = estimate_velocity(ts['doys'], ts['east'])
        vn, _, _ = estimate_velocity(ts['doys'], ts['north'])
        vu, _, _ = estimate_velocity(ts['doys'], ts['up'])

        data.append({
            'Station': sta,
            'Days': len(ts['doys']),
            'E Std (mm)': e_std,
            'N Std (mm)': n_std,
            'U Std (mm)': u_std,
            'Ve (mm/yr)': ve,
            'Vn (mm/yr)': vn,
            'Vu (mm/yr)': vu
        })

    return pd.DataFrame(data).sort_values('U Std (mm)', ascending=False)


# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================

def get_available_doys(base_dir: str, year: int) -> List[int]:
    """Get list of available DOYs with processing results"""
    doys = []
    year_dir = f"{base_dir}/{year}"

    if not os.path.exists(year_dir):
        return doys

    for item in os.listdir(year_dir):
        try:
            doy = int(item)
            if 1 <= doy <= 366:
                # Check if OUT directory exists
                out_dir = f"{year_dir}/{item}/OUT"
                if os.path.exists(out_dir):
                    doys.append(doy)
        except ValueError:
            continue

    return sorted(doys)


if __name__ == '__main__':
    # Test the module
    base_dir = "/home/ahunegnaw/GPSRESULTS/ahunegnaw/OCT2025_MGX"
    year = 2025

    print("Testing DD Visualizations Module")
    print("=" * 50)

    # Test available DOYs
    doys = get_available_doys(base_dir, year)
    print(f"\nAvailable DOYs: {len(doys)}")
    if doys:
        print(f"  Range: {min(doys)} - {max(doys)}")

    # Test repeatability
    if len(doys) >= 2:
        print("\n1. Testing Baseline Repeatability...")
        stations = load_hlm_timeseries(base_dir, year, min(doys), min(min(doys)+5, max(doys)))
        print(f"   Stations with timeseries: {len(stations)}")
        if stations:
            summary = get_repeatability_summary(stations)
            print(f"   Summary rows: {len(summary)}")

    # Test satellite health
    print("\n2. Testing Satellite Health Monitor...")
    chk_path = get_chk_path(base_dir, year, doys[0] if doys else 302)
    if chk_path:
        sats = parse_chk_summary(chk_path)
        print(f"   Satellites parsed: {len(sats)}")

    # Test AR dashboard
    print("\n3. Testing AR Dashboard...")
    amb_path = get_amb_path(base_dir, year, doys[0] if doys else 302)
    if amb_path:
        baselines, summary = parse_amb_summary(amb_path)
        print(f"   Baselines parsed: {len(baselines)}")
        if summary['wl_total'] > 0:
            print(f"   WL Rate: {summary['wl_fixed']/summary['wl_total']*100:.1f}%")
        if summary['nl_total'] > 0:
            print(f"   NL Rate: {summary['nl_fixed']/summary['nl_total']*100:.1f}%")

    # Test troposphere
    print("\n4. Testing Troposphere Gradients...")
    trp_path = get_trp_path(base_dir, year, doys[0] if doys else 302)
    if trp_path:
        epochs = parse_trp_file(trp_path)
        print(f"   Epochs parsed: {len(epochs)}")
        if epochs:
            unique_stations = len(set(e.station for e in epochs))
            unique_times = len(set(e.epoch for e in epochs))
            print(f"   Unique stations: {unique_stations}")
            print(f"   Unique times: {unique_times}")

    # Test baseline network
    print("\n5. Testing Baseline Network...")
    rms_path = get_rms_path(base_dir, year, doys[0] if doys else 302)
    if rms_path:
        baselines = parse_rms_file(rms_path)
        print(f"   Baselines parsed: {len(baselines)}")
        if baselines:
            connectivity = get_station_connectivity(baselines)
            print(f"   Stations in network: {len(connectivity)}")

    # Test coordinate time series
    if len(doys) >= 2:
        print("\n6. Testing Coordinate Time Series...")
        timeseries = load_coordinate_timeseries(base_dir, year, min(doys), min(min(doys)+5, max(doys)))
        print(f"   Stations with timeseries: {len(timeseries)}")
        if timeseries:
            summary = get_coordinate_summary(timeseries)
            print(f"   Summary rows: {len(summary)}")

    print("\n" + "=" * 50)
    print("Module tests completed!")
