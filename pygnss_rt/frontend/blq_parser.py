"""
BLQ Ocean Loading File Parser and Displacement Calculator

Parses Bernese BLQ format files containing ocean tide loading coefficients
and calculates displacement due to ocean loading at any given time.

Ocean tide loading causes periodic displacements of GNSS stations due to
the gravitational attraction of the sun and moon on the ocean water mass.

Tidal Constituents (11 standard):
- M2: Principal lunar semidiurnal (period ~12.42 hours)
- S2: Principal solar semidiurnal (period 12 hours)
- N2: Larger lunar elliptic semidiurnal
- K2: Lunisolar semidiurnal
- K1: Lunisolar diurnal (period ~23.93 hours)
- O1: Principal lunar diurnal
- P1: Principal solar diurnal
- Q1: Larger lunar elliptic diurnal
- MF: Lunar fortnightly (period ~13.66 days)
- MM: Lunar monthly (period ~27.55 days)
- SSA: Solar semiannual (period ~182.62 days)
"""

import os
import re
import math
import numpy as np
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from typing import Optional, Dict, List, Tuple


# Tidal constituent angular frequencies (degrees per hour)
# These are approximate mean values
TIDAL_FREQUENCIES = {
    'M2': 28.984104,   # Principal lunar semidiurnal
    'S2': 30.000000,   # Principal solar semidiurnal
    'N2': 28.439730,   # Larger lunar elliptic semidiurnal
    'K2': 30.082137,   # Lunisolar semidiurnal
    'K1': 15.041069,   # Lunisolar diurnal
    'O1': 13.943036,   # Principal lunar diurnal
    'P1': 14.958931,   # Principal solar diurnal
    'Q1': 13.398661,   # Larger lunar elliptic diurnal
    'MF': 1.098033,    # Lunar fortnightly
    'MM': 0.544375,    # Lunar monthly
    'SSA': 0.082137    # Solar semiannual
}

# Tidal constituent periods (hours)
TIDAL_PERIODS = {
    'M2': 12.4206,
    'S2': 12.0000,
    'N2': 12.6583,
    'K2': 11.9672,
    'K1': 23.9345,
    'O1': 25.8193,
    'P1': 24.0659,
    'Q1': 26.8684,
    'MF': 327.86,
    'MM': 661.31,
    'SSA': 4383.05
}

CONSTITUENT_NAMES = ['M2', 'S2', 'N2', 'K2', 'K1', 'O1', 'P1', 'Q1', 'MF', 'MM', 'SSA']


@dataclass
class StationOceanLoading:
    """Ocean loading coefficients for a single station"""
    station_id: str
    longitude: float
    latitude: float
    height: float

    # Amplitudes in meters [11 constituents]
    amp_radial: List[float] = field(default_factory=list)      # Up component
    amp_east: List[float] = field(default_factory=list)        # East component
    amp_north: List[float] = field(default_factory=list)       # North component

    # Phases in degrees [11 constituents]
    phase_radial: List[float] = field(default_factory=list)    # Up component
    phase_east: List[float] = field(default_factory=list)      # East component
    phase_north: List[float] = field(default_factory=list)     # North component

    def get_max_displacement(self) -> Dict[str, float]:
        """Get maximum possible displacement for each component (all constituents in phase)"""
        return {
            'up_mm': sum(self.amp_radial) * 1000,
            'east_mm': sum(self.amp_east) * 1000,
            'north_mm': sum(self.amp_north) * 1000,
            'total_3d_mm': math.sqrt(
                sum(self.amp_radial)**2 +
                sum(self.amp_east)**2 +
                sum(self.amp_north)**2
            ) * 1000
        }

    def get_rms_displacement(self) -> Dict[str, float]:
        """Get RMS displacement for each component"""
        return {
            'up_mm': math.sqrt(sum(a**2 for a in self.amp_radial) / 2) * 1000,
            'east_mm': math.sqrt(sum(a**2 for a in self.amp_east) / 2) * 1000,
            'north_mm': math.sqrt(sum(a**2 for a in self.amp_north) / 2) * 1000
        }

    def get_constituent_amplitudes(self) -> Dict[str, Dict[str, float]]:
        """Get amplitudes by constituent"""
        result = {}
        for i, name in enumerate(CONSTITUENT_NAMES):
            result[name] = {
                'up_mm': self.amp_radial[i] * 1000 if i < len(self.amp_radial) else 0,
                'east_mm': self.amp_east[i] * 1000 if i < len(self.amp_east) else 0,
                'north_mm': self.amp_north[i] * 1000 if i < len(self.amp_north) else 0
            }
        return result


def parse_blq_file(filepath: str) -> Dict[str, StationOceanLoading]:
    """
    Parse a BLQ file and return ocean loading coefficients for all stations.

    Args:
        filepath: Path to the BLQ file

    Returns:
        Dictionary mapping station IDs to StationOceanLoading objects
    """
    stations = {}

    try:
        with open(filepath, 'r') as f:
            lines = f.readlines()

        i = 0
        while i < len(lines):
            line = lines[i].strip()

            # Skip comment lines and empty lines
            if line.startswith('$$') or not line:
                i += 1
                continue

            # Check if this is a station name line (starts with 2+ letters, no $$)
            if re.match(r'^[A-Z0-9]{4}\s*$', line):
                station_id = line.strip()

                # Look for lon/lat in comment lines following station name
                longitude, latitude, height = 0.0, 0.0, 0.0
                j = i + 1
                while j < len(lines) and lines[j].strip().startswith('$$'):
                    comment = lines[j]
                    # Look for lon/lat pattern
                    match = re.search(r'lon/lat:\s*([\d.-]+)\s+([\d.-]+)\s+([\d.-]+)', comment)
                    if match:
                        longitude = float(match.group(1))
                        latitude = float(match.group(2))
                        height = float(match.group(3))
                    j += 1

                # Skip to data lines (after comments)
                while j < len(lines) and lines[j].strip().startswith('$$'):
                    j += 1

                # Read 6 data lines
                if j + 5 < len(lines):
                    try:
                        amp_radial = [float(x) for x in lines[j].split()]
                        amp_east = [float(x) for x in lines[j+1].split()]
                        amp_north = [float(x) for x in lines[j+2].split()]
                        phase_radial = [float(x) for x in lines[j+3].split()]
                        phase_east = [float(x) for x in lines[j+4].split()]
                        phase_north = [float(x) for x in lines[j+5].split()]

                        stations[station_id] = StationOceanLoading(
                            station_id=station_id,
                            longitude=longitude,
                            latitude=latitude,
                            height=height,
                            amp_radial=amp_radial,
                            amp_east=amp_east,
                            amp_north=amp_north,
                            phase_radial=phase_radial,
                            phase_east=phase_east,
                            phase_north=phase_north
                        )

                        i = j + 6
                        continue
                    except (ValueError, IndexError) as e:
                        print(f"Error parsing data for station {station_id}: {e}")

            i += 1

        return stations

    except Exception as e:
        print(f"Error reading BLQ file {filepath}: {e}")
        return {}


def calculate_tidal_argument(constituent: str, dt: datetime) -> float:
    """
    Calculate the tidal argument (phase) for a constituent at a given time.

    This is a simplified calculation using mean frequencies.
    For precise calculations, use full astronomical arguments.

    Args:
        constituent: Tidal constituent name (M2, S2, etc.)
        dt: Datetime for calculation

    Returns:
        Tidal argument in degrees
    """
    # Reference epoch: J2000.0 (2000-01-01 12:00:00 UTC)
    j2000 = datetime(2000, 1, 1, 12, 0, 0)

    # Hours since J2000
    hours_since_j2000 = (dt - j2000).total_seconds() / 3600.0

    # Angular frequency (degrees/hour)
    omega = TIDAL_FREQUENCIES.get(constituent, 0)

    # Tidal argument
    argument = omega * hours_since_j2000

    # Normalize to 0-360
    argument = argument % 360.0

    return argument


def calculate_displacement(station: StationOceanLoading, dt: datetime) -> Dict[str, float]:
    """
    Calculate ocean loading displacement for a station at a given time.

    Args:
        station: StationOceanLoading object with coefficients
        dt: Datetime for calculation

    Returns:
        Dictionary with displacement components in mm
    """
    disp_up = 0.0
    disp_east = 0.0
    disp_north = 0.0

    for i, constituent in enumerate(CONSTITUENT_NAMES):
        if i >= len(station.amp_radial):
            break

        # Get tidal argument at time dt
        tidal_arg = calculate_tidal_argument(constituent, dt)

        # Amplitudes (meters)
        amp_u = station.amp_radial[i]
        amp_e = station.amp_east[i]
        amp_n = station.amp_north[i]

        # Phases (degrees) - Greenwich phase lag
        phase_u = station.phase_radial[i]
        phase_e = station.phase_east[i]
        phase_n = station.phase_north[i]

        # Calculate displacement: d = A * cos(argument - phase)
        # Note: Phase is positive when displacement lags behind the tidal potential
        arg_u = math.radians(tidal_arg - phase_u)
        arg_e = math.radians(tidal_arg - phase_e)
        arg_n = math.radians(tidal_arg - phase_n)

        disp_up += amp_u * math.cos(arg_u)
        disp_east += amp_e * math.cos(arg_e)
        disp_north += amp_n * math.cos(arg_n)

    # Convert to mm
    return {
        'up_mm': disp_up * 1000,
        'east_mm': disp_east * 1000,
        'north_mm': disp_north * 1000,
        'total_3d_mm': math.sqrt(disp_up**2 + disp_east**2 + disp_north**2) * 1000
    }


def calculate_displacement_timeseries(
    station: StationOceanLoading,
    start_time: datetime,
    end_time: datetime,
    interval_minutes: int = 60
) -> List[Dict]:
    """
    Calculate ocean loading displacement time series for a station.

    Args:
        station: StationOceanLoading object
        start_time: Start of time series
        end_time: End of time series
        interval_minutes: Time interval in minutes

    Returns:
        List of dictionaries with time and displacement components
    """
    results = []
    current_time = start_time

    while current_time <= end_time:
        disp = calculate_displacement(station, current_time)
        results.append({
            'datetime': current_time,
            'up_mm': disp['up_mm'],
            'east_mm': disp['east_mm'],
            'north_mm': disp['north_mm'],
            'total_3d_mm': disp['total_3d_mm']
        })
        current_time += timedelta(minutes=interval_minutes)

    return results


def get_station_summary(stations: Dict[str, StationOceanLoading]) -> List[Dict]:
    """
    Get a summary of ocean loading effects for all stations.

    Args:
        stations: Dictionary of StationOceanLoading objects

    Returns:
        List of summary dictionaries sorted by total displacement
    """
    summaries = []

    for station_id, station in stations.items():
        max_disp = station.get_max_displacement()
        rms_disp = station.get_rms_displacement()

        # Get dominant constituent
        constituent_amps = station.get_constituent_amplitudes()
        dominant = max(constituent_amps.items(),
                      key=lambda x: x[1]['up_mm'] + x[1]['east_mm'] + x[1]['north_mm'])

        summaries.append({
            'station_id': station_id,
            'latitude': station.latitude,
            'longitude': station.longitude,
            'height': station.height,
            'max_up_mm': max_disp['up_mm'],
            'max_east_mm': max_disp['east_mm'],
            'max_north_mm': max_disp['north_mm'],
            'max_total_mm': max_disp['total_3d_mm'],
            'rms_up_mm': rms_disp['up_mm'],
            'rms_east_mm': rms_disp['east_mm'],
            'rms_north_mm': rms_disp['north_mm'],
            'dominant_constituent': dominant[0],
            'm2_up_mm': constituent_amps['M2']['up_mm'],
            'm2_east_mm': constituent_amps['M2']['east_mm'],
            'm2_north_mm': constituent_amps['M2']['north_mm']
        })

    # Sort by maximum total displacement (descending)
    summaries.sort(key=lambda x: x['max_total_mm'], reverse=True)

    return summaries


if __name__ == '__main__':
    # Test the parser
    import sys

    if len(sys.argv) > 1:
        blq_file = sys.argv[1]
    else:
        blq_file = '/home/ahunegnaw/GPSDATA/CAMPAIGN54/OCT2025_MGX/STA/FIN_EGA.BLQ'

    print(f"Parsing BLQ file: {blq_file}")
    stations = parse_blq_file(blq_file)

    print(f"\nFound {len(stations)} stations")
    print("\n" + "="*80)
    print("OCEAN LOADING DISPLACEMENT SUMMARY")
    print("="*80)

    summaries = get_station_summary(stations)

    print(f"\n{'Station':<10} {'Lat':>8} {'Lon':>9} {'Max Up':>8} {'Max E':>8} {'Max N':>8} {'Max 3D':>8} {'Dominant':<8}")
    print(f"{'':10} {'(deg)':>8} {'(deg)':>9} {'(mm)':>8} {'(mm)':>8} {'(mm)':>8} {'(mm)':>8} {'':8}")
    print("-"*80)

    for s in summaries[:20]:  # Show top 20
        print(f"{s['station_id']:<10} {s['latitude']:>8.3f} {s['longitude']:>9.3f} "
              f"{s['max_up_mm']:>8.2f} {s['max_east_mm']:>8.2f} {s['max_north_mm']:>8.2f} "
              f"{s['max_total_mm']:>8.2f} {s['dominant_constituent']:<8}")

    # Calculate current displacement for first station
    if stations:
        first_station = list(stations.values())[0]
        now = datetime.utcnow()
        disp = calculate_displacement(first_station, now)
        print(f"\nCurrent displacement for {first_station.station_id} at {now} UTC:")
        print(f"  Up:    {disp['up_mm']:>8.3f} mm")
        print(f"  East:  {disp['east_mm']:>8.3f} mm")
        print(f"  North: {disp['north_mm']:>8.3f} mm")
        print(f"  3D:    {disp['total_3d_mm']:>8.3f} mm")
