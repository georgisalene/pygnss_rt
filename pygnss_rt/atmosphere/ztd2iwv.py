"""
ZTD to IWV conversion module.

Converts Zenith Tropospheric Delay (ZTD) estimates to
Integrated Water Vapor (IWV) products.

Includes meteorological station lookup and pressure/temperature
extrapolation from nearby WMO stations to GPS antenna height.

Replaces Perl ZTD2IWV.pm module.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

from pygnss_rt.utils.logging import get_logger


logger = get_logger(__name__)


# Physical constants (from Mendes, 1999 and Bevis, 1994)
R_DRY = 287.0586  # Gas constant for dry air (J/kg/K)
R_VAPOR = 461.525  # Gas constant for water vapor (J/kg/K)
K1 = 77.6  # K/hPa (from Bevis, 1994)
K2 = 70.4  # K/hPa (from Bevis, 1994)
K3 = 373900  # K^2/hPa (from Bevis, 1994)
G = 9.80665  # Standard gravity (m/s^2)
EPS = R_DRY / R_VAPOR  # Ratio of gas constants
DELTA = 1 / EPS - 1  # For virtual temperature

# ICAO standard atmosphere constants
ALPHA = -0.0065  # Temperature lapse rate (K/m)
R_ICAO = 287.05  # Gas constant for dry air (J/kg/K)

# Sonntag (1994) coefficients for saturation vapor pressure
SONNTAG_N0 = -6096.9385
SONNTAG_N1 = 16.635794
SONNTAG_N2 = -2.711193e-2
SONNTAG_N3 = 1.6739521e-5
SONNTAG_N4 = 2.433502


@dataclass
class MeteoStation:
    """WMO meteorological station information."""

    station_id: str  # 5-digit WMO ID
    full_name: str
    height: float  # Orthometric height in meters
    latitude: float  # Degrees
    longitude: float  # Degrees


@dataclass
class MeteoObservation:
    """Meteorological observation from a station."""

    station_id: str
    temperature: float  # Kelvin
    dew_point: float  # Kelvin
    msl_pressure: float  # hPa (mean sea level pressure)
    height: float  # Station height in meters


@dataclass
class IWVResult:
    """IWV calculation result."""

    station_id: str
    timestamp: datetime
    mjd: float
    ztd: float  # Zenith Total Delay (mm)
    ztd_sigma: float  # ZTD uncertainty (mm)
    zhd: float  # Zenith Hydrostatic Delay (mm)
    zwd: float  # Zenith Wet Delay (mm)
    iwv: float  # Integrated Water Vapor (kg/m^2)
    iwv_sigma: float  # IWV uncertainty (kg/m^2)
    latitude: float
    longitude: float
    height: float  # Ellipsoidal height (m)
    height_ortho: float | None = None  # Orthometric height (m)
    pressure: float | None = None  # Pressure at GPS (hPa)
    temperature: float | None = None  # Temperature at GPS (K)
    met_station_id: str | None = None  # Closest met station used
    met_station_name: str | None = None
    met_distance: float | None = None  # Distance to met station (m)
    relative_humidity: float | None = None  # %


class MeteoStationDatabase:
    """Database of WMO meteorological stations."""

    def __init__(self):
        """Initialize station database."""
        self._stations: dict[str, MeteoStation] = {}

    def load_wmo_file(self, wmo_file: Path | str) -> int:
        """Load WMO station information file.

        Expected format (delimiter ':-:'):
        station_id:-:full_name:-:height:-:latitude:-:longitude

        Args:
            wmo_file: Path to WMO station info file

        Returns:
            Number of stations loaded
        """
        path = Path(wmo_file)
        if not path.exists():
            raise FileNotFoundError(f"WMO file not found: {path}")

        count = 0
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue

                parts = line.split(":-:")
                if len(parts) < 5:
                    continue

                try:
                    station_id = parts[0].strip()
                    full_name = parts[1].strip()
                    height = float(parts[2].strip())
                    latitude = float(parts[3].strip())
                    longitude = float(parts[4].strip())

                    self._stations[station_id] = MeteoStation(
                        station_id=station_id,
                        full_name=full_name,
                        height=height,
                        latitude=latitude,
                        longitude=longitude,
                    )
                    count += 1
                except (ValueError, IndexError) as e:
                    logger.warning(
                        "Failed to parse WMO station line",
                        line=line[:50],
                        error=str(e),
                    )

        logger.info("Loaded WMO stations", path=str(path), count=count)
        return count

    def find_closest(
        self,
        latitude: float,
        longitude: float,
        exclude: set[str] | None = None,
    ) -> tuple[MeteoStation | None, float]:
        """Find closest meteorological station to a point.

        Args:
            latitude: Target latitude in degrees
            longitude: Target longitude in degrees
            exclude: Set of station IDs to exclude

        Returns:
            Tuple of (closest station, distance in meters)
        """
        from pygnss_rt.stations.coordinates import calculate_distance

        exclude = exclude or set()
        closest_station = None
        min_distance = float("inf")

        for station_id, station in self._stations.items():
            if station_id in exclude:
                continue

            distance = calculate_distance(
                latitude, longitude,
                station.latitude, station.longitude
            )

            if distance < min_distance:
                min_distance = distance
                closest_station = station

        return closest_station, min_distance

    def get_station(self, station_id: str) -> MeteoStation | None:
        """Get station by ID."""
        return self._stations.get(station_id)

    def __len__(self) -> int:
        return len(self._stations)


class MeteoDataReader:
    """Reader for meteorological observation files."""

    def __init__(self, met_database: MeteoStationDatabase | None = None):
        """Initialize reader.

        Args:
            met_database: Optional station database for validation
        """
        self.met_database = met_database
        self._observations: dict[str, MeteoObservation] = {}

    def load_met_file(self, met_file: Path | str) -> int:
        """Load meteorological observation file.

        Expected format: space-separated with fields:
        block_id station_id year month day hour height ... temp dew_point pressure

        Args:
            met_file: Path to met observation file

        Returns:
            Number of observations loaded
        """
        path = Path(met_file)
        if not path.exists():
            logger.warning("Met file not found", path=str(path))
            return 0

        count = 0
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "*" in line:
                    continue

                # Normalize whitespace
                parts = line.split()
                if len(parts) < 13:
                    continue

                try:
                    # Build station ID from block and station number
                    block_id = parts[0].replace(".00", "").zfill(2)
                    station_num = parts[1].replace(".00", "").zfill(3)
                    station_id = f"{block_id}{station_num}"

                    height = float(parts[4])
                    temperature = float(parts[10])  # Kelvin
                    dew_point = float(parts[11])  # Kelvin
                    pressure = float(parts[12]) / 100.0  # Convert Pa to hPa

                    self._observations[station_id] = MeteoObservation(
                        station_id=station_id,
                        temperature=temperature,
                        dew_point=dew_point,
                        msl_pressure=pressure,
                        height=height,
                    )
                    count += 1
                except (ValueError, IndexError) as e:
                    continue

        logger.debug("Loaded met observations", path=str(path), count=count)
        return count

    def get_observation(self, station_id: str) -> MeteoObservation | None:
        """Get observation for a station."""
        return self._observations.get(station_id)

    def has_valid_data(self, station_id: str) -> bool:
        """Check if station has valid observation data."""
        obs = self._observations.get(station_id)
        if not obs:
            return False
        # Check for invalid values
        if obs.temperature <= 0 or obs.dew_point <= 0 or obs.msl_pressure <= 0:
            return False
        return True


def calculate_saturation_vapor_pressure(temperature: float) -> float:
    """Calculate saturation vapor pressure using Sonntag (1994).

    Args:
        temperature: Temperature in Kelvin

    Returns:
        Saturation vapor pressure in hPa
    """
    return math.exp(
        SONNTAG_N0 / temperature +
        SONNTAG_N1 +
        SONNTAG_N2 * temperature +
        SONNTAG_N3 * temperature ** 2 +
        SONNTAG_N4 * math.log(temperature)
    )


def extrapolate_pressure_to_height(
    msl_pressure: float,
    temperature_station: float,
    height_station: float,
    height_target: float,
) -> tuple[float, float]:
    """Extrapolate pressure from met station to target height.

    Uses ICAO standard atmosphere model with temperature lapse rate.

    Args:
        msl_pressure: Mean sea level pressure (hPa)
        temperature_station: Temperature at met station (K)
        height_station: Met station orthometric height (m)
        height_target: Target orthometric height (m)

    Returns:
        Tuple of (pressure at target, temperature at target) both in (hPa, K)
    """
    # Temperature at MSL from temperature at met station
    t_msl = temperature_station + ALPHA * (0 - height_station)

    # Surface pressure at met station from MSL pressure
    p_surface = msl_pressure * ((temperature_station / t_msl) ** (-G / (R_ICAO * ALPHA)))

    # Temperature at target from temperature at met station
    t_target = temperature_station + ALPHA * (height_target - height_station)

    # Pressure at target from surface pressure at met station
    p_target = p_surface * ((t_target / temperature_station) ** (-G / (R_ICAO * ALPHA)))

    return p_target, t_target


class ZTD2IWV:
    """Converts ZTD estimates to IWV with meteorological station lookup."""

    def __init__(
        self,
        tm_method: str = "bevis",
        met_database: MeteoStationDatabase | None = None,
        default_pressure: float | None = None,
    ):
        """Initialize converter.

        Args:
            tm_method: Method for mean temperature calculation
                      ('bevis' or 'fixed')
            met_database: WMO station database for met data lookup
            default_pressure: Default surface pressure if not available
        """
        self.tm_method = tm_method
        self.met_database = met_database
        self.default_pressure = default_pressure
        self.results: list[IWVResult] = []
        self._met_reader: MeteoDataReader | None = None

    def load_met_data(self, met_file: Path | str) -> int:
        """Load meteorological observation file.

        Args:
            met_file: Path to met observation file

        Returns:
            Number of observations loaded
        """
        self._met_reader = MeteoDataReader(self.met_database)
        return self._met_reader.load_met_file(met_file)

    def calculate_zhd_saastamoinen(
        self,
        pressure: float,
        latitude: float,
        height_ortho: float,
    ) -> float:
        """Calculate ZHD using Saastamoinen model.

        Args:
            pressure: Surface pressure at GPS in hPa
            latitude: Station latitude in radians
            height_ortho: Orthometric height in meters

        Returns:
            ZHD in mm
        """
        # Saastamoinen formula (result in mm)
        zhd = (K1 * R_DRY * pressure) / (
            9.784 * (1 - 0.0026 * math.cos(2 * latitude) - 2.8e-7 * height_ortho)
        ) * 1e-3

        return zhd

    def calculate_mean_temperature(
        self,
        temperature_gps: float,
    ) -> float:
        """Calculate mean atmospheric temperature.

        Uses Bevis et al. (1992) relation: Tm = 83.0 + 0.673 * Ts

        Args:
            temperature_gps: Temperature at GPS antenna (K)

        Returns:
            Mean temperature in Kelvin
        """
        if self.tm_method == "bevis":
            return 83.0 + 0.673 * temperature_gps
        else:
            # Fixed value for mid-latitudes
            return 280.0

    def calculate_iwv_from_zwd(
        self,
        zwd: float,
        mean_temperature: float,
    ) -> float:
        """Calculate IWV from ZWD and mean temperature.

        Args:
            zwd: Zenith Wet Delay in mm
            mean_temperature: Mean atmospheric temperature in Kelvin

        Returns:
            IWV in kg/m^2
        """
        # Conversion factor
        denominator = R_VAPOR * (K3 / mean_temperature + K2 - K1 * R_DRY / R_VAPOR)
        iwv = zwd / denominator * 1e5

        return iwv

    def estimate_pressure(
        self,
        height: float,
        latitude: float | None = None,
    ) -> float:
        """Estimate surface pressure from height using standard atmosphere.

        Args:
            height: Station height in meters
            latitude: Station latitude (optional)

        Returns:
            Estimated pressure in hPa
        """
        P0 = 1013.25  # Sea level pressure (hPa)
        T0 = 288.15  # Sea level temperature (K)
        L = 0.0065  # Temperature lapse rate (K/m)

        pressure = P0 * (1 - L * height / T0) ** 5.255
        return pressure

    def process(
        self,
        station_id: str,
        ztd: float,
        ztd_sigma: float,
        timestamp: datetime,
        latitude: float,
        longitude: float,
        height: float,
        height_ortho: float | None = None,
        geoid_height: float | None = None,
        pressure: float | None = None,
        temperature: float | None = None,
    ) -> IWVResult:
        """Process a single ZTD observation.

        Args:
            station_id: Station identifier
            ztd: Zenith Total Delay in mm
            ztd_sigma: ZTD uncertainty in mm
            timestamp: Observation timestamp
            latitude: Station latitude in degrees
            longitude: Station longitude in degrees
            height: Station ellipsoidal height in meters
            height_ortho: Station orthometric height (if known)
            geoid_height: Geoid undulation (height_ortho = height - geoid_height)
            pressure: Surface pressure in hPa (optional, will lookup)
            temperature: Surface temperature in Kelvin (optional, will lookup)

        Returns:
            IWVResult with all computed values
        """
        # Convert latitude to radians for calculations
        lat_rad = math.radians(latitude)

        # Calculate orthometric height if not provided
        if height_ortho is None:
            if geoid_height is not None:
                height_ortho = height - geoid_height
            else:
                # Approximate: use ellipsoidal height
                height_ortho = height

        # Track met station info
        met_station_id = None
        met_station_name = None
        met_distance = None
        relative_humidity = None

        # Try to get meteorological data from nearby station
        if pressure is None or temperature is None:
            met_result = self._lookup_meteorological_data(
                latitude, longitude, height_ortho
            )
            if met_result:
                pressure = met_result["pressure"]
                temperature = met_result["temperature"]
                met_station_id = met_result.get("station_id")
                met_station_name = met_result.get("station_name")
                met_distance = met_result.get("distance")
                relative_humidity = met_result.get("relative_humidity")

        # Fallback to standard atmosphere estimate
        if pressure is None:
            pressure = self.estimate_pressure(height_ortho, latitude)
        if temperature is None:
            # Estimate from standard atmosphere
            temperature = 288.15 - 0.0065 * height_ortho

        # Calculate ZHD using Saastamoinen
        zhd = self.calculate_zhd_saastamoinen(pressure, lat_rad, height_ortho)

        # Calculate ZWD
        zwd = ztd - zhd

        # Calculate mean temperature
        mean_temp = self.calculate_mean_temperature(temperature)

        # Calculate IWV
        iwv = self.calculate_iwv_from_zwd(zwd, mean_temp)

        # Propagate uncertainty
        zhd_sigma = 2.0  # ~2mm uncertainty in ZHD
        zwd_sigma = math.sqrt(ztd_sigma ** 2 + zhd_sigma ** 2)
        iwv_sigma = zwd_sigma / (R_VAPOR * (K3 / mean_temp + K2 - K1 * R_DRY / R_VAPOR)) * 1e5

        # Calculate MJD
        from pygnss_rt.utils.dates import mjd_from_date
        mjd = mjd_from_date(
            timestamp.year, timestamp.month, timestamp.day,
            timestamp.hour, timestamp.minute, timestamp.second
        )

        result = IWVResult(
            station_id=station_id,
            timestamp=timestamp,
            mjd=mjd,
            ztd=ztd,
            ztd_sigma=ztd_sigma,
            zhd=zhd,
            zwd=zwd,
            iwv=iwv,
            iwv_sigma=iwv_sigma,
            latitude=latitude,
            longitude=longitude,
            height=height,
            height_ortho=height_ortho,
            pressure=pressure,
            temperature=temperature,
            met_station_id=met_station_id,
            met_station_name=met_station_name,
            met_distance=met_distance,
            relative_humidity=relative_humidity,
        )

        self.results.append(result)
        return result

    def _lookup_meteorological_data(
        self,
        latitude: float,
        longitude: float,
        height_ortho: float,
        max_iterations: int = 10,
    ) -> dict[str, Any] | None:
        """Lookup meteorological data from nearest station.

        Finds closest station with valid data and extrapolates
        pressure/temperature to GPS antenna height.

        Args:
            latitude: GPS station latitude (degrees)
            longitude: GPS station longitude (degrees)
            height_ortho: GPS orthometric height (m)
            max_iterations: Max attempts to find valid station

        Returns:
            Dict with pressure, temperature, and met station info
        """
        if self.met_database is None or self._met_reader is None:
            return None

        excluded: set[str] = set()

        for _ in range(max_iterations):
            # Find closest station not yet excluded
            station, distance = self.met_database.find_closest(
                latitude, longitude, exclude=excluded
            )

            if station is None:
                return None

            # Check if we have valid data for this station
            if not self._met_reader.has_valid_data(station.station_id):
                excluded.add(station.station_id)
                continue

            obs = self._met_reader.get_observation(station.station_id)
            if obs is None:
                excluded.add(station.station_id)
                continue

            # Extrapolate pressure and temperature to GPS height
            p_gps, t_gps = extrapolate_pressure_to_height(
                obs.msl_pressure,
                obs.temperature,
                obs.height,
                height_ortho,
            )

            # Calculate relative humidity
            e_sat = calculate_saturation_vapor_pressure(obs.temperature)
            e = calculate_saturation_vapor_pressure(obs.dew_point)
            rh = 100.0 * e / e_sat if e_sat > 0 else None

            return {
                "pressure": p_gps,
                "temperature": t_gps,
                "station_id": station.station_id,
                "station_name": station.full_name,
                "distance": distance,
                "relative_humidity": rh,
                "met_temperature": obs.temperature,
                "met_dew_point": obs.dew_point,
                "met_pressure_msl": obs.msl_pressure,
            }

        return None

    def write_cost716_file(
        self,
        output_path: Path | str,
        project: str = "E-GVAP",
        processing_center: str = "ULRH",
        status: str = "TEST",
    ) -> None:
        """Write results in COST-716 V2.2 format.

        Args:
            output_path: Output file path
            project: Project name
            processing_center: Processing center code
            status: Product status (TEST, OPER)
        """
        path = Path(output_path)
        path.parent.mkdir(parents=True, exist_ok=True)

        # Group results by station
        station_results: dict[str, list[IWVResult]] = {}
        for r in self.results:
            if r.station_id not in station_results:
                station_results[r.station_id] = []
            station_results[r.station_id].append(r)

        with open(path, "w") as f:
            for station_id, results in sorted(station_results.items()):
                # Sort results by timestamp
                results = sorted(results, key=lambda x: x.timestamp)

                # Format header
                format_str = "COST-716 V2.2"
                f.write(f"{format_str:<20}     {project:<20}     {status:<20}\n")

                # Station info (DOMES placeholder)
                domes = "XXXXXXXXX"
                f.write(f"{station_id.upper():<4} {domes:<9}      Unknown\n")

                # Receiver/antenna (placeholders)
                f.write(f"{'UNKNOWN':<20}     {'UNKNOWN':<20}\n")

                # Position
                r0 = results[0]
                height_ortho = r0.height_ortho or r0.height
                f.write(
                    f"{r0.latitude:12.6f}{r0.longitude:12.6f}"
                    f"{r0.height:12.3f}{height_ortho:12.3f}{0.0:12.3f}\n"
                )

                # Time stamps
                ts = results[0].timestamp
                time_str = ts.strftime("%d-%b-%Y %H:%M:%S").upper()
                creation_str = datetime.now().strftime("%d-%b-%Y %H:%M:%S").upper()
                f.write(f"{time_str:<20}     {creation_str:<20}\n")

                # Processing info
                proc_method = "BERNESE V5.4"
                orbits = "IGSULT"
                source_met = "OBS/NEARBY"
                f.write(
                    f"{processing_center:<20}     {proc_method:<20}     "
                    f"{orbits:<20}     {source_met:<20}\n"
                )

                # Time parameters
                time_inc = 15
                time_upd = 60
                lobts = 300
                f.write(f"{time_inc:5d}{time_upd:5d}{lobts:5d}\n")

                # PDCH flags
                pdch = 0b1110101
                f.write(f"{pdch:08X}\n")

                # Number of samples
                f.write(f"{len(results):4d}\n")

                # Data records
                pcdd = "FFFFFFFF"
                grad_default = 999.99
                grad_error = -9.99
                tec_default = -99.999
                numb_slant = 0

                for r in results:
                    f.write(
                        f"{r.timestamp.hour:3d}{r.timestamp.minute:3d}"
                        f"{r.timestamp.second:3d}{pcdd:>9s}"
                        f"{r.ztd:7.1f}{r.ztd_sigma:7.1f}"
                        f"{r.zwd:7.1f}{r.iwv:7.1f}"
                        f"{r.pressure or -9.9:7.1f}"
                        f"{r.temperature or -9.9:7.1f}"
                        f"{r.relative_humidity or -9.9:7.1f}"
                        f"{grad_default:7.2f}{grad_default:7.2f}"
                        f"{grad_error:7.2f}{grad_error:7.2f}"
                        f"{tec_default:8.3f}\n"
                    )
                    f.write(f"{numb_slant:4d}\n")

                # Section separator
                f.write("-" * 100 + "\n\n\n")

        logger.info("Wrote COST-716 file", path=str(path), stations=len(station_results))

    def write_csv(self, output_path: Path | str) -> None:
        """Write results as CSV.

        Args:
            output_path: Output file path
        """
        path = Path(output_path)
        path.parent.mkdir(parents=True, exist_ok=True)

        with open(path, "w") as f:
            # Header
            f.write(
                "station,timestamp,mjd,ztd,ztd_sigma,zhd,zwd,iwv,iwv_sigma,"
                "lat,lon,height,height_ortho,pressure,temperature,"
                "met_station,met_distance,rh\n"
            )

            for r in self.results:
                f.write(
                    f"{r.station_id},{r.timestamp.isoformat()},{r.mjd:.6f},"
                    f"{r.ztd:.2f},{r.ztd_sigma:.2f},{r.zhd:.2f},{r.zwd:.2f},"
                    f"{r.iwv:.2f},{r.iwv_sigma:.2f},"
                    f"{r.latitude:.6f},{r.longitude:.6f},{r.height:.2f},"
                    f"{r.height_ortho or '':.2f},{r.pressure or '':.1f},"
                    f"{r.temperature or '':.1f},"
                    f"{r.met_station_id or ''},{r.met_distance or '':.0f},"
                    f"{r.relative_humidity or '':.1f}\n"
                )

        logger.info("Wrote CSV file", path=str(path), records=len(self.results))

    def write_iwv_log(self, output_path: Path | str) -> None:
        """Write detailed IWV conversion log.

        Matches the format of the Perl IWV_CONV.log files.

        Args:
            output_path: Output file path
        """
        path = Path(output_path)
        path.parent.mkdir(parents=True, exist_ok=True)

        with open(path, "w") as f:
            # Header
            f.write(
                "*sta mjd         ZTD    RMS_Z ZHD    ZWD   IWV   "
                "closest_met_station                  it metID lat_deg  "
                "lon_deg  met_h distance T     dewT  MSL_P  surf_P "
                "e_sat  e      RH    q       GPS_T MAT   GPS_P  "
                "h_ell    h_bench  h_ortho  h_geoid\n"
            )

            for r in self.results:
                f.write(
                    f"{r.station_id:4s}:{r.mjd:11.5f}:"
                    f"{r.ztd:6.1f}:{r.ztd_sigma:5.1f}:"
                    f"{r.zhd:6.1f}:{r.zwd:5.1f}:{r.iwv:5.1f}:"
                    f"{(r.met_station_name or 'UNKNOWN')[:35]:<35s}:01:"
                    f"{r.met_station_id or '00000':5s}:"
                    f"{r.latitude:8.3f}:{r.longitude:8.3f}:"
                    f"{r.height_ortho or 0:6.1f}:{r.met_distance or 0:8.0f}:"
                    f"{r.temperature or -9.9:5.1f}:-9.9:"
                    f"-9.9:-9.9:-9.9:-9.9:"
                    f"{r.relative_humidity or -9.9:5.1f}:-9.9:"
                    f"{r.temperature or -9.9:5.1f}:-9.9:"
                    f"{r.pressure or -9.9:6.1f}:"
                    f"{r.height:9.4f}:0.0000:"
                    f"{r.height_ortho or 0:9.4f}:0.000\n"
                )

        logger.info("Wrote IWV log file", path=str(path), records=len(self.results))


def read_ztd_file(file_path: Path | str) -> list[dict[str, Any]]:
    """Read ZTD values from Bernese TRP file.

    Args:
        file_path: Path to TRP file

    Returns:
        List of dictionaries with ZTD data
    """
    path = Path(file_path)
    results: list[dict[str, Any]] = []

    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or line.startswith("*"):
                continue

            parts = line.split()
            if len(parts) < 4:
                continue

            try:
                results.append({
                    "station": parts[0].lower(),
                    "mjd": float(parts[1]),
                    "ztd": float(parts[2]),
                    "ztd_sigma": float(parts[3]) if len(parts) > 3 else 1.0,
                })
            except (ValueError, IndexError):
                continue

    return results


def read_tro_file(file_path: Path | str) -> tuple[dict[str, dict], list[dict]]:
    """Read Bernese TRO (troposphere) file.

    Extracts both coordinates and ZTD values.

    Args:
        file_path: Path to TRO file

    Returns:
        Tuple of (coordinates dict, ZTD records list)
    """
    import re

    path = Path(file_path)
    coordinates: dict[str, dict] = {}
    ztd_records: list[dict] = []

    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            # Coordinate lines: station followed by X Y Z
            if re.match(r"^\s[A-Z0-9]{4}\s{1,2}\w{1,2}\s", line):
                parts = line.split()
                if len(parts) >= 7:
                    station = parts[0].upper()
                    coordinates[station] = {
                        "X": float(parts[4]),
                        "Y": float(parts[5]),
                        "Z": float(parts[6]),
                    }

            # ZTD lines: station YY:DOY:SOD ZTD STD
            elif re.match(r"^\s[A-Z0-9]{4}\s", line) and re.search(r"\d\d:\d{3}:\d{5}", line):
                parts = line.split()
                if len(parts) >= 4:
                    station = parts[0].upper()
                    time_parts = parts[1].split(":")
                    if len(time_parts) == 3:
                        from pygnss_rt.utils.format import year_2c_to_4c
                        from pygnss_rt.utils.dates import GNSSDate

                        year = year_2c_to_4c(int(time_parts[0]))
                        doy = int(time_parts[1])
                        sod = int(time_parts[2])

                        gd = GNSSDate.from_doy(year, doy)
                        hour = sod // 3600
                        minute = (sod % 3600) // 60
                        second = sod % 60

                        ztd_records.append({
                            "station": station.lower(),
                            "year": year,
                            "doy": doy,
                            "sod": sod,
                            "hour": hour,
                            "minute": minute,
                            "second": second,
                            "mjd": gd.mjd + sod / 86400.0,
                            "ztd": float(parts[2]),
                            "ztd_sigma": float(parts[3]),
                        })

    return coordinates, ztd_records
