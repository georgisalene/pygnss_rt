"""
Daily NRT Coordinate Generator.

Computes a-priori coordinates for NRDDP processing based on aligned
solutions from daily PPP runs over a 21-50 day window. Uses iterative
outlier rejection to produce robust mean coordinates.

Replaces the Perl script:
- iGNSS_D_CRD_54.pl / iGNSS_D_CRD.pl

Output files:
- DNR{YY}{DOY}0.CRD: Single-day coordinate solution
- ANR{YY}{DOY}0.CRD: Combined solution (current + previous day) for NRDDP

Usage:
    from pygnss_rt.processing.daily_crd import DailyCRDProcessor, DailyCRDConfig

    processor = DailyCRDProcessor()
    result = processor.process(year=2024, doy=260)

Cron usage:
    python -m pygnss_rt.processing.daily_crd --cron
"""

from __future__ import annotations

import gzip
import shutil
import statistics
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any


@dataclass
class NetworkArchive:
    """Configuration for a network's archived PPP solutions.

    Attributes:
        network_id: Network identifier (e.g., 'IG', 'EU', 'GB')
        root: Root archive directory
        organization: Directory organization ('yyyy/doy' or flat)
        campaign_pattern: Campaign directory pattern (e.g., 'YYDOYIG')
        prefix: File prefix (e.g., 'AIG')
        body_pattern: File body pattern (e.g., 'YYDOY0')
        directory: Subdirectory within campaign (e.g., 'STA')
        extension: File extension (e.g., '.CRD')
        compression: Compression extension (e.g., '.gz')
    """

    network_id: str
    root: Path
    organization: str = "yyyy/doy"
    campaign_pattern: str = "YYDOYIG"
    prefix: str = "AIG"
    body_pattern: str = "YYDOY0"
    directory: str = "STA"
    extension: str = ".CRD"
    compression: str = ".gz"


@dataclass
class DailyCRDConfig:
    """Configuration for daily CRD processing.

    Attributes:
        output_dir: Directory for output CRD files
        ppp_root: Root directory for archived PPP solutions
        networks: List of network configurations to process
        window_start_days: Start of averaging window (days before current)
        window_end_days: End of averaging window (days before current)
        max_iterations: Maximum outlier rejection iterations
        outlier_sigma: Sigma level for outlier rejection
        min_records: Minimum records required per station
        max_std_meters: Maximum allowed std deviation (meters)
        datum: Reference datum name
        latency_hours: Processing latency in hours for cron mode
    """

    output_dir: Path = field(
        default_factory=lambda: Path("/home/nrt105/data54/nrtCoord")
    )
    ppp_root: Path = field(
        default_factory=lambda: Path("/home/nrt105/data54/campaigns/ppp")
    )

    networks: list[NetworkArchive] = field(default_factory=list)

    # Averaging window
    window_start_days: int = 51  # Start of window (days back from current)
    window_end_days: int = 22  # End of window (days back from current)

    # Outlier rejection parameters
    max_iterations: int = 10
    outlier_sigma: float = 3.0
    min_records: int = 7
    max_std_meters: float = 0.010  # 10mm

    # Reference frame
    datum: str = "IGS20"

    # Cron mode settings
    latency_hours: int = 12


@dataclass
class StationCoordinate:
    """Coordinate for a single station.

    Attributes:
        station: 4-character station ID
        x: X coordinate (meters)
        y: Y coordinate (meters)
        z: Z coordinate (meters)
        std_x: Standard deviation of X (meters)
        std_y: Standard deviation of Y (meters)
        std_z: Standard deviation of Z (meters)
        n_records: Number of records used
        flag: Coordinate flag/quality indicator
    """

    station: str
    x: float
    y: float
    z: float
    std_x: float = 0.0
    std_y: float = 0.0
    std_z: float = 0.0
    n_records: int = 0
    flag: str = "D"  # D = Derived


@dataclass
class DailyCRDResult:
    """Result of daily CRD processing.

    Attributes:
        year: Processing year
        doy: Day of year
        success: Whether processing succeeded
        dnr_file: Path to DNR (single-day) CRD file
        anr_file: Path to ANR (combined) CRD file
        n_stations: Number of stations in final solution
        n_rejected: Number of rejected stations
        processing_time: Processing time in seconds
        error_message: Error message if failed
    """

    year: int
    doy: int
    success: bool = False
    dnr_file: Path | None = None
    anr_file: Path | None = None
    n_stations: int = 0
    n_rejected: int = 0
    processing_time: float = 0.0
    error_message: str = ""


class DailyCRDProcessor:
    """Processor for computing daily NRT coordinates.

    Collects coordinates from archived PPP solutions over a configurable
    window (default 21-50 days), performs iterative outlier rejection,
    and produces robust mean coordinates for NRDDP processing.

    Usage:
        processor = DailyCRDProcessor()

        # Process specific date
        result = processor.process(year=2024, doy=260)

        # Process in cron mode
        result = processor.process_cron()
    """

    def __init__(
        self,
        config: DailyCRDConfig | None = None,
        verbose: bool = False,
    ):
        """Initialize processor.

        Args:
            config: Processing configuration
            verbose: Enable verbose output
        """
        self.config = config or self._create_default_config()
        self.verbose = verbose
        self._log_file = None

    def _create_default_config(self) -> DailyCRDConfig:
        """Create default configuration with standard networks."""
        ppp_root = Path("/home/nrt105/data54/campaigns/ppp")

        networks = [
            NetworkArchive(
                network_id="IG",
                root=ppp_root,
                campaign_pattern="YYDOYIG",
                prefix="AIG",
            ),
            NetworkArchive(
                network_id="EU",
                root=ppp_root,
                campaign_pattern="YYDOYEU",
                prefix="AEU",
            ),
            NetworkArchive(
                network_id="GB",
                root=ppp_root,
                campaign_pattern="YYDOYGB",
                prefix="AGB",
            ),
            NetworkArchive(
                network_id="IR",
                root=ppp_root,
                campaign_pattern="YYDOYIR",
                prefix="AIR",
            ),
            NetworkArchive(
                network_id="IS",
                root=ppp_root,
                campaign_pattern="YYDOYIS",
                prefix="AIS",
            ),
            NetworkArchive(
                network_id="RG",
                root=ppp_root,
                campaign_pattern="YYDOYRG",
                prefix="ARG",
            ),
            NetworkArchive(
                network_id="SS",
                root=ppp_root,
                campaign_pattern="YYDOYSS",
                prefix="ASS",
            ),
            NetworkArchive(
                network_id="CA",
                root=ppp_root,
                campaign_pattern="YYDOYCA",
                prefix="ACA",
            ),
        ]

        return DailyCRDConfig(networks=networks)

    def _log(self, message: str) -> None:
        """Write to log file if open."""
        if self._log_file:
            self._log_file.write(message + "\n")
        if self.verbose:
            print(message)

    def process(self, year: int, doy: int) -> DailyCRDResult:
        """Process coordinates for a specific date.

        Args:
            year: Processing year
            doy: Day of year

        Returns:
            Processing result
        """
        start_time = datetime.now(timezone.utc)
        result = DailyCRDResult(year=year, doy=doy)

        # Create output filenames
        y2c = f"{year % 100:02d}"
        dnr_filename = f"DNR{y2c}{doy:03d}0.CRD"
        anr_filename = f"ANR{y2c}{doy:03d}0.CRD"
        log_filename = f"ANR{y2c}{doy:03d}0.log"

        dnr_path = self.config.output_dir / dnr_filename
        anr_path = self.config.output_dir / anr_filename
        log_path = self.config.output_dir / log_filename

        result.dnr_file = dnr_path
        result.anr_file = anr_path

        try:
            # Open log file
            self._log_file = open(log_path, 'w')

            self._log("=" * 80)
            self._log(f"NRDDP CRD FILES FOR {year}/{doy:03d}")
            self._log(f"Window: MJD-{self.config.window_start_days} to MJD-{self.config.window_end_days}")
            self._log("=" * 80)

            # Collect coordinates from archived PPP solutions
            station_data = self._collect_coordinates(year, doy)

            if not station_data:
                result.error_message = "No coordinate data found"
                self._log(f"ERROR: {result.error_message}")
                return result

            self._log(f"\nCollected data for {len(station_data)} stations")

            # Compute robust mean coordinates
            self._log("\n\nCOMPUTING SOLUTION\n")
            coordinates, rejected = self._compute_coordinates(station_data)

            if not coordinates:
                result.error_message = "No valid coordinates computed"
                self._log(f"ERROR: {result.error_message}")
                return result

            result.n_stations = len(coordinates)
            result.n_rejected = rejected

            self._log(f"\nAccepted: {len(coordinates)} stations")
            self._log(f"Rejected: {rejected} stations")

            # Write DNR (single-day) CRD file
            self._write_crd_file(dnr_path, coordinates, year, doy)
            self._log(f"\nWrote DNR file: {dnr_path}")

            # Combine with previous day to produce ANR file
            self._combine_with_previous_day(
                dnr_path, anr_path, year, doy
            )
            self._log(f"Wrote ANR file: {anr_path}")
            self._log(f"\nFILE TO USE FOR NRDDP: {anr_path}")

            result.success = True

        except Exception as e:
            result.error_message = str(e)
            self._log(f"ERROR: {e}")

        finally:
            if self._log_file:
                self._log_file.close()
                self._log_file = None

            result.processing_time = (
                datetime.now(timezone.utc) - start_time
            ).total_seconds()

        return result

    def process_cron(self) -> DailyCRDResult:
        """Process in cron mode using current time and latency.

        Returns:
            Processing result
        """
        now = datetime.now(timezone.utc)
        proc_time = now - timedelta(hours=self.config.latency_hours)

        year = proc_time.year
        doy = proc_time.timetuple().tm_yday

        if self.verbose:
            print(f"CRON mode: Processing {year}/{doy:03d} (latency: {self.config.latency_hours}h)")

        return self.process(year, doy)

    def _collect_coordinates(
        self,
        year: int,
        doy: int,
    ) -> dict[str, dict[int, dict[str, float]]]:
        """Collect coordinates from archived PPP solutions.

        Args:
            year: Target year
            doy: Target DOY

        Returns:
            Nested dict: station -> mjd -> {X, Y, Z}
        """
        from pygnss_rt.utils.dates import mjd_from_date

        # Calculate MJD for target date
        target_dt = datetime(year, 1, 1) + timedelta(days=doy - 1)
        target_mjd = mjd_from_date(target_dt.year, target_dt.month, target_dt.day)

        # Calculate window
        start_mjd = int(target_mjd) - self.config.window_start_days
        end_mjd = int(target_mjd) - self.config.window_end_days

        self._log(f"\nPeriod: MJD {start_mjd} to {end_mjd}")

        # Create temp directory for decompression
        tmp_dir = Path("/tmp/i-GNSS/D_CRD") / str(datetime.now().timestamp())
        tmp_dir.mkdir(parents=True, exist_ok=True)

        station_data: dict[str, dict[int, dict[str, float]]] = {}

        try:
            for network in self.config.networks:
                self._log(f"\n  SOLUTION: {network.network_id}")

                for mjd in range(start_mjd, end_mjd + 1):
                    crd_file = self._get_archive_file(network, mjd, tmp_dir)

                    if crd_file and crd_file.exists():
                        self._load_crd_file(crd_file, mjd, station_data)

        finally:
            # Clean up temp directory
            if tmp_dir.exists():
                shutil.rmtree(tmp_dir)

        return station_data

    def _get_archive_file(
        self,
        network: NetworkArchive,
        mjd: int,
        tmp_dir: Path,
    ) -> Path | None:
        """Get and decompress an archived CRD file.

        Args:
            network: Network configuration
            mjd: Modified Julian Date
            tmp_dir: Temporary directory for decompression

        Returns:
            Path to decompressed file, or None if not found
        """
        from pygnss_rt.utils.dates import date_from_mjd

        # Get year/doy from MJD
        dt = date_from_mjd(mjd)
        year = dt.year
        doy = dt.timetuple().tm_yday
        y2c = f"{year % 100:02d}"

        # Build campaign directory path
        if network.organization == "yyyy/doy":
            campaign_path = network.root / str(year) / f"{doy:03d}"
        else:
            campaign_path = network.root

        # Add campaign subdirectory based on pattern
        if "YYDOY" in network.campaign_pattern:
            suffix = network.campaign_pattern.replace("YYDOY", "")
            campaign_dir = f"{y2c}{doy:03d}{suffix}"
            campaign_path = campaign_path / campaign_dir

        # Add STA directory
        campaign_path = campaign_path / network.directory

        # Build filename
        if network.body_pattern == "YYDOY0":
            filename = f"{network.prefix}{y2c}{doy:03d}0{network.extension}"
        else:
            filename = f"{network.prefix}{y2c}{doy:03d}{network.extension}"

        compressed_file = campaign_path / f"{filename}{network.compression}"
        uncompressed_file = tmp_dir / filename

        if not compressed_file.exists():
            self._log(f"    Missing: {compressed_file}")
            return None

        # Decompress
        try:
            if network.compression == ".gz":
                with gzip.open(compressed_file, 'rb') as f_in:
                    with open(uncompressed_file, 'wb') as f_out:
                        f_out.write(f_in.read())
            else:
                shutil.copy(compressed_file, uncompressed_file)

            return uncompressed_file

        except Exception as e:
            self._log(f"    Decompression failed: {e}")
            return None

    def _load_crd_file(
        self,
        path: Path,
        mjd: int,
        data: dict[str, dict[int, dict[str, float]]],
    ) -> None:
        """Load coordinates from a CRD file.

        Args:
            path: Path to CRD file
            mjd: MJD for this file
            data: Dictionary to update
        """
        try:
            with open(path, 'r') as f:
                for line_num, line in enumerate(f, 1):
                    # Skip header (first 6 lines typically)
                    if line_num <= 6:
                        continue

                    # Skip lines without decimal point (not coordinate lines)
                    if '.' not in line:
                        continue

                    try:
                        # Parse BSW CRD format
                        # NUM  STATION DOMES       X (M)          Y (M)          Z (M)
                        station = line[5:9].strip().upper()
                        x = float(line[22:36])
                        y = float(line[37:51])
                        z = float(line[52:66])

                        if station not in data:
                            data[station] = {}

                        data[station][mjd] = {"X": x, "Y": y, "Z": z}

                    except (ValueError, IndexError):
                        continue

        except Exception as e:
            self._log(f"    Error loading {path}: {e}")

    def _compute_coordinates(
        self,
        station_data: dict[str, dict[int, dict[str, float]]],
    ) -> tuple[list[StationCoordinate], int]:
        """Compute robust mean coordinates with outlier rejection.

        Args:
            station_data: Collected coordinate data

        Returns:
            Tuple of (accepted coordinates, number rejected)
        """
        coordinates = []
        rejected = 0

        for station in sorted(station_data.keys()):
            mjd_data = station_data[station]

            # Extract coordinate arrays
            x_vals = [mjd_data[m]["X"] for m in mjd_data]
            y_vals = [mjd_data[m]["Y"] for m in mjd_data]
            z_vals = [mjd_data[m]["Z"] for m in mjd_data]

            if len(x_vals) < 2:
                self._log(f"    Less than 2 records for station {station}! Skipped")
                rejected += 1
                continue

            # Iterative outlier rejection
            for iteration in range(self.config.max_iterations):
                if len(x_vals) < 2:
                    break

                avg_x = statistics.mean(x_vals)
                avg_y = statistics.mean(y_vals)
                avg_z = statistics.mean(z_vals)

                std_x = statistics.stdev(x_vals) if len(x_vals) > 1 else 0
                std_y = statistics.stdev(y_vals) if len(y_vals) > 1 else 0
                std_z = statistics.stdev(z_vals) if len(z_vals) > 1 else 0

                # Find outliers
                outlier_found = False
                new_x, new_y, new_z = [], [], []

                for i in range(len(x_vals)):
                    diff_x = abs(x_vals[i] - avg_x)
                    diff_y = abs(y_vals[i] - avg_y)
                    diff_z = abs(z_vals[i] - avg_z)

                    threshold_x = self.config.outlier_sigma * std_x if std_x > 0 else float('inf')
                    threshold_y = self.config.outlier_sigma * std_y if std_y > 0 else float('inf')
                    threshold_z = self.config.outlier_sigma * std_z if std_z > 0 else float('inf')

                    if diff_x > threshold_x or diff_y > threshold_y or diff_z > threshold_z:
                        self._log(
                            f"    *{station}: remove outlier at it.{iteration+1} "
                            f"({diff_x:.4f}, {diff_y:.4f}, {diff_z:.4f})"
                        )
                        outlier_found = True
                    else:
                        new_x.append(x_vals[i])
                        new_y.append(y_vals[i])
                        new_z.append(z_vals[i])

                if not outlier_found:
                    break

                x_vals, y_vals, z_vals = new_x, new_y, new_z

            # Final statistics
            n_records = len(x_vals)

            if n_records < self.config.min_records:
                self._log(
                    f" ** REJECT {station} because less than "
                    f"{self.config.min_records} ({n_records}) records"
                )
                rejected += 1
                continue

            avg_x = statistics.mean(x_vals)
            avg_y = statistics.mean(y_vals)
            avg_z = statistics.mean(z_vals)

            std_x = statistics.stdev(x_vals) if len(x_vals) > 1 else 0
            std_y = statistics.stdev(y_vals) if len(y_vals) > 1 else 0
            std_z = statistics.stdev(z_vals) if len(z_vals) > 1 else 0

            if (std_x >= self.config.max_std_meters or
                std_y >= self.config.max_std_meters or
                std_z >= self.config.max_std_meters):
                self._log(
                    f" ** REJECT {station:4s} "
                    f"{avg_x:15.4f} +/- {std_x:6.4f}  "
                    f"{avg_y:15.4f} +/- {std_y:6.4f}  "
                    f"{avg_z:15.4f} +/- {std_z:6.4f}"
                )
                rejected += 1
                continue

            # Accept station
            self._log(
                f"    ACCEPT {station:4s} "
                f"{avg_x:15.4f} +/- {std_x:6.4f}  "
                f"{avg_y:15.4f} +/- {std_y:6.4f}  "
                f"{avg_z:15.4f} +/- {std_z:6.4f} ({n_records:2d})"
            )

            coordinates.append(StationCoordinate(
                station=station,
                x=avg_x,
                y=avg_y,
                z=avg_z,
                std_x=std_x,
                std_y=std_y,
                std_z=std_z,
                n_records=n_records,
            ))

        return coordinates, rejected

    def _write_crd_file(
        self,
        path: Path,
        coordinates: list[StationCoordinate],
        year: int,
        doy: int,
    ) -> None:
        """Write BSW CRD file.

        Args:
            path: Output path
            coordinates: Coordinate list
            year: Year
            doy: Day of year
        """
        now = datetime.now()

        # Get date components
        target_dt = datetime(year, 1, 1) + timedelta(days=doy - 1)

        with open(path, 'w') as f:
            # Header line with filename and creation timestamp
            f.write(
                f"{path.name:<63s} "
                f"{now.year:4d}-{now.month:02d}-{now.day:02d} "
                f"{now.hour:02d}:{now.minute:02d} \n"
            )
            f.write("-" * 80 + "\n")
            f.write(
                f"{'LOCAL GEODETIC DATUM: ' + self.config.datum:<40s}"
                f"EPOCH: {target_dt.year:4d}-{target_dt.month:02d}-{target_dt.day:02d} 12:00:00\n\n"
            )
            f.write(
                "NUM  STATION NAME           X (M)          Y (M)          Z (M)     FLAG\n\n"
            )

            for num, coord in enumerate(coordinates, 1):
                f.write(
                    f"{num:3d}  {coord.station:<16s}"
                    f"{coord.x:15.5f}{coord.y:15.5f}{coord.z:15.5f}    {coord.flag:1s}\n"
                )

    def _combine_with_previous_day(
        self,
        dnr_path: Path,
        anr_path: Path,
        year: int,
        doy: int,
    ) -> None:
        """Combine current DNR file with previous day's file.

        For ADDNEQ and early hours of the day, some stations may be
        missing from today's solution. We combine with yesterday's
        solution to fill gaps.

        Args:
            dnr_path: Current day's DNR file
            anr_path: Output ANR file path
            year: Year
            doy: Day of year
        """
        # Calculate previous day
        target_dt = datetime(year, 1, 1) + timedelta(days=doy - 1)
        prev_dt = target_dt - timedelta(days=1)
        prev_year = prev_dt.year
        prev_doy = prev_dt.timetuple().tm_yday
        y2c_prev = f"{prev_year % 100:02d}"

        # Previous day's DNR file
        prev_dnr_filename = f"DNR{y2c_prev}{prev_doy:03d}0.CRD"
        prev_dnr_path = self.config.output_dir / prev_dnr_filename

        self._log(f"\n  Combining last 2 files:")
        self._log(f"    current: {dnr_path}")
        self._log(f"    previous: {prev_dnr_path}")

        # Load current day's coordinates
        current_coords = self._load_simple_crd(dnr_path)

        # Load previous day's coordinates
        if prev_dnr_path.exists():
            prev_coords = self._load_simple_crd(prev_dnr_path)
        else:
            self._log(f"    Previous file missing, using current only")
            prev_coords = {}

        # Find stations in previous but not in current
        current_stations = set(current_coords.keys())
        prev_stations = set(prev_coords.keys())

        missing_stations = prev_stations - current_stations
        new_stations = current_stations - prev_stations

        if missing_stations:
            self._log(f"    MISSING STATIONS: {' '.join(sorted(missing_stations))}")

        # Combine: use current for all current stations, fill from previous
        combined: dict[str, tuple[float, float, float]] = {}

        for station in current_stations:
            combined[station] = current_coords[station]

        for station in missing_stations:
            combined[station] = prev_coords[station]

        # Write combined ANR file
        combined_coords = [
            StationCoordinate(
                station=sta,
                x=xyz[0],
                y=xyz[1],
                z=xyz[2],
            )
            for sta, xyz in sorted(combined.items())
        ]

        self._write_crd_file(anr_path, combined_coords, year, doy)

    def _load_simple_crd(self, path: Path) -> dict[str, tuple[float, float, float]]:
        """Load coordinates from a CRD file.

        Args:
            path: Path to CRD file

        Returns:
            Dict mapping station to (X, Y, Z) tuple
        """
        coords: dict[str, tuple[float, float, float]] = {}

        try:
            with open(path, 'r') as f:
                for line_num, line in enumerate(f, 1):
                    if line_num <= 6:
                        continue
                    if '.' not in line:
                        continue

                    try:
                        station = line[5:9].strip().upper()
                        x = float(line[22:36])
                        y = float(line[37:51])
                        z = float(line[52:66])
                        coords[station] = (x, y, z)
                    except (ValueError, IndexError):
                        continue

        except Exception:
            pass

        return coords


def create_daily_crd_config(
    output_dir: str = "/home/nrt105/data54/nrtCoord",
    ppp_root: str = "/home/nrt105/data54/campaigns/ppp",
) -> DailyCRDConfig:
    """Create a daily CRD configuration.

    Args:
        output_dir: Output directory for CRD files
        ppp_root: Root directory for PPP archives

    Returns:
        DailyCRDConfig instance
    """
    ppp_path = Path(ppp_root)

    networks = [
        NetworkArchive(network_id="IG", root=ppp_path, campaign_pattern="YYDOYIG", prefix="AIG"),
        NetworkArchive(network_id="EU", root=ppp_path, campaign_pattern="YYDOYEU", prefix="AEU"),
        NetworkArchive(network_id="GB", root=ppp_path, campaign_pattern="YYDOYGB", prefix="AGB"),
        NetworkArchive(network_id="IR", root=ppp_path, campaign_pattern="YYDOYIR", prefix="AIR"),
        NetworkArchive(network_id="IS", root=ppp_path, campaign_pattern="YYDOYIS", prefix="AIS"),
        NetworkArchive(network_id="RG", root=ppp_path, campaign_pattern="YYDOYRG", prefix="ARG"),
        NetworkArchive(network_id="SS", root=ppp_path, campaign_pattern="YYDOYSS", prefix="ASS"),
        NetworkArchive(network_id="CA", root=ppp_path, campaign_pattern="YYDOYCA", prefix="ACA"),
    ]

    return DailyCRDConfig(
        output_dir=Path(output_dir),
        ppp_root=ppp_path,
        networks=networks,
    )


def main():
    """Main entry point for command-line usage."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Daily NRT Coordinate Generator"
    )
    parser.add_argument(
        "--year", "-y",
        type=int,
        help="Processing year",
    )
    parser.add_argument(
        "--doy", "-d",
        type=int,
        help="Day of year",
    )
    parser.add_argument(
        "--cron",
        action="store_true",
        help="Run in cron mode (auto-detect date)",
    )
    parser.add_argument(
        "--latency",
        type=int,
        default=12,
        help="Latency in hours for cron mode (default: 12)",
    )
    parser.add_argument(
        "--output-dir", "-o",
        type=str,
        default="/home/nrt105/data54/nrtCoord",
        help="Output directory for CRD files",
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable verbose output",
    )

    args = parser.parse_args()

    # Create config
    config = create_daily_crd_config(output_dir=args.output_dir)
    config.latency_hours = args.latency

    # Create processor
    processor = DailyCRDProcessor(config=config, verbose=args.verbose)

    # Process
    if args.cron:
        result = processor.process_cron()
    elif args.year and args.doy:
        result = processor.process(year=args.year, doy=args.doy)
    else:
        parser.error("Either --cron or both --year and --doy are required")

    # Report result
    if result.success:
        print(f"Success: Generated {result.n_stations} coordinates")
        print(f"  DNR: {result.dnr_file}")
        print(f"  ANR: {result.anr_file}")
        return 0
    else:
        print(f"Failed: {result.error_message}")
        return 1


if __name__ == "__main__":
    import sys
    sys.exit(main())
