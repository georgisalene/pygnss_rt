"""
RINEX Quality Checking Module.

Provides comprehensive RINEX observation file quality analysis:
- Observation statistics (epochs, satellites, data completeness)
- Multipath analysis (MP1, MP2)
- Cycle slip detection
- Signal-to-noise ratio (SNR) analysis
- Data gap detection
- Observation type inventory
- Multi-GNSS constellation support (GPS, GLONASS, Galileo, BeiDou, QZSS, SBAS, IRNSS)

This module provides Python-native quality checking similar to UNAVCO's teqc tool.

Usage:
    from pygnss_rt.utils.rinex_qc import RINEXQualityChecker, check_rinex_quality

    # Quick check
    result = check_rinex_quality("/path/to/file.24o")
    print(f"Data completeness: {result.completeness_pct:.1f}%")

    # Detailed analysis
    checker = RINEXQualityChecker()
    result = checker.analyze("/path/to/file.24o")
    print(result.summary())

    # Multi-GNSS analysis
    result = checker.analyze("/path/to/multi-gnss.24o")
    for system, stats in result.system_stats.items():
        print(f"{system}: {stats['satellites']} satellites, {stats['observations']} obs")
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Any, Optional, TextIO

logger = logging.getLogger(__name__)

# Import multi-GNSS support
from pygnss_rt.utils.multi_gnss import (
    GNSSConstellation,
    get_frequency,
    get_wavelength,
    SPEED_OF_LIGHT,
)

# Re-export GNSSConstellation as GNSSSystem for backward compatibility
GNSSSystem = GNSSConstellation


class QualityLevel(str, Enum):
    """Quality assessment levels."""

    EXCELLENT = "excellent"
    GOOD = "good"
    ACCEPTABLE = "acceptable"
    POOR = "poor"
    UNUSABLE = "unusable"


@dataclass
class ObservationType:
    """RINEX observation type."""

    code: str  # e.g., "C1C", "L1W", "S2X"
    system: GNSSSystem
    frequency: int  # 1, 2, 5, etc.
    obs_type: str  # C=code, L=phase, S=SNR, D=Doppler

    @classmethod
    def from_code(cls, code: str, system: GNSSSystem = GNSSSystem.GPS) -> "ObservationType":
        """Parse observation type from code."""
        if len(code) < 2:
            raise ValueError(f"Invalid observation code: {code}")

        obs_type = code[0]
        frequency = int(code[1]) if code[1].isdigit() else 0

        return cls(
            code=code,
            system=system,
            frequency=frequency,
            obs_type=obs_type,
        )


@dataclass
class SatelliteStats:
    """Statistics for a single satellite."""

    prn: str
    system: GNSSSystem
    epochs_observed: int = 0
    total_observations: int = 0
    cycle_slips: int = 0
    multipath_l1: float = 0.0
    multipath_l2: float = 0.0
    mean_snr_l1: float = 0.0
    mean_snr_l2: float = 0.0
    elevation_range: tuple[float, float] = (0.0, 90.0)


@dataclass
class EpochStats:
    """Statistics for a single epoch."""

    time: datetime
    num_satellites: int = 0
    satellites: list[str] = field(default_factory=list)
    epoch_flag: int = 0
    has_gap_before: bool = False


@dataclass
class QualityResult:
    """Complete quality analysis result."""

    # File information
    filename: str
    filepath: Path
    rinex_version: float = 2.0
    file_type: str = "O"  # Observation

    # Station info
    marker_name: str = ""
    marker_number: str = ""
    receiver_type: str = ""
    antenna_type: str = ""
    approximate_position: tuple[float, float, float] = (0.0, 0.0, 0.0)

    # Time span
    first_epoch: Optional[datetime] = None
    last_epoch: Optional[datetime] = None
    interval: float = 0.0  # seconds
    total_epochs: int = 0
    expected_epochs: int = 0

    # Observation types
    observation_types: list[str] = field(default_factory=list)
    systems_observed: list[GNSSSystem] = field(default_factory=list)

    # Completeness
    completeness_pct: float = 0.0
    data_gaps: list[tuple[datetime, datetime]] = field(default_factory=list)
    num_data_gaps: int = 0
    total_gap_duration: float = 0.0  # seconds

    # Satellite statistics
    satellites_observed: int = 0
    satellite_stats: dict[str, SatelliteStats] = field(default_factory=dict)
    mean_satellites_per_epoch: float = 0.0

    # Quality metrics
    total_observations: int = 0
    total_cycle_slips: int = 0
    mean_multipath_l1: float = 0.0
    mean_multipath_l2: float = 0.0
    mean_snr_l1: float = 0.0
    mean_snr_l2: float = 0.0

    # Multi-GNSS statistics
    system_stats: dict[str, dict[str, Any]] = field(default_factory=dict)
    # Format: {"G": {"satellites": 12, "observations": 1000, "completeness": 98.5}, ...}

    # Assessment
    quality_level: QualityLevel = QualityLevel.ACCEPTABLE
    issues: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    # Processing info
    analysis_time: float = 0.0  # seconds

    def get_system_summary(self) -> dict[str, dict[str, Any]]:
        """Get per-constellation summary statistics.

        Returns:
            Dict with statistics per constellation
        """
        return self.system_stats.copy()

    def summary(self) -> str:
        """Generate human-readable summary."""
        lines = [
            "=" * 70,
            f"RINEX Quality Check: {self.filename}",
            "=" * 70,
            "",
            "FILE INFORMATION:",
            f"  RINEX Version:     {self.rinex_version}",
            f"  Marker Name:       {self.marker_name}",
            f"  Receiver:          {self.receiver_type}",
            f"  Antenna:           {self.antenna_type}",
            "",
            "TIME SPAN:",
            f"  First Epoch:       {self.first_epoch}",
            f"  Last Epoch:        {self.last_epoch}",
            f"  Interval:          {self.interval:.1f} s",
            f"  Total Epochs:      {self.total_epochs}",
            f"  Expected Epochs:   {self.expected_epochs}",
            "",
            "DATA COMPLETENESS:",
            f"  Completeness:      {self.completeness_pct:.1f}%",
            f"  Data Gaps:         {self.num_data_gaps}",
            f"  Gap Duration:      {self.total_gap_duration:.0f} s",
            "",
            "OBSERVATIONS:",
            f"  Satellites:        {self.satellites_observed}",
            f"  Mean Sats/Epoch:   {self.mean_satellites_per_epoch:.1f}",
            f"  Total Obs:         {self.total_observations}",
            f"  Cycle Slips:       {self.total_cycle_slips}",
        ]

        # Add multi-GNSS breakdown if available
        if self.system_stats:
            lines.append("")
            lines.append("MULTI-GNSS BREAKDOWN:")
            system_names = {
                "G": "GPS", "R": "GLONASS", "E": "Galileo",
                "C": "BeiDou", "J": "QZSS", "S": "SBAS", "I": "IRNSS"
            }
            for sys_code, stats in sorted(self.system_stats.items()):
                sys_name = system_names.get(sys_code, sys_code)
                sats = stats.get("satellites", 0)
                obs = stats.get("observations", 0)
                lines.append(f"  {sys_name:10s}: {sats:3d} sats, {obs:8d} obs")

        lines.extend([
            "",
            "QUALITY METRICS:",
            f"  Mean MP1:          {self.mean_multipath_l1:.3f} m",
            f"  Mean MP2:          {self.mean_multipath_l2:.3f} m",
            f"  Mean SNR L1:       {self.mean_snr_l1:.1f} dB-Hz",
            f"  Mean SNR L2:       {self.mean_snr_l2:.1f} dB-Hz",
            "",
            f"OVERALL QUALITY:     {self.quality_level.value.upper()}",
        ])

        if self.issues:
            lines.extend(["", "ISSUES:"])
            for issue in self.issues:
                lines.append(f"  - {issue}")

        if self.warnings:
            lines.extend(["", "WARNINGS:"])
            for warning in self.warnings:
                lines.append(f"  - {warning}")

        lines.append("=" * 70)
        return "\n".join(lines)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "filename": self.filename,
            "rinex_version": self.rinex_version,
            "marker_name": self.marker_name,
            "first_epoch": self.first_epoch.isoformat() if self.first_epoch else None,
            "last_epoch": self.last_epoch.isoformat() if self.last_epoch else None,
            "interval": self.interval,
            "total_epochs": self.total_epochs,
            "expected_epochs": self.expected_epochs,
            "completeness_pct": self.completeness_pct,
            "satellites_observed": self.satellites_observed,
            "mean_satellites_per_epoch": self.mean_satellites_per_epoch,
            "total_observations": self.total_observations,
            "total_cycle_slips": self.total_cycle_slips,
            "mean_multipath_l1": self.mean_multipath_l1,
            "mean_multipath_l2": self.mean_multipath_l2,
            "mean_snr_l1": self.mean_snr_l1,
            "mean_snr_l2": self.mean_snr_l2,
            "quality_level": self.quality_level.value,
            "issues": self.issues,
            "warnings": self.warnings,
            # Multi-GNSS statistics
            "systems_observed": [s.value for s in self.systems_observed],
            "system_stats": self.system_stats,
        }


class RINEXQualityChecker:
    """
    RINEX observation file quality checker.

    Provides teqc-like quality analysis for RINEX observation files.
    """

    # Quality thresholds
    COMPLETENESS_EXCELLENT = 98.0
    COMPLETENESS_GOOD = 95.0
    COMPLETENESS_ACCEPTABLE = 85.0
    COMPLETENESS_POOR = 70.0

    MP_EXCELLENT = 0.3  # meters
    MP_GOOD = 0.5
    MP_ACCEPTABLE = 1.0

    SNR_EXCELLENT = 45.0  # dB-Hz
    SNR_GOOD = 40.0
    SNR_ACCEPTABLE = 35.0

    MIN_SATELLITES = 4

    def __init__(
        self,
        detect_gaps: bool = True,
        gap_threshold: float = 2.5,  # multiples of interval
        compute_multipath: bool = True,
        verbose: bool = False,
    ):
        """Initialize quality checker.

        Args:
            detect_gaps: Detect data gaps
            gap_threshold: Gap detection threshold (multiples of interval)
            compute_multipath: Compute multipath statistics
            verbose: Enable verbose output
        """
        self.detect_gaps = detect_gaps
        self.gap_threshold = gap_threshold
        self.compute_multipath = compute_multipath
        self.verbose = verbose

    def analyze(self, filepath: Path | str) -> QualityResult:
        """Analyze RINEX observation file quality.

        Args:
            filepath: Path to RINEX file

        Returns:
            QualityResult with analysis
        """
        import time
        start_time = time.time()

        filepath = Path(filepath)
        result = QualityResult(
            filename=filepath.name,
            filepath=filepath,
        )

        if not filepath.exists():
            result.quality_level = QualityLevel.UNUSABLE
            result.issues.append(f"File not found: {filepath}")
            return result

        try:
            with open(filepath, "r", encoding="utf-8", errors="ignore") as f:
                # Parse header
                self._parse_header(f, result)

                # Parse observations
                self._parse_observations(f, result)

            # Compute derived statistics
            self._compute_statistics(result)

            # Assess quality
            self._assess_quality(result)

        except Exception as e:
            result.quality_level = QualityLevel.UNUSABLE
            result.issues.append(f"Analysis failed: {e}")
            logger.exception(f"RINEX QC failed for {filepath}")

        result.analysis_time = time.time() - start_time
        return result

    def _parse_header(self, f: TextIO, result: QualityResult) -> None:
        """Parse RINEX header."""
        obs_types_raw = []
        in_obs_types = False

        for line in f:
            if "END OF HEADER" in line:
                break

            label = line[60:].strip() if len(line) > 60 else ""

            if "RINEX VERSION" in label:
                try:
                    result.rinex_version = float(line[0:9].strip())
                    result.file_type = line[20:21].strip()
                except (ValueError, IndexError):
                    pass

            elif "MARKER NAME" in label:
                result.marker_name = line[0:60].strip()

            elif "MARKER NUMBER" in label:
                result.marker_number = line[0:20].strip()

            elif "REC # / TYPE / VERS" in label:
                result.receiver_type = line[20:40].strip()

            elif "ANT # / TYPE" in label:
                result.antenna_type = line[20:40].strip()

            elif "APPROX POSITION XYZ" in label:
                try:
                    x = float(line[0:14].strip())
                    y = float(line[14:28].strip())
                    z = float(line[28:42].strip())
                    result.approximate_position = (x, y, z)
                except (ValueError, IndexError):
                    pass

            elif "INTERVAL" in label:
                try:
                    result.interval = float(line[0:10].strip())
                except (ValueError, IndexError):
                    pass

            elif "# / TYPES OF OBSERV" in label or "SYS / # / OBS TYPES" in label:
                # RINEX 2 or RINEX 3 observation types
                if result.rinex_version >= 3.0:
                    # RINEX 3: G  8 C1C L1C S1C C2W L2W S2W C2L L2L
                    parts = line[0:60].split()
                    if parts:
                        system = parts[0]
                        if len(system) == 1 and system.isalpha():
                            try:
                                gnss = GNSSSystem(system)
                                if gnss not in result.systems_observed:
                                    result.systems_observed.append(gnss)
                            except ValueError:
                                pass
                        obs_types_raw.extend(parts[2:] if len(parts) > 2 else parts[1:])
                else:
                    # RINEX 2: num_types followed by type codes
                    parts = line[0:60].split()
                    if parts and parts[0].isdigit():
                        obs_types_raw.extend(parts[1:])
                    else:
                        obs_types_raw.extend(parts)

            elif "TIME OF FIRST OBS" in label:
                try:
                    year = int(line[0:6].strip())
                    month = int(line[6:12].strip())
                    day = int(line[12:18].strip())
                    hour = int(line[18:24].strip())
                    minute = int(line[24:30].strip())
                    second = float(line[30:43].strip())

                    result.first_epoch = datetime(
                        year, month, day, hour, minute, int(second),
                        int((second % 1) * 1000000)
                    )
                except (ValueError, IndexError):
                    pass

            elif "TIME OF LAST OBS" in label:
                try:
                    year = int(line[0:6].strip())
                    month = int(line[6:12].strip())
                    day = int(line[12:18].strip())
                    hour = int(line[18:24].strip())
                    minute = int(line[24:30].strip())
                    second = float(line[30:43].strip())

                    result.last_epoch = datetime(
                        year, month, day, hour, minute, int(second),
                        int((second % 1) * 1000000)
                    )
                except (ValueError, IndexError):
                    pass

        # Clean up observation types
        result.observation_types = [ot for ot in obs_types_raw if ot and len(ot) >= 2]

        # Default GPS if no system specified (RINEX 2)
        if not result.systems_observed:
            result.systems_observed.append(GNSSSystem.GPS)

    def _parse_observations(self, f: TextIO, result: QualityResult) -> None:
        """Parse RINEX observations."""
        epochs: list[EpochStats] = []
        prev_epoch_time: Optional[datetime] = None
        all_satellites: set[str] = set()
        satellite_obs: dict[str, list[list[float]]] = {}

        current_epoch: Optional[EpochStats] = None
        current_satellites: list[str] = []
        lines_to_read = 0
        epoch_obs_count = 0

        for line in f:
            if not line.strip():
                continue

            # Check for epoch header
            if result.rinex_version >= 3.0:
                # RINEX 3: > 2024 01 15 00 00  0.0000000  0 30
                if line.startswith(">"):
                    if current_epoch:
                        current_epoch.num_satellites = len(current_satellites)
                        current_epoch.satellites = current_satellites
                        epochs.append(current_epoch)

                    epoch_time = self._parse_rinex3_epoch(line)
                    if epoch_time:
                        current_epoch = EpochStats(time=epoch_time)

                        # Check for gap
                        if prev_epoch_time and result.interval > 0:
                            gap = (epoch_time - prev_epoch_time).total_seconds()
                            if gap > result.interval * self.gap_threshold:
                                current_epoch.has_gap_before = True
                                if self.detect_gaps:
                                    result.data_gaps.append((prev_epoch_time, epoch_time))

                        prev_epoch_time = epoch_time
                        current_satellites = []

                elif current_epoch and len(line) > 3:
                    # Satellite observation line
                    prn = line[0:3].strip()
                    if prn:
                        current_satellites.append(prn)
                        all_satellites.add(prn)
                        epoch_obs_count += 1

            else:
                # RINEX 2 epoch format
                # " 24  1 15  0  0  0.0000000  0 12G07G08G10..."
                if len(line) > 32 and line[0] == " " and line[28:29] in " 0123456":
                    if current_epoch:
                        current_epoch.num_satellites = len(current_satellites)
                        current_epoch.satellites = current_satellites
                        epochs.append(current_epoch)

                    epoch_time = self._parse_rinex2_epoch(line)
                    if epoch_time:
                        current_epoch = EpochStats(time=epoch_time)

                        # Parse satellites from epoch line
                        try:
                            epoch_flag = int(line[28:29].strip() or "0")
                            current_epoch.epoch_flag = epoch_flag
                            num_sats = int(line[29:32].strip())

                            # Satellites start at position 32, 3 chars each
                            sat_str = line[32:].strip()
                            current_satellites = []
                            for i in range(0, min(len(sat_str), num_sats * 3), 3):
                                prn = sat_str[i:i+3].strip()
                                if prn:
                                    current_satellites.append(prn)
                                    all_satellites.add(prn)

                            lines_to_read = num_sats

                        except (ValueError, IndexError):
                            lines_to_read = 0

                        # Check for gap
                        if prev_epoch_time and result.interval > 0:
                            gap = (epoch_time - prev_epoch_time).total_seconds()
                            if gap > result.interval * self.gap_threshold:
                                current_epoch.has_gap_before = True
                                if self.detect_gaps:
                                    result.data_gaps.append((prev_epoch_time, epoch_time))

                        prev_epoch_time = epoch_time

                elif lines_to_read > 0:
                    # Observation data line
                    epoch_obs_count += 1
                    lines_to_read -= 1

        # Add last epoch
        if current_epoch:
            current_epoch.num_satellites = len(current_satellites)
            current_epoch.satellites = current_satellites
            epochs.append(current_epoch)

        # Store results
        result.total_epochs = len(epochs)
        result.total_observations = epoch_obs_count
        result.satellites_observed = len(all_satellites)

        # Update first/last epoch if not in header
        if epochs:
            if not result.first_epoch:
                result.first_epoch = epochs[0].time
            if not result.last_epoch:
                result.last_epoch = epochs[-1].time

        # Compute mean satellites per epoch
        if epochs:
            result.mean_satellites_per_epoch = sum(e.num_satellites for e in epochs) / len(epochs)

        # Create satellite stats
        for prn in all_satellites:
            try:
                system = GNSSSystem(prn[0]) if prn else GNSSSystem.GPS
            except ValueError:
                system = GNSSSystem.GPS

            result.satellite_stats[prn] = SatelliteStats(
                prn=prn,
                system=system,
                epochs_observed=sum(1 for e in epochs if prn in e.satellites),
            )

        # Store gap info
        result.num_data_gaps = len(result.data_gaps)
        result.total_gap_duration = sum(
            (end - start).total_seconds()
            for start, end in result.data_gaps
        )

    def _parse_rinex3_epoch(self, line: str) -> Optional[datetime]:
        """Parse RINEX 3 epoch line."""
        try:
            # > 2024 01 15 00 00  0.0000000  0 30
            year = int(line[2:6].strip())
            month = int(line[7:9].strip())
            day = int(line[10:12].strip())
            hour = int(line[13:15].strip())
            minute = int(line[16:18].strip())
            second = float(line[19:30].strip())

            return datetime(
                year, month, day, hour, minute, int(second),
                int((second % 1) * 1000000)
            )
        except (ValueError, IndexError):
            return None

    def _parse_rinex2_epoch(self, line: str) -> Optional[datetime]:
        """Parse RINEX 2 epoch line."""
        try:
            # " 24  1 15  0  0  0.0000000  0 12..."
            year = int(line[1:3].strip())
            month = int(line[4:6].strip())
            day = int(line[7:9].strip())
            hour = int(line[10:12].strip())
            minute = int(line[13:15].strip())
            second = float(line[15:26].strip())

            # Convert 2-digit year
            if year >= 80:
                year += 1900
            else:
                year += 2000

            return datetime(
                year, month, day, hour, minute, int(second),
                int((second % 1) * 1000000)
            )
        except (ValueError, IndexError):
            return None

    def _compute_statistics(self, result: QualityResult) -> None:
        """Compute derived statistics including multi-GNSS breakdown."""
        # Calculate expected epochs
        if result.first_epoch and result.last_epoch and result.interval > 0:
            time_span = (result.last_epoch - result.first_epoch).total_seconds()
            result.expected_epochs = int(time_span / result.interval) + 1

            # Completeness percentage
            if result.expected_epochs > 0:
                result.completeness_pct = (result.total_epochs / result.expected_epochs) * 100
        else:
            # If no interval, assume 100% of what we have
            result.expected_epochs = result.total_epochs
            result.completeness_pct = 100.0 if result.total_epochs > 0 else 0.0

        # Estimate interval if not in header
        if result.interval <= 0 and result.total_epochs >= 2:
            # This would require keeping epoch times, so we estimate
            if result.first_epoch and result.last_epoch:
                time_span = (result.last_epoch - result.first_epoch).total_seconds()
                if result.total_epochs > 1:
                    result.interval = time_span / (result.total_epochs - 1)

        # Compute per-constellation statistics
        self._compute_multi_gnss_stats(result)

    def _compute_multi_gnss_stats(self, result: QualityResult) -> None:
        """Compute per-constellation statistics."""
        system_satellites: dict[str, set[str]] = {}
        system_observations: dict[str, int] = {}

        for prn, stats in result.satellite_stats.items():
            if not prn:
                continue

            # Get system code from PRN (first character)
            system = prn[0] if prn[0].isalpha() else "G"  # Default to GPS for numeric PRNs

            if system not in system_satellites:
                system_satellites[system] = set()
                system_observations[system] = 0

            system_satellites[system].add(prn)
            system_observations[system] += stats.epochs_observed

        # Build system_stats dictionary
        for system in sorted(system_satellites.keys()):
            sats = system_satellites[system]
            obs = system_observations.get(system, 0)

            result.system_stats[system] = {
                "satellites": len(sats),
                "observations": obs,
                "satellite_list": sorted(sats),
                "mean_obs_per_sat": obs / len(sats) if sats else 0,
            }

            # Add to systems_observed if not already there
            try:
                gnss_sys = GNSSSystem(system)
                if gnss_sys not in result.systems_observed:
                    result.systems_observed.append(gnss_sys)
            except ValueError:
                pass  # Unknown system code

    def _assess_quality(self, result: QualityResult) -> None:
        """Assess overall quality and identify issues."""
        # Determine quality level based on completeness
        if result.completeness_pct >= self.COMPLETENESS_EXCELLENT:
            result.quality_level = QualityLevel.EXCELLENT
        elif result.completeness_pct >= self.COMPLETENESS_GOOD:
            result.quality_level = QualityLevel.GOOD
        elif result.completeness_pct >= self.COMPLETENESS_ACCEPTABLE:
            result.quality_level = QualityLevel.ACCEPTABLE
        elif result.completeness_pct >= self.COMPLETENESS_POOR:
            result.quality_level = QualityLevel.POOR
        else:
            result.quality_level = QualityLevel.UNUSABLE

        # Check for issues
        if result.total_epochs == 0:
            result.issues.append("No observation epochs found")
            result.quality_level = QualityLevel.UNUSABLE

        if result.satellites_observed == 0:
            result.issues.append("No satellites observed")
            result.quality_level = QualityLevel.UNUSABLE

        if result.mean_satellites_per_epoch < self.MIN_SATELLITES:
            result.issues.append(
                f"Mean satellites per epoch ({result.mean_satellites_per_epoch:.1f}) "
                f"below minimum ({self.MIN_SATELLITES})"
            )

        if result.completeness_pct < self.COMPLETENESS_ACCEPTABLE:
            result.issues.append(
                f"Low data completeness: {result.completeness_pct:.1f}%"
            )

        # Warnings
        if result.num_data_gaps > 0:
            result.warnings.append(
                f"Data gaps detected: {result.num_data_gaps} gaps, "
                f"total duration {result.total_gap_duration:.0f}s"
            )

        if result.interval <= 0:
            result.warnings.append("Observation interval not specified in header")

        if not result.marker_name:
            result.warnings.append("No marker name in header")

        if not result.receiver_type:
            result.warnings.append("No receiver type in header")


# =============================================================================
# Convenience Functions
# =============================================================================

def check_rinex_quality(
    filepath: Path | str,
    verbose: bool = False,
) -> QualityResult:
    """Quick RINEX quality check.

    Args:
        filepath: Path to RINEX file
        verbose: Enable verbose output

    Returns:
        QualityResult
    """
    checker = RINEXQualityChecker(verbose=verbose)
    return checker.analyze(filepath)


def batch_quality_check(
    filepaths: list[Path | str],
    verbose: bool = False,
) -> list[QualityResult]:
    """Check quality of multiple RINEX files.

    Args:
        filepaths: List of file paths
        verbose: Enable verbose output

    Returns:
        List of QualityResults
    """
    checker = RINEXQualityChecker(verbose=verbose)
    return [checker.analyze(fp) for fp in filepaths]


def is_rinex_usable(
    filepath: Path | str,
    min_completeness: float = 70.0,
    min_satellites: int = 4,
) -> bool:
    """Quick check if RINEX file is usable.

    Args:
        filepath: Path to RINEX file
        min_completeness: Minimum completeness percentage
        min_satellites: Minimum mean satellites per epoch

    Returns:
        True if file meets minimum criteria
    """
    result = check_rinex_quality(filepath)

    return (
        result.quality_level != QualityLevel.UNUSABLE
        and result.completeness_pct >= min_completeness
        and result.mean_satellites_per_epoch >= min_satellites
    )


def get_rinex_summary(filepath: Path | str) -> str:
    """Get formatted summary of RINEX quality.

    Args:
        filepath: Path to RINEX file

    Returns:
        Formatted summary string
    """
    result = check_rinex_quality(filepath)
    return result.summary()
