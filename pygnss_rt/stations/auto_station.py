"""
AutoStation Processor - Port of i-BSWSTA main scripts.

Unified processor that combines:
- FTP/SFTP site log downloading
- Site log parsing (ASCII to data structures)
- Bernese .STA file generation

Replaces the Perl call_autoSta_*.pl scripts:
- call_autoSta_NEWNRT52_IGS.pl.works_for_IGS20rh
- call_autoSta_OSGB_sftp_with_IGS20_54_name.pl

Usage:
    from pygnss_rt.stations.auto_station import AutoStationProcessor

    processor = AutoStationProcessor(
        work_dir="/data/station_info",
        use_domes=False,
    )

    # Full pipeline: download -> parse -> generate STA
    result = processor.process(
        sources=["IGS", "EUREF"],
        output_sta="/path/to/STATIONS.STA",
        station_filter=["algo", "nrc1", "dubo"],
    )

    # Or step by step
    processor.download_site_logs(sources=["IGS"])
    processor.parse_site_logs()
    processor.generate_sta_file("/path/to/output.STA")
"""

from __future__ import annotations

import logging
import shutil
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Optional

from pygnss_rt.stations.site_log_downloader import (
    SiteLogDownloader,
    SiteLogDownloadResult,
    SiteLogSource,
    DEFAULT_SITE_LOG_SOURCES,
)
from pygnss_rt.stations.site_log_parser import (
    SiteLogParser,
    SiteLogData,
    parse_site_logs_directory,
)
from pygnss_rt.stations.sta_file_writer import (
    STAFileWriter,
    write_sta_file,
)

logger = logging.getLogger(__name__)


# =============================================================================
# Bad Stations Lists (from i-BSWSTA Perl scripts)
# =============================================================================

# IGS stations with known issues (from call_autoSta_NEWNRT52_IGS.pl)
# Reasons:
# - dund: IGS station with duplicate name to OSGB
# - str2: non-conforming IGS siteLog
# - sey2: non-conforming IGS siteLog
# - elat: unrecognized receiver
# - katz: unrecognized receiver
# - ohig: duplicate DOMES number issues
IGS_BAD_STATIONS = [
    "dund", "str2", "sey2", "elat", "katz", "ohig",
]

# Dead/problematic OSGB stations (from call_autoSta_OSGB_sftp_with_IGS20_54_name.pl)
OSGB_BAD_STATIONS = [
    "abbs", "abea", "abed", "abee", "abep", "aber", "abgi", "abii",
    "abki", "abmf", "ablf", "abnd", "abni", "abnz", "aboc", "abov",
    "abrd", "abrs", "abrw", "abry", "absc", "absk", "absy", "abtm",
    "abty", "abud", "abwe", "abwh", "abwi", "abwl", "abwn", "abwo",
    "alde", "aldb", "badw", "benb", "bost", "burn", "camb", "carl",
    "cata", "colc", "droi", "dund", "east", "erds", "hern", "high",
    "hull", "inve", "keig", "kirk", "leek", "leic", "lerw", "lisk",
    "live", "lond", "maid", "newc", "newp", "norm", "nott", "pers",
    "plym", "pool", "pres", "read", "roth", "sbhx", "shef", "shre",
    "soke", "sunb", "suth", "swan", "taun", "thur", "uist", "ware",
    "watt", "wiga", "winc", "wolv", "wore", "ynys",
]

# Combined default bad stations for all networks
DEFAULT_BAD_STATIONS = list(set(IGS_BAD_STATIONS + OSGB_BAD_STATIONS))


@dataclass
class AutoStationConfig:
    """Configuration for AutoStation processor.

    Port of configuration from call_autoSta_*.pl scripts.
    """

    # Working directory for site logs
    work_dir: str | Path = "/data/station_info"

    # Site log subdirectory
    site_logs_subdir: str = "sitelogs"

    # Output directory for STA files
    sta_output_dir: str | Path = ""

    # Whether to include DOMES numbers in station names
    use_domes: bool = False

    # Title for generated STA files
    sta_title: str = "i-BSWSTA generated"

    # Bad stations to always exclude
    bad_stations: list[str] = field(default_factory=list)

    # Whether to keep downloaded site logs after processing
    keep_downloads: bool = True

    # Verbose output
    verbose: bool = False


@dataclass
class AutoStationResult:
    """Result of AutoStation processing."""

    success: bool = False
    download_results: list[SiteLogDownloadResult] = field(default_factory=list)
    parsed_stations: int = 0
    sta_stations_written: int = 0
    sta_file_path: str = ""
    duration_seconds: float = 0.0
    errors: list[str] = field(default_factory=list)


class AutoStationProcessor:
    """Automated station information processing.

    Combines site log download, parsing, and STA file generation
    into a single unified workflow.

    Port of call_autoSta_*.pl scripts from i-BSWSTA.
    """

    def __init__(
        self,
        config: AutoStationConfig | None = None,
        work_dir: str | Path | None = None,
        use_domes: bool = False,
        verbose: bool = False,
    ):
        """Initialize AutoStation processor.

        Args:
            config: Full configuration object
            work_dir: Working directory (overrides config if provided)
            use_domes: Include DOMES numbers (overrides config if provided)
            verbose: Verbose output (overrides config if provided)
        """
        if config:
            self.config = config
        else:
            self.config = AutoStationConfig()

        # Apply overrides
        if work_dir:
            self.config.work_dir = Path(work_dir)
        if use_domes:
            self.config.use_domes = use_domes
        if verbose:
            self.config.verbose = verbose

        # Ensure paths are Path objects
        self.config.work_dir = Path(self.config.work_dir)
        if self.config.sta_output_dir:
            self.config.sta_output_dir = Path(self.config.sta_output_dir)

        # Internal state
        self._site_logs_dir: Optional[Path] = None
        self._parsed_data: dict[str, SiteLogData] = {}
        self._downloader: Optional[SiteLogDownloader] = None

    @property
    def site_logs_dir(self) -> Path:
        """Get site logs directory."""
        if self._site_logs_dir:
            return self._site_logs_dir
        return self.config.work_dir / self.config.site_logs_subdir

    @property
    def parsed_data(self) -> dict[str, SiteLogData]:
        """Get parsed site log data."""
        return self._parsed_data

    def process(
        self,
        sources: list[str] | None = None,
        output_sta: str | Path | None = None,
        station_filter: list[str] | None = None,
        exclude_stations: list[str] | None = None,
        overwrite_downloads: bool = False,
    ) -> AutoStationResult:
        """Run the complete processing pipeline.

        Download site logs -> Parse -> Generate STA file

        Args:
            sources: List of source names (default: ["IGS"])
            output_sta: Output STA file path
            station_filter: Only process these stations
            exclude_stations: Exclude these stations
            overwrite_downloads: Overwrite existing downloads

        Returns:
            AutoStationResult with processing statistics
        """
        start_time = datetime.now()
        result = AutoStationResult()

        sources = sources or ["IGS"]

        try:
            # Step 1: Download site logs
            logger.info(f"Downloading site logs from: {sources}")
            result.download_results = self.download_site_logs(
                sources=sources,
                station_filter=station_filter,
                exclude_stations=exclude_stations,
                overwrite=overwrite_downloads,
            )

            # Step 2: Parse site logs
            logger.info("Parsing site logs")
            result.parsed_stations = self.parse_site_logs(station_filter=station_filter)

            # Step 3: Generate STA file
            if output_sta:
                logger.info(f"Generating STA file: {output_sta}")
                result.sta_stations_written = self.generate_sta_file(output_sta)
                result.sta_file_path = str(output_sta)

            result.success = True

        except Exception as e:
            result.errors.append(str(e))
            logger.error(f"AutoStation processing failed: {e}")

        result.duration_seconds = (datetime.now() - start_time).total_seconds()

        logger.info(
            f"AutoStation complete: {result.parsed_stations} parsed, "
            f"{result.sta_stations_written} written to STA"
        )

        return result

    def download_site_logs(
        self,
        sources: list[str] | None = None,
        station_filter: list[str] | None = None,
        exclude_stations: list[str] | None = None,
        overwrite: bool = False,
    ) -> list[SiteLogDownloadResult]:
        """Download site logs from specified sources.

        Args:
            sources: List of source names (default: ["IGS"])
            station_filter: Only download these stations
            exclude_stations: Exclude these stations
            overwrite: Overwrite existing files

        Returns:
            List of download results
        """
        sources = sources or ["IGS"]

        # Ensure site logs directory exists
        self.site_logs_dir.mkdir(parents=True, exist_ok=True)

        # Initialize downloader
        if not self._downloader:
            self._downloader = SiteLogDownloader(
                bad_stations=self.config.bad_stations,
                verbose=self.config.verbose,
            )

        results = []

        for source_name in sources:
            try:
                result = self._downloader.download(
                    source=source_name,
                    destination=self.site_logs_dir,
                    station_filter=station_filter,
                    exclude_stations=exclude_stations,
                    overwrite=overwrite,
                    remove_duplicates=True,
                )
                results.append(result)

            except Exception as e:
                logger.error(f"Failed to download from {source_name}: {e}")
                results.append(
                    SiteLogDownloadResult(source=source_name, errors=[str(e)])
                )

        return results

    def parse_site_logs(
        self,
        station_filter: list[str] | None = None,
    ) -> int:
        """Parse all site logs in the work directory.

        Args:
            station_filter: Only parse these stations

        Returns:
            Number of stations successfully parsed
        """
        if not self.site_logs_dir.exists():
            logger.warning(f"Site logs directory does not exist: {self.site_logs_dir}")
            return 0

        # Parse all site logs
        self._parsed_data = parse_site_logs_directory(self.site_logs_dir)

        # Apply station filter if provided
        if station_filter:
            filter_set = set(s.lower() for s in station_filter)
            self._parsed_data = {
                k: v for k, v in self._parsed_data.items()
                if k.lower() in filter_set
            }

        logger.info(f"Parsed {len(self._parsed_data)} site logs")

        return len(self._parsed_data)

    def generate_sta_file(
        self,
        output_path: str | Path,
        station_filter: list[str] | None = None,
    ) -> int:
        """Generate Bernese .STA file from parsed data.

        Args:
            output_path: Output STA file path
            station_filter: Only include these stations

        Returns:
            Number of stations written
        """
        if not self._parsed_data:
            logger.warning("No parsed data available. Run parse_site_logs() first.")
            return 0

        station_data = list(self._parsed_data.values())

        # Apply station filter if provided
        if station_filter:
            filter_set = set(s.lower() for s in station_filter)
            station_data = [
                s for s in station_data
                if s.station_id and s.station_id.lower() in filter_set
            ]

        # Write STA file
        count = write_sta_file(
            output_path=output_path,
            station_data=station_data,
            use_domes=self.config.use_domes,
            title=self.config.sta_title,
        )

        return count

    def get_station_info(self, station_id: str) -> Optional[SiteLogData]:
        """Get parsed information for a specific station.

        Args:
            station_id: 4-character station ID

        Returns:
            SiteLogData if found, None otherwise
        """
        station_id = station_id.lower()

        # Try exact match first
        if station_id in self._parsed_data:
            return self._parsed_data[station_id]

        # Try case-insensitive search
        for key, data in self._parsed_data.items():
            if key.lower() == station_id:
                return data

        return None

    def list_stations(self) -> list[str]:
        """List all parsed station IDs.

        Returns:
            Sorted list of station IDs
        """
        return sorted(self._parsed_data.keys())

    def cleanup(self, remove_downloads: bool = False) -> None:
        """Cleanup temporary files.

        Args:
            remove_downloads: Also remove downloaded site logs
        """
        if remove_downloads and self.site_logs_dir.exists():
            shutil.rmtree(self.site_logs_dir)
            logger.info(f"Removed site logs directory: {self.site_logs_dir}")

        self._parsed_data.clear()


def process_station_metadata(
    sources: list[str] | None = None,
    output_sta: str | Path = "STATIONS.STA",
    work_dir: str | Path = "/data/station_info",
    station_filter: list[str] | None = None,
    use_domes: bool = False,
    verbose: bool = False,
) -> AutoStationResult:
    """Convenience function for complete station processing.

    Downloads site logs, parses them, and generates a Bernese STA file.

    Args:
        sources: Data sources (default: ["IGS"])
        output_sta: Output STA file path
        work_dir: Working directory
        station_filter: Only process these stations
        use_domes: Include DOMES numbers
        verbose: Verbose output

    Returns:
        AutoStationResult
    """
    processor = AutoStationProcessor(
        work_dir=work_dir,
        use_domes=use_domes,
        verbose=verbose,
    )

    return processor.process(
        sources=sources,
        output_sta=output_sta,
        station_filter=station_filter,
    )


def update_sta_file(
    existing_sta: str | Path,
    sources: list[str] | None = None,
    work_dir: str | Path = "/data/station_info",
    station_filter: list[str] | None = None,
    use_domes: bool = False,
    backup: bool = True,
) -> AutoStationResult:
    """Update an existing STA file with fresh site log data.

    Args:
        existing_sta: Path to existing STA file
        sources: Data sources
        work_dir: Working directory
        station_filter: Only update these stations
        use_domes: Include DOMES numbers
        backup: Create backup of existing file

    Returns:
        AutoStationResult
    """
    existing_sta = Path(existing_sta)

    # Create backup if requested and file exists
    if backup and existing_sta.exists():
        backup_path = existing_sta.with_suffix(
            f".bak.{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        )
        shutil.copy2(existing_sta, backup_path)
        logger.info(f"Created backup: {backup_path}")

    # Run processing
    return process_station_metadata(
        sources=sources,
        output_sta=existing_sta,
        work_dir=work_dir,
        station_filter=station_filter,
        use_domes=use_domes,
    )
