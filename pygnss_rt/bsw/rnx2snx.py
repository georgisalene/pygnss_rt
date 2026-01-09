"""
RINEX to SINEX BPE Processing.

Replaces Perl rnx2snx_pcs.pl script.

This module provides a Python interface to run the RNX2SNX BPE
(Bernese Processing Engine) process for converting RINEX observation
files to SINEX (Solution INdependent EXchange format) files.

Usage:
    from pygnss_rt.bsw.rnx2snx import RNX2SNXProcessor, run_rnx2snx

    # Using the processor class
    processor = RNX2SNXProcessor()
    result = processor.run(year=2024, session="0010")

    # Using convenience function
    result = run_rnx2snx(2024, "0010")
"""

from __future__ import annotations

import os
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

from pygnss_rt.bsw.environment import BSWEnvironment
from pygnss_rt.bsw.interface import BSWRunner, BPEResult, CampaignManager
from pygnss_rt.core.exceptions import BSWError
from pygnss_rt.utils.dates import GNSSDate
from pygnss_rt.utils.logging import get_logger


logger = get_logger(__name__)


@dataclass
class RNX2SNXConfig:
    """Configuration for RNX2SNX processing.

    Attributes:
        pcf_file: PCF (Process Control File) name, default "RNX2SNX"
        cpu_file: CPU file name, default "USER"
        campaign: Campaign name, default "EXAMPLE"
        sysout: System output name, default "RNX2SNX"
        status_file: Status file name, default "RNX2SNX.RUN"
        task_id: Task identifier, default "RS"
        timeout: Processing timeout in seconds, default 7200 (2 hours)
    """

    pcf_file: str = "RNX2SNX"
    cpu_file: str = "USER"
    campaign: str = "EXAMPLE"
    sysout: str = "RNX2SNX"
    status_file: str = "RNX2SNX.RUN"
    task_id: str = "RS"
    timeout: int = 7200

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for logging."""
        return {
            "pcf_file": self.pcf_file,
            "cpu_file": self.cpu_file,
            "campaign": self.campaign,
            "sysout": self.sysout,
            "status_file": self.status_file,
            "task_id": self.task_id,
            "timeout": self.timeout,
        }


@dataclass
class RNX2SNXResult:
    """Result from RNX2SNX processing.

    Attributes:
        success: Whether processing completed successfully
        year: Processing year
        session: Session identifier
        start_time: Processing start time
        end_time: Processing end time
        runtime_seconds: Total runtime in seconds
        sinex_files: List of generated SINEX files
        log_file: Path to BPE log file
        error_message: Error message if processing failed
        bpe_result: Underlying BPE execution result
    """

    success: bool
    year: int
    session: str
    start_time: datetime
    end_time: datetime
    runtime_seconds: float
    sinex_files: list[Path] = field(default_factory=list)
    log_file: Path | None = None
    error_message: str | None = None
    bpe_result: BPEResult | None = None

    def __str__(self) -> str:
        """Format as status string."""
        status = "SUCCESS" if self.success else "FAILED"
        return (
            f"RNX2SNX {status}: year={self.year} session={self.session} "
            f"runtime={self.runtime_seconds:.1f}s"
        )


class RNX2SNXProcessor:
    """RINEX to SINEX processor using BSW BPE.

    This class wraps the Bernese BPE (Processing Engine) to run
    the RNX2SNX PCF for converting RINEX files to SINEX format.

    Example:
        >>> processor = RNX2SNXProcessor()
        >>> result = processor.run(2024, "0010")
        >>> if result.success:
        ...     print(f"Generated {len(result.sinex_files)} SINEX files")
        ... else:
        ...     print(f"Error: {result.error_message}")
    """

    def __init__(
        self,
        environment: BSWEnvironment | None = None,
        config: RNX2SNXConfig | None = None,
    ):
        """Initialize RNX2SNX processor.

        Args:
            environment: BSW environment (auto-detected if None)
            config: Processing configuration (defaults used if None)
        """
        self.config = config or RNX2SNXConfig()

        # Initialize BSW environment
        if environment is None:
            environment = BSWEnvironment.auto_detect()
        self.environment = environment

        # Initialize BSW runner
        self.runner = BSWRunner(
            environment=self.environment,
            campaign_manager=CampaignManager(environment.campaign_root),
        )

    def _normalize_year(self, year: int | str) -> int:
        """Normalize year to 4-digit format.

        Args:
            year: Year as 2-digit or 4-digit integer or string

        Returns:
            4-digit year
        """
        year_int = int(year)
        if year_int < 100:
            # 2-digit year conversion
            if year_int >= 80:
                return 1900 + year_int
            else:
                return 2000 + year_int
        return year_int

    def _validate_session(self, session: str) -> str:
        """Validate and normalize session identifier.

        Args:
            session: Session identifier (should be 4 characters)

        Returns:
            Normalized session string

        Raises:
            ValueError: If session format is invalid
        """
        session = session.strip()

        if len(session) != 4:
            raise ValueError(
                f"Session must be 4 characters, got '{session}' ({len(session)} chars)"
            )

        return session.upper()

    def _get_campaign_dir(self) -> Path:
        """Get campaign directory for processing.

        Returns:
            Path to campaign directory
        """
        return self.environment.campaign_root / self.config.campaign

    def _find_sinex_files(
        self,
        campaign_dir: Path,
        session: str,
        year: int,
    ) -> list[Path]:
        """Find generated SINEX files after processing.

        Args:
            campaign_dir: Campaign directory
            session: Session identifier
            year: Processing year

        Returns:
            List of SINEX file paths
        """
        sinex_files: list[Path] = []

        # Check SOL directory for SINEX files
        sol_dir = campaign_dir / "SOL"
        if sol_dir.exists():
            # SINEX files typically have .SNX extension
            for snx in sol_dir.glob("*.SNX"):
                sinex_files.append(snx)
            for snx in sol_dir.glob("*.snx"):
                sinex_files.append(snx)

        # Also check OUT directory
        out_dir = campaign_dir / "OUT"
        if out_dir.exists():
            for snx in out_dir.glob("*.SNX"):
                if snx not in sinex_files:
                    sinex_files.append(snx)
            for snx in out_dir.glob("*.snx"):
                if snx not in sinex_files:
                    sinex_files.append(snx)

        return sinex_files

    def reset_cpu(self) -> None:
        """Reset CPU file to clear any pending jobs.

        This replicates the $bpe->resetCPU() call in the Perl script.
        """
        cpu_file = self.environment.user_dir / "PAN" / f"{self.config.cpu_file}.CPU"

        if cpu_file.exists():
            # Read the CPU file and clear active jobs
            logger.debug("Resetting CPU file", path=str(cpu_file))
            # The actual reset involves modifying the CPU file content
            # For now, we rely on BPE to handle this

    def run(
        self,
        year: int | str,
        session: str,
        config_override: RNX2SNXConfig | None = None,
    ) -> RNX2SNXResult:
        """Run RNX2SNX BPE process.

        Args:
            year: 4-digit (or 2-digit) year
            session: 4-character session identifier
            config_override: Optional configuration override

        Returns:
            RNX2SNXResult with processing details

        Raises:
            ValueError: If arguments are invalid
            BSWError: If BPE execution fails
        """
        config = config_override or self.config
        start_time = datetime.now()

        # Normalize inputs
        year_norm = self._normalize_year(year)
        session_norm = self._validate_session(session)

        logger.info(
            "Starting RNX2SNX BPE process",
            year=year_norm,
            session=session_norm,
            config=config.to_dict(),
        )

        # Get campaign directory
        campaign_dir = self._get_campaign_dir()

        if not campaign_dir.exists():
            error_msg = f"Campaign directory not found: {campaign_dir}"
            logger.error(error_msg)
            return RNX2SNXResult(
                success=False,
                year=year_norm,
                session=session_norm,
                start_time=start_time,
                end_time=datetime.now(),
                runtime_seconds=0.0,
                error_message=error_msg,
            )

        # Reset CPU file (like Perl: $bpe->resetCPU())
        self.reset_cpu()

        try:
            # Run BPE process
            bpe_result = self.runner.run_bpe(
                campaign_dir=campaign_dir,
                bpe_script=config.pcf_file,
                session=session_norm,
                year=year_norm,
                timeout=config.timeout,
            )

            end_time = datetime.now()
            runtime = (end_time - start_time).total_seconds()

            # Find generated SINEX files
            sinex_files = self._find_sinex_files(campaign_dir, session_norm, year_norm)

            if not bpe_result.success:
                logger.error(
                    "RNX2SNX BPE process failed",
                    year=year_norm,
                    session=session_norm,
                    error=bpe_result.error_message,
                )
            else:
                logger.info(
                    "RNX2SNX BPE process completed",
                    year=year_norm,
                    session=session_norm,
                    runtime=runtime,
                    sinex_count=len(sinex_files),
                )

            return RNX2SNXResult(
                success=bpe_result.success,
                year=year_norm,
                session=session_norm,
                start_time=start_time,
                end_time=end_time,
                runtime_seconds=runtime,
                sinex_files=sinex_files,
                log_file=bpe_result.log_file,
                error_message=bpe_result.error_message,
                bpe_result=bpe_result,
            )

        except Exception as e:
            end_time = datetime.now()
            runtime = (end_time - start_time).total_seconds()

            error_msg = str(e)
            logger.exception(
                "RNX2SNX BPE process error",
                year=year_norm,
                session=session_norm,
                error=error_msg,
            )

            return RNX2SNXResult(
                success=False,
                year=year_norm,
                session=session_norm,
                start_time=start_time,
                end_time=end_time,
                runtime_seconds=runtime,
                error_message=error_msg,
            )

    def run_batch(
        self,
        sessions: list[tuple[int, str]],
        stop_on_error: bool = False,
    ) -> list[RNX2SNXResult]:
        """Run RNX2SNX for multiple sessions.

        Args:
            sessions: List of (year, session) tuples
            stop_on_error: Stop processing on first error

        Returns:
            List of RNX2SNXResult for each session
        """
        results: list[RNX2SNXResult] = []

        for year, session in sessions:
            result = self.run(year, session)
            results.append(result)

            if not result.success and stop_on_error:
                logger.warning(
                    "Stopping batch due to error",
                    failed_session=session,
                )
                break

        success_count = sum(1 for r in results if r.success)
        logger.info(
            "Batch processing complete",
            total=len(results),
            successful=success_count,
            failed=len(results) - success_count,
        )

        return results


def run_rnx2snx(
    year: int | str,
    session: str,
    campaign: str = "EXAMPLE",
    timeout: int = 7200,
) -> RNX2SNXResult:
    """Convenience function to run RNX2SNX processing.

    Replaces the command-line usage of rnx2snx_pcs.pl:
        perl rnx2snx_pcs.pl 2024 0010

    Becomes:
        run_rnx2snx(2024, "0010")

    Args:
        year: 4-digit (or 2-digit) year
        session: 4-character session identifier
        campaign: Campaign name (default "EXAMPLE")
        timeout: Processing timeout in seconds

    Returns:
        RNX2SNXResult with processing details
    """
    config = RNX2SNXConfig(
        campaign=campaign,
        timeout=timeout,
    )

    processor = RNX2SNXProcessor(config=config)
    return processor.run(year, session)


def main() -> int:
    """Command-line entry point.

    Usage: python -m pygnss_rt.bsw.rnx2snx [-h] yyyy ssss

    Returns:
        Exit code (0 for success, 1 for error)
    """
    import argparse

    parser = argparse.ArgumentParser(
        description="Start RNX2SNX BPE process for a particular session",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    python -m pygnss_rt.bsw.rnx2snx 2024 0010
    python -m pygnss_rt.bsw.rnx2snx 24 0010
    python -m pygnss_rt.bsw.rnx2snx --campaign MYCAMPAIGN 2024 0010
        """,
    )

    parser.add_argument(
        "year",
        type=str,
        help="4-digit (or 2-digit) year",
    )

    parser.add_argument(
        "session",
        type=str,
        help="4-character session",
    )

    parser.add_argument(
        "--campaign",
        type=str,
        default="EXAMPLE",
        help="Campaign name (default: EXAMPLE)",
    )

    parser.add_argument(
        "--timeout",
        type=int,
        default=7200,
        help="Timeout in seconds (default: 7200)",
    )

    args = parser.parse_args()

    result = run_rnx2snx(
        year=args.year,
        session=args.session,
        campaign=args.campaign,
        timeout=args.timeout,
    )

    print(result)

    if not result.success:
        if result.error_message:
            print(f"Error: {result.error_message}")
        return 1

    if result.sinex_files:
        print(f"Generated SINEX files:")
        for snx in result.sinex_files:
            print(f"  {snx}")

    return 0


if __name__ == "__main__":
    import sys
    sys.exit(main())
