"""
Bernese GNSS Software interface.

Provides Python interface for running BSW programs and managing campaigns.
"""

from __future__ import annotations

import os
import shutil
import subprocess
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from pygnss_rt.bsw.environment import BSWEnvironment
from pygnss_rt.core.exceptions import BSWError
from pygnss_rt.utils.dates import GNSSDate
from pygnss_rt.utils.logging import get_logger


logger = get_logger(__name__)


@dataclass
class CampaignConfig:
    """Campaign configuration."""

    name: str
    year: int
    session: str
    stations: list[str]
    proc_type: str = "PPP"
    orbit_product: str = "IGS"
    orbit_tier: str = "final"


@dataclass
class BPEResult:
    """Result from BPE execution."""

    success: bool
    return_code: int
    output_files: list[Path] = field(default_factory=list)
    log_file: Path | None = None
    error_message: str | None = None
    runtime_seconds: float = 0.0


class CampaignManager:
    """Manages BSW campaign directories."""

    # Standard campaign subdirectories
    SUBDIRS = [
        "ATM", "BPE", "GRD", "OBS", "ORB", "ORX",
        "OUT", "RAW", "SOL", "STA", "GEN",
    ]

    def __init__(self, campaign_root: Path | str):
        """Initialize campaign manager.

        Args:
            campaign_root: Root directory for campaigns
        """
        self.campaign_root = Path(campaign_root)

    def create_campaign(self, config: CampaignConfig) -> Path:
        """Create a new campaign directory structure.

        Args:
            config: Campaign configuration

        Returns:
            Path to created campaign directory
        """
        campaign_dir = self.campaign_root / config.name

        # Create main directory and subdirectories
        for subdir in self.SUBDIRS:
            (campaign_dir / subdir).mkdir(parents=True, exist_ok=True)

        logger.info(
            "Created campaign",
            name=config.name,
            path=str(campaign_dir),
        )

        return campaign_dir

    def get_campaign_path(self, name: str) -> Path:
        """Get path to existing campaign."""
        return self.campaign_root / name

    def cleanup_campaign(self, name: str, keep_results: bool = True) -> None:
        """Clean up campaign directory.

        Args:
            name: Campaign name
            keep_results: Keep OUT and SOL directories
        """
        campaign_dir = self.campaign_root / name

        if not campaign_dir.exists():
            return

        for subdir in self.SUBDIRS:
            if keep_results and subdir in ("OUT", "SOL"):
                continue

            subdir_path = campaign_dir / subdir
            if subdir_path.exists():
                for item in subdir_path.iterdir():
                    if item.is_file():
                        item.unlink()
                    elif item.is_dir():
                        shutil.rmtree(item)

        logger.info("Cleaned campaign", name=name)


class BSWRunner:
    """Runs BSW programs and BPE scripts."""

    def __init__(
        self,
        environment: BSWEnvironment,
        campaign_manager: CampaignManager | None = None,
    ):
        """Initialize BSW runner.

        Args:
            environment: BSW environment configuration
            campaign_manager: Campaign manager (created if not provided)
        """
        self.env = environment
        self.campaign_manager = campaign_manager or CampaignManager(
            environment.campaign_root
        )
        self._env_vars: dict[str, str] | None = None

    def _get_env(self) -> dict[str, str]:
        """Get environment variables for BSW execution."""
        if self._env_vars is None:
            self._env_vars = self.env.setup()
        return self._env_vars

    def run_program(
        self,
        program: str,
        args: list[str] | None = None,
        campaign: str | None = None,
        timeout: int = 3600,
    ) -> tuple[int, str, str]:
        """Run a BSW program.

        Args:
            program: Program name (e.g., 'ORBGEN', 'GPSEST')
            args: Command line arguments
            campaign: Campaign name for context
            timeout: Timeout in seconds

        Returns:
            Tuple of (return_code, stdout, stderr)
        """
        exe_path = self.env.exec_dir / program

        if not exe_path.exists():
            raise BSWError(program, f"Executable not found: {exe_path}")

        cmd = [str(exe_path)] + (args or [])

        logger.info(
            "Running BSW program",
            program=program,
            campaign=campaign,
        )

        try:
            result = subprocess.run(
                cmd,
                env=self._get_env(),
                capture_output=True,
                text=True,
                timeout=timeout,
                cwd=str(self.campaign_manager.get_campaign_path(campaign))
                if campaign
                else None,
            )

            if result.returncode != 0:
                logger.warning(
                    "BSW program returned non-zero",
                    program=program,
                    return_code=result.returncode,
                )

            return result.returncode, result.stdout, result.stderr

        except subprocess.TimeoutExpired:
            raise BSWError(program, f"Timeout after {timeout} seconds")
        except Exception as e:
            raise BSWError(program, str(e)) from e

    def run_bpe(
        self,
        campaign_dir: Path,
        bpe_script: str,
        session: str,
        year: int,
        timeout: int = 7200,
    ) -> BPEResult:
        """Run a BPE (Bernese Processing Engine) script.

        Args:
            campaign_dir: Campaign directory
            bpe_script: BPE script name (e.g., 'PPP_AR')
            session: Session identifier
            year: Year
            timeout: Timeout in seconds

        Returns:
            BPEResult with execution details
        """
        import time

        start_time = time.time()

        # Build BPE command
        bpe_exe = self.env.queue_dir / "BPE"
        if not bpe_exe.exists():
            raise BSWError("BPE", f"BPE executable not found: {bpe_exe}")

        cmd = [
            str(bpe_exe),
            bpe_script,
            str(campaign_dir),
            session,
            str(year),
        ]

        logger.info(
            "Running BPE script",
            script=bpe_script,
            campaign=str(campaign_dir),
            session=session,
        )

        try:
            result = subprocess.run(
                cmd,
                env=self._get_env(),
                capture_output=True,
                text=True,
                timeout=timeout,
                cwd=str(campaign_dir),
            )

            runtime = time.time() - start_time

            # Check for output files
            output_dir = campaign_dir / "OUT"
            output_files = list(output_dir.glob(f"*{session}*")) if output_dir.exists() else []

            # Find log file
            log_file = None
            log_dir = campaign_dir / "BPE"
            if log_dir.exists():
                logs = list(log_dir.glob(f"*{bpe_script}*.LOG"))
                if logs:
                    log_file = logs[0]

            return BPEResult(
                success=result.returncode == 0,
                return_code=result.returncode,
                output_files=output_files,
                log_file=log_file,
                error_message=result.stderr if result.returncode != 0 else None,
                runtime_seconds=runtime,
            )

        except subprocess.TimeoutExpired:
            return BPEResult(
                success=False,
                return_code=-1,
                error_message=f"Timeout after {timeout} seconds",
                runtime_seconds=timeout,
            )
        except Exception as e:
            return BPEResult(
                success=False,
                return_code=-1,
                error_message=str(e),
                runtime_seconds=time.time() - start_time,
            )

    def prepare_rinex(
        self,
        rinex_files: list[Path],
        campaign_dir: Path,
    ) -> list[Path]:
        """Prepare RINEX files for processing.

        Copies and decompresses RINEX files to campaign RAW directory.

        Args:
            rinex_files: List of RINEX file paths
            campaign_dir: Campaign directory

        Returns:
            List of prepared file paths
        """
        raw_dir = campaign_dir / "RAW"
        raw_dir.mkdir(parents=True, exist_ok=True)

        prepared: list[Path] = []

        for rinex in rinex_files:
            dest = raw_dir / rinex.name

            # Copy file
            shutil.copy2(rinex, dest)

            # Decompress if needed
            if dest.suffix in (".Z", ".gz"):
                import gzip
                import lzma

                if dest.suffix == ".gz":
                    with gzip.open(dest, "rb") as f_in:
                        decompressed = dest.with_suffix("")
                        with open(decompressed, "wb") as f_out:
                            f_out.write(f_in.read())
                    dest.unlink()
                    dest = decompressed

            prepared.append(dest)

        return prepared
