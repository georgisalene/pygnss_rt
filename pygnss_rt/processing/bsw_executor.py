"""
Bernese GNSS Software (BSW) execution management.

This module handles BSW/BPE execution for GNSS processing.
Extracted from orchestrator_main.py for better modularity.
"""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

from pygnss_rt.processing.processing_config import ProcessingConfig
from pygnss_rt.utils.logging import get_logger


logger = get_logger(__name__)


class BSWExecutor:
    """Execute Bernese GNSS Software processing.

    Handles:
    - PCF file setup
    - BPE (Bernese Processing Engine) execution
    - Result collection
    """

    def __init__(self, config: ProcessingConfig):
        """Initialize BSW executor.

        Args:
            config: Processing configuration
        """
        self.config = config

    def prepare_campaign(self) -> bool:
        """Prepare BSW campaign directory.

        Returns:
            True if preparation successful
        """
        if not self.config.bsw_campaign_dir:
            logger.error("BSW campaign directory not configured")
            return False

        # Create required subdirectories
        subdirs = ["ATM", "BPE", "GRD", "NEQ", "OBS", "ORB", "OUT", "RAW", "SOL", "STA"]
        for subdir in subdirs:
            dir_path = self.config.bsw_campaign_dir / subdir
            dir_path.mkdir(parents=True, exist_ok=True)

        return True

    def run_bpe(self, pcf_file: Path, session: str) -> tuple[bool, str]:
        """Run Bernese Processing Engine.

        Args:
            pcf_file: Path to PCF control file
            session: Session identifier

        Returns:
            Tuple of (success, output/error message)
        """
        bpe_dir = os.environ.get("BPE", "")
        if not bpe_dir:
            return False, "BPE environment variable not set"

        # Build command
        cmd = [
            f"{bpe_dir}/startBPE",
            "-c", str(self.config.bsw_campaign_dir),
            "-pcf", str(pcf_file),
            "-s", session,
        ]

        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=3600,  # 1 hour timeout
            )

            if result.returncode == 0:
                return True, result.stdout
            else:
                return False, result.stderr or result.stdout

        except subprocess.TimeoutExpired:
            return False, "BPE execution timed out"
        except Exception as e:
            return False, str(e)
