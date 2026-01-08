"""
Bernese GNSS Software environment setup.

Handles environment variables and paths required for BSW execution.
Replaces Perl LOADENV.pm module.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from pygnss_rt.core.exceptions import BSWError
from pygnss_rt.utils.logging import get_logger


logger = get_logger(__name__)


@dataclass
class BSWEnvironment:
    """BSW environment configuration."""

    bsw_root: Path
    user_dir: Path
    exec_dir: Path
    queue_dir: Path
    temp_dir: Path
    campaign_root: Path

    # Environment variables to set
    env_vars: dict[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Initialize environment variables."""
        self.env_vars = {
            "C": str(self.bsw_root),
            "U": str(self.user_dir),
            "X": str(self.exec_dir),
            "Q": str(self.queue_dir),
            "T": str(self.temp_dir),
            "P": str(self.campaign_root),
        }

    def setup(self) -> dict[str, str]:
        """Set up environment variables for BSW.

        Returns:
            Dictionary of environment variables that were set
        """
        env = os.environ.copy()
        env.update(self.env_vars)

        # Add BSW executables to PATH
        path = env.get("PATH", "")
        exec_path = str(self.exec_dir)
        if exec_path not in path:
            env["PATH"] = f"{exec_path}:{path}"

        logger.info(
            "BSW environment configured",
            bsw_root=str(self.bsw_root),
            user_dir=str(self.user_dir),
        )

        return env

    def validate(self) -> bool:
        """Validate BSW installation.

        Returns:
            True if all required paths exist
        """
        required_paths = [
            self.bsw_root,
            self.user_dir,
            self.exec_dir,
        ]

        for path in required_paths:
            if not path.exists():
                logger.error("BSW path not found", path=str(path))
                return False

        return True


def load_bsw_environment(setvar_file: Path | str) -> BSWEnvironment:
    """Load BSW environment from LOADGPS.setvar file.

    Args:
        setvar_file: Path to LOADGPS.setvar file

    Returns:
        BSWEnvironment instance
    """
    path = Path(setvar_file)
    if not path.exists():
        raise BSWError("LOADGPS.setvar", f"File not found: {path}")

    # Parse setvar file
    vars_dict: dict[str, str] = {}

    with open(path) as f:
        for line in f:
            line = line.strip()

            # Skip comments and empty lines
            if not line or line.startswith("#"):
                continue

            # Parse export VAR=value or setenv VAR value
            if line.startswith("export "):
                match = re.match(r'export\s+(\w+)=["\'"]?(.+?)["\'"]?\s*$', line)
                if match:
                    vars_dict[match.group(1)] = match.group(2)
            elif line.startswith("setenv "):
                match = re.match(r'setenv\s+(\w+)\s+["\'"]?(.+?)["\'"]?\s*$', line)
                if match:
                    vars_dict[match.group(1)] = match.group(2)

    # Expand variable references
    def expand(value: str) -> str:
        """Expand $VAR and ${VAR} references."""
        for var, val in vars_dict.items():
            value = value.replace(f"${var}", val)
            value = value.replace(f"${{{var}}}", val)
        # Also expand from environment
        return os.path.expandvars(value)

    for key, value in vars_dict.items():
        vars_dict[key] = expand(value)

    # Create environment
    bsw_root = Path(vars_dict.get("C", "/opt/BERN54"))
    user_dir = Path(vars_dict.get("U", bsw_root / "GPS"))

    return BSWEnvironment(
        bsw_root=bsw_root,
        user_dir=user_dir,
        exec_dir=Path(vars_dict.get("X", user_dir / "EXE")),
        queue_dir=Path(vars_dict.get("Q", user_dir / "BPE")),
        temp_dir=Path(vars_dict.get("T", Path("/tmp/bsw"))),
        campaign_root=Path(vars_dict.get("P", Path("campaigns"))),
    )
