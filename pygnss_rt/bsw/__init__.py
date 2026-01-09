"""Bernese GNSS Software (BSW) integration."""

from pygnss_rt.bsw.environment import BSWEnvironment, load_bsw_environment
from pygnss_rt.bsw.interface import BSWRunner, CampaignManager, CampaignConfig, BPEResult
from pygnss_rt.bsw.rnx2snx import (
    RNX2SNXProcessor,
    RNX2SNXConfig,
    RNX2SNXResult,
    run_rnx2snx,
)

__all__ = [
    # Environment
    "BSWEnvironment",
    "load_bsw_environment",
    # Interface
    "BSWRunner",
    "CampaignManager",
    "CampaignConfig",
    "BPEResult",
    # RNX2SNX processing
    "RNX2SNXProcessor",
    "RNX2SNXConfig",
    "RNX2SNXResult",
    "run_rnx2snx",
]
