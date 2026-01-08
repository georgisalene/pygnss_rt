"""
PyGNSS-RT: Python GNSS Real-Time Processing Framework

A modern Python implementation for real-time GNSS data processing, integrating with
Bernese GNSS Software (BSW) for Precise Point Positioning (PPP) and
tropospheric parameter estimation.
"""

__version__ = "1.0.0"
__author__ = "PyGNSS-RT Team"

from pygnss_rt.core.orchestrator import IGNSS
from pygnss_rt.core.config import Settings

__all__ = ["IGNSS", "Settings", "__version__"]
