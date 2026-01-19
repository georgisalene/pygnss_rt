"""
Configuration for Quality Monitoring Frontend

Uses DuckDB database integrated with pygnss_rt.
"""

import os
from pathlib import Path

# DuckDB Database Path (same as pygnss_rt)
DB_PATH = os.environ.get(
    "PYGNSS_DB_PATH",
    str(Path(__file__).parent.parent / "data" / "pygnss_rt.duckdb")
)

# Bernese Campaign Paths
CAMPAIGN_PATH = os.environ.get(
    "CAMPAIGN_PATH",
    "/home/ahunegnaw/GPSDATA/CAMPAIGN54"
)

# Dashboard Settings
DASHBOARD_CONFIG = {
    "title": "GNSS Quality Monitoring Dashboard",
    "refresh_interval": 60,  # seconds
    "default_days": 30,  # days to show by default
    "rms_threshold_good": 0.002,  # meters - green
    "rms_threshold_warn": 0.005,  # meters - yellow, above is red
}

# Station list for monitoring (your 38 stations)
MONITORED_STATIONS = [
    "0531", "0929", "0935", "ARL0", "BASC", "BOR1", "BRST", "BRUX",
    "DARE", "ECH2", "ENTZ", "ERPL", "EUSK", "GRAS", "GRAZ", "HERS",
    "JOZE", "KBG2", "LONG", "LROC", "MABO", "MAR6", "MATE", "MLVL",
    "ONSA", "POTS", "ROUL", "STAS", "THNV", "TLSE", "TRI2", "TROS",
    "WALF", "WARN", "WSRT", "WTZR", "YEBE", "ZIM2"
]
