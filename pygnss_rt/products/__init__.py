"""GNSS products module.

Includes orbit/clock/ERP product management and SP3 file parsing.
"""

# Product-related functionality is in database.products
from pygnss_rt.database.products import ProductManager
from pygnss_rt.database.models import ProductType, ProductTier, Product

# Orbit and SP3 functionality
from pygnss_rt.products.orbit import (
    # SP3 data classes
    SP3File,
    SP3Header,
    SP3Epoch,
    SP3Position,
    SP3Velocity,
    SP3Version,
    TimeSystem,
    # SP3 reader/writer
    SP3Reader,
    SP3Writer,
    # Orbit database management
    OrbitDataManager,
    OrbitEntry,
    OrbitStatus,
    OrbitType,
    # Utility functions
    build_orbit_filename,
    parse_orbit_filename,
)

__all__ = [
    # Base product management
    "ProductManager",
    "ProductType",
    "ProductTier",
    "Product",
    # SP3 data classes
    "SP3File",
    "SP3Header",
    "SP3Epoch",
    "SP3Position",
    "SP3Velocity",
    "SP3Version",
    "TimeSystem",
    # SP3 reader/writer
    "SP3Reader",
    "SP3Writer",
    # Orbit database management
    "OrbitDataManager",
    "OrbitEntry",
    "OrbitStatus",
    "OrbitType",
    # Utilities
    "build_orbit_filename",
    "parse_orbit_filename",
]
