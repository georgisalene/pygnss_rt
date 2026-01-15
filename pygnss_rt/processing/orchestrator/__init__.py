"""
Processing Orchestrator Subpackage.

This package provides modular access to the orchestrator components.
For backward compatibility, all classes are re-exported from this module.

Module structure:
- config.py: Configuration dataclasses and enums
- products.py: Product checking and downloading (future)
- data.py: Data management (future)
- executor.py: BSW execution (future)
- base.py: Main orchestrator class (future)

Usage:
    from pygnss_rt.processing.orchestrator import (
        ProcessingConfig,
        ProcessingResult,
        ProcessingType,
        IGNSSOrchestrator,
    )
"""

# Import from config module
from pygnss_rt.processing.orchestrator.config import (
    ProcessingType,
    ProductCategory,
    ProductConfig,
    DataSourceConfig,
    DatabaseConfig,
    DCMConfig,
    ProcessingConfig,
    ProcessingResult,
)

# For backward compatibility, also import from main orchestrator module
# This allows existing code to continue working
from pygnss_rt.processing.orchestrator_main import (
    ProductChecker,
    ProcProductConfig,
    PPPProductArgs,
    PPPProductDownloader,
    DataManager,
    BSWExecutor,
    IGNSSOrchestrator,
    create_daily_config,
    create_hourly_config,
    run_daily_processing,
    run_hourly_processing,
)

__all__ = [
    # Config classes
    "ProcessingType",
    "ProductCategory",
    "ProductConfig",
    "DataSourceConfig",
    "DatabaseConfig",
    "DCMConfig",
    "ProcessingConfig",
    "ProcessingResult",
    # Main classes
    "ProductChecker",
    "ProcProductConfig",
    "PPPProductArgs",
    "PPPProductDownloader",
    "DataManager",
    "BSWExecutor",
    "IGNSSOrchestrator",
    # Helper functions
    "create_daily_config",
    "create_hourly_config",
    "run_daily_processing",
    "run_hourly_processing",
]
