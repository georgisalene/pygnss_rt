"""Processing modules for PyGNSS-RT."""

from pygnss_rt.processing.networks import (
    NetworkID,
    NetworkProfile,
    StationFilter,
    FTPDataSource,
    ProductSource,
    ArchiveFileSpec,
    create_network_profiles,
    get_network_profile,
    list_networks,
    PCF_PPP_AR,
    PCF_NETWORK_DD,
)
from pygnss_rt.processing.daily_ppp import (
    DailyPPPProcessor,
    DailyPPPArgs,
    DailyPPPResult,
    process_all_networks,
)
from pygnss_rt.processing.bsw_options import (
    BSWOptionsParser,
    BSWOptionsConfig,
    BSWProgramOptions,
    BSWStepOptions,
    load_bsw_options,
    get_option_dirs,
    PPP_OPTION_DIRS,
    NRDDP_OPTION_DIRS,
)

__all__ = [
    # Network configuration
    "NetworkID",
    "NetworkProfile",
    "StationFilter",
    "FTPDataSource",
    "ProductSource",
    "ArchiveFileSpec",
    "create_network_profiles",
    "get_network_profile",
    "list_networks",
    "PCF_PPP_AR",
    "PCF_NETWORK_DD",
    # Daily PPP processing
    "DailyPPPProcessor",
    "DailyPPPArgs",
    "DailyPPPResult",
    "process_all_networks",
    # BSW options
    "BSWOptionsParser",
    "BSWOptionsConfig",
    "BSWProgramOptions",
    "BSWStepOptions",
    "load_bsw_options",
    "get_option_dirs",
    "PPP_OPTION_DIRS",
    "NRDDP_OPTION_DIRS",
]
