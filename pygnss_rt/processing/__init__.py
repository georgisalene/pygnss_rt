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
from pygnss_rt.processing.neq_stacking import (
    NEQStacker,
    NEQStackingConfig,
    NEQNameScheme,
    NEQFileInfo,
    create_neq_stacking_config,
    NRDDP_TRO_STACKING,
    NRDDP_TRO_SUBHOURLY_STACKING,
    NO_STACKING,
)
from pygnss_rt.processing.nrt_coordinates import (
    NRTCoordinateManager,
    NRTCoordinateConfig,
    CoordinateFileInfo,
    create_nrt_coordinate_config,
    NRDDP_TRO_COORDINATES,
    NRDDP_TRO_WITH_FALLBACK,
)
from pygnss_rt.processing.station_merger import (
    StationMerger,
    NetworkSource,
    StationInfo,
    MergerConfig,
    create_nrddp_merger,
    NRDDP_STATION_SOURCES,
)
from pygnss_rt.processing.nrddp_tro import (
    NRDDPTROProcessor,
    NRDDPTROArgs,
    NRDDPTROResult,
    NRDDPTROConfig,
    create_nrddp_tro_config,
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
    # NEQ stacking
    "NEQStacker",
    "NEQStackingConfig",
    "NEQNameScheme",
    "NEQFileInfo",
    "create_neq_stacking_config",
    "NRDDP_TRO_STACKING",
    "NRDDP_TRO_SUBHOURLY_STACKING",
    "NO_STACKING",
    # NRT coordinates
    "NRTCoordinateManager",
    "NRTCoordinateConfig",
    "CoordinateFileInfo",
    "create_nrt_coordinate_config",
    "NRDDP_TRO_COORDINATES",
    "NRDDP_TRO_WITH_FALLBACK",
    # Station merger
    "StationMerger",
    "NetworkSource",
    "StationInfo",
    "MergerConfig",
    "create_nrddp_merger",
    "NRDDP_STATION_SOURCES",
    # NRDDP TRO processing
    "NRDDPTROProcessor",
    "NRDDPTROArgs",
    "NRDDPTROResult",
    "NRDDPTROConfig",
    "create_nrddp_tro_config",
]
