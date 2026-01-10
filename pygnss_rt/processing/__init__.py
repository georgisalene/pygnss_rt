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
from pygnss_rt.processing.orchestrator import (
    # Main orchestrator (replaces IGNSS.pm)
    IGNSSOrchestrator,
    ProcessingConfig,
    ProcessingResult,
    ProcessingType,
    # Product handling
    ProductChecker,
    ProductConfig,
    ProductCategory,
    # Data handling
    DataManager,
    DataSourceConfig,
    # BSW execution
    BSWExecutor,
    # Database config
    DatabaseConfig,
    # DCM (Data/Campaign Management) config
    DCMConfig,
    # Convenience functions
    create_daily_config,
    create_hourly_config,
    run_daily_processing,
    run_hourly_processing,
)
from pygnss_rt.processing.daily_crd import (
    # Daily NRT coordinate generation (replaces iGNSS_D_CRD_54.pl)
    DailyCRDProcessor,
    DailyCRDConfig,
    DailyCRDResult,
    NetworkArchive,
    StationCoordinate,
    create_daily_crd_config,
)
from pygnss_rt.processing.campaign_archival import (
    # Campaign archival (replaces IGNSS::dcm)
    CampaignArchiver,
    CampaignArchiveConfig,
    CampaignArchiveResult,
    CampaignRestoreConfig,
    CompressionMethod,
    ArchiveOrganization,
    ArchiveStatus,
    # Convenience functions
    archive_campaign,
    clean_campaign,
    compress_campaign,
    restore_campaign,
    list_archived_campaigns,
    # Default cleanup lists
    DEFAULT_DIRS_TO_CLEAN,
    DEFAULT_RAW_PATTERNS_TO_CLEAN,
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
    # Main orchestrator (replaces IGNSS.pm)
    "IGNSSOrchestrator",
    "ProcessingConfig",
    "ProcessingResult",
    "ProcessingType",
    "ProductChecker",
    "ProductConfig",
    "ProductCategory",
    "DataManager",
    "DataSourceConfig",
    "BSWExecutor",
    "DatabaseConfig",
    "DCMConfig",
    "create_daily_config",
    "create_hourly_config",
    "run_daily_processing",
    "run_hourly_processing",
    # Daily CRD generation (replaces iGNSS_D_CRD_54.pl)
    "DailyCRDProcessor",
    "DailyCRDConfig",
    "DailyCRDResult",
    "NetworkArchive",
    "StationCoordinate",
    "create_daily_crd_config",
    # Campaign archival (replaces IGNSS::dcm)
    "CampaignArchiver",
    "CampaignArchiveConfig",
    "CampaignArchiveResult",
    "CampaignRestoreConfig",
    "CompressionMethod",
    "ArchiveOrganization",
    "ArchiveStatus",
    "archive_campaign",
    "clean_campaign",
    "compress_campaign",
    "restore_campaign",
    "list_archived_campaigns",
    "DEFAULT_DIRS_TO_CLEAN",
    "DEFAULT_RAW_PATTERNS_TO_CLEAN",
]
