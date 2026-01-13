"""
Network configuration profiles for daily PPP processing.

Defines network-specific configurations for different GNSS station networks:
- IG: IGS core stations (global reference network)
- EU: EUREF stations (European reference network)
- GB: Great Britain stations (OS active, scientific, local IGS)
- RG: RGP France stations (French permanent network)
- SS: Supersites (Netherlands/European supersites)

Replaces hardcoded configurations in Perl caller scripts:
- iGNSS_D_PPP_AR_IG_IGS54_direct_NRT.pl
- iGNSS_D_PPP_AR_EU_IGS54_direct_NRT.pl
- iGNSS_D_PPP_AR_GB_IGS54_direct_NRT.pl
- iGNSS_D_PPP_AR_RG_IGS54_direct_NRT.pl
- iGNSS_D_PPP_AR_SS_IGS54_direct_NRT.pl

Author: Addisu Hunegnaw
Date: January 2026
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any

from pygnss_rt.core.paths import PathConfig, get_paths


# PCF file names for different processing modes
PCF_PPP_AR = "PPP54IGS.PCF"  # PPP with Ambiguity Resolution
PCF_NETWORK_DD = "SMHI_TGX_OCT2025_MGX.PCF"  # Network Double Difference (NRDDP)


class NetworkID(str, Enum):
    """Network identifiers for daily PPP processing."""

    IG = "IG"  # IGS core stations
    EU = "EU"  # EUREF stations
    GB = "GB"  # Great Britain stations
    RG = "RG"  # RGP France stations
    SS = "SS"  # Supersites (Netherlands)


@dataclass
class StationFilter:
    """Station selection criteria for a network."""

    xml_file: str  # Station XML info file
    primary_net: str | None = None  # Primary network filter (e.g., "IGS20")
    station_type: str | None = None  # Station type filter (e.g., "core", "active")
    use_nrt: bool = True  # Filter for NRT-enabled stations
    additional_types: list[str] = field(default_factory=list)  # Additional types to include
    exclude_stations: list[str] = field(default_factory=list)  # Stations to exclude


@dataclass
class FTPDataSource:
    """FTP data source configuration."""

    server_id: str  # Server ID from FTP config
    data_type: str = "data"
    category: str = "daily"


@dataclass
class ProductSource:
    """Product (orbit/ERP/clock) source configuration."""

    enabled: bool = True
    provider: str = "IGS"  # IGS, CODE, etc.
    tier: str = "final"  # final, rapid, ultra
    ftp_servers: list[str] = field(default_factory=lambda: ["CDDIS", "BKGE_IGS"])


@dataclass
class ArchiveFileSpec:
    """Specification for archived files to copy from previous runs."""

    root: str
    organization: str  # e.g., "yyyy/doy"
    campaign_pattern: str  # e.g., "YYDOYIG"
    prefix: str  # e.g., "AIG"
    body_pattern: str  # e.g., "YYDOY0"
    source_dir: str  # e.g., "STA"
    extensions: list[str]  # e.g., [".CRD", ".FIX"]
    compression: str = ".gz"
    dest_dir: str = "STA"
    option_name: str = ""  # BSW option name


@dataclass
class NetworkProfile:
    """Complete network configuration profile for daily PPP processing."""

    network_id: NetworkID
    description: str
    session_id: str  # 2-char session ID (e.g., "IG", "EU")
    task_id: str  # Task ID for BSW

    # Station configuration
    station_filter: StationFilter

    # FTP data sources for station data
    data_ftp_sources: list[FTPDataSource]

    # Product sources
    orbit_source: ProductSource
    erp_source: ProductSource
    clock_source: ProductSource

    # BSW configuration files
    pcf_file: str  # PCF file path pattern
    bsw_options_xml: str  # BSW options XML file

    # Information files (BSW auxiliary files)
    info_files: dict[str, str] = field(default_factory=dict)

    # Coordinate file for this network
    coord_file: str = ""  # infoCRD path

    # Archive files from previous runs (for alignment)
    archive_files: dict[str, ArchiveFileSpec] = field(default_factory=dict)

    # Whether this network depends on IGS alignment (needs IGS processed first)
    requires_igs_alignment: bool = False

    # DCM (Delete/Compress/Move) settings
    dcm_enabled: bool = True
    dcm_dirs_to_delete: list[str] = field(
        default_factory=lambda: ["RAW", "BPE", "OBS", "ORX", "INP", "ORB", "GRD", "GEN"]
    )
    dcm_archive_dir: str = ""
    dcm_organization: str = "yyyy/doy"

    # Processing options
    antenna_phase_center: str = "ABSOLUTE"  # ABSOLUTE or RELATIVE
    datum: str = "IGS20"
    min_elevation: int = 5

    # Latency for cron mode (days)
    cron_latency_days: int = 21


def get_default_info_files(paths: PathConfig) -> dict[str, str]:
    """Get default BSW information files using PathConfig.

    Args:
        paths: PathConfig instance

    Returns:
        Dictionary of info file paths
    """
    return {
        "sessions": str(paths.sessions_file),
        "station": str(paths.station_info_file),
        "ocean_loading": str(paths.ocean_loading_file),
        "abbreviations": str(paths.abbreviations_file),
        "obs_selection": str(paths.obs_selection_file),
        "sinex_skeleton": str(paths.sinex_skeleton_file),
        "phase_center": str(paths.phase_center_file),
    }


def get_igs_archive_specs(paths: PathConfig) -> dict[str, ArchiveFileSpec]:
    """Get archive file specs for networks needing IGS alignment.

    Args:
        paths: PathConfig instance

    Returns:
        Dictionary of archive file specifications
    """
    ppp_dir = str(paths.ppp_campaigns_dir) if paths.ppp_campaigns_dir else ""

    return {
        "alignment": ArchiveFileSpec(
            root=ppp_dir,
            organization="yyyy/doy",
            campaign_pattern="YYDOYIG",
            prefix="AIG",
            body_pattern="YYDOY0",
            source_dir="STA",
            extensions=[".CRD", ".FIX"],
            dest_dir="STA",
            option_name="opt_IGSALI",
        ),
        "ppp_coords": ArchiveFileSpec(
            root=ppp_dir,
            organization="yyyy/doy",
            campaign_pattern="YYDOYIG",
            prefix="PIG",
            body_pattern="YYDOY0",
            source_dir="STA",
            extensions=[".CRD"],
            dest_dir="STA",
            option_name="opt_IGSPPP",
        ),
    }


def create_network_profiles(
    paths: PathConfig | None = None,
) -> dict[NetworkID, NetworkProfile]:
    """Create all network profiles.

    Args:
        paths: PathConfig instance (uses global instance if None)

    Returns:
        Dictionary of NetworkID -> NetworkProfile
    """
    if paths is None:
        paths = get_paths()

    default_info = get_default_info_files(paths)
    igs_archives = get_igs_archive_specs(paths)

    # Get directory paths as strings
    station_data_dir = str(paths.station_data_dir)
    bsw_configs_dir = str(paths.bsw_configs_dir)
    pcf_dir = str(paths.pcf_dir) if paths.pcf_dir else ""
    ppp_dir = str(paths.ppp_campaigns_dir) if paths.ppp_campaigns_dir else ""

    # Default product sources (same for all networks)
    default_orbit = ProductSource(
        enabled=True,
        provider="IGS",
        tier="final",
        ftp_servers=["CDDIS", "BKGE_IGS"],
    )
    default_erp = ProductSource(
        enabled=True,
        provider="IGS",
        tier="final",
        ftp_servers=["CDDIS", "BKGE_IGS"],
    )
    default_clock = ProductSource(
        enabled=True,
        provider="IGS",
        tier="final",
        ftp_servers=["CDDIS", "BKGE_IGS"],
    )

    profiles = {}

    # IG - IGS Core Network (primary reference)
    profiles[NetworkID.IG] = NetworkProfile(
        network_id=NetworkID.IG,
        description="IGS core stations (global reference network)",
        session_id="IG",
        task_id="IG",
        station_filter=StationFilter(
            xml_file=f"{station_data_dir}/IGS20rh.xml",
            primary_net="IGS20",
            station_type="core",
            exclude_stations=["lpgs", "yel2"],
        ),
        data_ftp_sources=[
            # RINEX3 from CDDIS (primary), fallback to RINEX2 from FTP servers
            FTPDataSource(server_id="CDDIS", category="daily"),
            FTPDataSource(server_id="BKGE_IGS", category="daily"),
            FTPDataSource(server_id="IGN_IGS", category="daily"),
        ],
        orbit_source=default_orbit,
        erp_source=default_erp,
        clock_source=default_clock,
        pcf_file=f"{pcf_dir}/{PCF_PPP_AR}",
        bsw_options_xml=f"{bsw_configs_dir}/iGNSS_D_PPP_AR_IG_IGS54_direct.yaml",
        info_files={**default_info},
        coord_file=str(paths.igs20_coord_file),
        archive_files={},  # IGS is primary, no archive dependencies
        requires_igs_alignment=False,
        dcm_archive_dir=ppp_dir,
    )

    # EU - EUREF Network
    profiles[NetworkID.EU] = NetworkProfile(
        network_id=NetworkID.EU,
        description="EUREF stations (European reference network)",
        session_id="EU",
        task_id="EU",
        station_filter=StationFilter(
            xml_file=f"{station_data_dir}/eurefrh.xml",
            station_type="EUREF",
            use_nrt=True,
            exclude_stations=["newl"],
        ),
        data_ftp_sources=[
            FTPDataSource(server_id="BKGE_EUREF", category="daily"),
            FTPDataSource(server_id="BKGE_IGS", category="daily"),
        ],
        orbit_source=default_orbit,
        erp_source=default_erp,
        clock_source=default_clock,
        pcf_file=f"{pcf_dir}/{PCF_PPP_AR}",
        bsw_options_xml=f"{bsw_configs_dir}/iGNSS_D_PPP_AR_EU_IGS54_direct.yaml",
        info_files={**default_info},
        coord_file=str(paths.nrt_coord_file),
        archive_files=igs_archives,
        requires_igs_alignment=True,
        dcm_archive_dir=ppp_dir,
    )

    # GB - Great Britain Network
    profiles[NetworkID.GB] = NetworkProfile(
        network_id=NetworkID.GB,
        description="Great Britain stations (OS active, scientific, IGS)",
        session_id="GB",
        task_id="GB",
        station_filter=StationFilter(
            xml_file=f"{station_data_dir}/stationsrh.xml",
            use_nrt=True,
            additional_types=["OS active", "scientific", "IGS"],
            exclude_stations=["newl00gbr", "cari00gbr", "hart00gbr"],
        ),
        data_ftp_sources=[
            FTPDataSource(server_id="OSGB_HOURLY", category="hourly"),
        ],
        orbit_source=default_orbit,
        erp_source=default_erp,
        clock_source=default_clock,
        pcf_file=f"{pcf_dir}/{PCF_PPP_AR}",
        bsw_options_xml=f"{bsw_configs_dir}/iGNSS_D_PPP_AR_GB_IGS54_direct.yaml",
        info_files={**default_info},
        coord_file=str(paths.nrt_coord_file),
        archive_files=igs_archives,
        requires_igs_alignment=True,
        dcm_archive_dir=ppp_dir,
    )

    # RG - RGP France Network
    profiles[NetworkID.RG] = NetworkProfile(
        network_id=NetworkID.RG,
        description="RGP France stations (French permanent network)",
        session_id="RG",
        task_id="RG",
        station_filter=StationFilter(
            xml_file=f"{station_data_dir}/RGPrh.xml",
            station_type="active",
            use_nrt=True,
        ),
        data_ftp_sources=[
            FTPDataSource(server_id="RGP", category="daily"),
        ],
        orbit_source=default_orbit,
        erp_source=default_erp,
        clock_source=default_clock,
        pcf_file=f"{pcf_dir}/{PCF_PPP_AR}",
        bsw_options_xml=f"{bsw_configs_dir}/iGNSS_D_PPP_AR_RG_IGS54_direct.yaml",
        info_files={**default_info},
        coord_file=str(paths.nrt_coord_file),
        archive_files=igs_archives,
        requires_igs_alignment=True,
        dcm_archive_dir=ppp_dir,
    )

    # SS - Supersites Network (Netherlands)
    profiles[NetworkID.SS] = NetworkProfile(
        network_id=NetworkID.SS,
        description="Supersites (Netherlands/European supersites)",
        session_id="SS",
        task_id="SS",
        station_filter=StationFilter(
            xml_file=f"{station_data_dir}/supersitesrh.xml",
            station_type="active",
            use_nrt=True,
            exclude_stations=["stav00nld", "bors00nld", "warm00nld"],
        ),
        data_ftp_sources=[
            FTPDataSource(server_id="CDDIS", category="daily"),
        ],
        orbit_source=default_orbit,
        erp_source=default_erp,
        clock_source=default_clock,
        pcf_file=f"{pcf_dir}/{PCF_PPP_AR}",
        bsw_options_xml=f"{bsw_configs_dir}/iGNSS_D_PPP_AR_SS_IGS54_direct.yaml",
        info_files={**default_info},
        coord_file=str(paths.nrt_coord_file),
        archive_files=igs_archives,
        requires_igs_alignment=True,
        dcm_archive_dir=ppp_dir,
    )

    return profiles


def get_network_profile(
    network_id: NetworkID | str,
    paths: PathConfig | None = None,
) -> NetworkProfile:
    """Get a specific network profile.

    Args:
        network_id: Network ID (e.g., "IG", "EU", NetworkID.IG)
        paths: PathConfig instance (uses global instance if None)

    Returns:
        NetworkProfile for the specified network

    Raises:
        ValueError: If network_id is invalid
    """
    if isinstance(network_id, str):
        try:
            network_id = NetworkID(network_id.upper())
        except ValueError:
            valid = ", ".join(n.value for n in NetworkID)
            raise ValueError(f"Invalid network ID '{network_id}'. Valid: {valid}")

    profiles = create_network_profiles(paths)
    return profiles[network_id]


def list_networks() -> list[dict[str, str]]:
    """List available networks with descriptions.

    Returns:
        List of dicts with network info
    """
    profiles = create_network_profiles()
    return [
        {
            "id": profile.network_id.value,
            "description": profile.description,
            "session_id": profile.session_id,
            "requires_alignment": "Yes" if profile.requires_igs_alignment else "No",
        }
        for profile in profiles.values()
    ]
