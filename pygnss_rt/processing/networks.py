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

PCF Files:
- PPP-AR processing: /home/ahunegnaw/GPSUSER54_LANT/PCF/PPP54IGS.PCF
- Network DD (NRDDP): /home/ahunegnaw/GPSUSER54_LANT/PCF/SMHI_TGX_OCT2025_MGX.PCF
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any


# PCF file paths for different processing modes
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
        default_factory=lambda: ["OBS", "RAW", "ORX", "INP"]
    )
    dcm_archive_dir: str = ""
    dcm_organization: str = "yyyy/doy"

    # Processing options
    antenna_phase_center: str = "ABSOLUTE"  # ABSOLUTE or RELATIVE
    datum: str = "IGS20"
    min_elevation: int = 5

    # Latency for cron mode (days)
    cron_latency_days: int = 21


def get_default_info_files(ignss_dir: str) -> dict[str, str]:
    """Get default BSW information files."""
    return {
        "sessions": f"{ignss_dir}/info/SESSIONS.SES",
        "station": f"{ignss_dir}/info/IGS20_54.STA",
        "ocean_loading": f"{ignss_dir}/info/IGS20_54.BLQ",
        "abbreviations": f"{ignss_dir}/info/IGS20_54.ABB",
        "obs_selection": f"{ignss_dir}/info/OBSSEL.SEL",
        "sinex_skeleton": f"{ignss_dir}/info/SINEX.SKL",
        "phase_center": f"{ignss_dir}/info/ANTENNA_I20.PCV",
    }


def get_igs_archive_specs(data_root: str) -> dict[str, ArchiveFileSpec]:
    """Get archive file specs for networks needing IGS alignment."""
    return {
        "alignment": ArchiveFileSpec(
            root=f"{data_root}/campaigns/ppp",
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
            root=f"{data_root}/campaigns/ppp",
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
    ignss_dir: str = "/home/ahunegnaw/Python_IGNSS/i-GNSS",
    data_root: str = "/home/ahunegnaw/data54",
    gpsuser_dir: str = "/home/ahunegnaw/GPSUSER54_LANT",
) -> dict[NetworkID, NetworkProfile]:
    """Create all network profiles.

    Args:
        ignss_dir: i-GNSS installation directory
        data_root: Root data directory
        gpsuser_dir: GPSUSER directory for BSW

    Returns:
        Dictionary of NetworkID -> NetworkProfile
    """
    default_info = get_default_info_files(ignss_dir)
    igs_archives = get_igs_archive_specs(data_root)

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
            xml_file=f"{ignss_dir}/info/IGS20rh.xml",
            primary_net="IGS20",
            station_type="core",
            exclude_stations=["lpgs", "yel2"],
        ),
        data_ftp_sources=[
            FTPDataSource(server_id="CDDIS", category="daily"),
        ],
        orbit_source=default_orbit,
        erp_source=default_erp,
        clock_source=default_clock,
        pcf_file=f"{gpsuser_dir}/PCF/{PCF_PPP_AR}",
        bsw_options_xml=f"{ignss_dir}/callers/iGNSS_D_PPP_AR_IG_IGS54_direct.xml",
        info_files={**default_info},
        coord_file=f"{ignss_dir}/info/IGS20_54.CRD",
        archive_files={},  # IGS is primary, no archive dependencies
        requires_igs_alignment=False,
        dcm_archive_dir=f"{data_root}/campaigns/ppp",
    )

    # EU - EUREF Network
    profiles[NetworkID.EU] = NetworkProfile(
        network_id=NetworkID.EU,
        description="EUREF stations (European reference network)",
        session_id="EU",
        task_id="EU",
        station_filter=StationFilter(
            xml_file=f"{ignss_dir}/info/eurefrh.xml",
            station_type="EUREF",
            use_nrt=True,
            exclude_stations=["newl"],
        ),
        data_ftp_sources=[
            FTPDataSource(server_id="BKGE", category="daily"),
            FTPDataSource(server_id="BKGE_IGS", category="daily"),
            FTPDataSource(server_id="BEV", category="daily"),
        ],
        orbit_source=default_orbit,
        erp_source=default_erp,
        clock_source=default_clock,
        pcf_file=f"{gpsuser_dir}/PCF/{PCF_PPP_AR}",
        bsw_options_xml=f"{ignss_dir}/callers/iGNSS_D_PPP_AR_EU_IGS54_direct.xml",
        info_files={**default_info},
        coord_file=f"{ignss_dir}/info/NEWNRT54.CRD",
        archive_files=igs_archives,
        requires_igs_alignment=True,
        dcm_archive_dir=f"{data_root}/campaigns/ppp",
    )

    # GB - Great Britain Network
    profiles[NetworkID.GB] = NetworkProfile(
        network_id=NetworkID.GB,
        description="Great Britain stations (OS active, scientific, IGS)",
        session_id="GB",
        task_id="GB",
        station_filter=StationFilter(
            xml_file=f"{ignss_dir}/info/stationsrh.xml",
            use_nrt=True,
            additional_types=["OS active", "scientific", "IGS"],
            exclude_stations=["newl00gbr", "cari00gbr", "hart00gbr"],
        ),
        data_ftp_sources=[
            FTPDataSource(server_id="OSGB", category="daily"),
        ],
        orbit_source=default_orbit,
        erp_source=default_erp,
        clock_source=default_clock,
        pcf_file=f"{gpsuser_dir}/PCF/{PCF_PPP_AR}",
        bsw_options_xml=f"{ignss_dir}/callers/iGNSS_D_PPP_AR_GB_IGS54_direct.xml",
        info_files={**default_info},
        coord_file=f"{ignss_dir}/info/NEWNRT54.CRD",
        archive_files=igs_archives,
        requires_igs_alignment=True,
        dcm_archive_dir=f"{data_root}/campaigns/ppp",
    )

    # RG - RGP France Network
    profiles[NetworkID.RG] = NetworkProfile(
        network_id=NetworkID.RG,
        description="RGP France stations (French permanent network)",
        session_id="RG",
        task_id="RG",
        station_filter=StationFilter(
            xml_file=f"{ignss_dir}/info/RGPrh.xml",
            station_type="active",
            use_nrt=True,
        ),
        data_ftp_sources=[
            FTPDataSource(server_id="RGPDATA", category="daily"),
        ],
        orbit_source=default_orbit,
        erp_source=default_erp,
        clock_source=default_clock,
        pcf_file=f"{gpsuser_dir}/PCF/{PCF_PPP_AR}",
        bsw_options_xml=f"{ignss_dir}/callers/iGNSS_D_PPP_AR_RG_IGS54_direct.xml",
        info_files={**default_info},
        coord_file=f"{ignss_dir}/info/NEWNRT54.CRD",
        archive_files=igs_archives,
        requires_igs_alignment=True,
        dcm_archive_dir=f"{data_root}/campaigns/ppp",
    )

    # SS - Supersites Network (Netherlands)
    profiles[NetworkID.SS] = NetworkProfile(
        network_id=NetworkID.SS,
        description="Supersites (Netherlands/European supersites)",
        session_id="SS",
        task_id="SS",
        station_filter=StationFilter(
            xml_file=f"{ignss_dir}/info/supersitesrh.xml",
            station_type="active",
            use_nrt=True,
            exclude_stations=["stav00nld", "bors00nld", "warm00nld"],
        ),
        data_ftp_sources=[
            FTPDataSource(server_id="Kadaster", category="daily"),
        ],
        orbit_source=default_orbit,
        erp_source=default_erp,
        clock_source=default_clock,
        pcf_file=f"{gpsuser_dir}/PCF/{PCF_PPP_AR}",
        bsw_options_xml=f"{ignss_dir}/callers/iGNSS_D_PPP_AR_SS_IGS54_direct.xml",
        info_files={**default_info},
        coord_file=f"{ignss_dir}/info/NEWNRT54.CRD",
        archive_files=igs_archives,
        requires_igs_alignment=True,
        dcm_archive_dir=f"{data_root}/campaigns/ppp",
    )

    return profiles


def get_network_profile(
    network_id: NetworkID | str,
    ignss_dir: str | None = None,
    data_root: str | None = None,
    gpsuser_dir: str | None = None,
) -> NetworkProfile:
    """Get a specific network profile.

    Args:
        network_id: Network ID (e.g., "IG", "EU", NetworkID.IG)
        ignss_dir: Override i-GNSS directory
        data_root: Override data root directory
        gpsuser_dir: Override GPSUSER directory

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

    kwargs = {}
    if ignss_dir:
        kwargs["ignss_dir"] = ignss_dir
    if data_root:
        kwargs["data_root"] = data_root
    if gpsuser_dir:
        kwargs["gpsuser_dir"] = gpsuser_dir

    profiles = create_network_profiles(**kwargs) if kwargs else create_network_profiles()
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
