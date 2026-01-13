"""
Central path configuration for pygnss_rt.

This module provides a centralized configuration for all paths used by pygnss_rt,
ensuring clean separation from the old Perl i-GNSS installation.

All paths can be overridden via environment variables or configuration.

Author: Addisu Hunegnaw
Date: January 2026
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import ClassVar


def _get_pygnss_rt_dir() -> Path:
    """Get the pygnss_rt installation directory."""
    return Path(__file__).parent.parent.resolve()


@dataclass
class PathConfig:
    """Central configuration for all paths used by pygnss_rt.

    Paths are resolved in this priority order:
    1. Explicitly set values (constructor arguments)
    2. Environment variables
    3. Default values (relative to pygnss_rt directory)

    Environment Variables:
        PYGNSS_RT_DIR: pygnss_rt installation directory
        BERN54_DIR: Bernese 5.4 installation directory
        GPSUSER_DIR: Bernese user area directory
        DATA_ROOT: Root directory for data storage
    """

    # Class-level singleton instance
    _instance: ClassVar[PathConfig | None] = None

    # pygnss_rt installation directory
    pygnss_rt_dir: Path = field(default_factory=_get_pygnss_rt_dir)

    # External dependencies (Bernese installation)
    bern54_dir: Path | None = None
    gpsuser_dir: Path | None = None

    # Data directories
    data_root: Path | None = None
    campaign_root: Path | None = None  # BSW campaign root (GPSDATA/CAMPAIGN54)
    nrt_coord_dir: Path | None = None  # NRT coordinate directory
    tro_campaign_root: Path | None = None  # TRO campaign root
    ppp_campaign_root: Path | None = None  # PPP campaign root
    vmf_source_dir: Path | None = None  # VMF3 source directory
    apriori_source_dir: Path | None = None  # CODE apriori source directory

    def __post_init__(self) -> None:
        """Resolve paths from environment variables if not explicitly set."""
        # Ensure pygnss_rt_dir is a Path
        if isinstance(self.pygnss_rt_dir, str):
            self.pygnss_rt_dir = Path(self.pygnss_rt_dir)

        # Resolve from environment variables
        if self.bern54_dir is None:
            env_bern54 = os.environ.get("BERN54_DIR")
            if env_bern54:
                self.bern54_dir = Path(env_bern54)
            else:
                # Try common locations (prefer user's home, then system-wide)
                for path in [Path.home() / "BERN54", Path("/opt/BERN54"), Path("/usr/local/BERN54")]:
                    if Path(path).exists():
                        self.bern54_dir = Path(path)
                        break
        elif isinstance(self.bern54_dir, str):
            self.bern54_dir = Path(self.bern54_dir)

        if self.gpsuser_dir is None:
            env_gpsuser = os.environ.get("GPSUSER_DIR")
            if env_gpsuser:
                self.gpsuser_dir = Path(env_gpsuser)
            else:
                # Try common locations (prefer user's home)
                for path in [Path.home() / "GPSUSER54_LANT", Path.home() / "GPSUSER54"]:
                    if Path(path).exists():
                        self.gpsuser_dir = Path(path)
                        break
        elif isinstance(self.gpsuser_dir, str):
            self.gpsuser_dir = Path(self.gpsuser_dir)

        if self.data_root is None:
            env_data = os.environ.get("DATA_ROOT")
            if env_data:
                self.data_root = Path(env_data)
            else:
                # Try common locations (prefer current user's home)
                for path in [Path.home() / "data54", Path("/data54")]:
                    if Path(path).exists():
                        self.data_root = Path(path)
                        break
        elif isinstance(self.data_root, str):
            self.data_root = Path(self.data_root)

        # Resolve campaign_root from environment or data_root
        if self.campaign_root is None:
            env_campaign = os.environ.get("CAMPAIGN_ROOT")
            if env_campaign:
                self.campaign_root = Path(env_campaign)
            elif self.data_root:
                # Default: GPSDATA/CAMPAIGN54 relative to home
                for path in [Path.home() / "GPSDATA" / "CAMPAIGN54", self.data_root / "campaigns"]:
                    if Path(path).exists():
                        self.campaign_root = Path(path)
                        break
        elif isinstance(self.campaign_root, str):
            self.campaign_root = Path(self.campaign_root)

        # Resolve nrt_coord_dir from environment or data_root
        if self.nrt_coord_dir is None:
            env_nrt_coord = os.environ.get("NRT_COORD_DIR")
            if env_nrt_coord:
                self.nrt_coord_dir = Path(env_nrt_coord)
            elif self.data_root:
                self.nrt_coord_dir = self.data_root / "nrtCoord"
        elif isinstance(self.nrt_coord_dir, str):
            self.nrt_coord_dir = Path(self.nrt_coord_dir)

        # Resolve tro_campaign_root
        if self.tro_campaign_root is None:
            env_tro = os.environ.get("TRO_CAMPAIGN_ROOT")
            if env_tro:
                self.tro_campaign_root = Path(env_tro)
            elif self.data_root:
                self.tro_campaign_root = self.data_root / "campaigns" / "tro"
        elif isinstance(self.tro_campaign_root, str):
            self.tro_campaign_root = Path(self.tro_campaign_root)

        # Resolve ppp_campaign_root
        if self.ppp_campaign_root is None:
            env_ppp = os.environ.get("PPP_CAMPAIGN_ROOT")
            if env_ppp:
                self.ppp_campaign_root = Path(env_ppp)
            elif self.data_root:
                self.ppp_campaign_root = self.data_root / "campaigns" / "ppp"
        elif isinstance(self.ppp_campaign_root, str):
            self.ppp_campaign_root = Path(self.ppp_campaign_root)

        # Resolve VMF source directory
        if self.vmf_source_dir is None:
            env_vmf = os.environ.get("VMF_SOURCE_DIR")
            if env_vmf:
                self.vmf_source_dir = Path(env_vmf)
            else:
                # Try common locations
                for path in [Path.home() / "tiga" / "VMF3", Path.home() / "data" / "VMF3"]:
                    if Path(path).exists():
                        self.vmf_source_dir = Path(path)
                        break
        elif isinstance(self.vmf_source_dir, str):
            self.vmf_source_dir = Path(self.vmf_source_dir)

        # Resolve apriori source directory
        if self.apriori_source_dir is None:
            env_apriori = os.environ.get("APRIORI_SOURCE_DIR")
            if env_apriori:
                self.apriori_source_dir = Path(env_apriori)
            else:
                # Try common locations
                for path in [Path.home() / "tiga" / "CODE_APRIORI", Path.home() / "data" / "CODE_APRIORI"]:
                    if Path(path).exists():
                        self.apriori_source_dir = Path(path)
                        break
        elif isinstance(self.apriori_source_dir, str):
            self.apriori_source_dir = Path(self.apriori_source_dir)

    # =========================================================================
    # pygnss_rt internal directories
    # =========================================================================

    @property
    def station_data_dir(self) -> Path:
        """Directory containing station data files (STA, BLQ, CRD, XML, etc.)."""
        return self.pygnss_rt_dir / "station_data"

    @property
    def bsw_configs_dir(self) -> Path:
        """Directory containing BSW configuration/options XML files."""
        return self.pygnss_rt_dir / "bsw_configs"

    # Backward compatibility aliases
    @property
    def info_dir(self) -> Path:
        """Alias for station_data_dir (backward compatibility)."""
        return self.station_data_dir

    @property
    def callers_dir(self) -> Path:
        """Alias for bsw_configs_dir (backward compatibility)."""
        return self.bsw_configs_dir

    @property
    def config_dir(self) -> Path:
        """Directory containing configuration files (YAML, etc.)."""
        return self.pygnss_rt_dir / "config"

    # =========================================================================
    # BSW Information Files (in station_data directory)
    # =========================================================================

    @property
    def sessions_file(self) -> Path:
        """Session table file (SESSIONS.SES)."""
        return self.station_data_dir / "SESSIONS.SES"

    @property
    def station_info_file(self) -> Path:
        """Station information file (IGS20_54.STA)."""
        return self.station_data_dir / "IGS20_54.STA"

    @property
    def ocean_loading_file(self) -> Path:
        """Ocean loading file (IGS20_54.BLQ)."""
        return self.station_data_dir / "IGS20_54.BLQ"

    @property
    def abbreviations_file(self) -> Path:
        """Abbreviations file (IGS20_54.ABB)."""
        return self.station_data_dir / "IGS20_54.ABB"

    @property
    def obs_selection_file(self) -> Path:
        """Observation selection file (OBSSEL.SEL)."""
        return self.station_data_dir / "OBSSEL.SEL"

    @property
    def sinex_skeleton_file(self) -> Path:
        """SINEX skeleton file (SINEX.SKL)."""
        return self.station_data_dir / "SINEX.SKL"

    @property
    def phase_center_file(self) -> Path:
        """Antenna phase center file (ANTENNA_I20.PCV)."""
        return self.station_data_dir / "ANTENNA_I20.PCV"

    @property
    def atx_file(self) -> Path:
        """ANTEX file (I20.ATX)."""
        return self.station_data_dir / "I20.ATX"

    # =========================================================================
    # Coordinate Files (in station_data directory)
    # =========================================================================

    @property
    def igs20_coord_file(self) -> Path:
        """IGS20 coordinate file (IGS20_54.CRD)."""
        return self.station_data_dir / "IGS20_54.CRD"

    @property
    def nrt_coord_file(self) -> Path:
        """NRT coordinate file (NEWNRT54.CRD)."""
        return self.station_data_dir / "NEWNRT54.CRD"

    @property
    def nrt_station_file(self) -> Path:
        """NRT station file (NEWNRT54.STA)."""
        return self.station_data_dir / "NEWNRT54.STA"

    # =========================================================================
    # Station XML Files (in station_data directory)
    # =========================================================================

    @property
    def igs_stations_xml(self) -> Path:
        """IGS stations XML file."""
        return self.station_data_dir / "IGS20rh.xml"

    @property
    def euref_stations_xml(self) -> Path:
        """EUREF stations XML file."""
        return self.station_data_dir / "eurefrh.xml"

    @property
    def gb_stations_xml(self) -> Path:
        """Great Britain stations XML file."""
        return self.station_data_dir / "stationsrh.xml"

    @property
    def rgp_stations_xml(self) -> Path:
        """RGP France stations XML file."""
        return self.station_data_dir / "RGPrh.xml"

    @property
    def supersites_xml(self) -> Path:
        """Supersites stations XML file."""
        return self.station_data_dir / "supersitesrh.xml"

    # =========================================================================
    # BSW Options XML Files (in bsw_configs directory)
    # =========================================================================

    def get_bsw_options_file(self, network_id: str) -> Path:
        """Get BSW options file for a network.

        Prefers YAML format, falls back to XML if YAML doesn't exist.

        Args:
            network_id: Network ID (IG, EU, GB, RG, SS)

        Returns:
            Path to the BSW options file (YAML preferred)
        """
        yaml_path = self.bsw_configs_dir / f"iGNSS_D_PPP_AR_{network_id}_IGS54_direct.yaml"
        if yaml_path.exists():
            return yaml_path
        return self.bsw_configs_dir / f"iGNSS_D_PPP_AR_{network_id}_IGS54_direct.xml"

    def get_bsw_options_xml(self, network_id: str) -> Path:
        """Get BSW options file for a network (backward compatibility alias).

        Args:
            network_id: Network ID (IG, EU, GB, RG, SS)

        Returns:
            Path to the BSW options file
        """
        return self.get_bsw_options_file(network_id)

    # =========================================================================
    # Configuration Files
    # =========================================================================

    @property
    def ftp_servers_yaml(self) -> Path:
        """FTP servers configuration YAML file."""
        return self.config_dir / "ftp_servers.yaml"

    # =========================================================================
    # External Bernese Paths (require BERN54/GPSUSER installation)
    # =========================================================================

    @property
    def loadgps_setvar(self) -> Path | None:
        """Bernese LOADGPS.setvar file."""
        if self.bern54_dir:
            return self.bern54_dir / "LOADGPS.setvar"
        return None

    @property
    def pcf_dir(self) -> Path | None:
        """PCF directory in GPSUSER."""
        if self.gpsuser_dir:
            return self.gpsuser_dir / "PCF"
        return None

    @property
    def opt_dir(self) -> Path | None:
        """OPT directory in GPSUSER."""
        if self.gpsuser_dir:
            return self.gpsuser_dir / "OPT"
        return None

    @property
    def ref_local_dir(self) -> Path | None:
        """REF54_LOCAL directory in GPSUSER."""
        if self.gpsuser_dir:
            return self.gpsuser_dir / "REF54_LOCAL"
        return None

    # =========================================================================
    # Data Directories
    # =========================================================================

    @property
    def campaigns_dir(self) -> Path | None:
        """Campaigns directory for BSW processing."""
        if self.data_root:
            return self.data_root / "campaigns"
        return None

    @property
    def ppp_campaigns_dir(self) -> Path | None:
        """PPP campaigns directory."""
        if self.data_root:
            return self.data_root / "campaigns" / "ppp"
        return None

    @property
    def products_dir(self) -> Path | None:
        """Products directory for orbit, clock, etc."""
        if self.data_root:
            return self.data_root / "products"
        return None

    @property
    def rinex_dir(self) -> Path | None:
        """RINEX data directory."""
        if self.data_root:
            return self.data_root / "rinex"
        return None

    # =========================================================================
    # Helper Methods
    # =========================================================================

    def get_info_files(self) -> dict[str, Path]:
        """Get dictionary of BSW information files.

        Returns:
            Dictionary mapping file type to path
        """
        return {
            "sessions": self.sessions_file,
            "station": self.station_info_file,
            "ocean_loading": self.ocean_loading_file,
            "abbreviations": self.abbreviations_file,
            "obs_selection": self.obs_selection_file,
            "sinex_skeleton": self.sinex_skeleton_file,
            "phase_center": self.phase_center_file,
        }

    def validate(self) -> list[str]:
        """Validate that required paths exist.

        Returns:
            List of error messages for missing paths
        """
        errors = []

        # Check pygnss_rt directories
        if not self.station_data_dir.exists():
            errors.append(f"Station data directory not found: {self.station_data_dir}")
        if not self.bsw_configs_dir.exists():
            errors.append(f"BSW configs directory not found: {self.bsw_configs_dir}")
        if not self.config_dir.exists():
            errors.append(f"Config directory not found: {self.config_dir}")

        # Check critical info files
        critical_files = [
            ("SESSIONS.SES", self.sessions_file),
            ("IGS20_54.STA", self.station_info_file),
            ("ANTENNA_I20.PCV", self.phase_center_file),
        ]
        for name, path in critical_files:
            if not path.exists():
                errors.append(f"Required file not found: {name} at {path}")

        # Check external dependencies
        if self.bern54_dir is None:
            errors.append("BERN54_DIR not configured (set environment variable or constructor)")
        elif not self.bern54_dir.exists():
            errors.append(f"BERN54 directory not found: {self.bern54_dir}")

        if self.gpsuser_dir is None:
            errors.append("GPSUSER_DIR not configured (set environment variable or constructor)")
        elif not self.gpsuser_dir.exists():
            errors.append(f"GPSUSER directory not found: {self.gpsuser_dir}")

        return errors

    @classmethod
    def get_instance(cls) -> PathConfig:
        """Get singleton instance of PathConfig.

        Returns:
            PathConfig singleton instance
        """
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    @classmethod
    def reset_instance(cls) -> None:
        """Reset the singleton instance (mainly for testing)."""
        cls._instance = None


# Module-level convenience function
def get_paths() -> PathConfig:
    """Get the global PathConfig instance.

    Returns:
        PathConfig singleton instance
    """
    return PathConfig.get_instance()
