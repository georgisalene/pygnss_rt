"""Campaign setup mixin for DailyPPPProcessor."""

from __future__ import annotations

import os
import shutil
from pathlib import Path
from typing import TYPE_CHECKING

from pygnss_rt.processing.networks import NetworkProfile
from pygnss_rt.utils.dates import GNSSDate

if TYPE_CHECKING:
    from pygnss_rt.processing.daily_ppp import DailyPPPArgs


class CampaignMixin:
    """Campaign setup methods for DailyPPPProcessor."""

    def _check_alignment_files(
        self,
        profile: NetworkProfile,
        date: GNSSDate,
    ) -> bool:
        """Check if IGS alignment files are available.

        Args:
            profile: Network profile
            date: Processing date

        Returns:
            True if alignment files exist
        """
        if not profile.archive_files:
            return True

        for name, spec in profile.archive_files.items():
            # Build expected path
            y4 = date.year
            y2 = date.year % 100
            doy = date.doy
            path_pattern = spec.organization.replace("yyyy", str(y4)).replace("doy", f"{doy:03d}")
            camp_pattern = spec.campaign_pattern.replace("YY", f"{y2:02d}").replace("DOY", f"{doy:03d}")

            base_dir = Path(spec.root) / path_pattern / camp_pattern / spec.source_dir

            for ext in spec.extensions:
                file_pattern = f"{spec.prefix}{y2:02d}{doy:03d}0{ext}{spec.compression}"
                expected_file = base_dir / file_pattern
                # In production, check if file exists
                # For now, just log
                print(f"  Checking: {expected_file}")

        return True

    def _setup_campaign(
        self,
        profile: NetworkProfile,
        date: GNSSDate,
        session_name: str,
        stations: list[str],
        args: DailyPPPArgs,
    ) -> Path:
        """Setup BSW campaign directory.

        Creates the campaign directory structure and copies reference files
        (CRD, STA, BLQ, etc.) from the info directory to the campaign STA directory.

        Args:
            profile: Network profile
            date: Processing date
            session_name: Session name
            stations: List of station IDs to process
            args: Processing arguments

        Returns:
            Path to campaign directory
        """
        # Campaign root from config or PathConfig
        campaign_root_cfg = self._config.get("bsw", {}).get("campaign_root")
        if campaign_root_cfg:
            campaign_root = Path(campaign_root_cfg)
        elif self.paths.campaign_root:
            campaign_root = self.paths.campaign_root
        else:
            campaign_root = Path.home() / "GPSDATA" / "CAMPAIGN54"

        campaign_dir = campaign_root / session_name

        if not args.dry_run:
            # Create campaign directory structure
            for subdir in ["ATM", "BPE", "GEN", "GRD", "INP", "OBS", "ORB", "ORX", "OUT", "RAW", "SOL", "STA"]:
                (campaign_dir / subdir).mkdir(parents=True, exist_ok=True)

            # Copy reference files from info directory to campaign STA
            self._copy_info_files_to_campaign(profile, campaign_dir, args)

        return campaign_dir

    def _copy_info_files_to_campaign(
        self,
        profile: NetworkProfile,
        campaign_dir: Path,
        args: DailyPPPArgs,
    ) -> None:
        """Copy reference files from info directory to campaign STA directory.

        Copies files like IGS20_54.CRD, IGS20_54.STA, IGS20_54.BLQ to the campaign
        STA directory for BSW processing.

        Args:
            profile: Network profile containing info file paths
            campaign_dir: Campaign directory path
            args: Processing arguments
        """
        sta_dir = campaign_dir / "STA"
        info_dir = self.paths.info_dir

        # Map of info file types to their source files (relative to info dir)
        # These are the essential reference files for BSW processing
        # Filenames must match what the PCF file expects:
        #   V_CRDINF = NEWNRT52 -> NEWNRT52.CRD
        #   V_STAINF = NEWNRT54 -> NEWNRT54.STA
        #   V_BLQINF = NEWNRT52 -> NEWNRT52.BLQ
        #   V_ATLINF = NEWNRT52 -> NEWNRT52.ATL (if exists)
        reference_files = {
            # Station coordinates file (V_CRDINF = NEWNRT52)
            "coord_newnrt52": "NEWNRT52.CRD",
            # Also NEWNRT54 coordinates
            "coord_newnrt54": "NEWNRT54.CRD",
            # Reference coordinates
            "coord_igs20": "IGS20_R.CRD",
            # IGS20_54 coordinates (V_CRDINF = IGS20_54)
            "coord_igs20_54": "IGS20_54.CRD",
            # Station information file (V_STAINF = NEWNRT54)
            "station": "NEWNRT54.STA",
            # Also NEWNRT52 STA
            "station_52": "NEWNRT52.STA",
            # IGS20_54 station info (V_STAINF = IGS20_54)
            "station_igs20_54": "IGS20_54.STA",
            # Ocean loading file (V_BLQINF = NEWNRT52)
            "ocean_loading": "NEWNRT52.BLQ",
            # Also copy IGS20_54.BLQ for compatibility
            "ocean_loading_igs": "IGS20_54.BLQ",
            # Abbreviations file
            "abbreviations_52": "NEWNRT52.ABB",
            "abbreviations_igs": "IGS20_54.ABB",
            # Observation selection file
            "obs_selection": "OBSSEL.SEL",
            # Sessions file
            "sessions": "SESSIONS.SES",
            # Velocity file
            "velocity": "IGS20_54.VEL",
        }

        if args.verbose:
            print(f"  Copying reference files to {sta_dir}")

        for file_type, filename in reference_files.items():
            source_path = info_dir / filename
            dest_path = sta_dir / filename

            if source_path.exists():
                try:
                    shutil.copy2(source_path, dest_path)
                    if args.verbose:
                        print(f"    Copied: {filename}")
                except Exception as e:
                    print(f"    Warning: Failed to copy {filename}: {e}")
            else:
                # Try alternate filenames from profile info_files
                alt_path = profile.info_files.get(file_type, "")
                if alt_path and Path(alt_path).exists():
                    try:
                        shutil.copy2(alt_path, sta_dir / Path(alt_path).name)
                        if args.verbose:
                            print(f"    Copied: {Path(alt_path).name} (alternate)")
                    except Exception as e:
                        print(f"    Warning: Failed to copy {alt_path}: {e}")
                elif args.verbose:
                    print(f"    Warning: {filename} not found at {source_path}")

        # Also copy antenna phase center file to GEN directory
        gen_dir = campaign_dir / "GEN"
        pcv_files = ["ANTENNA_I20.PCV", "I20.ATX"]
        for pcv_file in pcv_files:
            pcv_source = info_dir / pcv_file
            if pcv_source.exists():
                try:
                    shutil.copy2(pcv_source, gen_dir / pcv_file)
                    if args.verbose:
                        print(f"    Copied: {pcv_file} -> GEN/")
                except Exception as e:
                    print(f"    Warning: Failed to copy {pcv_file}: {e}")
                break  # Only copy one PCV file

        # Copy observation selection file (OBSERV_COD.SEL) to GEN directory
        # This file is required by RNXSMT and RNXGRA programs
        ref54_local_dir = Path(os.environ.get("U", "")) / "REF54_LOCAL"
        if not ref54_local_dir.exists() and self.paths.ref_local_dir:
            ref54_local_dir = self.paths.ref_local_dir

        observ_sel_file = ref54_local_dir / "OBSERV_COD.SEL"
        if observ_sel_file.exists():
            try:
                shutil.copy2(observ_sel_file, gen_dir / "OBSERV_COD.SEL")
                if args.verbose:
                    print(f"    Copied: OBSERV_COD.SEL -> GEN/")
            except Exception as e:
                print(f"    Warning: Failed to copy OBSERV_COD.SEL: {e}")
        else:
            # Try info directory as fallback
            observ_sel_info = info_dir / "OBSERV_COD.SEL"
            if observ_sel_info.exists():
                try:
                    shutil.copy2(observ_sel_info, gen_dir / "OBSERV_COD.SEL")
                    if args.verbose:
                        print(f"    Copied: OBSERV_COD.SEL -> GEN/ (from info)")
                except Exception as e:
                    print(f"    Warning: Failed to copy OBSERV_COD.SEL: {e}")
