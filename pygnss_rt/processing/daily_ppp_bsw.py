"""BSW processing mixin for DailyPPPProcessor."""

from __future__ import annotations

import os
import re
import shutil
from pathlib import Path
from typing import Any, TYPE_CHECKING

from pygnss_rt.processing.networks import NetworkProfile
from pygnss_rt.processing.bsw_options import (
    BSWOptionsParser,
    BSWOptionsConfig,
    get_option_dirs,
    xml_step_to_opt_dir,
)
from pygnss_rt.utils.dates import GNSSDate

if TYPE_CHECKING:
    from pygnss_rt.processing.daily_ppp import DailyPPPArgs


class BSWMixin:
    """BSW execution methods for DailyPPPProcessor."""

    def _run_bsw_processing(
        self,
        profile: NetworkProfile,
        date: GNSSDate,
        session_name: str,
        campaign_dir: Path,
        args: DailyPPPArgs,
    ) -> bool:
        """Run Bernese GNSS Software processing.

        Args:
            profile: Network profile
            date: Processing date
            session_name: Session name
            campaign_dir: Campaign directory
            args: Processing arguments

        Returns:
            True if processing succeeded
        """
        print(f"  PCF: {profile.pcf_file}")
        print(f"  Options: {profile.bsw_options_xml}")

        # Load and parse BSW options XML
        bsw_options = self._load_bsw_options(profile, date)
        if bsw_options is None:
            print("  Warning: Could not load BSW options XML")
            return False

        if args.verbose:
            print(f"  Processing steps: {bsw_options.list_steps()}")

        # Build processing arguments for BSW
        bsw_args = self._build_bsw_args(profile, date, session_name, bsw_options, args)

        if args.verbose:
            print(f"  BSW args prepared: {len(bsw_args)} parameters")

        # Load BSW environment
        from pygnss_rt.bsw.environment import load_bsw_environment
        from pygnss_rt.bsw.bpe_runner import BPERunner, BPEConfig, parse_bsw_options_xml

        # Find LOADGPS.setvar using PathConfig
        loadgps_path = self.paths.loadgps_setvar
        if loadgps_path is None or not loadgps_path.exists():
            # Try common locations as fallback
            for loc in [
                Path(self.gpsuser_dir).parent / "LOADGPS.setvar" if self.gpsuser_dir else None,
                Path(os.environ.get("C", "")) / "LOADGPS.setvar",
            ]:
                if loc and loc.exists():
                    loadgps_path = loc
                    break

        if loadgps_path is None or not loadgps_path.exists():
            print(f"  Warning: LOADGPS.setvar not found")
            print("  Set BERN54_DIR environment variable or configure paths")
            print("  Skipping actual BSW execution (dry run mode)")
            return True

        try:
            # Load environment
            env = load_bsw_environment(loadgps_path)
            print(f"  BSW Environment loaded from {loadgps_path}")

            # Create BPE runner
            runner = BPERunner(env)

            # Normalize system selector once for both PCF and OPT overrides
            systems = self._normalize_systems(args.systems)

            # Build session string for BPE (DOY + session char)
            doy = date.doy
            session_char = "0"  # Daily processing uses "0"
            bpe_session = f"{doy:03d}{session_char}"

            # For non-default systems, create a campaign-local PCF copy
            # and override V_SATSYS / V_GNSSAR without touching global GPSUSER PCF.
            pcf_override_path: Path | None = None
            if systems != "GRE":
                pcf_override_path = self._prepare_systems_pcf(
                    profile=profile,
                    campaign_dir=campaign_dir,
                    session_name=session_name,
                    systems=systems,
                    verbose=args.verbose,
                )
                if pcf_override_path is None:
                    print("  Warning: Could not prepare systems-specific PCF, using default PCF")

            # Create BPE config
            # Use session_name for output files (e.g., 25358IG.OUT, 25358IG.RUN)
            pcf_stem = Path(profile.pcf_file).stem
            config = BPEConfig(
                pcf_file=pcf_stem,  # Base PCF name for BPE temp area naming
                pcf_path=str(pcf_override_path) if pcf_override_path else None,
                pcf_selector=(pcf_override_path.stem if pcf_override_path else pcf_stem),
                campaign=session_name,
                session=bpe_session,
                year=date.year,
                task_id=profile.task_id,
                sysout=session_name,  # Output: 25358IG.OUT
                status=f"{session_name}.RUN",  # Status: 25358IG.RUN
            )

            # Get option directories
            opt_dirs = get_option_dirs("ppp")

            # Parse BSW options from XML for INP customization
            xml_options = parse_bsw_options_xml(Path(profile.bsw_options_xml))

            # Convert XML step names to OPT directory names
            converted_options: dict[str, dict[str, dict[str, str]]] = {}
            for xml_step, inp_files in xml_options.items():
                opt_dir = xml_step_to_opt_dir(xml_step)
                converted_options[opt_dir] = inp_files

            # Apply GNSS system selection override (G / GE / GR / GRE)
            keys_updated = self._apply_systems_override(converted_options, systems)
            print(f"  Systems: {systems} (updated {keys_updated} option keys)")

            # Apply troposphere gradient interval override if requested
            if args.tro_gradient_interval:
                grd_updated = self._apply_numgrd_override(converted_options, args.tro_gradient_interval)
                print(f"  Tro gradient interval: {args.tro_gradient_interval} (updated {grd_updated} GPSEST NUMGRD keys)")

            # Add default options to disable ATL (Atmospheric Loading)
            # ATL file not available - we need to set the count to 0, not just the path
            # The INP format is: ATMLOAD <count> "<path>" - setting count to 0 disables it
            # TODO: Implement ATL file download/generation in the future
            # Note: This requires modifying the INP line format, not just the value

            # Build variable substitutions (opt_* prefixed values)
            var_subs = {k: v for k, v in bsw_args.items() if k.startswith("opt_")}

            print(f"  Starting BPE execution...")
            print(f"    Campaign: {session_name}")
            print(f"    Session: {bpe_session}")
            print(f"    PCF: {config.pcf_path or profile.pcf_file}")

            # Run BPE
            result = runner.run(
                config=config,
                opt_dirs=opt_dirs,
                bsw_options=converted_options,
                variable_substitutions=var_subs,
                timeout=7200,  # 2 hours
            )

            if result.success:
                print(f"  BPE completed successfully in {result.runtime_seconds:.1f}s")
                print(f"    Sessions finished: {result.sessions_finished}")
                if result.output_file:
                    print(f"    Output: {result.output_file}")
                return True
            else:
                print(f"  BPE failed: {result.error_message}")
                print(f"    Return code: {result.return_code}")
                if result.sessions_error > 0:
                    print(f"    Sessions with errors: {result.sessions_error}")
                return False

        except Exception as e:
            print(f"  BSW execution error: {e}")
            import traceback
            if args.verbose:
                traceback.print_exc()
            return False

    @staticmethod
    def _normalize_systems(systems: str | None) -> str:
        """Normalize GNSS systems selector."""
        normalized = (systems or "GRE").upper()
        if normalized not in {"G", "GE", "GR", "GRE"}:
            raise ValueError(f"Invalid systems selection '{systems}', expected one of: G, GE, GR, GRE")
        return normalized

    @staticmethod
    def _set_pcf_variable(pcf_path: Path, variable: str, value: str) -> int:
        """Set a PCF variable assignment (e.g., V_SATSYS = GRE;)."""
        try:
            content = pcf_path.read_text()
        except Exception:
            return 0

        pattern = rf"^(\s*{re.escape(variable)}\s*=\s*)([^;]*)(\s*;.*)$"
        updated, count = re.subn(pattern, rf"\1{value}\3", content, count=1, flags=re.MULTILINE)
        if count > 0 and updated != content:
            pcf_path.write_text(updated)
        return count

    def _prepare_systems_pcf(
        self,
        profile: NetworkProfile,
        campaign_dir: Path,
        session_name: str,
        systems: str,
        verbose: bool = False,
    ) -> Path | None:
        """Create a campaign-local PCF copy with systems overrides."""
        source_pcf = Path(profile.pcf_file)
        if not source_pcf.exists():
            return None

        systems = self._normalize_systems(systems)
        target_pcf = campaign_dir / "BPE" / f"{source_pcf.stem}_{session_name}.PCF"

        try:
            shutil.copy2(source_pcf, target_pcf)
            updates = 0
            updates += self._set_pcf_variable(target_pcf, "V_SATSYS", systems)
            updates += self._set_pcf_variable(target_pcf, "V_GNSSAR", systems)
            if updates < 2:
                print(
                    f"  Warning: PCF override updated {updates}/2 system variables "
                    f"(V_SATSYS, V_GNSSAR) in {target_pcf.name}"
                )
            if verbose:
                print(f"  PCF override: {target_pcf.name} (systems={systems}, vars updated={updates})")
            return target_pcf
        except Exception as e:
            print(f"  Warning: Failed to create systems PCF override: {e}")
            return None

    def _apply_systems_override(
        self,
        bsw_options: dict[str, dict[str, dict[str, str]]],
        systems: str,
    ) -> int:
        """Apply GNSS system selection to BSW USE_* and USEAR_* keys.

        BSW 5.4 CODSPP only supports a maximum of 2 GNSS systems, so
        CODSPP is always kept at GPS-only for clock synchronization.
        The full system selection is applied only to GPSEST and other
        programs that have no such limitation.
        """
        systems = self._normalize_systems(systems)
        enable_g = "1"
        enable_r = "1" if systems in {"GR", "GRE"} else "0"
        enable_e = "1" if systems in {"GE", "GRE"} else "0"

        # Full system selection for GPSEST and other programs
        full_values = {
            "USE_G": enable_g,
            "USE_R": enable_r,
            "USE_E": enable_e,
            "USE_S": "0",
            "USE_C": "0",
            "USE_J": "0",
            "USE_I": "0",
            "USEAR_G": enable_g,
            "USEAR_R": enable_r,
            "USEAR_E": enable_e,
            "USEAR_S": "0",
            "USEAR_C": "0",
            "USEAR_J": "0",
        }

        # CODSPP: GPS-only (BSW 5.4 max 2 GNSS limit)
        codspp_values = {
            "USE_G": "1",
            "USE_R": "0",
            "USE_E": "0",
            "USE_S": "0",
            "USE_C": "0",
            "USE_J": "0",
            "USE_I": "0",
        }

        updates = 0
        for inp_files in bsw_options.values():
            for inp_name, keys in inp_files.items():
                is_codspp = "CODSPP" in inp_name.upper()
                target_values = codspp_values if is_codspp else full_values
                for key, value in target_values.items():
                    if key in keys and keys[key] != value:
                        keys[key] = value
                        updates += 1

        return updates

    @staticmethod
    def _apply_numgrd_override(
        bsw_options: dict[str, dict[str, dict[str, str]]],
        interval: str,
    ) -> int:
        """Override NUMGRD (troposphere gradient parameter interval) in all GPSEST INP files."""
        updates = 0
        for inp_files in bsw_options.values():
            for inp_name, keys in inp_files.items():
                if "GPSEST" in inp_name.upper() and "NUMGRD" in keys:
                    if keys["NUMGRD"] != interval:
                        keys["NUMGRD"] = interval
                        updates += 1
        return updates

    def _load_bsw_options(
        self,
        profile: NetworkProfile,
        date: GNSSDate,
    ) -> BSWOptionsConfig | None:
        """Load BSW options from XML file.

        Args:
            profile: Network profile containing XML path
            date: Processing date for variable substitution

        Returns:
            Parsed BSW options config or None if error
        """
        xml_path = Path(profile.bsw_options_xml)
        if not xml_path.exists():
            return None

        try:
            parser = BSWOptionsParser()
            config = parser.load(xml_path)
            return config
        except Exception as e:
            print(f"  Error loading BSW options: {e}")
            return None

    def _build_bsw_args(
        self,
        profile: NetworkProfile,
        date: GNSSDate,
        session_name: str,
        bsw_options: BSWOptionsConfig,
        args: DailyPPPArgs,
    ) -> dict[str, Any]:
        """Build BSW processing arguments.

        Corresponds to the Perl %args hash that gets passed to IGNSS->new().

        Args:
            profile: Network profile
            date: Processing date
            session_name: Session name
            bsw_options: Parsed BSW options
            args: Processing arguments

        Returns:
            Dictionary of BSW arguments
        """
        y4c = str(date.year)
        y2c = f"{date.year % 100:02d}"
        doy = f"{date.doy:03d}"

        # Build session string (for daily: DOY + "0")
        session_str = f"{doy}0"

        bsw_args = {
            # Processing type
            "procType": "daily",
            # Date components
            "y4c": y4c,
            "y2c": y2c,
            "doy": doy,
            "ha": "0",  # Hour character (0 for daily)
            # Session info
            "session": session_name,
            "sessID2char": profile.session_id,
            "TASKID": profile.task_id,
            # PCF and options
            "PCF_FILE": profile.pcf_file,
            "bswOpt": profile.bsw_options_xml,
            # Option directories mapping
            "optDirs": get_option_dirs("ppp"),
            # Datum and reference frame
            "datum": profile.datum,
            "ABS_REL": profile.antenna_phase_center,
            # Minimum elevation
            "opt_MINEL": profile.min_elevation,
            # VMF3 file pattern - full Bernese path with ${P} variable
            # Format: ${P}/campaign/GRD/VMF3_YYDDD0.GRD
            "opt_VMF3": f"${{P}}/{session_name}/GRD/VMF3_{y2c}{doy}0.GRD",
            # Information files
            "infoSES": profile.info_files.get("sessions", ""),
            "infoSTA": profile.info_files.get("station", ""),
            "infoOTL": profile.info_files.get("ocean_loading", ""),
            "infoABB": profile.info_files.get("abbreviations", ""),
            "infoSEL": profile.info_files.get("obs_selection", ""),
            "infoSNX": profile.info_files.get("sinex_skeleton", ""),
            "infoPCV": profile.info_files.get("phase_center", ""),
            "infoCRD": profile.coord_file,
            # Satellite/phase options (derived from ABS_REL)
            "opt_SATELL": "SATELLIT_I20" if profile.antenna_phase_center == "ABSOLUTE" else "SATELLIT_I01",
            "opt_PHASECC": "ANTENNA_I20.I20" if profile.antenna_phase_center == "ABSOLUTE" else "ANTENNA_I01.I01",
            # CRX option
            "opt_CRX": f"SAT_{y4c}",
            # OBSFIL pattern
            "opt_OBSFIL": f"????{doy}0",
            # Campaign directory pattern
            "CAMP_DRV": "${P}/",
            # DCM settings
            "DCM": {
                "yesORno": "yes" if profile.dcm_enabled else "no",
                "dir2del": profile.dcm_dirs_to_delete,
                "compUtil": "gzip",
                "mv2dir": profile.dcm_archive_dir,
                "org": profile.dcm_organization,
            },
            # Control
            "controlArgs": {
                "yesORno": "yes",
                "type": "NRT",
            },
        }

        # Add archive file specifications if needed
        if profile.requires_igs_alignment:
            bsw_args["archFiles"] = {}
            for arch_name, arch_spec in profile.archive_files.items():
                bsw_args["archFiles"][arch_name] = {
                    "root": arch_spec.root,
                    "org": arch_spec.organization,
                    "campPat": arch_spec.campaign_pattern,
                    "prefix": arch_spec.prefix,
                    "body": arch_spec.body_pattern,
                    "srcDir": arch_spec.source_dir,
                    "ext": arch_spec.extensions,
                    "dstDir": arch_spec.dest_dir,
                }

        # Add NEQ stacking configuration
        from pygnss_rt.processing.neq_stacking import NO_STACKING
        neq_config = args.neq_stacking or NO_STACKING
        bsw_args["COMBNEQ"] = {
            "yesORno": "yes" if neq_config.enabled else "no",
            "n2stack": neq_config.n_hours_to_stack,
            "nameScheme": neq_config.name_scheme.value if hasattr(neq_config.name_scheme, 'value') else str(neq_config.name_scheme),
        }

        return bsw_args
