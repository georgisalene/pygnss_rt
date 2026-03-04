"""
BPE (Bernese Processing Engine) runner.

Replaces Perl startBPE.pm module.
Manages the execution of BSW processing through the BPE server.
"""

from __future__ import annotations

import os
import re
import shutil
import subprocess
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from pygnss_rt.bsw.environment import BSWEnvironment, load_bsw_environment
from pygnss_rt.core.exceptions import BSWError
from pygnss_rt.core.paths import get_paths
from pygnss_rt.utils.dates import GNSSDate
from pygnss_rt.utils.logging import get_logger


logger = get_logger(__name__)

# Default path to SESSIONS.SES template file (from station_data directory)
DEFAULT_SESSIONS_FILE = get_paths().station_data_dir / "SESSIONS.SES"


@dataclass
class BPEConfig:
    """BPE execution configuration.

    Corresponds to the RUNBPE.INP keywords in BSW.
    """

    # Mandatory parameters
    pcf_file: str  # PCF file name (without path)
    campaign: str  # Campaign name/path
    session: str  # Session (e.g., "2600" for DOY 260, daily)
    year: int  # Year
    pcf_path: str | None = None  # Optional absolute PCF file path override
    pcf_selector: str | None = None  # Optional selector name for RUNBPE panel

    # Processing identification
    task_id: str = "IG"  # 2-char task identifier

    # CPU file
    cpu_file: str = "CPUFILE"
    cpu_update_rate: int = 300

    # Session control
    num_sessions: int = 1
    modulo_sessions: int = 1
    next_session: int = 0

    # Output control
    sysout: str = "NEWBPE"  # BPE output file name
    syserr: str = "ERROR"  # Error output name
    status: str = "NEWBPE.SUM"  # Status file name

    # Debug options
    debug: bool = False
    no_clean: bool = False

    # Timeout
    max_time: int = 0  # 0 = no limit


@dataclass
class BPEResult:
    """Result from BPE execution."""

    success: bool
    return_code: int
    output_file: Path | None = None
    status_file: Path | None = None
    error_message: str | None = None
    runtime_seconds: float = 0.0
    sessions_finished: int = 0
    sessions_error: int = 0


class BPERunner:
    """Runs Bernese Processing Engine.

    Mimics the Perl RUNBPE.pm module behavior:
    1. Creates temporary user area ($T/BPE_{pcf}_{year}_{session}_{pid})
    2. Copies INP files from OPT directories to temp $U/PAN/
    3. Customizes INP files with putKey operations
    4. Creates MENU_$$.TMP control file
    5. Executes menu.sh to run BPE scripts

    The key difference from startBPE.pm is that RUNBPE.pm creates a temporary
    work area and copies INP files there, then runs menu.sh with proper
    control files.

    Usage:
        env = load_bsw_environment("/path/to/LOADGPS.setvar")
        runner = BPERunner(env)

        config = BPEConfig(
            pcf_file="PPP54IGS",
            campaign="24260IG",
            session="2600",
            year=2024,
            task_id="IG",
        )

        result = runner.run(config, opt_dirs={"i1": "PPP_GEN", ...})
    """

    def __init__(self, environment: BSWEnvironment):
        """Initialize BPE runner.

        Args:
            environment: BSW environment configuration
        """
        self.env = environment
        self._env_vars: dict[str, str] | None = None
        self._temp_user_area: Path | None = None

    def _get_env(self) -> dict[str, str]:
        """Get environment variables for BSW execution."""
        if self._env_vars is None:
            self._env_vars = self.env.setup()
        return self._env_vars

    def _create_temp_user_area(self, config: BPEConfig, port: int = 0) -> Path:
        """Create temporary user area like RUNBPE.pm copyUarea().

        Creates directory structure:
        $T/BPE_{pcf}_{port}_{year}_{session}_{pid}_{sub_pid}/
            INP/
            PAN/
            WORK/
            WORK/T:/
            WORK/T:/AUTO_TMP/

        Args:
            config: BPE configuration
            port: Port number (default 0 for local execution)

        Returns:
            Path to temporary user area
        """
        env = self._get_env()
        t_old = Path(env.get("T", "/tmp/bsw"))

        pid = os.getpid()
        sub_pid = 0  # No parallel sub-processing for now

        # Create temp user area path like RUNBPE.pm
        pcf_name = config.pcf_file.split(".")[0] if "." in config.pcf_file else config.pcf_file
        u_new = t_old / f"BPE_{pcf_name}_{port}_{config.year}_{config.session}_{pid}_{sub_pid}"

        # Create directory structure
        (u_new / "INP").mkdir(parents=True, exist_ok=True)
        (u_new / "PAN").mkdir(parents=True, exist_ok=True)
        (u_new / "WORK").mkdir(parents=True, exist_ok=True)

        # Create T: directory in WORK (Unix convention from RUNBPE.pm)
        t_new = u_new / "WORK" / "T:"
        t_new.mkdir(parents=True, exist_ok=True)
        (t_new / "AUTO_TMP").mkdir(parents=True, exist_ok=True)

        # Create symlinks to user directories that the menu expects under ${U}
        # The menu binary uses ${U}/PCF, ${U}/SCRIPT, etc. which point to temp area
        # but the actual files are in the original user directory
        # NOTE: OPT is NOT symlinked - it will be copied so we can customize INP files
        user_dir = self.env.user_dir
        for subdir in ["PCF", "SCRIPT", "OUT"]:
            src = user_dir / subdir
            dst = u_new / subdir
            if src.exists() and not dst.exists():
                os.symlink(src, dst)
                logger.debug(f"Created symlink: {dst} -> {src}")

        # Create OPT directory structure (will be populated by copy_opt_to_pan)
        (u_new / "OPT").mkdir(parents=True, exist_ok=True)

        self._temp_user_area = u_new
        logger.debug(f"Created temp user area: {u_new}")

        return u_new

    def _cleanup_temp_user_area(self) -> None:
        """Remove temporary user area."""
        if self._temp_user_area and self._temp_user_area.exists():
            try:
                shutil.rmtree(self._temp_user_area)
                logger.debug(f"Cleaned up temp user area: {self._temp_user_area}")
            except Exception as e:
                logger.warning(f"Failed to cleanup temp area: {e}")
            self._temp_user_area = None

    def _wait_for_bpe_completion(
        self,
        output_file: Path,
        timeout: int,
        poll_interval: int = 5,
    ) -> tuple[bool, int, int]:
        """Wait for BPE to complete by monitoring the output file.

        BPE runs asynchronously - menu.sh spawns background processes and returns
        immediately. We need to monitor NEWBPE.OUT for the completion marker:
        "Sessions finished:  OK:  N     Error:  M"

        Args:
            output_file: Path to BPE output file (e.g., NEWBPE.OUT)
            timeout: Maximum time to wait in seconds
            poll_interval: How often to check file in seconds

        Returns:
            Tuple of (completed, sessions_ok, sessions_error)
        """
        start_time = time.time()
        last_size = 0
        check_count = 0

        logger.info(f"Waiting for BPE completion (timeout: {timeout}s)")
        logger.debug(f"Monitoring output file: {output_file}")

        while time.time() - start_time < timeout:
            check_count += 1
            if output_file.exists():
                current_size = output_file.stat().st_size
                if current_size != last_size:
                    last_size = current_size
                    # Check for completion marker
                    try:
                        content = output_file.read_text()
                        match = re.search(
                            r"Sessions finished:\s*OK:\s*(\d+)\s+Error:\s*(\d+)",
                            content
                        )
                        if match:
                            sessions_ok = int(match.group(1))
                            sessions_error = int(match.group(2))
                            logger.info(
                                f"BPE completed: OK={sessions_ok}, Error={sessions_error}"
                            )
                            return True, sessions_ok, sessions_error
                    except Exception as e:
                        logger.warning(f"Error reading output file: {e}")
            elif check_count % 12 == 1:  # Log every minute (12 * 5 seconds)
                logger.debug(f"Output file not yet created: {output_file}")

            time.sleep(poll_interval)

        logger.warning(f"BPE timeout after {timeout} seconds")
        return False, 0, 0

    def ensure_campaign_essentials(
        self,
        campaign_dir: Path,
        sessions_file: Path | None = None,
    ) -> None:
        """Ensure campaign has essential files and directories.

        BSW requires certain directories and files to exist in the campaign.
        This method creates them if missing.

        Args:
            campaign_dir: Path to campaign directory
            sessions_file: Path to SESSIONS.SES template (uses default if None)
        """
        # Create essential campaign subdirectories
        essential_dirs = [
            "ATM", "BPE", "GEN", "GRD", "OBS", "ORB", "OUT", "RAW", "SOL", "STA"
        ]
        for subdir in essential_dirs:
            (campaign_dir / subdir).mkdir(parents=True, exist_ok=True)

        gen_dir = campaign_dir / "GEN"
        station_data_dir = get_paths().station_data_dir

        # Essential GEN files to copy if missing
        essential_gen_files = [
            ("SESSIONS.SES", sessions_file or DEFAULT_SESSIONS_FILE),
            ("ANTENNA_I20.PCV", station_data_dir / "ANTENNA_I20.PCV"),
            ("I20.ATX", station_data_dir / "I20.ATX"),
            ("SINEX_PPP.SKL", station_data_dir / "SINEX_PPP.SKL"),
        ]

        for dest_name, src_path in essential_gen_files:
            dest_file = gen_dir / dest_name
            if not dest_file.exists() and src_path.exists():
                shutil.copy2(src_path, dest_file)
                logger.debug(f"Copied {dest_name} to campaign GEN")

    def add_campaign(self, campaign: str) -> None:
        """Add campaign to MENU_CMP.INP.

        Mimics startBPE::addCamp() method.

        Args:
            campaign: Campaign name
        """
        menu_cmp = self.env.user_dir / "PAN" / "MENU_CMP.INP"
        if not menu_cmp.exists():
            # Try XG location
            menu_cmp = Path(self._get_env().get("X", self.env.user_dir)) / "PAN" / "MENU_CMP.INP"

        if not menu_cmp.exists():
            logger.warning("MENU_CMP.INP not found, skipping campaign registration")
            return

        # Check if campaign already exists
        content = menu_cmp.read_text()
        campaign_path = f'"${{P}}/{campaign}"'

        if campaign_path in content or f'/{campaign}"' in content:
            logger.debug(f"Campaign {campaign} already in MENU_CMP.INP")
            return

        # Parse and update the file
        lines = content.splitlines()
        new_lines = []
        campaign_count_updated = False

        for line in lines:
            # Update campaign count
            if line.startswith("CAMPAIGN") and not campaign_count_updated:
                match = re.match(r"CAMPAIGN\s+(\d+)\s+(\d+)", line)
                if match:
                    count = int(match.group(2)) + 1
                    line = f"CAMPAIGN {match.group(1)} {count}"
                    campaign_count_updated = True

            # Insert campaign before the widget marker
            if "## widget = uniline" in line:
                new_lines.append(f'  "{campaign_path}"')

            new_lines.append(line)

        menu_cmp.write_text("\n".join(new_lines) + "\n")
        logger.info(f"Added campaign {campaign} to MENU_CMP.INP")

    def remove_campaign(self, campaign: str) -> None:
        """Remove campaign from MENU_CMP.INP.

        Args:
            campaign: Campaign name
        """
        menu_cmp = self.env.user_dir / "PAN" / "MENU_CMP.INP"
        if not menu_cmp.exists():
            return

        content = menu_cmp.read_text()
        campaign_path = f'"${{P}}/{campaign}"'

        if campaign_path not in content and f'/{campaign}"' not in content:
            return

        lines = content.splitlines()
        new_lines = []
        campaign_count_updated = False

        for line in lines:
            # Skip the campaign line
            if campaign in line and '"${P}/' in line:
                continue

            # Update campaign count
            if line.startswith("CAMPAIGN") and not campaign_count_updated:
                match = re.match(r"CAMPAIGN\s+(\d+)\s+(\d+)", line)
                if match:
                    count = max(0, int(match.group(2)) - 1)
                    line = f"CAMPAIGN {match.group(1)} {count}"
                    campaign_count_updated = True

            new_lines.append(line)

        menu_cmp.write_text("\n".join(new_lines) + "\n")
        logger.info(f"Removed campaign {campaign} from MENU_CMP.INP")

    def copy_opt_to_temp(
        self,
        temp_user_area: Path,
        opt_dirs: dict[str, str],
        prod_mode: bool = False,
    ) -> None:
        """Copy OPT directories to temp user area, preserving structure.

        Copies from $U_OLD/OPT/{opt}/*.INP to $U_NEW/OPT/{opt}/*.INP
        so that the INP files can be customized and Bernese will use them.
        Also symlinks any other OPT directories (like NO_OPT) that BPE may need.

        Args:
            temp_user_area: Temporary user area path ($U_new)
            opt_dirs: Mapping of i1,i2,... to OPT directory names
            prod_mode: If True, use _PROD suffix on source directories
        """
        opt_root = self.env.user_dir / "OPT"
        temp_opt_root = temp_user_area / "OPT"

        # Get set of OPT directories we need to copy (for customization)
        opt_dirs_to_copy = set(opt_dirs.values())

        files_copied = 0
        for key, opt_name in sorted(opt_dirs.items()):
            # Handle PROD mode suffix
            source_opt = opt_name + "_PROD" if prod_mode else opt_name
            source_dir = opt_root / source_opt

            if not source_dir.exists():
                # Try without _PROD suffix
                source_dir = opt_root / opt_name
                if not source_dir.exists():
                    logger.warning(f"OPT directory not found: {source_dir}")
                    continue

            # Create temp OPT subdirectory
            dest_dir = temp_opt_root / opt_name
            dest_dir.mkdir(parents=True, exist_ok=True)

            # Copy all .INP and .IN1 files to temp OPT directory (preserve structure)
            for pattern in ["*.INP", "*.IN1"]:
                for inp_file in source_dir.glob(pattern):
                    dest_file = dest_dir / inp_file.name
                    shutil.copy2(inp_file, dest_file)
                    files_copied += 1

            logger.debug(f"Copied OPT/{source_opt}/*.INP to temp OPT/{opt_name}/")

        # Symlink any other OPT directories that we didn't copy (like NO_OPT)
        # These are used by scripts that don't need customization
        for source_dir in opt_root.iterdir():
            if source_dir.is_dir() and source_dir.name not in opt_dirs_to_copy:
                dest_dir = temp_opt_root / source_dir.name
                if not dest_dir.exists():
                    os.symlink(source_dir, dest_dir)

        logger.info(f"Copied {files_copied} INP files to temp OPT directories")

    def put_key(self, inp_file: Path, key: str, value: str, selector: str | None = None) -> bool:
        """Set a key value in an INP file.

        Direct file manipulation for BSW INP format: KEY 1  "value"
        Also handles selector comments (# NAME) that follow the value.

        Args:
            inp_file: Path to INP file
            key: Key name
            value: Value to set
            selector: Optional selector name to set (e.g., "PPP54IGS" for PCF files)

        Returns:
            True if successful
        """
        if not inp_file.exists():
            logger.warning(f"INP file not found: {inp_file}")
            return False

        content = inp_file.read_text()

        # BSW INP format: KEY 1  "value" or KEY 1  value
        # Pattern to match: KEY followed by number, then quoted value
        # Example: SESSION_CHAR 1  "0"  or  MODJULDATE 1  "60567.5"
        # Use [ \t]+ instead of \s+ to avoid matching across lines
        pattern = rf'^(\s*{re.escape(key)}[ \t]+\d+[ \t]+)"[^"]*"'

        # Use function-based replacement to avoid regex special character issues
        # Values like $(ORB)_$YYYSS+0 contain $ which can be misinterpreted
        def replace_quoted(m: re.Match) -> str:
            return m.group(1) + f'"{value}"'

        new_content, count = re.subn(pattern, replace_quoted, content, flags=re.MULTILINE)

        if count > 0:
            # Also update the selector comment if provided
            # Selector is a line like "  # OLD_NAME" that appears after ## widget lines
            if selector is not None:
                # Pattern: after KEY line and ## widget line(s), find "  # NAME" line
                selector_pattern = rf'(^\s*{re.escape(key)}\s+\d+\s+"[^"]*"\n(?:\s+##[^\n]*\n)+)\s+#\s+\S+'
                def replace_selector(m: re.Match) -> str:
                    return m.group(1) + f'  # {selector}'
                new_content = re.sub(selector_pattern, replace_selector, new_content, flags=re.MULTILINE)
            inp_file.write_text(new_content)
            return True

        # Try pattern without quotes in original (less common)
        # Use [ \t]+ instead of \s+ to avoid matching across lines
        pattern = rf'^(\s*{re.escape(key)}[ \t]+\d+[ \t]+)(\S+)'

        def replace_unquoted(m: re.Match) -> str:
            return m.group(1) + f'"{value}"'

        new_content, count = re.subn(pattern, replace_unquoted, content, flags=re.MULTILINE)

        if count > 0:
            if selector is not None:
                selector_pattern = rf'(^\s*{re.escape(key)}\s+\d+\s+"[^"]*"\n(?:\s+##[^\n]*\n)+)\s+#\s+\S+'
                def replace_selector2(m: re.Match) -> str:
                    return m.group(1) + f'  # {selector}'
                new_content = re.sub(selector_pattern, replace_selector2, new_content, flags=re.MULTILINE)
            inp_file.write_text(new_content)
            return True

        # Try pattern for key with count but no value (e.g., "VMF_FILES 0" with no value)
        # This handles keys that were previously disabled (count=0) and now have count>0
        # When adding a value, always set count to 1 to enable the feature
        pattern = rf'^(\s*{re.escape(key)}[ \t]+)(\d+)$'

        def add_value(m: re.Match) -> str:
            return m.group(1) + '1' + f'  "{value}"'

        new_content, count = re.subn(pattern, add_value, content, flags=re.MULTILINE)

        if count > 0:
            inp_file.write_text(new_content)
            return True

        logger.warning(f"Key {key} not found in {inp_file}")
        return False

    def set_key_count(self, inp_file: Path, key: str, count: int) -> bool:
        """Set the count (number of values) for a key in an INP file.

        BSW INP format: KEY <count> "value1" "value2" ...
        Setting count to 0 effectively disables the feature.

        Args:
            inp_file: Path to INP file
            key: Key name
            count: New count value (0 to disable)

        Returns:
            True if successful
        """
        if not inp_file.exists():
            logger.warning(f"INP file not found: {inp_file}")
            return False

        content = inp_file.read_text()

        # Pattern to match: KEY followed by number
        # Example: ATMLOAD 1  "./DUMMY/STA/file.ATL"
        pattern = rf'^(\s*{re.escape(key)}\s+)(\d+)'
        replacement = rf'\g<1>{count}'

        new_content, match_count = re.subn(pattern, replacement, content, flags=re.MULTILINE)

        if match_count > 0:
            inp_file.write_text(new_content)
            logger.debug(f"Set {key} count to {count} in {inp_file}")
            return True

        logger.warning(f"Key {key} not found in {inp_file}")
        return False

    def customize_inp_files(
        self,
        temp_opt_dir: Path,
        bsw_options: dict[str, dict[str, dict[str, str]]],
        variable_substitutions: dict[str, str] | None = None,
    ) -> int:
        """Customize INP files based on BSW options from YAML/XML.

        Modifies INP files in the temp OPT directories so Bernese uses
        our customized values instead of the original OPT files.

        Args:
            temp_opt_dir: Temp OPT directory (temp_user_area / "OPT")
            bsw_options: Nested dict of opt_dir -> inp_file -> key -> value
            variable_substitutions: Variable substitutions (opt_* prefixed)

        Returns:
            Number of keys set
        """
        var_subs = variable_substitutions or {}
        keys_set = 0

        for opt_dir, inp_files in bsw_options.items():
            for inp_name, keys in inp_files.items():
                # INP files are in OPT subdirectories: OPT/{opt_dir}/{inp_name}.INP
                inp_file = temp_opt_dir / opt_dir / f"{inp_name}.INP"

                if not inp_file.exists():
                    logger.debug(f"INP file not found: {inp_file}")
                    continue

                for key, value in keys.items():
                    # Handle opt_* variable substitutions
                    if value and "opt_" in str(value):
                        for var_name, var_value in var_subs.items():
                            if var_name.startswith("opt_"):
                                # Ensure both values are strings
                                value = str(value).replace(var_name, str(var_value))

                    if self.put_key(inp_file, key, str(value)):
                        keys_set += 1

                        # Special handling for VMF_FILES: also set count to 1 when path is provided
                        # The INP format is: VMF_FILES <count> "<path>"
                        # When count=0, VMF is disabled. We need count=1 to enable it.
                        if key == "VMF_FILES" and value and str(value).strip():
                            self.set_key_count(inp_file, "VMF_FILES", 1)
                            logger.debug(f"Enabled VMF_FILES (count=1) in {inp_file}")

        return keys_set

    def substitute_bpe_user_vars(
        self,
        temp_opt_dir: Path,
        temp_pan_dir: Path,
        user_vars: dict[str, str] | None = None,
    ) -> int:
        """Substitute BPE user variables in INP files.

        Bernese Perl scripts set variables like FL, CLUSTER, FFFF using setUserVar().
        Since Python bypasses these scripts, we substitute them directly in INP files.

        Args:
            temp_opt_dir: Temp OPT directory with INP files
            temp_pan_dir: Temp PAN directory with INP files
            user_vars: Optional custom user variable values

        Returns:
            Number of substitutions made
        """
        # Default BPE user variable values
        # ONLY substitute variables that are set by Perl scripts we bypass.
        # DO NOT substitute variables handled by Perl scripts that still run:
        #   - $(CLUSTER), $(FFFF), $(V_CLUSTER) are set by setClusterVar() in scripts
        #     like RNXSMT_P, RXOBV3_P, CODSPP_P which still execute
        #   - These get dynamically set to actual station IDs during processing
        #
        # FL: 'F' = Float (initial), 'L' = Last (after screening)
        #     Set by PPPEDT_P.pm loop logic which we might bypass
        defaults = {
            "$(FL)": "L",  # Final/Last value after data screening
            "$(PPP)": "PPP",  # Preliminary PPP solution prefix
            "$(FIN)": "FIN",  # Final solution prefix
            # DO NOT add $(CLUSTER), $(FFFF), $(V_CLUSTER) - handled by Perl scripts
        }

        # Merge with user-provided values
        vars_to_sub = {**defaults, **(user_vars or {})}

        substitutions = 0

        # Process all INP files in temp OPT directories
        if temp_opt_dir.exists():
            for inp_file in temp_opt_dir.rglob("*.INP"):
                try:
                    content = inp_file.read_text()
                    modified = False

                    for var_name, var_value in vars_to_sub.items():
                        if var_name in content:
                            content = content.replace(var_name, var_value)
                            modified = True
                            substitutions += 1

                    if modified:
                        inp_file.write_text(content)
                        logger.debug(f"Substituted BPE user vars in {inp_file.name}")

                except Exception as e:
                    logger.warning(f"Failed to substitute vars in {inp_file}: {e}")

        # Also process INP files in temp PAN directory
        if temp_pan_dir.exists():
            for inp_file in temp_pan_dir.glob("*.INP"):
                try:
                    content = inp_file.read_text()
                    modified = False

                    for var_name, var_value in vars_to_sub.items():
                        if var_name in content:
                            content = content.replace(var_name, var_value)
                            modified = True
                            substitutions += 1

                    if modified:
                        inp_file.write_text(content)
                        logger.debug(f"Substituted BPE user vars in {inp_file.name}")

                except Exception as e:
                    logger.warning(f"Failed to substitute vars in {inp_file}: {e}")

        if substitutions > 0:
            logger.info(f"Substituted {substitutions} BPE user variables in INP files")

        return substitutions

    def _create_mw_copies(self, obs_dir: Path) -> int:
        """Create PZH/PZO copies of CZH/CZO files for Melbourne-Wuebbena AR.

        SAVCOD=1 creates only CZH/CZO files (combined code+phase observations).
        Melbourne-Wuebbena linear combination requires paired phase (PZH/PZO)
        and code (CZH/CZO) files. We copy the files (not symlink) because GPSEST
        detects symlinks and treats them as duplicates, resulting in 0 observations.

        Args:
            obs_dir: Path to campaign OBS directory

        Returns:
            Number of files copied
        """
        import shutil

        if not obs_dir.exists():
            logger.debug(f"OBS directory does not exist: {obs_dir}")
            return 0

        files_copied = 0

        # Copy CZH files to PZH
        for czh_file in obs_dir.glob("*.CZH"):
            pzh_file = czh_file.with_suffix(".PZH")
            if not pzh_file.exists():
                try:
                    shutil.copy2(czh_file, pzh_file)
                    logger.debug(f"Copied: {czh_file.name} -> {pzh_file.name}")
                    files_copied += 1
                except OSError as e:
                    logger.warning(f"Failed to copy {czh_file} to {pzh_file}: {e}")

        # Copy CZO files to PZO
        for czo_file in obs_dir.glob("*.CZO"):
            pzo_file = czo_file.with_suffix(".PZO")
            if not pzo_file.exists():
                try:
                    shutil.copy2(czo_file, pzo_file)
                    logger.debug(f"Copied: {czo_file.name} -> {pzo_file.name}")
                    files_copied += 1
                except OSError as e:
                    logger.warning(f"Failed to copy {czo_file} to {pzo_file}: {e}")

        if files_copied > 0:
            logger.info(f"Copied {files_copied} CZ files to PZ for Melbourne-Wuebbena AR")

        return files_copied

    # ── Phase methods ──────────────────────────────────────────────────

    def _phase_setup(
        self,
        config: BPEConfig,
        opt_dirs: dict[str, str] | None,
        prod_mode: bool,
    ) -> tuple[Path, Path, Path]:
        """Phase 1: Setup temporary environment.

        Creates temp user area, ensures campaign essentials, adds campaign,
        copies OPT directories and PAN files.

        Returns:
            Tuple of (temp_user_area, campaign_dir, menu_exe)
        """
        env = self._get_env()
        camp_root = Path(env.get("P", str(self.env.campaign_root)))
        campaign_dir = camp_root / config.campaign

        self.ensure_campaign_essentials(campaign_dir)
        self.add_campaign(config.campaign)

        temp_u = self._create_temp_user_area(config)
        pan_dir = temp_u / "PAN"

        # Install PCF override if provided
        if config.pcf_path:
            override_pcf = Path(config.pcf_path)
            pcf_link = temp_u / "PCF"
            if override_pcf.exists() and pcf_link.is_symlink():
                original_pcf_dir = pcf_link.resolve()
                pcf_link.unlink()
                pcf_link.mkdir(parents=True, exist_ok=True)
                for f in original_pcf_dir.iterdir():
                    dst = pcf_link / f.name
                    if not dst.exists():
                        os.symlink(f, dst)
                shutil.copy2(override_pcf, pcf_link / override_pcf.name)
                logger.debug(f"Installed PCF override: {override_pcf.name} into temp PCF dir")

        # Copy INP files from OPT to temp OPT directories
        if opt_dirs:
            self.copy_opt_to_temp(temp_u, opt_dirs, prod_mode)

        # Copy PAN files (MENU*.INP, program panels, CPU files)
        user_pan = self.env.user_dir / "PAN"
        u_orig = str(self.env.user_dir)
        if user_pan.exists():
            for src in user_pan.glob("*.INP"):
                shutil.copy2(src, pan_dir / src.name)
            for src in user_pan.glob("*.CPU"):
                shutil.copy2(src, pan_dir / src.name)

        # Fix ${U} paths in MENU.INP and MENU_EXT.INP to use original user directory
        for menu_file in ["MENU.INP", "MENU_EXT.INP"]:
            menu_path = pan_dir / menu_file
            if menu_path.exists():
                content = menu_path.read_text()
                content = content.replace('${U}/', f'{u_orig}/')
                menu_path.write_text(content)

        # Find menu.sh executable
        xq = env.get("XQ", str(self.env.queue_dir))
        menu_exe = Path(xq) / "menu.sh"
        if not menu_exe.exists():
            menu_exe = Path(xq) / "menu"
        if not menu_exe.exists():
            raise BSWError("BPE", f"menu executable not found in {xq}")

        return temp_u, campaign_dir, menu_exe

    def _phase_customize(
        self,
        temp_u: Path,
        config: BPEConfig,
        bsw_options: dict[str, dict[str, dict[str, str]]] | None,
        variable_substitutions: dict[str, str] | None,
    ) -> None:
        """Phase 2: Customize processing options.

        Sets session/campaign values in MENU.INP, customizes INP files
        from bsw_options, and substitutes BPE user variables.
        """
        pan_dir = temp_u / "PAN"
        temp_opt = temp_u / "OPT"

        mjd = GNSSDate(config.year, 1, 1).add_days(int(config.session[:3]) - 1).mjd
        session_char = config.session[3] if len(config.session) > 3 else "0"

        self.put_key(pan_dir / "MENU.INP", "ACTIVE_CAMPAIGN", f"${{P}}/{config.campaign}")
        self.put_key(pan_dir / "MENU.INP", "SESSION_CHAR", session_char)
        self.put_key(pan_dir / "MENU.INP", "MODJULDATE", str(mjd))
        self.put_key(pan_dir / "MENU.INP", "SESSION_TABLE", f"${{P}}/{config.campaign}/GEN/SESSIONS.SES")

        if bsw_options:
            keys_set = self.customize_inp_files(
                temp_opt, bsw_options, variable_substitutions
            )
            logger.info(f"Customized {keys_set} INP keys in temp OPT")

        self.substitute_bpe_user_vars(temp_opt, pan_dir)

    def _phase_configure_runbpe(
        self,
        temp_u: Path,
        config: BPEConfig,
        campaign_dir: Path,
        menu_exe: Path,
    ) -> None:
        """Phase 3: Configure RUNBPE.INP and expand variables.

        Writes all BPE settings into RUNBPE.INP, then runs menu.sh
        to expand Bernese variables.
        """
        env = self._get_env()
        pan_dir = temp_u / "PAN"
        work_dir = temp_u / "WORK"
        u_orig = str(self.env.user_dir)
        session_char = config.session[3] if len(config.session) > 3 else "0"
        session_full = config.session[:3] + session_char

        runbpe_inp = pan_dir / "RUNBPE.INP"
        if not runbpe_inp.exists():
            return

        loadgps_setvar = env.get("C", str(self.env.bsw_root)) + "/LOADGPS.setvar"
        if config.pcf_path:
            pcf_path = config.pcf_path
            pcf_name = config.pcf_selector or Path(config.pcf_path).stem
        else:
            pcf_name = config.pcf_file.replace(".PCF", "")
            pcf_path = f"{u_orig}/PCF/{pcf_name}.PCF"
        cpu_path = f"{u_orig}/PAN/{config.cpu_file}.CPU"
        session_table = f"${{P}}/{config.campaign}/GEN/SESSIONS.SES"
        sysout_path = f"${{P}}/{config.campaign}/BPE/{config.sysout}.OUT"
        status_path = f"${{P}}/{config.campaign}/BPE/{config.status}"

        self.put_key(runbpe_inp, "BPE_CLIENT", f"${{BPE}}/RUNBPE.sh")
        self.put_key(runbpe_inp, "CLIENT_ENV", loadgps_setvar)
        self.put_key(runbpe_inp, "PCF_FILE", pcf_path, selector=pcf_name)
        self.put_key(runbpe_inp, "CPU_FILE", cpu_path)
        self.put_key(runbpe_inp, "CPUUPDRATE", str(config.cpu_update_rate))
        self.put_key(runbpe_inp, "BPE_CAMPAIGN", str(campaign_dir))
        self.put_key(runbpe_inp, "SESSION_TABLE", session_table)
        self.put_key(runbpe_inp, "YEAR", str(config.year))
        self.put_key(runbpe_inp, "SESSION", session_full)
        self.put_key(runbpe_inp, "NUM_SESS", str(config.num_sessions))
        self.put_key(runbpe_inp, "MODULO_SESS", str(config.modulo_sessions))
        self.put_key(runbpe_inp, "NEXTSESS", str(config.next_session))
        self.put_key(runbpe_inp, "TASKID", config.task_id)
        self.put_key(runbpe_inp, "SYSOUT", sysout_path, selector=config.sysout)
        status_base = config.status.replace(".RUN", "").replace(".SUM", "")
        self.put_key(runbpe_inp, "STATUS", status_path, selector=status_base)
        self.put_key(runbpe_inp, "DEBUG", "1" if config.debug else "0")
        self.put_key(runbpe_inp, "NOCLEAN", "1" if config.no_clean else "0")
        self.put_key(runbpe_inp, "BPE_MAXTIME", str(config.max_time))
        logger.debug("Configured RUNBPE.INP for BPE execution")

        # Run menu.sh to expand variables in RUNBPE.INP
        pid = os.getpid()
        exec_env = env.copy()
        exec_env["U"] = str(temp_u)
        exec_env["T"] = str(temp_u / "WORK" / "T:")

        setvar_file = work_dir / f"SETVAR.MEN_{pid}"
        with open(setvar_file, "w") as f:
            f.write(f"INP_FILE_NAME 1 {runbpe_inp}\n\n")
            f.write(f"OUT_FILE_NAME 1 {runbpe_inp}\n\n")

        result = subprocess.run(
            [str(menu_exe), str(pan_dir / "MENU.INP"), str(setvar_file)],
            env=exec_env,
            stdin=subprocess.DEVNULL,
            capture_output=True,
            text=True,
            timeout=60,
            cwd=str(work_dir),
        )

        if result.returncode != 0:
            logger.warning(f"Variable expansion returned {result.returncode}: {result.stderr}")

    def _phase_execute(
        self,
        temp_u: Path,
        config: BPEConfig,
        campaign_dir: Path,
        menu_exe: Path,
        timeout: int,
        start_time: float,
    ) -> BPEResult:
        """Phase 4: Execute BPE and wait for completion.

        Spawns BPE via menu.sh, monitors output file for completion,
        and returns the result.
        """
        env = self._get_env()
        pan_dir = temp_u / "PAN"
        work_dir = temp_u / "WORK"
        pid = os.getpid()
        runbpe_inp = pan_dir / "RUNBPE.INP"

        exec_env = env.copy()
        exec_env["U"] = str(temp_u)
        exec_env["T"] = str(temp_u / "WORK" / "T:")

        # Create RUN_BPE command file
        runbpe_men = work_dir / f"RUNBPE.MEN_{pid}"
        with open(runbpe_men, "w") as f:
            f.write(f"RUN_BPE 1 {runbpe_inp}\n\n")
            f.write("PRINT_PID 1 \"1\"\n\n")

        logger.info("Executing BPE via menu.sh", temp_u=str(temp_u))

        output_file = campaign_dir / "BPE" / f"{config.sysout}.OUT"
        status_file = campaign_dir / "BPE" / f"{config.status}"
        logger.debug(f"Campaign directory: {campaign_dir}")
        logger.debug(f"BPE output file will be: {output_file}")

        # Clear any existing output file to detect new output
        if output_file.exists():
            logger.debug(f"Removing existing output file: {output_file}")
            output_file.unlink()

        proc = subprocess.Popen(
            [str(menu_exe), str(pan_dir / "MENU.INP"), str(runbpe_men)],
            env=exec_env,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            cwd=str(work_dir),
        )

        # Wait briefly for menu.sh to spawn BPE processes
        time.sleep(2)

        # menu.sh returns immediately after spawning BPE - get its return code
        menu_returncode = proc.poll()
        if menu_returncode is None:
            try:
                proc.wait(timeout=30)
                menu_returncode = proc.returncode
            except subprocess.TimeoutExpired:
                menu_returncode = 0  # Assume OK if still running

        if menu_returncode != 0:
            _, stderr = proc.communicate(timeout=5)
            return BPEResult(
                success=False,
                return_code=menu_returncode,
                error_message=f"menu.sh failed: {stderr}",
                runtime_seconds=time.time() - start_time,
            )

        # Wait for BPE to complete by monitoring output file
        completed, sessions_finished, sessions_error = self._wait_for_bpe_completion(
            output_file, timeout
        )

        runtime = time.time() - start_time

        # Cleanup temp user area only AFTER BPE is done
        if not config.no_clean and completed:
            self._cleanup_temp_user_area()

        success = completed and sessions_error == 0

        error_msg = None
        if not completed:
            error_msg = f"BPE did not complete within {timeout}s"
        elif sessions_error > 0:
            error_msg = f"BPE completed with {sessions_error} session errors"

        return BPEResult(
            success=success,
            return_code=0 if completed else -1,
            output_file=output_file if output_file.exists() else None,
            status_file=status_file if status_file.exists() else None,
            error_message=error_msg,
            runtime_seconds=runtime,
            sessions_finished=sessions_finished,
            sessions_error=sessions_error,
        )

    # ── Main entry point ─────────────────────────────────────────────

    def run(
        self,
        config: BPEConfig,
        opt_dirs: dict[str, str] | None = None,
        bsw_options: dict[str, dict[str, dict[str, str]]] | None = None,
        variable_substitutions: dict[str, str] | None = None,
        prod_mode: bool = False,
        timeout: int = 7200,
    ) -> BPEResult:
        """Run BPE processing.

        Follows the RUNBPE.pm pattern in 4 phases:
        1. Setup - temp user area, campaign essentials, OPT/PAN copies
        2. Customize - session/campaign values, BSW options, user variables
        3. Configure - RUNBPE.INP settings, variable expansion
        4. Execute - spawn BPE via menu.sh, monitor completion

        Args:
            config: BPE configuration
            opt_dirs: OPT directory mapping (i1 -> PPP_GEN, etc.)
            bsw_options: Options to customize in INP files
            variable_substitutions: Variable substitutions for opt_* values
            prod_mode: Use production OPT directories
            timeout: Timeout in seconds

        Returns:
            BPEResult with execution details
        """
        start_time = time.time()

        logger.info(
            "Starting BPE",
            pcf=config.pcf_file,
            campaign=config.campaign,
            session=config.session,
            year=config.year,
        )

        try:
            # Phase 1: Setup temporary environment
            temp_u, campaign_dir, menu_exe = self._phase_setup(
                config, opt_dirs, prod_mode
            )

            # Phase 2: Customize processing options
            self._phase_customize(
                temp_u, config, bsw_options, variable_substitutions
            )

            # Phase 3: Configure RUNBPE.INP
            self._phase_configure_runbpe(
                temp_u, config, campaign_dir, menu_exe
            )

            # Phase 4: Execute BPE and wait for completion
            return self._phase_execute(
                temp_u, config, campaign_dir, menu_exe, timeout, start_time
            )

        except subprocess.TimeoutExpired:
            return BPEResult(
                success=False,
                return_code=-1,
                error_message=f"BPE timeout after {timeout} seconds",
                runtime_seconds=timeout,
            )
        except Exception as e:
            logger.exception("BPE execution failed")
            return BPEResult(
                success=False,
                return_code=-1,
                error_message=str(e),
                runtime_seconds=time.time() - start_time,
            )


# Backward compatibility - re-export from bsw_options_parser
from pygnss_rt.bsw.bsw_options_parser import (  # noqa: E402, F401
    parse_bsw_options_file,
    _parse_bsw_options_yaml,
    _parse_bsw_options_xml,
    parse_bsw_options_xml,
)
