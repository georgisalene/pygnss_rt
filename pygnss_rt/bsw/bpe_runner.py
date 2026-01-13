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
        user_dir = self.env.user_dir
        for subdir in ["PCF", "SCRIPT", "OPT", "OUT"]:
            src = user_dir / subdir
            dst = u_new / subdir
            if src.exists() and not dst.exists():
                os.symlink(src, dst)
                logger.debug(f"Created symlink: {dst} -> {src}")

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

        logger.info(f"Waiting for BPE completion (timeout: {timeout}s)")

        while time.time() - start_time < timeout:
            if output_file.exists():
                current_size = output_file.stat().st_size
                if current_size != last_size:
                    last_size = current_size
                    # Check for completion marker
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

    def copy_opt_to_pan(
        self,
        temp_user_area: Path,
        opt_dirs: dict[str, str],
        prod_mode: bool = False,
    ) -> None:
        """Copy OPT INP files to temp user area PAN directory.

        Mimics RUNBPE.pm copyInpFiles() which copies from $U_OLD/OPT/{opt}/*.INP
        to $U/PAN/ (the temp user area's PAN directory).

        This is the key difference from the original implementation - files go
        to PAN, not campaign/INP.

        Args:
            temp_user_area: Temporary user area path ($U_new)
            opt_dirs: Mapping of i1,i2,... to OPT directory names
            prod_mode: If True, use _PROD suffix on source directories
        """
        opt_root = self.env.user_dir / "OPT"
        pan_dir = temp_user_area / "PAN"

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

            # Copy all .INP and .IN1 files to PAN directory (flat, like RUNBPE.pm)
            for pattern in ["*.INP", "*.IN1"]:
                for inp_file in source_dir.glob(pattern):
                    dest_file = pan_dir / inp_file.name
                    shutil.copy2(inp_file, dest_file)
                    files_copied += 1

            logger.debug(f"Copied OPT/{source_opt}/*.INP to temp PAN/")

        logger.info(f"Copied {files_copied} INP files to temp PAN directory")

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
        pattern = rf'^(\s*{re.escape(key)}\s+\d+\s+)"[^"]*"'

        # Keep value as-is, wrap in quotes
        replacement = rf'\g<1>"{value}"'

        new_content, count = re.subn(pattern, replacement, content, flags=re.MULTILINE)

        if count > 0:
            # Also update the selector comment if provided
            # Selector is a line like "  # OLD_NAME" that appears after ## widget lines
            if selector is not None:
                # Pattern: after KEY line and ## widget line(s), find "  # NAME" line
                selector_pattern = rf'(^\s*{re.escape(key)}\s+\d+\s+"[^"]*"\n(?:\s+##[^\n]*\n)+)\s+#\s+\S+'
                selector_replacement = rf'\g<1>  # {selector}'
                new_content = re.sub(selector_pattern, selector_replacement, new_content, flags=re.MULTILINE)
            inp_file.write_text(new_content)
            return True

        # Try pattern without quotes in original (less common)
        pattern = rf'^(\s*{re.escape(key)}\s+\d+\s+)(\S+)'
        replacement = rf'\g<1>"{value}"'
        new_content, count = re.subn(pattern, replacement, content, flags=re.MULTILINE)

        if count > 0:
            if selector is not None:
                selector_pattern = rf'(^\s*{re.escape(key)}\s+\d+\s+"[^"]*"\n(?:\s+##[^\n]*\n)+)\s+#\s+\S+'
                selector_replacement = rf'\g<1>  # {selector}'
                new_content = re.sub(selector_pattern, selector_replacement, new_content, flags=re.MULTILINE)
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
        pan_dir: Path,
        bsw_options: dict[str, dict[str, dict[str, str]]],
        variable_substitutions: dict[str, str] | None = None,
    ) -> int:
        """Customize INP files based on BSW options from XML.

        Mimics the Perl XML::Smart parsing and putKey loop.
        Files are in $U/PAN/ directory (flat structure).

        Args:
            pan_dir: PAN directory containing INP files
            bsw_options: Nested dict of opt_dir -> inp_file -> key -> value
            variable_substitutions: Variable substitutions (opt_* prefixed)

        Returns:
            Number of keys set
        """
        var_subs = variable_substitutions or {}
        keys_set = 0

        for opt_dir, inp_files in bsw_options.items():
            for inp_name, keys in inp_files.items():
                # INP files are flat in PAN directory (not in subdirs)
                inp_file = pan_dir / f"{inp_name}.INP"

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

        return keys_set

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

        Follows the RUNBPE.pm pattern:
        1. Create temporary user area ($T/BPE_...)
        2. Copy INP files from OPT to temp $U/PAN
        3. Customize INP files with session/campaign info
        4. Create MENU_$$.TMP control file
        5. Execute menu.sh with proper arguments
        6. Parse results and cleanup

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

        # Get environment
        env = self._get_env()

        # Campaign directory
        camp_root = Path(env.get("P", str(self.env.campaign_root)))
        campaign_dir = camp_root / config.campaign

        logger.info(
            "Starting BPE",
            pcf=config.pcf_file,
            campaign=config.campaign,
            session=config.session,
            year=config.year,
        )

        try:
            # Step 1: Ensure campaign has essential files (SESSIONS.SES, etc.)
            self.ensure_campaign_essentials(campaign_dir)

            # Step 2: Add campaign to MENU_CMP.INP
            self.add_campaign(config.campaign)

            # Step 3: Create temporary user area (like RUNBPE::copyUarea)
            temp_u = self._create_temp_user_area(config)
            pan_dir = temp_u / "PAN"
            work_dir = temp_u / "WORK"

            # Step 4: Copy INP files from OPT to temp PAN directory
            if opt_dirs:
                self.copy_opt_to_pan(temp_u, opt_dirs, prod_mode)

            # Also copy essential MENU*.INP files from user PAN
            user_pan = self.env.user_dir / "PAN"
            u_orig = str(self.env.user_dir)
            for menu_file in ["MENU.INP", "MENU_VAR.INP", "MENU_PGM.INP", "MENU_EXT.INP", "MENU_CMP.INP", "RUNBPE.INP", "NEWCAMP.INP", "USER.CPU"]:
                src = user_pan / menu_file
                if src.exists():
                    shutil.copy2(src, pan_dir / menu_file)

            # Fix paths in MENU.INP to use original user directory
            # The menu.INP file has references like ${U}/PAN/MENU_CMP.INP that
            # need to point to the original user directory, not the temp area
            menu_inp = pan_dir / "MENU.INP"
            if menu_inp.exists():
                content = menu_inp.read_text()
                # Replace ${U} with the original user directory
                content = content.replace('${U}/', f'{u_orig}/')
                menu_inp.write_text(content)

            # Fix paths in MENU_EXT.INP to use original user directory
            # The menu system uses PTH_* paths to find files
            menu_ext = pan_dir / "MENU_EXT.INP"
            if menu_ext.exists():
                content = menu_ext.read_text()
                # Replace ${U} with the original user directory in path definitions
                content = content.replace('${U}/', f'{u_orig}/')
                menu_ext.write_text(content)

            # Step 5: Customize INP files with session/campaign values
            mjd = GNSSDate(config.year, 1, 1).add_days(int(config.session[:3]) - 1).mjd
            session_char = config.session[3] if len(config.session) > 3 else "0"
            session_full = config.session[:3] + session_char

            # Update MENU.INP
            self.put_key(pan_dir / "MENU.INP", "ACTIVE_CAMPAIGN", f"${{P}}/{config.campaign}")
            self.put_key(pan_dir / "MENU.INP", "SESSION_CHAR", session_char)
            self.put_key(pan_dir / "MENU.INP", "MODJULDATE", str(mjd))
            self.put_key(pan_dir / "MENU.INP", "SESSION_TABLE", f"${{P}}/{config.campaign}/GEN/SESSIONS.SES")

            # Step 6: Customize INP files from bsw_options
            if bsw_options:
                keys_set = self.customize_inp_files(
                    pan_dir, bsw_options, variable_substitutions
                )
                logger.info(f"Customized {keys_set} INP keys")

            # Step 7: Configure RUNBPE.INP with BPE settings (like startBPE::run)
            runbpe_inp = pan_dir / "RUNBPE.INP"
            if runbpe_inp.exists():
                # Construct full paths for various settings
                # Use original user directory for PCF and CPU since temp area doesn't have them
                loadgps_setvar = env.get("C", str(self.env.bsw_root)) + "/LOADGPS.setvar"
                pcf_path = f"{u_orig}/PCF/{config.pcf_file}.PCF"
                cpu_path = f"{u_orig}/PAN/{config.cpu_file}.CPU"
                bpe_campaign = str(campaign_dir)
                session_table = f"${{P}}/{config.campaign}/GEN/SESSIONS.SES"
                sysout_path = f"${{P}}/{config.campaign}/BPE/{config.sysout}.OUT"
                status_path = f"${{P}}/{config.campaign}/BPE/{config.status}"

                # Set RUNBPE.INP keys
                self.put_key(runbpe_inp, "BPE_CLIENT", f"${{BPE}}/RUNBPE.sh")
                self.put_key(runbpe_inp, "CLIENT_ENV", loadgps_setvar)
                # PCF_FILE needs selector to update the "# NAME" comment that menu uses
                pcf_name = config.pcf_file.replace(".PCF", "")
                self.put_key(runbpe_inp, "PCF_FILE", pcf_path, selector=pcf_name)
                self.put_key(runbpe_inp, "CPU_FILE", cpu_path)
                self.put_key(runbpe_inp, "CPUUPDRATE", str(config.cpu_update_rate))
                self.put_key(runbpe_inp, "BPE_CAMPAIGN", bpe_campaign)
                self.put_key(runbpe_inp, "SESSION_TABLE", session_table)
                self.put_key(runbpe_inp, "YEAR", str(config.year))
                self.put_key(runbpe_inp, "SESSION", session_full)
                self.put_key(runbpe_inp, "NUM_SESS", str(config.num_sessions))
                self.put_key(runbpe_inp, "MODULO_SESS", str(config.modulo_sessions))
                self.put_key(runbpe_inp, "NEXTSESS", str(config.next_session))
                self.put_key(runbpe_inp, "TASKID", config.task_id)
                self.put_key(runbpe_inp, "SYSOUT", sysout_path)
                self.put_key(runbpe_inp, "STATUS", status_path)
                self.put_key(runbpe_inp, "DEBUG", "1" if config.debug else "0")
                self.put_key(runbpe_inp, "NOCLEAN", "1" if config.no_clean else "0")
                self.put_key(runbpe_inp, "BPE_MAXTIME", str(config.max_time))
                logger.debug("Configured RUNBPE.INP for BPE execution")

            # Step 8: Create environment for execution
            pid = os.getpid()
            exec_env = env.copy()
            exec_env["U"] = str(temp_u)
            exec_env["T"] = str(temp_u / "WORK" / "T:")

            # Step 9: Find menu.sh executable
            xq = env.get("XQ", str(self.env.queue_dir))
            menu_exe = Path(xq) / "menu.sh"

            if not menu_exe.exists():
                menu_exe = Path(xq) / "menu"

            if not menu_exe.exists():
                raise BSWError("BPE", f"menu executable not found in {xq}")

            # Step 10: Run menu to expand variables in RUNBPE.INP (like startBPE)
            setvar_file = work_dir / f"SETVAR.MEN_{pid}"
            with open(setvar_file, "w") as f:
                f.write(f"INP_FILE_NAME 1 {runbpe_inp}\n\n")
                f.write(f"OUT_FILE_NAME 1 {runbpe_inp}\n\n")

            # Execute menu.sh to expand variables
            result = subprocess.run(
                [str(menu_exe), str(pan_dir / "MENU.INP"), str(setvar_file)],
                env=exec_env,
                capture_output=True,
                text=True,
                timeout=60,
                cwd=str(work_dir),
            )

            if result.returncode != 0:
                logger.warning(f"Variable expansion returned {result.returncode}: {result.stderr}")

            # Step 11: Create RUN_BPE command file (like startBPE::run)
            runbpe_men = work_dir / f"RUNBPE.MEN_{pid}"
            with open(runbpe_men, "w") as f:
                f.write(f"RUN_BPE 1 {runbpe_inp}\n\n")
                f.write("PRINT_PID 1 \"1\"\n\n")

            logger.info("Executing BPE via menu.sh", temp_u=str(temp_u))

            # Step 12: Execute BPE via menu.sh with RUN_BPE command
            # Use Popen because menu.sh spawns background BPE processes and returns
            # immediately. We need to wait for BPE completion by monitoring output.
            output_file = campaign_dir / "BPE" / f"{config.sysout}.OUT"
            status_file = campaign_dir / "BPE" / f"{config.status}"

            # Clear any existing output file to detect new output
            if output_file.exists():
                output_file.unlink()

            # menu.sh expects: menu.sh "$MENU_INP" "$RUNBPE_MEN"
            # First arg: MENU.INP file with environment settings
            # Second arg: the command file containing RUN_BPE command
            proc = subprocess.Popen(
                [str(menu_exe), str(pan_dir / "MENU.INP"), str(runbpe_men)],
                env=exec_env,
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
                # Still running, wait a bit more
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

            # Step 13: Wait for BPE to complete by monitoring output file
            # BPE writes "Sessions finished: OK: N  Error: M" when done
            completed, sessions_finished, sessions_error = self._wait_for_bpe_completion(
                output_file, timeout
            )

            runtime = time.time() - start_time

            # Step 14: Cleanup temp user area only AFTER BPE is done
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


def parse_bsw_options_file(config_path: Path) -> dict[str, dict[str, dict[str, str]]]:
    """Parse BSW options from YAML or XML file.

    Supports both YAML (preferred) and XML (legacy) formats.

    YAML format:
        bern_options:
            D_PPPGEN:
                CODSPP:
                    key1: value1

    XML format:
        <recipe>
            <bernOptions>
                <D_PPPGEN>
                    <CODSPP>
                        <key1>value1</key1>
                    </CODSPP>
                </D_PPPGEN>
            </bernOptions>
        </recipe>

    Args:
        config_path: Path to YAML or XML file

    Returns:
        Nested dict: opt_dir -> inp_file -> key -> value
    """
    path = Path(config_path)

    # Try YAML first
    yaml_path = path.with_suffix('.yaml')
    xml_path = path.with_suffix('.xml')

    if yaml_path.exists():
        return _parse_bsw_options_yaml(yaml_path)
    elif path.suffix == '.yaml' and path.exists():
        return _parse_bsw_options_yaml(path)
    elif xml_path.exists():
        return _parse_bsw_options_xml(xml_path)
    elif path.exists():
        if path.suffix == '.yaml':
            return _parse_bsw_options_yaml(path)
        else:
            return _parse_bsw_options_xml(path)

    return {}


def _parse_bsw_options_yaml(yaml_path: Path) -> dict[str, dict[str, dict[str, str]]]:
    """Parse BSW options from YAML file.

    Args:
        yaml_path: Path to YAML file

    Returns:
        Nested dict: opt_dir -> inp_file -> key -> value
    """
    import yaml

    if not yaml_path.exists():
        return {}

    with open(yaml_path) as f:
        data = yaml.safe_load(f)

    result: dict[str, dict[str, dict[str, str]]] = {}

    bern_opts = data.get("bern_options", {})

    for opt_name, opt_data in bern_opts.items():
        result[opt_name] = {}
        if isinstance(opt_data, dict):
            for inp_name, inp_data in opt_data.items():
                result[opt_name][inp_name] = {}
                if isinstance(inp_data, dict):
                    for key_name, key_value in inp_data.items():
                        # Convert to string for consistency
                        result[opt_name][inp_name][key_name] = str(key_value) if key_value is not None else ""

    return result


def _parse_bsw_options_xml(xml_path: Path) -> dict[str, dict[str, dict[str, str]]]:
    """Parse BSW options from XML file (legacy format).

    Args:
        xml_path: Path to XML file

    Returns:
        Nested dict: opt_dir -> inp_file -> key -> value
    """
    from xml.etree import ElementTree

    if not xml_path.exists():
        return {}

    tree = ElementTree.parse(xml_path)
    root = tree.getroot()

    result: dict[str, dict[str, dict[str, str]]] = {}

    # Find bernOptions element
    bern_opts = root.find(".//bernOptions")
    if bern_opts is None:
        # Try recipe/bernOptions
        bern_opts = root.find("recipe/bernOptions")

    if bern_opts is None:
        return {}

    # Iterate over OPT directories
    for opt_elem in bern_opts:
        opt_name = opt_elem.tag
        result[opt_name] = {}

        # Iterate over INP files
        for inp_elem in opt_elem:
            inp_name = inp_elem.tag
            result[opt_name][inp_name] = {}

            # Iterate over keys
            for key_elem in inp_elem:
                key_name = key_elem.tag
                key_value = key_elem.text or ""
                result[opt_name][inp_name][key_name] = key_value.strip()

    return result


# Backward compatibility alias
def parse_bsw_options_xml(xml_path: Path) -> dict[str, dict[str, dict[str, str]]]:
    """Parse BSW options file (backward compatibility alias).

    Now supports both YAML and XML formats.
    """
    return parse_bsw_options_file(xml_path)
