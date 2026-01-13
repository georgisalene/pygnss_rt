"""
BSW (Bernese GNSS Software) Options XML Parser.

Parses BSW processing options from XML files used by i-GNSS caller scripts.
Each XML file contains Bernese program configurations organized by processing step.

XML Structure:
    <recipe target="Processor" version="1.0">
        <bernOptions>
            <STEP_NAME>           # Processing step (e.g., D_PPPGEN, NRDDPGEN)
                <PROGRAM>         # BSW program (e.g., POLUPD, ORBGEN, GPSEST)
                    <OPTION>value</OPTION>  # Program option
                </PROGRAM>
            </STEP_NAME>
        </bernOptions>
    </recipe>

Variable Substitution:
    The XML uses Bernese-style placeholders that get substituted at runtime:
    - $Y+0, $Y      : 4-digit year (e.g., 2024)
    - $YY, $y2c     : 2-digit year (e.g., 24)
    - $D+0, $D      : 3-digit DOY (e.g., 260)
    - $S+0, $S      : Session string (e.g., 2601, 2600)
    - $YYYSS+0      : Combined year/session (e.g., 24260)
    - $YMD_STR+0    : Date string (e.g., 2024 09 16)
    - $(ORB)        : Orbit file prefix (e.g., IGS, COD)
    - opt_SATELL    : Satellite info file reference
    - opt_PHASECC   : Antenna phase center reference
    - SCRIPT        : Runtime file from script

Usage:
    from pygnss_rt.processing.bsw_options import BSWOptionsParser

    parser = BSWOptionsParser()
    parser.load("callers/iGNSS_D_PPP_AR_IG_IGS54_direct.xml")

    # Get all options for a processing step
    ppp_gen = parser.get_step_options("D_PPPGEN")

    # Get options for a specific program
    gpsest = parser.get_program_options("D_PPPFIN", "GPSEST")

    # Substitute variables for a specific date
    resolved = parser.substitute_variables(
        options=gpsest,
        year=2024,
        doy=260,
        session="2600",
        orbit_prefix="IGS"
    )
"""

from __future__ import annotations

import re
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


@dataclass
class BSWProgramOptions:
    """Options for a single BSW program within a processing step."""

    program_name: str
    options: dict[str, str] = field(default_factory=dict)

    def get(self, key: str, default: str = "") -> str:
        """Get option value with optional default."""
        return self.options.get(key, default)

    def __getitem__(self, key: str) -> str:
        return self.options[key]

    def __contains__(self, key: str) -> bool:
        return key in self.options


@dataclass
class BSWStepOptions:
    """Options for a processing step containing multiple programs."""

    step_name: str
    programs: dict[str, BSWProgramOptions] = field(default_factory=dict)

    def get_program(self, program_name: str) -> BSWProgramOptions | None:
        """Get options for a specific program."""
        return self.programs.get(program_name)

    def list_programs(self) -> list[str]:
        """List all programs in this step."""
        return list(self.programs.keys())


@dataclass
class BSWOptionsConfig:
    """Complete BSW options configuration from an XML file."""

    xml_path: Path
    target: str = "Processor"
    version: str = "1.0"
    author: str = ""
    steps: dict[str, BSWStepOptions] = field(default_factory=dict)

    # Option directories mapping (from Perl optDirs)
    option_dirs: dict[str, str] = field(default_factory=dict)

    def get_step(self, step_name: str) -> BSWStepOptions | None:
        """Get options for a processing step."""
        return self.steps.get(step_name)

    def list_steps(self) -> list[str]:
        """List all processing steps."""
        return list(self.steps.keys())

    def get_program_options(
        self, step_name: str, program_name: str
    ) -> BSWProgramOptions | None:
        """Get options for a specific program in a step."""
        step = self.steps.get(step_name)
        if step:
            return step.get_program(program_name)
        return None


class BSWOptionsParser:
    """Parser for BSW options XML files.

    Handles loading and parsing of Bernese GNSS Software option configurations
    from i-GNSS XML files.
    """

    # Known processing steps and their typical programs
    PPP_STEPS = [
        "D_PPPGEN",  # General preparation (POLUPD, CCPREORB, ORBGEN, etc.)
        "D_PPPPH1",  # Phase processing step 1
        "D_PPPPH2",  # Phase processing step 2
        "D_PPPEDT",  # Edit processing
        "D_PPPFIN",  # Final solution (GPSEST)
        "D_PPPCMB",  # Combine solutions
    ]

    NRDDP_STEPS = [
        "NRDDPGEN",  # General preparation
        "NRDDPGL1",  # GPS L1 processing
        "NRDDPGL2",  # GPS L2 processing
        "NRDDPGE2",  # Galileo E2 processing
        "NRDDPEDT",  # Edit processing
        "NRDDPQIF",  # QIF processing
        "NRDDPL12",  # L1/L2 combination
        "NRDDPL53",  # L5/L3 processing
        "NRDDPIAR",  # Integer ambiguity resolution
        "NRDDPFIN",  # Final solution
    ]

    # Variable pattern for substitution
    VAR_PATTERN = re.compile(r"\$\((\w+)\)|\$(\w+)(\+\d+)?")

    def __init__(self) -> None:
        """Initialize the BSW options parser."""
        self._xml_tree: ET.ElementTree | None = None
        self._xml_root: ET.Element | None = None
        self._config: BSWOptionsConfig | None = None

    @property
    def config(self) -> BSWOptionsConfig | None:
        """Get the loaded configuration."""
        return self._config

    def load(self, xml_path: Path | str) -> BSWOptionsConfig:
        """Load and parse BSW options from XML file.

        Args:
            xml_path: Path to the BSW options XML file

        Returns:
            Parsed BSWOptionsConfig

        Raises:
            FileNotFoundError: If XML file doesn't exist
            ET.ParseError: If XML is malformed
        """
        path = Path(xml_path)
        if not path.exists():
            raise FileNotFoundError(f"BSW options XML not found: {path}")

        self._xml_tree = ET.parse(path)
        self._xml_root = self._xml_tree.getroot()

        # Parse recipe attributes
        target = self._xml_root.get("target", "Processor")
        version = self._xml_root.get("version", "1.0")
        author = self._xml_root.get("author", "")

        self._config = BSWOptionsConfig(
            xml_path=path,
            target=target,
            version=version,
            author=author,
        )

        # Parse bernOptions section
        bern_options = self._xml_root.find("bernOptions")
        if bern_options is not None:
            self._parse_bern_options(bern_options)

        return self._config

    def _parse_bern_options(self, bern_options: ET.Element) -> None:
        """Parse the bernOptions section containing all processing steps."""
        if self._config is None:
            return

        for step_elem in bern_options:
            step_name = step_elem.tag
            step_options = BSWStepOptions(step_name=step_name)

            # Each child of step is a program
            for prog_elem in step_elem:
                program_name = prog_elem.tag
                program_options = BSWProgramOptions(program_name=program_name)

                # Each child of program is an option
                for opt_elem in prog_elem:
                    opt_name = opt_elem.tag
                    opt_value = opt_elem.text or ""
                    program_options.options[opt_name] = opt_value.strip()

                step_options.programs[program_name] = program_options

            self._config.steps[step_name] = step_options

    def get_step_options(self, step_name: str) -> BSWStepOptions | None:
        """Get all options for a processing step.

        Args:
            step_name: Name of the processing step (e.g., "D_PPPGEN")

        Returns:
            BSWStepOptions or None if step not found
        """
        if self._config is None:
            return None
        return self._config.get_step(step_name)

    def get_program_options(
        self, step_name: str, program_name: str
    ) -> BSWProgramOptions | None:
        """Get options for a specific program in a step.

        Args:
            step_name: Name of the processing step
            program_name: Name of the BSW program (e.g., "GPSEST")

        Returns:
            BSWProgramOptions or None if not found
        """
        if self._config is None:
            return None
        return self._config.get_program_options(step_name, program_name)

    def substitute_variables(
        self,
        text: str,
        year: int,
        doy: int,
        session: str,
        hour: str = "0",
        orbit_prefix: str = "IGS",
        opt_satell: str = "SATELLIT_I20",
        opt_phasecc: str = "ANTENNA_I20.I20",
    ) -> str:
        """Substitute Bernese-style variables in text.

        Args:
            text: Text containing variable placeholders
            year: 4-digit year
            doy: Day of year (1-366)
            session: Session string (e.g., "2600" or "0")
            hour: Hour character (a-x for 0-23, 0 for daily)
            orbit_prefix: Orbit product prefix (IGS, COD, etc.)
            opt_satell: SATELL option value
            opt_phasecc: PHASECC option value

        Returns:
            Text with variables substituted
        """
        y4c = str(year)
        y2c = str(year)[-2:]
        doy_str = f"{doy:03d}"

        # Build YYYSS - 2-digit year + session
        yyyss = f"{y2c}{session}"

        # Date string format: YYYY MM DD
        from datetime import datetime, timedelta

        base_date = datetime(year, 1, 1) + timedelta(days=doy - 1)
        ymd_str = base_date.strftime("%Y %m %d")

        # Variable substitutions
        replacements = {
            # Year variants
            "$Y+0": y4c,
            "$Y": y4c,
            "$y4c": y4c,
            "$YY": y2c,
            "$y2c": y2c,
            # DOY variants
            "$D+0": doy_str,
            "$D": doy_str,
            "$doy": doy_str,
            # Session variants
            "$S+0": session,
            "$S": session,
            # Combined
            "$YYYSS+0": yyyss,
            "$YYYSS": yyyss,
            "$YYYD+-": f"{y2c}{doy_str}",
            "$YMD_STR+0": ymd_str,
            "$YMD_STR": ymd_str,
            # Hour
            "$ha": hour,
            # Option references
            "opt_SATELL": opt_satell,
            "opt_PHASECC": opt_phasecc,
            # Orbit reference
            "$(ORB)": orbit_prefix,
        }

        result = text
        for var, value in replacements.items():
            result = result.replace(var, value)

        return result

    def substitute_options(
        self,
        options: BSWProgramOptions,
        year: int,
        doy: int,
        session: str,
        hour: str = "0",
        orbit_prefix: str = "IGS",
        **kwargs: Any,
    ) -> dict[str, str]:
        """Substitute variables in all program options.

        Args:
            options: BSWProgramOptions to substitute
            year: 4-digit year
            doy: Day of year
            session: Session string
            hour: Hour character
            orbit_prefix: Orbit product prefix
            **kwargs: Additional substitution parameters

        Returns:
            Dictionary of options with variables substituted
        """
        result = {}
        for key, value in options.options.items():
            result[key] = self.substitute_variables(
                text=value,
                year=year,
                doy=doy,
                session=session,
                hour=hour,
                orbit_prefix=orbit_prefix,
                **kwargs,
            )
        return result

    def to_dict(self) -> dict[str, Any]:
        """Export configuration as nested dictionary.

        Returns:
            Dictionary representation of the configuration
        """
        if self._config is None:
            return {}

        return {
            "xml_path": str(self._config.xml_path),
            "target": self._config.target,
            "version": self._config.version,
            "author": self._config.author,
            "steps": {
                step_name: {
                    "programs": {
                        prog_name: prog.options
                        for prog_name, prog in step.programs.items()
                    }
                }
                for step_name, step in self._config.steps.items()
            },
        }

    def list_steps(self) -> list[str]:
        """List all processing steps in the loaded configuration."""
        if self._config is None:
            return []
        return self._config.list_steps()

    def list_programs(self, step_name: str) -> list[str]:
        """List all programs in a processing step."""
        step = self.get_step_options(step_name)
        if step is None:
            return []
        return step.list_programs()


# Option directory mappings for different processing types
#
# IMPORTANT: There are TWO naming conventions:
#
# 1. XML Step Names (used in iGNSS_D_PPP_AR_*.xml for parsing program options):
#    D_PPPGEN, D_PPPGE2, D_PPPVEL, D_PPPIAR, D_PPPAUX, D_PPPSNX, D_PPPFIN
#    These are the <D_PPPGEN>...</D_PPPGEN> sections in the XML configuration.
#
# 2. BSW OPT Directories (actual directories in $GPSUSER/OPT/):
#    PPP_GEN, PPP_GE2, PPP_VEL, PPP_IAR, PPP_AUX, PPP_SNX, PPP_FIN
#    These are referenced in the PCF file's OPT_DIR column.
#
# The mapping: i1, i2, ... -> step/directory names

# XML Step Names (for parsing iGNSS XML configuration files)
# These match the XML element names like <D_PPPGEN>, <D_PPPGE2>, etc.
PPP_XML_STEPS = {
    "i1": "D_PPPGEN",  # General preparation (POLUPD, ORBGEN, RNXSMT, CODSPP, etc.)
    "i2": "D_PPPGE2",  # Extended general (MAUPRP preprocessing)
    "i3": "D_PPPVEL",  # Velocity processing (optional)
    "i4": "D_PPPIAR",  # Integer ambiguity resolution
    "i5": "D_PPPAUX",  # Auxiliary processing (CRDMERGE, ADDNEQ2, PPP_HLM)
    "i6": "D_PPPSNX",  # SINEX generation (ADDNEQ2)
    "i7": "D_PPPFIN",  # Final solution (GPSEST with fixed ambiguities)
}

# BSW OPT Directories (actual directories in $GPSUSER/OPT/)
# These match the PCF file's OPT_DIR column (PPP54IGS.PCF)
PPP_OPTION_DIRS = {
    "i1": "PPP_GEN",  # General preparation
    "i2": "PPP_GE2",  # Extended general (MAUPRP)
    "i3": "PPP_VEL",  # Velocity processing
    "i4": "PPP_IAR",  # Integer ambiguity resolution
    "i5": "PPP_AUX",  # Auxiliary processing
    "i6": "PPP_SNX",  # SINEX generation
    "i7": "PPP_FIN",  # Final solution
}

# Mapping from XML step names to OPT directory names
XML_TO_OPT_DIR = {
    "D_PPPGEN": "PPP_GEN",
    "D_PPPGE2": "PPP_GE2",
    "D_PPPVEL": "PPP_VEL",
    "D_PPPIAR": "PPP_IAR",
    "D_PPPAUX": "PPP_AUX",
    "D_PPPSNX": "PPP_SNX",
    "D_PPPFIN": "PPP_FIN",
}

# Legacy alias (for backwards compatibility)
PPP_LEGACY_OPTION_DIRS = PPP_XML_STEPS

NRDDP_OPTION_DIRS = {
    "i1": "NRDDPGEN",
    "i2": "NRDDPGL1",
    "i3": "NRDDPGL2",
    "i4": "NRDDPGE2",
    "i5": "NRDDPEDT",
    "i6": "NRDDPQIF",
    "i7": "NRDDPL12",
    "i8": "NRDDPL53",
    "i9": "NRDDPIAR",
    "i10": "NRDDPFIN",
}


def load_bsw_options(xml_path: Path | str) -> BSWOptionsConfig:
    """Convenience function to load BSW options from XML.

    Args:
        xml_path: Path to BSW options XML file

    Returns:
        Parsed BSWOptionsConfig
    """
    parser = BSWOptionsParser()
    return parser.load(xml_path)


def get_option_dirs(processing_type: str = "ppp", for_xml: bool = False) -> dict[str, str]:
    """Get option directory mapping for processing type.

    Args:
        processing_type: Processing type string:
            - "ppp" or "ppp_ar": PPP with ambiguity resolution (7 steps)
            - "nrddp": NRDDP troposphere processing (10 steps)
        for_xml: If True, return XML step names (D_PPPGEN, etc.) for parsing
            iGNSS XML config files. If False (default), return BSW OPT
            directory names (PPP_GEN, etc.) for PCF file.

    Returns:
        Dictionary mapping i1, i2, etc. to step/directory names

    Example:
        # BSW OPT directory names (for PCF, actual $GPSUSER/OPT/ dirs)
        opt_dirs = get_option_dirs("ppp")
        # Returns: {'i1': 'PPP_GEN', 'i2': 'PPP_GE2', ...}

        # XML step names (for parsing iGNSS_D_PPP_AR_*.xml)
        xml_steps = get_option_dirs("ppp", for_xml=True)
        # Returns: {'i1': 'D_PPPGEN', 'i2': 'D_PPPGE2', ...}
    """
    proc_type = processing_type.lower()
    if proc_type == "nrddp":
        return NRDDP_OPTION_DIRS.copy()
    elif for_xml:
        return PPP_XML_STEPS.copy()
    else:
        # Default to BSW OPT directory names
        return PPP_OPTION_DIRS.copy()


def xml_step_to_opt_dir(xml_step: str) -> str:
    """Convert XML step name to BSW OPT directory name.

    Args:
        xml_step: XML step name (e.g., "D_PPPGEN")

    Returns:
        OPT directory name (e.g., "PPP_GEN")

    Example:
        opt_dir = xml_step_to_opt_dir("D_PPPGEN")  # Returns "PPP_GEN"
    """
    return XML_TO_OPT_DIR.get(xml_step, xml_step)
