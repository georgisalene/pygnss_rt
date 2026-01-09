"""Tests for BSW options XML parser."""

import tempfile
from pathlib import Path

import pytest

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


# Sample XML content for testing
SAMPLE_XML = """<?xml version="1.0" encoding="UTF-8"?>
<recipe target="Processor" version="1.0" author="Test Author">
<bernOptions>
    <D_PPPGEN>
        <POLUPD>
            <SHOWGEN>0</SHOWGEN>
            <IEPFIL>$(ORB)_$YYYSS+0</IEPFIL>
            <OUTFIL>$(ORB)_$YYYSS+0</OUTFIL>
            <TITLE>PPP_$YYYSS+0: Extract ERP information</TITLE>
            <SESSION_YEAR>$Y+0</SESSION_YEAR>
            <SESSION_STRG>$S+0</SESSION_STRG>
            <STADAT>$YMD_STR+0</STADAT>
        </POLUPD>
        <ORBGEN>
            <PREFIL>$(ORB)_$YYYSS+0</PREFIL>
            <POLE>$(ORB)_$YYYSS+0</POLE>
            <SATELL>opt_SATELL</SATELL>
            <SATCRUX>SAT_$Y+0</SATCRUX>
        </ORBGEN>
    </D_PPPGEN>
    <D_PPPFIN>
        <GPSEST>
            <TITLE>PPP_$YYYSS+0: Final solution</TITLE>
            <SAMPLING>30</SAMPLING>
            <MINEL>5</MINEL>
        </GPSEST>
    </D_PPPFIN>
</bernOptions>
</recipe>
"""


@pytest.fixture
def sample_xml_path(tmp_path: Path) -> Path:
    """Create a temporary XML file for testing."""
    xml_file = tmp_path / "test_bsw_options.xml"
    xml_file.write_text(SAMPLE_XML)
    return xml_file


class TestBSWOptionsParser:
    """Tests for BSWOptionsParser class."""

    def test_load_xml(self, sample_xml_path: Path) -> None:
        """Test loading XML file."""
        parser = BSWOptionsParser()
        config = parser.load(sample_xml_path)

        assert config is not None
        assert config.target == "Processor"
        assert config.version == "1.0"
        assert config.author == "Test Author"

    def test_load_nonexistent_file(self) -> None:
        """Test loading non-existent file raises error."""
        parser = BSWOptionsParser()
        with pytest.raises(FileNotFoundError):
            parser.load("/nonexistent/path.xml")

    def test_list_steps(self, sample_xml_path: Path) -> None:
        """Test listing processing steps."""
        parser = BSWOptionsParser()
        parser.load(sample_xml_path)

        steps = parser.list_steps()
        assert "D_PPPGEN" in steps
        assert "D_PPPFIN" in steps
        assert len(steps) == 2

    def test_list_programs(self, sample_xml_path: Path) -> None:
        """Test listing programs in a step."""
        parser = BSWOptionsParser()
        parser.load(sample_xml_path)

        programs = parser.list_programs("D_PPPGEN")
        assert "POLUPD" in programs
        assert "ORBGEN" in programs
        assert len(programs) == 2

    def test_get_step_options(self, sample_xml_path: Path) -> None:
        """Test getting step options."""
        parser = BSWOptionsParser()
        parser.load(sample_xml_path)

        step = parser.get_step_options("D_PPPGEN")
        assert step is not None
        assert step.step_name == "D_PPPGEN"
        assert "POLUPD" in step.programs
        assert "ORBGEN" in step.programs

    def test_get_program_options(self, sample_xml_path: Path) -> None:
        """Test getting program options."""
        parser = BSWOptionsParser()
        parser.load(sample_xml_path)

        polupd = parser.get_program_options("D_PPPGEN", "POLUPD")
        assert polupd is not None
        assert polupd.program_name == "POLUPD"
        assert polupd.get("SHOWGEN") == "0"
        assert "IEPFIL" in polupd
        assert polupd["IEPFIL"] == "$(ORB)_$YYYSS+0"

    def test_get_nonexistent_step(self, sample_xml_path: Path) -> None:
        """Test getting non-existent step returns None."""
        parser = BSWOptionsParser()
        parser.load(sample_xml_path)

        step = parser.get_step_options("NONEXISTENT")
        assert step is None

    def test_get_nonexistent_program(self, sample_xml_path: Path) -> None:
        """Test getting non-existent program returns None."""
        parser = BSWOptionsParser()
        parser.load(sample_xml_path)

        prog = parser.get_program_options("D_PPPGEN", "NONEXISTENT")
        assert prog is None


class TestVariableSubstitution:
    """Tests for variable substitution."""

    def test_substitute_year(self) -> None:
        """Test year variable substitution."""
        parser = BSWOptionsParser()

        result = parser.substitute_variables(
            text="SAT_$Y+0 and $YY",
            year=2024,
            doy=260,
            session="2600",
        )
        assert result == "SAT_2024 and 24"

    def test_substitute_doy(self) -> None:
        """Test DOY variable substitution."""
        parser = BSWOptionsParser()

        result = parser.substitute_variables(
            text="DOY=$D+0",
            year=2024,
            doy=5,
            session="0050",
        )
        assert result == "DOY=005"

    def test_substitute_session(self) -> None:
        """Test session variable substitution."""
        parser = BSWOptionsParser()

        result = parser.substitute_variables(
            text="SES=$S+0 YS=$YYYSS+0",
            year=2024,
            doy=260,
            session="2600",
        )
        assert result == "SES=2600 YS=242600"

    def test_substitute_date_string(self) -> None:
        """Test date string substitution."""
        parser = BSWOptionsParser()

        result = parser.substitute_variables(
            text="DATE=$YMD_STR+0",
            year=2024,
            doy=260,
            session="2600",
        )
        # DOY 260 in 2024 is September 16
        assert result == "DATE=2024 09 16"

    def test_substitute_orbit_prefix(self) -> None:
        """Test orbit prefix substitution."""
        parser = BSWOptionsParser()

        result = parser.substitute_variables(
            text="$(ORB)_$YYYSS+0",
            year=2024,
            doy=260,
            session="2600",
            orbit_prefix="COD",
        )
        assert result == "COD_242600"

    def test_substitute_options(self) -> None:
        """Test substituting all options in a program."""
        parser = BSWOptionsParser()

        options = BSWProgramOptions(
            program_name="TEST",
            options={
                "YEAR": "$Y+0",
                "DOY": "$D+0",
                "FILE": "$(ORB)_$YYYSS+0",
            },
        )

        result = parser.substitute_options(
            options=options,
            year=2024,
            doy=260,
            session="2600",
            orbit_prefix="IGS",
        )

        assert result["YEAR"] == "2024"
        assert result["DOY"] == "260"
        assert result["FILE"] == "IGS_242600"


class TestOptionDirs:
    """Tests for option directory mappings."""

    def test_ppp_option_dirs(self) -> None:
        """Test PPP option directories."""
        dirs = get_option_dirs("ppp")
        assert dirs["i1"] == "D_PPPGEN"
        assert dirs["i5"] == "D_PPPFIN"
        assert len(dirs) == len(PPP_OPTION_DIRS)

    def test_nrddp_option_dirs(self) -> None:
        """Test NRDDP option directories."""
        dirs = get_option_dirs("nrddp")
        assert dirs["i1"] == "NRDDPGEN"
        assert dirs["i10"] == "NRDDPFIN"
        assert len(dirs) == len(NRDDP_OPTION_DIRS)

    def test_get_option_dirs_case_insensitive(self) -> None:
        """Test option dirs works case-insensitively."""
        dirs_lower = get_option_dirs("nrddp")
        dirs_upper = get_option_dirs("NRDDP")
        assert dirs_lower == dirs_upper


class TestConvenienceFunctions:
    """Tests for convenience functions."""

    def test_load_bsw_options(self, sample_xml_path: Path) -> None:
        """Test convenience function for loading options."""
        config = load_bsw_options(sample_xml_path)

        assert isinstance(config, BSWOptionsConfig)
        assert config.target == "Processor"
        assert "D_PPPGEN" in config.steps


class TestToDict:
    """Tests for dictionary export."""

    def test_to_dict(self, sample_xml_path: Path) -> None:
        """Test exporting configuration as dictionary."""
        parser = BSWOptionsParser()
        parser.load(sample_xml_path)

        result = parser.to_dict()

        assert "xml_path" in result
        assert result["target"] == "Processor"
        assert result["version"] == "1.0"
        assert "steps" in result
        assert "D_PPPGEN" in result["steps"]
        assert "programs" in result["steps"]["D_PPPGEN"]
        assert "POLUPD" in result["steps"]["D_PPPGEN"]["programs"]


class TestRealXMLFiles:
    """Tests using real XML files from the project."""

    @pytest.fixture
    def ignss_dir(self) -> Path:
        """Get i-GNSS directory."""
        return Path("/home/ahunegnaw/Python_IGNSS/i-GNSS")

    def test_load_ppp_xml(self, ignss_dir: Path) -> None:
        """Test loading actual PPP options XML."""
        xml_path = ignss_dir / "callers" / "iGNSS_D_PPP_AR_IG_IGS54_direct.xml"
        if not xml_path.exists():
            pytest.skip(f"XML file not found: {xml_path}")

        parser = BSWOptionsParser()
        config = parser.load(xml_path)

        assert config is not None
        steps = parser.list_steps()
        assert len(steps) > 0
        # PPP files should have D_PPP* steps
        assert any(s.startswith("D_PPP") for s in steps)

    def test_load_nrddp_xml(self, ignss_dir: Path) -> None:
        """Test loading actual NRDDP options XML."""
        xml_path = (
            ignss_dir / "callers" / "NRDDP_TRO" / "iGNSS_NRDDP_TRO_BSW54_direct.xml"
        )
        if not xml_path.exists():
            pytest.skip(f"XML file not found: {xml_path}")

        parser = BSWOptionsParser()
        config = parser.load(xml_path)

        assert config is not None
        steps = parser.list_steps()
        assert len(steps) > 0
        # NRDDP files should have NRDDP* steps
        assert any(s.startswith("NRDDP") for s in steps)
