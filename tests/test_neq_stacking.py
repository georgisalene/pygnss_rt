"""Tests for NEQ stacking module."""

import tempfile
from pathlib import Path

import pytest

from pygnss_rt.processing.neq_stacking import (
    NEQStacker,
    NEQStackingConfig,
    NEQNameScheme,
    NEQFileInfo,
    mjdh_to_components,
    components_to_mjdh,
    create_neq_stacking_config,
    NRDDP_TRO_STACKING,
    NO_STACKING,
)


class TestMJDConversion:
    """Tests for MJD to date component conversion."""

    def test_mjdh_to_components_noon(self) -> None:
        """Test conversion at noon."""
        # MJD 60560.5 is approximately 2024-09-16 12:00 UTC
        year, doy, hour, hour_char = mjdh_to_components(60560.5)

        assert year == 2024
        assert doy == 260
        assert hour == 12
        assert hour_char == "m"  # 12th hour = 'm' (a=0, b=1, ..., m=12)

    def test_mjdh_to_components_midnight(self) -> None:
        """Test conversion at midnight."""
        year, doy, hour, hour_char = mjdh_to_components(60560.0)

        assert hour == 0
        assert hour_char == "a"

    def test_components_to_mjdh(self) -> None:
        """Test reverse conversion."""
        mjdh = components_to_mjdh(2024, 260, 12)

        # Should be approximately 60560.5
        assert abs(mjdh - 60560.5) < 0.01

    def test_roundtrip_conversion(self) -> None:
        """Test that conversion roundtrips correctly."""
        original_mjdh = 60560.75  # 18:00 UTC

        year, doy, hour, hour_char = mjdh_to_components(original_mjdh)
        recovered_mjdh = components_to_mjdh(year, doy, hour)

        assert abs(recovered_mjdh - original_mjdh) < 0.01


class TestNEQStackingConfig:
    """Tests for NEQStackingConfig."""

    def test_default_config(self) -> None:
        """Test default configuration."""
        config = NEQStackingConfig()

        assert config.enabled is False
        assert config.n_hours_to_stack == 4
        assert config.name_scheme == NEQNameScheme.HOURLY

    def test_enabled_config(self) -> None:
        """Test enabled configuration."""
        config = NEQStackingConfig(
            enabled=True,
            n_hours_to_stack=6,
            name_scheme=NEQNameScheme.SUB_HOURLY,
        )

        assert config.enabled is True
        assert config.n_hours_to_stack == 6
        assert config.name_scheme == NEQNameScheme.SUB_HOURLY

    def test_from_dict_perl_style(self) -> None:
        """Test creating config from Perl-style dictionary."""
        perl_dict = {
            "yesORno": "yes",
            "n2stack": 4,
            "nameScheme": "P1_yydoyU",
        }

        config = NEQStackingConfig.from_dict(perl_dict)

        assert config.enabled is True
        assert config.n_hours_to_stack == 4
        assert config.name_scheme == NEQNameScheme.HOURLY

    def test_from_dict_disabled(self) -> None:
        """Test disabled config from dict."""
        perl_dict = {
            "yesORno": "no",
            "n2stack": 0,
        }

        config = NEQStackingConfig.from_dict(perl_dict)

        assert config.enabled is False

    def test_string_name_scheme_conversion(self) -> None:
        """Test that string name scheme is converted to enum."""
        config = NEQStackingConfig(
            enabled=True,
            name_scheme="P1_yydoyU",
        )

        assert config.name_scheme == NEQNameScheme.HOURLY


class TestNEQFileInfo:
    """Tests for NEQFileInfo dataclass."""

    def test_base_name_hourly(self) -> None:
        """Test base name generation for hourly scheme."""
        info = NEQFileInfo(
            file_path=Path("/archive/SOL/P1_24260A.NQ0"),
            session_name="24260ANR",
            year=2024,
            doy=260,
            hour_char="a",
        )

        assert info.base_name == "P1_24260A.NQ0"

    def test_base_name_subhourly(self) -> None:
        """Test base name generation for sub-hourly scheme."""
        info = NEQFileInfo(
            file_path=Path("/archive/SOL/P1_24260A15.NQ0"),
            session_name="24260A1",
            year=2024,
            doy=260,
            hour_char="a",
            minutes="15",
        )

        assert info.base_name == "P1_24260A15.NQ0"


class TestNEQStacker:
    """Tests for NEQStacker class."""

    def test_stacker_disabled(self) -> None:
        """Test that disabled stacker returns empty list."""
        config = NEQStackingConfig(enabled=False)
        stacker = NEQStacker(config)

        files = stacker.get_neq_files_to_stack(
            current_mjdh=60560.5,
            archive_dir="/nonexistent",
        )

        assert files == []

    def test_get_hourly_neq_files(self, tmp_path: Path) -> None:
        """Test getting hourly NEQ files."""
        config = NEQStackingConfig(
            enabled=True,
            n_hours_to_stack=3,
            name_scheme=NEQNameScheme.HOURLY,
            archive_organization="yyyy/doy",
            session_suffix="NR",
        )
        stacker = NEQStacker(config)

        # MJD 60560.5 = 2024-09-16 12:00 UTC (hour 'm')
        files = stacker.get_neq_files_to_stack(
            current_mjdh=60560.5,
            archive_dir=tmp_path,
        )

        assert len(files) == 3

        # Check that we're looking for the previous 3 hours
        # Hour 12 (m) - 1 = 11 (l)
        # Hour 12 (m) - 2 = 10 (k)
        # Hour 12 (m) - 3 = 9 (j)
        hour_chars = [f.hour_char for f in files]
        assert "l" in hour_chars  # Hour 11
        assert "k" in hour_chars  # Hour 10
        assert "j" in hour_chars  # Hour 9

    def test_get_stacking_summary(self) -> None:
        """Test summary generation."""
        config = NEQStackingConfig(enabled=True, n_hours_to_stack=4)
        stacker = NEQStacker(config)

        files = [
            NEQFileInfo(
                file_path=Path("/test/P1_24260A.NQ0"),
                session_name="24260ANR",
                year=2024,
                doy=260,
                hour_char="a",
                exists=True,
            ),
            NEQFileInfo(
                file_path=Path("/test/P1_24260B.NQ0"),
                session_name="24260BNR",
                year=2024,
                doy=260,
                hour_char="b",
                exists=False,
            ),
        ]

        summary = stacker.get_stacking_summary(files)

        assert summary["total_requested"] == 2
        assert summary["available"] == 1
        assert summary["missing"] == 1
        assert summary["n_hours_to_stack"] == 4

    def test_copy_neq_files(self, tmp_path: Path) -> None:
        """Test copying NEQ files to campaign directory."""
        # Create a mock NEQ file
        source_dir = tmp_path / "archive" / "2024" / "260" / "24260ANR" / "SOL"
        source_dir.mkdir(parents=True)
        neq_file = source_dir / "P1_24260A.NQ0"
        neq_file.write_text("NEQ file content")

        config = NEQStackingConfig(enabled=True)
        stacker = NEQStacker(config, verbose=True)

        neq_info = NEQFileInfo(
            file_path=neq_file,
            session_name="24260ANR",
            year=2024,
            doy=260,
            hour_char="a",
            exists=True,
        )

        # Copy to campaign
        campaign_sol = tmp_path / "campaign" / "SOL"
        copied = stacker.copy_neq_files_to_campaign([neq_info], campaign_sol)

        assert len(copied) == 1
        assert copied[0].exists()
        assert copied[0].read_text() == "NEQ file content"


class TestPredefinedConfigs:
    """Tests for predefined configuration objects."""

    def test_nrddp_tro_stacking(self) -> None:
        """Test NRDDP TRO stacking configuration."""
        assert NRDDP_TRO_STACKING.enabled is True
        assert NRDDP_TRO_STACKING.n_hours_to_stack == 4
        assert NRDDP_TRO_STACKING.name_scheme == NEQNameScheme.HOURLY
        assert NRDDP_TRO_STACKING.session_suffix == "NR"

    def test_no_stacking(self) -> None:
        """Test no stacking configuration."""
        assert NO_STACKING.enabled is False


class TestConvenienceFunction:
    """Tests for convenience functions."""

    def test_create_neq_stacking_config(self) -> None:
        """Test convenience function for creating config."""
        config = create_neq_stacking_config(
            enabled=True,
            n_hours=6,
            name_scheme="P1_yydoyU",
            session_suffix="H",
        )

        assert config.enabled is True
        assert config.n_hours_to_stack == 6
        assert config.session_suffix == "H"
