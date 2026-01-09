"""Tests for NRDDP TRO processing modules."""

import tempfile
from pathlib import Path

import pytest

from pygnss_rt.processing.nrt_coordinates import (
    NRTCoordinateManager,
    NRTCoordinateConfig,
    CoordinateFileInfo,
    create_nrt_coordinate_config,
    NRDDP_TRO_COORDINATES,
)
from pygnss_rt.processing.station_merger import (
    StationMerger,
    NetworkSource,
    StationInfo,
    MergerConfig,
    NRDDP_STATION_SOURCES,
)
from pygnss_rt.processing.nrddp_tro import (
    NRDDPTROProcessor,
    NRDDPTROArgs,
    NRDDPTROResult,
    NRDDPTROConfig,
)


class TestNRTCoordinateConfig:
    """Tests for NRTCoordinateConfig."""

    def test_default_config(self) -> None:
        """Test default configuration."""
        config = NRTCoordinateConfig()

        assert config.main_prefix == "DNR"
        assert config.backup_prefix == "ANR"
        assert config.suffix == "0.CRD"
        assert config.remove_if_no_coord is True

    def test_custom_config(self) -> None:
        """Test custom configuration."""
        config = NRTCoordinateConfig(
            base_dir=Path("/custom/path"),
            main_prefix="XNR",
            remove_if_no_coord=False,
        )

        assert config.base_dir == Path("/custom/path")
        assert config.main_prefix == "XNR"
        assert config.remove_if_no_coord is False

    def test_predefined_config(self) -> None:
        """Test predefined NRDDP TRO config."""
        assert NRDDP_TRO_COORDINATES.main_prefix == "DNR"
        assert NRDDP_TRO_COORDINATES.backup_prefix == "ANR"
        assert NRDDP_TRO_COORDINATES.remove_if_no_coord is True


class TestNRTCoordinateManager:
    """Tests for NRTCoordinateManager."""

    def test_format_main_path(self) -> None:
        """Test main coordinate file path formatting."""
        config = NRTCoordinateConfig(base_dir=Path("/data/coord"))
        manager = NRTCoordinateManager(config=config)

        path = manager.get_main_coordinate_file(year=2024, doy=260)

        assert path == Path("/data/coord/DNR242600.CRD")

    def test_format_backup_path(self) -> None:
        """Test backup coordinate file path formatting."""
        config = NRTCoordinateConfig(base_dir=Path("/data/coord"))
        manager = NRTCoordinateManager(config=config)

        path = manager.get_backup_coordinate_file(year=2024, doy=260)

        assert path == Path("/data/coord/ANR242600.CRD")

    def test_year_wrapping(self) -> None:
        """Test 2-digit year wrapping."""
        config = NRTCoordinateConfig(base_dir=Path("/data"))
        manager = NRTCoordinateManager(config=config)

        path = manager.get_main_coordinate_file(year=2100, doy=1)

        # Year 2100 % 100 = 0
        assert "00001" in str(path)

    def test_get_coordinate_file_with_fallback(self, tmp_path: Path) -> None:
        """Test fallback to static file."""
        # Create static fallback file
        static_file = tmp_path / "static.CRD"
        static_file.write_text("STATIC COORDS")

        config = NRTCoordinateConfig(
            base_dir=tmp_path / "nrt",  # Doesn't exist
            static_fallback=static_file,
        )
        manager = NRTCoordinateManager(config=config)

        path = manager.get_coordinate_file(year=2024, doy=260)

        assert path == static_file

    def test_get_coordinate_file_not_found(self, tmp_path: Path) -> None:
        """Test FileNotFoundError when no files exist."""
        config = NRTCoordinateConfig(base_dir=tmp_path)
        manager = NRTCoordinateManager(config=config)

        with pytest.raises(FileNotFoundError):
            manager.get_coordinate_file(year=2024, doy=260)

    def test_build_bsw_args(self) -> None:
        """Test BSW args dictionary generation."""
        config = NRTCoordinateConfig(
            base_dir=Path("/data/coord"),
            remove_if_no_coord=True,
        )
        manager = NRTCoordinateManager(config=config)

        args = manager.build_bsw_args(year=2024, doy=260)

        assert args["infoCRD"] == "/data/coord/DNR242600.CRD"
        assert args["infoCRA"] == "/data/coord/ANR242600.CRD"
        assert args["remIfNoCoord"] == "yes"

    def test_convenience_function(self) -> None:
        """Test create_nrt_coordinate_config."""
        config = create_nrt_coordinate_config(
            base_dir="/custom/path",
            main_prefix="TEST",
        )

        assert config.base_dir == Path("/custom/path")
        assert config.main_prefix == "TEST"


class TestStationInfo:
    """Tests for StationInfo dataclass."""

    def test_hash_by_id(self) -> None:
        """Test hashing by station ID."""
        s1 = StationInfo(station_id="algo")
        s2 = StationInfo(station_id="ALGO")  # Different case

        assert hash(s1) == hash(s2)

    def test_equality_case_insensitive(self) -> None:
        """Test case-insensitive equality."""
        s1 = StationInfo(station_id="algo", name="Algonquin")
        s2 = StationInfo(station_id="ALGO", name="Different")

        assert s1 == s2


class TestStationMerger:
    """Tests for StationMerger."""

    @pytest.fixture
    def sample_xml(self, tmp_path: Path) -> Path:
        """Create a sample station XML file."""
        xml_content = """<?xml version="1.0"?>
<stations>
    <station id="algo" use_nrt="yes" type="core" latitude="45.9" longitude="-78.0"/>
    <station id="nrc1" use_nrt="yes" type="core" latitude="45.4" longitude="-75.6"/>
    <station id="dubo" use_nrt="no" type="secondary" latitude="50.2" longitude="-95.8"/>
</stations>
"""
        xml_file = tmp_path / "test_stations.xml"
        xml_file.write_text(xml_content)
        return xml_file

    def test_add_source(self, sample_xml: Path) -> None:
        """Test adding a network source."""
        config = MergerConfig(
            xml_paths={NetworkSource.IGS_CORE: sample_xml},
        )
        merger = StationMerger(config=config)

        count = merger.add_source(NetworkSource.IGS_CORE)

        assert count == 3  # All stations in file

    def test_get_merged_stations_nrt_only(self, sample_xml: Path) -> None:
        """Test NRT-only filtering."""
        config = MergerConfig(
            xml_paths={NetworkSource.IGS_CORE: sample_xml},
        )
        merger = StationMerger(config=config)
        merger.add_source(NetworkSource.IGS_CORE)

        stations = merger.get_merged_stations(nrt_only=True)

        assert len(stations) == 2
        ids = [s.station_id for s in stations]
        assert "algo" in ids
        assert "nrc1" in ids
        assert "dubo" not in ids

    def test_deduplication(self, tmp_path: Path) -> None:
        """Test station deduplication across sources."""
        # Create two XML files with overlapping stations
        xml1 = tmp_path / "net1.xml"
        xml1.write_text("""<?xml version="1.0"?>
<stations>
    <station id="algo" use_nrt="yes" type="core"/>
    <station id="nrc1" use_nrt="yes" type="core"/>
</stations>
""")

        xml2 = tmp_path / "net2.xml"
        xml2.write_text("""<?xml version="1.0"?>
<stations>
    <station id="ALGO" use_nrt="yes" type="EUREF"/>
    <station id="dubo" use_nrt="yes" type="EUREF"/>
</stations>
""")

        config = MergerConfig(
            xml_paths={
                NetworkSource.IGS_CORE: xml1,
                NetworkSource.EUREF: xml2,
            },
        )
        merger = StationMerger(config=config)
        merger.add_source(NetworkSource.IGS_CORE)
        merger.add_source(NetworkSource.EUREF)

        stations = merger.get_merged_stations(nrt_only=True)

        # Should have 3 unique stations (algo appears in both)
        assert len(stations) == 3
        ids = [s.station_id for s in stations]
        assert ids.count("algo") == 1  # No duplicate

    def test_get_station_ids(self, sample_xml: Path) -> None:
        """Test getting station ID list."""
        config = MergerConfig(
            xml_paths={NetworkSource.IGS_CORE: sample_xml},
        )
        merger = StationMerger(config=config)
        merger.add_source(NetworkSource.IGS_CORE)

        ids = merger.get_station_ids(nrt_only=True)

        assert isinstance(ids, list)
        assert "algo" in ids
        assert "nrc1" in ids

    def test_get_statistics(self, sample_xml: Path) -> None:
        """Test statistics generation."""
        config = MergerConfig(
            xml_paths={NetworkSource.IGS_CORE: sample_xml},
        )
        merger = StationMerger(config=config)
        merger.add_source(NetworkSource.IGS_CORE)

        stats = merger.get_statistics()

        assert stats["sources_loaded"] == 1
        assert stats["total_stations"] == 3
        assert stats["nrt_stations"] == 2
        assert "igs_core" in stats["by_source"]


class TestNRDDPTROResult:
    """Tests for NRDDPTROResult."""

    def test_mjdh_calculation(self) -> None:
        """Test MJD with hour fraction."""
        from pygnss_rt.utils.dates import GNSSDate

        result = NRDDPTROResult(
            session_name="24260ANR",
            date=GNSSDate.from_doy(2024, 260),
            hour=12,
            hour_char="m",
            success=True,
        )

        # MJD for 2024/260 noon should be ~60560.5
        assert abs(result.mjdh - 60560.5) < 0.01

    def test_duration_calculation(self) -> None:
        """Test duration calculation."""
        from datetime import datetime, timezone, timedelta

        result = NRDDPTROResult(
            session_name="test",
            date=None,  # type: ignore
            hour=0,
            hour_char="a",
            success=True,
            start_time=datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
            end_time=datetime(2024, 1, 1, 12, 5, 30, tzinfo=timezone.utc),
        )

        assert result.duration_seconds == 330.0  # 5 min 30 sec


class TestNRDDPTROConfig:
    """Tests for NRDDPTROConfig."""

    def test_default_config(self) -> None:
        """Test default configuration."""
        config = NRDDPTROConfig()

        assert config.pcf_file == "NRDDPTRO_BSW54.PCF"
        assert config.session_suffix == "NR"
        assert config.datum == "IGS20"
        assert config.dcm_enabled is True

    def test_custom_config(self) -> None:
        """Test custom configuration."""
        config = NRDDPTROConfig(
            session_suffix="H",
            min_elevation=10,
        )

        assert config.session_suffix == "H"
        assert config.min_elevation == 10


class TestNRDDPTROArgs:
    """Tests for NRDDPTROArgs."""

    def test_default_args(self) -> None:
        """Test default arguments."""
        args = NRDDPTROArgs()

        assert args.nrt_only is True
        assert args.latency_hours == 3
        assert args.generate_iwv is True
        assert args.neq_stacking.enabled is True
        assert args.neq_stacking.n_hours_to_stack == 4

    def test_custom_args(self) -> None:
        """Test custom arguments."""
        from pygnss_rt.processing.neq_stacking import NEQStackingConfig

        args = NRDDPTROArgs(
            latency_hours=6,
            nrt_only=False,
            neq_stacking=NEQStackingConfig(enabled=True, n_hours_to_stack=6),
        )

        assert args.latency_hours == 6
        assert args.nrt_only is False
        assert args.neq_stacking.n_hours_to_stack == 6


class TestNRDDPStationSources:
    """Tests for predefined station sources."""

    def test_nrddp_sources_order(self) -> None:
        """Test NRDDP station sources are in correct order."""
        assert NRDDP_STATION_SOURCES[0] == NetworkSource.IGS_CORE
        assert NRDDP_STATION_SOURCES[1] == NetworkSource.EUREF
        assert len(NRDDP_STATION_SOURCES) == 10

    def test_all_sources_defined(self) -> None:
        """Test all network sources are defined."""
        for source in NRDDP_STATION_SOURCES:
            assert isinstance(source, NetworkSource)
