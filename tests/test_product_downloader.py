"""
Tests for product downloader CODE product paths.

Ensures CODE products are correctly generated for PPP-AR processing.
"""

import pytest
from datetime import date
from unittest.mock import Mock, patch

from pygnss_rt.data_access.product_downloader import ProductDownloader
from pygnss_rt.utils.dates import GNSSDate


class TestCODEOrbitPaths:
    """Tests for CODE orbit file path generation."""

    @pytest.fixture
    def downloader(self):
        """Create ProductDownloader instance."""
        return ProductDownloader()

    @pytest.fixture
    def test_date(self):
        """Create test GNSS date (DOY 356, 2025 = Dec 22)."""
        return GNSSDate(year=2025, month=12, day=22)

    def test_code_orbit_filename_final(self, downloader, test_date):
        """CODE final orbit should have correct filename format."""
        paths = downloader._build_orbit_paths(test_date, provider="CODE", tier="final")

        # Check CDDIS path
        assert "CDDIS" in paths
        remote_path, filename = paths["CDDIS"]

        # Filename should match CODE long-format: COD0OPSFIN_YYYYDOY0000_01D_05M_ORB.SP3.gz
        assert filename.startswith("COD0OPSFIN_")
        assert "20253560000" in filename
        assert "_01D_05M_ORB.SP3.gz" in filename

    def test_code_orbit_filename_rapid(self, downloader, test_date):
        """CODE rapid orbit should have correct filename format."""
        paths = downloader._build_orbit_paths(test_date, provider="CODE", tier="rapid")

        _, filename = paths["CDDIS"]
        assert filename.startswith("COD0OPSRAP_")
        assert "_01D_05M_ORB.SP3.gz" in filename

    def test_igs_orbit_different_from_code(self, downloader, test_date):
        """IGS and CODE orbits should have different filenames."""
        code_paths = downloader._build_orbit_paths(test_date, provider="CODE", tier="final")
        igs_paths = downloader._build_orbit_paths(test_date, provider="IGS", tier="final")

        _, code_filename = code_paths["CDDIS"]
        _, igs_filename = igs_paths["CDDIS"]

        assert code_filename != igs_filename
        assert code_filename.startswith("COD")
        assert igs_filename.startswith("IGS")


class TestCODEClockPaths:
    """Tests for CODE clock file path generation."""

    @pytest.fixture
    def downloader(self):
        return ProductDownloader()

    @pytest.fixture
    def test_date(self):
        return GNSSDate(year=2025, month=12, day=22)

    def test_code_clock_filename_final(self, downloader, test_date):
        """CODE final clock should have correct filename format."""
        paths = downloader._build_clock_paths(test_date, provider="CODE", tier="final")

        assert "CDDIS" in paths
        _, filename = paths["CDDIS"]

        # Filename: COD0OPSFIN_YYYYDOY0000_01D_30S_CLK.CLK.gz
        assert filename.startswith("COD0OPSFIN_")
        assert "20253560000" in filename
        assert "_01D_30S_CLK.CLK.gz" in filename

    def test_code_clock_has_integer_property(self, downloader, test_date):
        """CODE clocks should be the integer-property variant for PPP-AR."""
        paths = downloader._build_clock_paths(test_date, provider="CODE", tier="final")
        _, filename = paths["CDDIS"]

        # CODE integer-property clocks use COD0OPSFIN prefix
        assert "COD0OPSFIN" in filename or "COD0OPSRAP" in filename

    def test_igs_clock_not_suitable_for_ppp_ar(self, downloader, test_date):
        """IGS combined clocks should NOT be used for PPP-AR."""
        # This test documents the requirement - IGS clocks don't have integer-cycle property
        igs_paths = downloader._build_clock_paths(test_date, provider="IGS", tier="final")
        _, igs_filename = igs_paths["CDDIS"]

        # IGS combined clocks start with IGS, not COD
        assert igs_filename.startswith("IGS")
        # Note: These clocks are NOT suitable for PPP-AR with CODE biases


class TestCODEERPPaths:
    """Tests for CODE ERP file path generation."""

    @pytest.fixture
    def downloader(self):
        return ProductDownloader()

    @pytest.fixture
    def test_date(self):
        return GNSSDate(year=2025, month=12, day=22)

    def test_erp_uses_code_format(self, downloader, test_date):
        """ERP should use CODE format (COD0OPSFIN)."""
        paths = downloader._build_erp_paths(test_date, provider="CODE")

        assert "CDDIS" in paths
        _, filename = paths["CDDIS"]

        # ERP uses CODE format: COD0OPSFIN_YYYYDOY0000_01D_01D_ERP.ERP.gz
        assert filename.startswith("COD0OPSFIN_")
        assert "_01D_01D_ERP.ERP.gz" in filename


class TestProductConsistency:
    """Tests for product consistency requirements."""

    @pytest.fixture
    def downloader(self):
        return ProductDownloader()

    @pytest.fixture
    def test_date(self):
        return GNSSDate(year=2025, month=12, day=22)

    def test_all_code_products_same_provider(self, downloader, test_date):
        """All CODE products should come from same provider for consistency."""
        orbit_paths = downloader._build_orbit_paths(test_date, provider="CODE", tier="final")
        clock_paths = downloader._build_clock_paths(test_date, provider="CODE", tier="final")
        erp_paths = downloader._build_erp_paths(test_date, provider="CODE")

        _, orbit_file = orbit_paths["CDDIS"]
        _, clock_file = clock_paths["CDDIS"]
        _, erp_file = erp_paths["CDDIS"]

        # All should start with COD0OPSFIN for final products
        assert orbit_file.startswith("COD0OPSFIN")
        assert clock_file.startswith("COD0OPSFIN")
        assert erp_file.startswith("COD0OPSFIN")

    def test_code_products_all_have_cddis_path(self, downloader, test_date):
        """All CODE products should have CDDIS download path."""
        orbit_paths = downloader._build_orbit_paths(test_date, provider="CODE", tier="final")
        clock_paths = downloader._build_clock_paths(test_date, provider="CODE", tier="final")
        erp_paths = downloader._build_erp_paths(test_date, provider="CODE")

        assert "CDDIS" in orbit_paths
        assert "CDDIS" in clock_paths
        assert "CDDIS" in erp_paths
