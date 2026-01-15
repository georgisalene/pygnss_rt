"""
Tests for network profile configuration.

Tests the network profiles used for daily PPP processing, ensuring
CODE products are properly configured for PPP-AR.
"""

import pytest
from pathlib import Path

from pygnss_rt.processing.networks import (
    NetworkID,
    NetworkProfile,
    get_network_profile,
    create_network_profiles,
    list_networks,
)


class TestNetworkID:
    """Tests for NetworkID enum."""

    def test_network_ids_exist(self):
        """All expected network IDs should exist."""
        assert NetworkID.IG == "IG"
        assert NetworkID.EU == "EU"
        assert NetworkID.GB == "GB"
        assert NetworkID.RG == "RG"
        assert NetworkID.SS == "SS"

    def test_network_id_count(self):
        """Should have exactly 5 network IDs."""
        assert len(NetworkID) == 5


class TestNetworkProfile:
    """Tests for network profile loading."""

    def test_get_ig_profile(self):
        """Should load IG (IGS) network profile."""
        profile = get_network_profile("IG")
        assert profile.network_id == NetworkID.IG
        assert profile.session_id == "IG"
        assert profile.description == "IGS core stations (global reference network)"

    def test_get_eu_profile(self):
        """Should load EU (EUREF) network profile."""
        profile = get_network_profile("EU")
        assert profile.network_id == NetworkID.EU
        assert profile.requires_igs_alignment is True

    def test_get_profile_case_insensitive(self):
        """Network ID lookup should be case-insensitive."""
        profile_lower = get_network_profile("ig")
        profile_upper = get_network_profile("IG")
        assert profile_lower.network_id == profile_upper.network_id

    def test_invalid_network_id_raises(self):
        """Should raise ValueError for invalid network ID."""
        with pytest.raises(ValueError, match="Invalid network ID"):
            get_network_profile("INVALID")

    def test_all_profiles_load(self):
        """All network profiles should load without error."""
        profiles = create_network_profiles()
        assert len(profiles) == 5
        for network_id in NetworkID:
            assert network_id in profiles


class TestCODEProductConfiguration:
    """Tests for CODE product configuration (critical for PPP-AR)."""

    def test_ig_uses_code_clocks(self):
        """IG network should use CODE clocks for PPP-AR."""
        profile = get_network_profile("IG")
        assert profile.clock_source.provider == "CODE"

    def test_ig_uses_code_orbits(self):
        """IG network should use CODE orbits for consistency."""
        profile = get_network_profile("IG")
        assert profile.orbit_source.provider == "CODE"

    def test_ig_uses_code_erp(self):
        """IG network should use CODE ERP for consistency."""
        profile = get_network_profile("IG")
        assert profile.erp_source.provider == "CODE"

    def test_all_networks_use_code_products(self):
        """All networks should use CODE products for PPP-AR consistency."""
        profiles = create_network_profiles()
        for network_id, profile in profiles.items():
            assert profile.clock_source.provider == "CODE", \
                f"{network_id} should use CODE clocks"
            assert profile.orbit_source.provider == "CODE", \
                f"{network_id} should use CODE orbits"
            assert profile.erp_source.provider == "CODE", \
                f"{network_id} should use CODE ERP"

    def test_code_products_use_final_tier(self):
        """CODE products should use final tier by default."""
        profile = get_network_profile("IG")
        assert profile.orbit_source.tier == "final"
        assert profile.clock_source.tier == "final"
        assert profile.erp_source.tier == "final"


class TestNetworkDependencies:
    """Tests for network processing dependencies."""

    def test_ig_no_alignment_required(self):
        """IG (primary) should not require IGS alignment."""
        profile = get_network_profile("IG")
        assert profile.requires_igs_alignment is False

    def test_dependent_networks_require_alignment(self):
        """EU, GB, RG, SS should require IGS alignment."""
        for network_id in ["EU", "GB", "RG", "SS"]:
            profile = get_network_profile(network_id)
            assert profile.requires_igs_alignment is True, \
                f"{network_id} should require IGS alignment"

    def test_dependent_networks_have_archive_files(self):
        """Dependent networks should have archive file specs."""
        for network_id in ["EU", "GB", "RG", "SS"]:
            profile = get_network_profile(network_id)
            assert len(profile.archive_files) > 0, \
                f"{network_id} should have archive files for alignment"


class TestListNetworks:
    """Tests for list_networks function."""

    def test_list_networks_returns_all(self):
        """Should return info for all 5 networks."""
        networks = list_networks()
        assert len(networks) == 5

    def test_list_networks_has_required_fields(self):
        """Each network info should have required fields."""
        networks = list_networks()
        for net in networks:
            assert "id" in net
            assert "description" in net
            assert "session_id" in net
            assert "requires_alignment" in net
