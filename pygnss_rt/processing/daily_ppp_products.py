"""Product management mixin for DailyPPPProcessor."""

from __future__ import annotations

import shutil
from pathlib import Path
from typing import TYPE_CHECKING

from pygnss_rt.processing.networks import NetworkProfile
from pygnss_rt.utils.dates import GNSSDate

if TYPE_CHECKING:
    from pygnss_rt.processing.daily_ppp import DailyPPPArgs


class ProductMixin:
    """Product download and management methods for DailyPPPProcessor."""

    def _check_products(
        self,
        profile: NetworkProfile,
        date: GNSSDate,
        args: DailyPPPArgs,
    ) -> bool:
        """Check if required products are available and download if needed.

        Downloads orbit (SP3), ERP, and clock products from CDDIS/IGS/CODE
        using the FTPConfigManager and ProductDownloader.

        Args:
            profile: Network profile
            date: Processing date
            args: Processing arguments

        Returns:
            True if all products available/downloaded
        """
        from pathlib import Path
        from pygnss_rt.data_access.ftp_config import FTPConfigManager
        from pygnss_rt.data_access.product_downloader import (
            ProductDownloader,
            ProductDownloadConfig,
        )

        if args.verbose:
            print(f"  Orbit: {profile.orbit_source.provider} {profile.orbit_source.tier}")
            print(f"  ERP: {profile.erp_source.provider} {profile.erp_source.tier}")
            print(f"  Clock: {profile.clock_source.provider} {profile.clock_source.tier}")

        # Build session name and campaign ORB directory
        session_name = self._build_session_name(profile, date, args.systems)
        campaign_root_cfg = self._config.get("bsw", {}).get("campaign_root")
        if campaign_root_cfg:
            campaign_root = Path(campaign_root_cfg)
        elif self.paths.campaign_root:
            campaign_root = self.paths.campaign_root
        else:
            campaign_root = Path.home() / "GPSDATA" / "CAMPAIGN54"
        orb_dir = campaign_root / session_name / "ORB"
        orb_dir.mkdir(parents=True, exist_ok=True)

        # Load FTP configuration
        ftp_config_path = Path(self.ignss_dir) / "conf" / "ftpConfig.xml"
        if not ftp_config_path.exists():
            print(f"  Warning: FTP config not found at {ftp_config_path}")
            # Try alternate location
            ftp_config_path = Path(self.ignss_dir) / "pygnss_rt" / "conf" / "ftpConfig.xml"

        # Use data_root as the product storage directory
        product_storage = Path(self.data_root)
        gps_week = date.gps_week
        product_week_dir = product_storage / "products" / str(gps_week)
        product_week_dir.mkdir(parents=True, exist_ok=True)

        if args.verbose:
            print(f"  Product storage: {product_week_dir}")
            print(f"  Campaign ORB dir: {orb_dir}")

        # Configure the product downloader
        config = ProductDownloadConfig(
            ftp_config_path=ftp_config_path if ftp_config_path.exists() else None,
            destination_dir=product_week_dir,
            max_retries=3,
            timeout=120,
            decompress=True,
        )

        all_products_ok = True

        with ProductDownloader(config) as downloader:
            # Download orbit (SP3) for 3 days (day-1, day0, day+1) for CCPREORB
            print(f"  Downloading orbit files (3-day window)...")
            orbit_ok = False
            for day_offset in [-1, 0, 1]:
                orbit_date = date.add_days(day_offset)
                orbit_result = downloader.download_orbit(
                    orbit_date,
                    provider=profile.orbit_source.provider,
                    tier=profile.orbit_source.tier,
                )
                if orbit_result.success:
                    offset_str = f"+{day_offset}" if day_offset >= 0 else str(day_offset)
                    print(f"    Orbit[{offset_str}]: {orbit_result.local_path.name} (from {orbit_result.source})")
                    # Copy/link to campaign ORB directory with Bernese naming
                    # Use PRE extension for CCPREORB/ORBMRG (same as SP3 format)
                    self._copy_product_to_campaign(orbit_result.local_path, orb_dir, orbit_date, "PRE")
                    if day_offset == 0:
                        orbit_ok = True
                else:
                    if day_offset == 0:
                        print(f"    Orbit download failed: {orbit_result.error_message}")
                    else:
                        print(f"    Orbit[{day_offset:+d}] not found (optional)")
            if not orbit_ok:
                all_products_ok = False

            # Download ERP
            print(f"  Downloading ERP file...")
            erp_result = downloader.download_erp(
                date,
                provider=profile.erp_source.provider,
            )
            if erp_result.success:
                print(f"    ERP: {erp_result.local_path.name} (from {erp_result.source})")
                # Copy to campaign ORB directory
                self._copy_product_to_campaign(erp_result.local_path, orb_dir, date, "IEP")
            else:
                print(f"    ERP download failed: {erp_result.error_message}")
                all_products_ok = False

            # Download clock (CLK)
            print(f"  Downloading clock file...")
            clock_result = downloader.download_clock(
                date,
                provider=profile.clock_source.provider,
                tier=profile.clock_source.tier,
            )
            if clock_result.success:
                print(f"    Clock: {clock_result.local_path.name} (from {clock_result.source})")
                # Copy to campaign OUT directory (CLK files go there for CCRNXC)
                out_dir = campaign_root / session_name / "OUT"
                out_dir.mkdir(parents=True, exist_ok=True)
                self._copy_product_to_campaign(clock_result.local_path, out_dir, date, "CLK")
            else:
                print(f"    Clock download failed: {clock_result.error_message}")
                all_products_ok = False

            # Download BIA/OSB (Signal Biases for PPP-AR)
            print(f"  Downloading BIA/OSB file...")
            bia_result = downloader.download_bia(date, provider="CODE")
            if bia_result.success:
                print(f"    BIA: {bia_result.local_path.name} (from {bia_result.source})")
                # Copy to campaign ORB directory
                self._copy_product_to_campaign(bia_result.local_path, orb_dir, date, "BIA")
            else:
                print(f"    BIA download failed: {bia_result.error_message}")
                # BIA is required for PPP-AR but not fatal
                print(f"    Warning: PPP-AR may not work without OSB/BIA file")

            # Download ION/GIM (Ionosphere model)
            print(f"  Downloading ION/GIM file...")
            ion_result = downloader.download_ion(date, provider="CODE")
            if ion_result.success:
                print(f"    ION: {ion_result.local_path.name} (from {ion_result.source})")
                # Copy to campaign ATM directory
                atm_dir = campaign_root / session_name / "ATM"
                atm_dir.mkdir(parents=True, exist_ok=True)
                self._copy_product_to_campaign(ion_result.local_path, atm_dir, date, "ION")
            else:
                print(f"    ION download failed: {ion_result.error_message}")
                # ION is optional but useful

            # Download VMF3 (Troposphere mapping functions) - combined 1x1 degree GRD file
            print(f"  Downloading VMF3 files...")
            grd_dir = campaign_root / session_name / "GRD"
            grd_dir.mkdir(parents=True, exist_ok=True)
            vmf_result = downloader.download_vmf3(date, destination=grd_dir)
            if vmf_result.success:
                print(f"    VMF3: Combined GRD file created - {vmf_result.local_path.name}")
            else:
                print(f"    VMF3: All downloads failed")
                # VMF3 is optional but useful for troposphere modeling

        return all_products_ok

    def _copy_product_to_campaign(
        self,
        source_path: Path,
        dest_dir: Path,
        date: GNSSDate,
        product_type: str,
    ) -> Path | None:
        """Copy a downloaded product file to campaign directory with Bernese naming.

        Converts IGS long-format names to Bernese short format:
        - SP3: IGS0OPSFIN_20253560000_01D_15M_ORB.SP3 -> COD_2025356.EPH
        - ERP: IGS0OPSFIN_20253500000_07D_01D_ERP.ERP -> COD_2025356.IEP
        - CLK: IGS0OPSFIN_20253560000_01D_30S_CLK.CLK -> COD_2025356.CLK
        - ION: COD0OPSFIN_...GIM.INX -> HOI_YYYYDDDS.ION (Higher Order Ionosphere)

        Args:
            source_path: Path to downloaded product file
            dest_dir: Destination campaign directory
            date: Processing date
            product_type: Type of product (EPH, IEP, CLK, ION, etc.)

        Returns:
            Path to copied file or None on error
        """
        # Build Bernese-style filename based on product type
        if product_type == "ION":
            # ION files need special naming: HOI_YYYYDDDS.ION (S = session, 0 for daily)
            bernese_name = f"HOI_{date.year}{date.doy:03d}0.ION"
        else:
            # Standard format: COD_YYYYDOY.EXT
            bernese_name = f"COD_{date.year}{date.doy:03d}.{product_type}"
        dest_path = dest_dir / bernese_name

        try:
            shutil.copy2(source_path, dest_path)
            return dest_path
        except Exception as e:
            print(f"    Warning: Could not copy {source_path.name} to {dest_path}: {e}")
            return None

    def _download_station_data(
        self,
        profile: NetworkProfile,
        date: GNSSDate,
        stations: list[str],
        args: DailyPPPArgs,
    ) -> bool:
        """Download station RINEX data.

        Two-step process (like the original Perl implementation):
        1. Download RINEX 3 files from CDDIS to central storage (data54/rinex/)
        2. Convert to Bernese 5.4 format and copy to campaign RAW directory

        Uses the StationDownloader which handles:
        - CDDIS HTTPS authentication (via NASA Earthdata Login)
        - RINEX 3 to Bernese 5.4 filename conversion (WTZR00DEU20252710.RXO)
        - Hatanaka decompression (.crx.gz -> .rnx -> .RXO)

        Args:
            profile: Network profile
            date: Processing date
            stations: List of stations
            args: Processing arguments

        Returns:
            True if sufficient data downloaded (>= 50% stations)
        """
        from pygnss_rt.data_access.station_downloader import (
            StationDownloader,
            RINEXType,
        )

        # Step 1: Central storage directory (like Perl's dataDir)
        # Downloads go to: {data_root}/rinex/{year}/{doy}/
        central_storage = Path(self.data_root) / "rinex" / str(date.year) / f"{date.doy:03d}"
        central_storage.mkdir(parents=True, exist_ok=True)

        # Step 2: Campaign RAW directory (final destination)
        campaign_root_cfg = self._config.get("bsw", {}).get("campaign_root")
        if campaign_root_cfg:
            campaign_root = Path(campaign_root_cfg)
        elif self.paths.campaign_root:
            campaign_root = self.paths.campaign_root
        else:
            campaign_root = Path.home() / "GPSDATA" / "CAMPAIGN54"
        session_name = self._build_session_name(profile, date, args.systems)
        raw_dir = campaign_root / session_name / "RAW"
        raw_dir.mkdir(parents=True, exist_ok=True)

        if args.verbose:
            print(f"  Central storage: {central_storage}")
            print(f"  Target RAW directory: {raw_dir}")
            for ftp in profile.data_ftp_sources:
                print(f"  FTP source: {ftp.server_id} ({ftp.category})")

        # Let the downloader use all available providers in priority order
        # The YAML config defines provider priority: CDDIS (RINEX3) first, then FTP fallbacks
        # This ensures we try CDDIS before falling back to RINEX2 servers
        if args.verbose:
            if args.local_rinex_dir:
                print(f"  Local RINEX source: {args.local_rinex_dir}")
                if args.local_only:
                    print(f"  Mode: LOCAL ONLY (no network downloads)")
                else:
                    print(f"  Mode: Local first, then network fallback")
            else:
                print(f"  Using all providers in priority order (CDDIS first)")

        # Download to central storage with Bernese 5.4 naming (flat_structure=True)
        # This will create files like WTZR00DEU20252710.RXO
        downloader = StationDownloader(
            download_dir=central_storage,
            verbose=args.verbose,
            max_retries=2,
            parallel_downloads=12,  # Increased for faster downloads (CDDIS dir is cached)
            flat_structure=True,  # Enables Bernese 5.4 long format naming
            local_rinex_dir=args.local_rinex_dir,  # Local RINEX source
            local_only=args.local_only,  # Use only local files if True
        )

        try:
            print(f"  Downloading {len(stations)} stations to central storage...")
            # Pass None for providers to use all available providers in priority order
            results = downloader.download_daily_data(
                stations=stations,
                year=date.year,
                doy=date.doy,
                providers=None,  # Use all providers in priority order (CDDIS first)
            )

            # Get summary
            summary = downloader.get_download_summary(results)
            successful_downloads = [r for r in results if r.success]

            if args.verbose:
                print(f"  Downloaded: {summary['successful']}/{summary['total']}")
                if summary['failed_stations']:
                    print(f"  Failed: {', '.join(summary['failed_stations'][:10])}")

            # Copy downloaded files to campaign RAW directory
            copied_count = 0
            for result in successful_downloads:
                if result.local_path and result.local_path.exists():
                    dest_file = raw_dir / result.local_path.name
                    try:
                        shutil.copy2(result.local_path, dest_file)
                        copied_count += 1
                        if args.verbose:
                            print(f"    Copied: {result.local_path.name} -> RAW/")
                    except Exception as e:
                        print(f"    Warning: Failed to copy {result.local_path.name}: {e}")

            print(f"  Copied {copied_count} files to campaign RAW directory")

            # Consider success if >= min_stations_pct stations downloaded
            success_rate = summary['success_rate']
            min_pct = args.min_stations_pct
            if success_rate >= min_pct:
                print(f"  Download success rate: {success_rate*100:.0f}% (min: {min_pct*100:.0f}%)")
                return True
            else:
                print(f"  Download success rate too low: {success_rate*100:.0f}% (min: {min_pct*100:.0f}%)")
                return False

        except Exception as e:
            print(f"  Download error: {e}")
            return False
        finally:
            downloader.close()
