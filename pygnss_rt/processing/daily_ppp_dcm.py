"""DCM (Delete, Compress, Move) archiving mixin for DailyPPPProcessor."""

from __future__ import annotations

import shutil
from pathlib import Path

from pygnss_rt.processing.networks import NetworkProfile
from pygnss_rt.utils.dates import GNSSDate


class DCMMixin:
    """DCM archiving methods for DailyPPPProcessor."""

    def _run_dcm(
        self,
        profile: NetworkProfile,
        date: GNSSDate,
        session_name: str,
        campaign_dir: Path,
    ) -> None:
        """Run Delete/Compress/Move archiving.

        This method:
        1. Archives final results (SNX, CRD, TRO, TRP) to archive location
        2. Deletes intermediate directories (RAW, BPE, etc.)
        3. Optionally compresses archived files

        Archive structure: {dcm_archive_dir}/yyyy/doy/
        - FIN_YYYYDDDS.SNX (from SOL directory)
        - FIN_YYYYDDDS.CRD (merged coordinates from STA)
        - FIN_YYYYDDDS.TRO (troposphere zenith delay from ATM)
        - FIN_YYYYDDDS.TRP (troposphere parameters from ATM)
        - Per-station NEQ files: FIN_YYYYDDDS_XXXX.NQ0 (from SOL)

        Args:
            profile: Network profile
            date: Processing date
            session_name: Session name
            campaign_dir: Campaign directory
        """
        if not profile.dcm_enabled:
            return

        y4 = date.year
        doy = date.doy
        session_suffix = f"{y4}{doy:03d}0"  # e.g., 20253570

        # Step 1: Archive final results before deletion
        if profile.dcm_archive_dir:
            org = profile.dcm_organization.replace("yyyy", str(y4)).replace("doy", f"{doy:03d}")
            archive_path = Path(profile.dcm_archive_dir) / org
            archive_path.mkdir(parents=True, exist_ok=True)
            print(f"  Archive path: {archive_path}")

            archived_count = 0

            # Archive SNX file from SOL directory (e.g., RED_20253570.SNX)
            sol_dir = campaign_dir / "SOL"
            if sol_dir.exists():
                for snx_file in sol_dir.glob(f"*_{session_suffix}.SNX"):
                    dest = archive_path / snx_file.name
                    try:
                        shutil.copy2(snx_file, dest)
                        print(f"    Archived: {snx_file.name}")
                        archived_count += 1
                    except Exception as e:
                        print(f"    Warning: Failed to archive {snx_file.name}: {e}")

                # Archive NEQ files (per-station: FIN_20253570_XXXX.NQ0)
                for neq_file in sol_dir.glob(f"FIN_{session_suffix}_*.NQ0"):
                    dest = archive_path / neq_file.name
                    try:
                        shutil.copy2(neq_file, dest)
                        archived_count += 1
                    except Exception as e:
                        print(f"    Warning: Failed to archive {neq_file.name}: {e}")
                if archived_count > 0:
                    print(f"    Archived: {archived_count - 1} NEQ files")

            # Archive merged CRD file from STA directory
            sta_dir = campaign_dir / "STA"
            if sta_dir.exists():
                # Look for FIN_YYYYDDDS.CRD (merged coordinates)
                crd_file = sta_dir / f"FIN_{session_suffix}.CRD"
                if crd_file.exists():
                    dest = archive_path / crd_file.name
                    try:
                        shutil.copy2(crd_file, dest)
                        print(f"    Archived: {crd_file.name}")
                        archived_count += 1
                    except Exception as e:
                        print(f"    Warning: Failed to archive {crd_file.name}: {e}")

            # Archive TRO and TRP files from ATM directory (final versions only)
            atm_dir = campaign_dir / "ATM"
            if atm_dir.exists():
                tro_trp_count = 0
                # Archive merged FIN TRO/TRP files
                for ext in ["TRO", "TRP"]:
                    merged_file = atm_dir / f"FIN_{session_suffix}.{ext}"
                    if merged_file.exists():
                        dest = archive_path / merged_file.name
                        try:
                            shutil.copy2(merged_file, dest)
                            print(f"    Archived: {merged_file.name}")
                            archived_count += 1
                        except Exception as e:
                            print(f"    Warning: Failed to archive {merged_file.name}: {e}")
                    else:
                        # Look for per-station final TRO/TRP files
                        for trp_file in atm_dir.glob(f"FIN_{session_suffix}_*.{ext}"):
                            dest = archive_path / trp_file.name
                            try:
                                shutil.copy2(trp_file, dest)
                                tro_trp_count += 1
                            except Exception as e:
                                print(f"    Warning: Failed to archive {trp_file.name}: {e}")
                if tro_trp_count > 0:
                    print(f"    Archived: {tro_trp_count} TRO/TRP files")
                    archived_count += tro_trp_count

            print(f"  Total files archived: {archived_count}")

        # Step 2: Delete specified directories (RAW, BPE, OBS, etc.)
        deleted_dirs = []
        for dir_name in profile.dcm_dirs_to_delete:
            dir_path = campaign_dir / dir_name
            if dir_path.exists():
                try:
                    shutil.rmtree(dir_path)
                    deleted_dirs.append(dir_name)
                except Exception as e:
                    print(f"  Warning: Failed to delete {dir_name}: {e}")

        if deleted_dirs:
            print(f"  Deleted directories: {', '.join(deleted_dirs)}")

        # Step 3: Optionally delete the entire campaign directory
        # For now, keep ATM, SOL, STA directories with remaining files
        # The campaign directory structure is preserved for potential reprocessing
