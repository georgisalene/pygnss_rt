#!/usr/bin/env python3
"""
Post-Processing Hook for Quality Database Update

This hook is called automatically after Bernese processing completes
to update the quality monitoring database.

Integration with pygnss_rt:
Add this to your processing pipeline:

    from frontend.post_processing_hook import update_quality_db

    # After processing completes
    update_quality_db(year=2025, doy=296, campaign_path="/path/to/campaign")

Or call from command line:
    python post_processing_hook.py 2025 296

The hook performs two main tasks:
1. Parse EDL output files for processing statistics (existing behavior)
2. Parse PPP-AR CRD files for coordinate solutions (new)
"""

import sys
import os
from pathlib import Path
from datetime import datetime

# Add parent to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from frontend.bernese_parser import parse_campaign_day, find_ig_campaign_dir
from frontend.db_models import QualityDatabase, PPPSolution
from frontend.config import DB_PATH, CAMPAIGN_PATH
from pygnss_rt.stations.coordinates import xyz_to_llh
from pygnss_rt.utils.dates import doy_to_mjd


def ingest_pppar_crd(db: QualityDatabase, year: int, doy: int,
                     campaign_path: str = CAMPAIGN_PATH,
                     verbose: bool = True,
                     preferred_session: str | None = None) -> int:
    """
    Ingest PPP-AR coordinates from CRD file into database.

    PPP-AR CRD files are located at:
    {campaign_path}/{yy}{doy}IG[/_*]/STA/FIN_{year}{doy}0.CRD

    Args:
        db: Database connection
        year: Processing year
        doy: Day of year
        campaign_path: Path to campaign directory
        verbose: Print progress messages
        preferred_session: Exact campaign/session directory to use (e.g., 25310IG_G)

    Returns:
        Number of solutions ingested
    """
    campaign_dir = find_ig_campaign_dir(
        campaign_path,
        year,
        doy,
        preferred_session=preferred_session,
    )
    if campaign_dir is None:
        if verbose:
            print(f"  PPP-AR campaign directory not found for {year}/{doy:03d} under {campaign_path}")
        return 0

    crd_path = f"{campaign_dir}/STA/FIN_{year}{doy:03d}0.CRD"

    if not os.path.exists(crd_path):
        if verbose:
            print(f"  PPP-AR CRD file not found: {crd_path}")
        return 0

    mjd = doy_to_mjd(year, doy)
    solutions = []

    try:
        with open(crd_path, 'r') as f:
            in_data = False
            for line in f:
                if 'NUM  STATION NAME' in line:
                    in_data = True
                    continue

                if not in_data or not line.strip():
                    continue

                parts = line.split()
                if len(parts) >= 5:
                    try:
                        station_id = parts[1]
                        x = float(parts[2])
                        y = float(parts[3])
                        z = float(parts[4])

                        lat, lon, height = xyz_to_llh(x, y, z)

                        solutions.append(PPPSolution(
                            station_id=station_id,
                            year=year,
                            doy=doy,
                            mjd=mjd,
                            x=x,
                            y=y,
                            z=z,
                            x_rms=0.001,  # Default 1mm RMS
                            y_rms=0.001,
                            z_rms=0.001,
                            lat=lat,
                            lon=lon,
                            height=height,
                            created_at=datetime.now()
                        ))
                    except (ValueError, IndexError):
                        continue

        # Insert solutions into database
        count = 0
        for sol in solutions:
            try:
                db.insert_solution(sol)
                count += 1
            except Exception as e:
                if verbose:
                    print(f"  Warning: Failed to insert {sol.station_id}: {e}")

        if verbose:
            print(f"  Ingested {count} PPP-AR solutions from CRD file")

        return count

    except Exception as e:
        if verbose:
            print(f"  Error parsing CRD file: {e}")
        return 0


def update_quality_db(year: int, doy: int, campaign_path: str = CAMPAIGN_PATH,
                      verbose: bool = True,
                      preferred_session: str | None = None) -> bool:
    """
    Update quality database after processing completes.

    This function:
    1. Parses EDL output files for processing statistics
    2. Ingests PPP-AR coordinates from CRD files

    Args:
        year: Processing year
        doy: Day of year
        campaign_path: Path to campaign directory
        verbose: Print progress messages
        preferred_session: Exact campaign/session directory to use

    Returns:
        True if successful, False otherwise
    """
    try:
        if verbose:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] "
                  f"Updating quality database for {year}/{doy:03d}")

        db = QualityDatabase(DB_PATH)

        # Ensure tables exist
        db.create_tables()

        # Parse and save EDL results (processing statistics)
        results = parse_campaign_day(
            campaign_path,
            year,
            doy,
            db,
            preferred_session=preferred_session,
        )

        if verbose:
            print(f"  Parsed {len(results)} EDL station solutions")

        # Ingest PPP-AR coordinates from CRD file
        pppar_count = ingest_pppar_crd(
            db,
            year,
            doy,
            campaign_path,
            verbose,
            preferred_session=preferred_session,
        )

        db.close()

        total = len(results) + pppar_count
        if verbose:
            print(f"Total: {total} records updated in database")

        return total > 0

    except Exception as e:
        print(f"Error updating quality database: {e}")
        return False


def integrate_with_daily_ppp():
    """
    Example integration with daily_ppp.py processing.

    Add this to DailyPPPProcessor._process_single_day() after successful processing:

    ```python
    # In daily_ppp.py, after result.success = True:
    from frontend.post_processing_hook import update_quality_db

    # ... existing code ...

    result.success = True
    result.stations_processed = len(stations)
    result.end_time = datetime.now(timezone.utc)

    # NEW: Update quality database with PPP-AR solutions
    try:
        from frontend.post_processing_hook import update_quality_db
        update_quality_db(date.year, date.doy, str(campaign_dir.parent))
    except Exception as e:
        print(f"  Warning: Quality DB update failed: {e}")

    print(f"Processing complete: {result.stations_processed} stations")
    ```

    Alternatively, add to _run_dcm() method after archiving:

    ```python
    # After archiving results, update quality database
    from frontend.post_processing_hook import update_quality_db
    campaign_parent = str(campaign_dir.parent)
    update_quality_db(date.year, date.doy, campaign_parent)
    ```
    """
    pass


def batch_ingest(year: int, start_doy: int, end_doy: int,
                 campaign_path: str = CAMPAIGN_PATH,
                 verbose: bool = True) -> int:
    """
    Batch ingest PPP-AR solutions for a range of days.

    Useful for initial database population or catching up after downtime.

    Args:
        year: Processing year
        start_doy: Starting day of year
        end_doy: Ending day of year
        campaign_path: Path to campaign directory
        verbose: Print progress messages

    Returns:
        Total number of solutions ingested
    """
    if verbose:
        print(f"Batch ingesting PPP-AR solutions for {year} DOY {start_doy}-{end_doy}")
        print(f"Campaign path: {campaign_path}")
        print(f"Database: {DB_PATH}")
        print()

    db = QualityDatabase(DB_PATH)
    db.create_tables()

    total = 0
    for doy in range(start_doy, end_doy + 1):
        count = ingest_pppar_crd(db, year, doy, campaign_path, verbose)
        total += count

    db.close()

    if verbose:
        print(f"\nTotal: {total} solutions ingested")

    return total


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description='Post-processing hook to update quality database',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Update database for a single day
  python post_processing_hook.py 2025 296

  # Update database for a range of days
  python post_processing_hook.py 2025 296 --end-doy 310

  # Ingest only PPP-AR coordinates (skip EDL parsing)
  python post_processing_hook.py 2025 296 --pppar-only

  # Specify custom campaign path
  python post_processing_hook.py 2025 296 --campaign /path/to/campaign
"""
    )
    parser.add_argument('year', type=int, help='Processing year')
    parser.add_argument('doy', type=int, help='Day of year (or start DOY if --end-doy specified)')
    parser.add_argument('--end-doy', type=int, help='End DOY for batch processing')
    parser.add_argument('--campaign', default=CAMPAIGN_PATH, help='Campaign path')
    parser.add_argument('--pppar-only', action='store_true',
                        help='Only ingest PPP-AR coordinates (skip EDL parsing)')
    args = parser.parse_args()

    if args.end_doy:
        # Batch mode
        if args.pppar_only:
            total = batch_ingest(args.year, args.doy, args.end_doy, args.campaign)
            sys.exit(0 if total > 0 else 1)
        else:
            # Full update for each day
            success_count = 0
            for doy in range(args.doy, args.end_doy + 1):
                if update_quality_db(args.year, doy, args.campaign):
                    success_count += 1
            print(f"\nProcessed {success_count}/{args.end_doy - args.doy + 1} days successfully")
            sys.exit(0 if success_count > 0 else 1)
    else:
        # Single day mode
        if args.pppar_only:
            db = QualityDatabase(DB_PATH)
            db.create_tables()
            count = ingest_pppar_crd(db, args.year, args.doy, args.campaign)
            db.close()
            sys.exit(0 if count > 0 else 1)
        else:
            success = update_quality_db(args.year, args.doy, args.campaign)
            sys.exit(0 if success else 1)
