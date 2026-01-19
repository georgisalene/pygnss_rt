#!/usr/bin/env python3
"""
Populate the quality monitoring database from Bernese output files.

Usage:
    # Populate single day
    python populate_db.py 2025 296

    # Populate date range
    python populate_db.py 2025 290 300

    # Watch for new data (continuous mode)
    python populate_db.py --watch

This script can be called:
1. Manually after processing
2. As a post-processing hook
3. In watch mode for continuous monitoring
"""

import argparse
import os
import sys
import time
import glob
from datetime import datetime
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from frontend.bernese_parser import parse_campaign_day, populate_database
from frontend.db_models import QualityDatabase
from frontend.config import DB_PATH, CAMPAIGN_PATH


def get_latest_processed_doy(campaign_path: str, year: int) -> int:
    """Find the most recently processed DOY"""
    pattern = os.path.join(campaign_path, f"{year % 100:02d}*IG", "OUT", "EDL_*.OUT")
    files = glob.glob(pattern)

    if not files:
        return 0

    latest_doy = 0
    for f in files:
        basename = os.path.basename(f)
        # EDL_20252960_BRUX.OUT
        try:
            doy = int(basename.split('_')[1][4:7])
            if doy > latest_doy:
                latest_doy = doy
        except:
            continue

    return latest_doy


def watch_mode(campaign_path: str, year: int, check_interval: int = 60):
    """
    Continuously watch for new processed data and update database.

    Args:
        campaign_path: Path to campaign directory
        year: Year to monitor
        check_interval: Seconds between checks
    """
    print(f"Watch mode: Monitoring {campaign_path} for year {year}")
    print(f"Check interval: {check_interval} seconds")
    print("Press Ctrl+C to stop")

    db = QualityDatabase(DB_PATH)
    db.create_tables()

    last_processed_doy = 0

    while True:
        try:
            current_doy = get_latest_processed_doy(campaign_path, year)

            if current_doy > last_processed_doy:
                print(f"\n[{datetime.now().strftime('%H:%M:%S')}] "
                      f"New data detected: DOY {current_doy}")

                # Parse and save the new day
                parse_campaign_day(campaign_path, year, current_doy, db)
                last_processed_doy = current_doy

                print(f"Database updated with DOY {current_doy}")

            time.sleep(check_interval)

        except KeyboardInterrupt:
            print("\nWatch mode stopped")
            break
        except Exception as e:
            print(f"Error: {e}")
            time.sleep(check_interval)

    db.close()


def main():
    parser = argparse.ArgumentParser(
        description='Populate quality monitoring database from Bernese output'
    )
    parser.add_argument('year', type=int, nargs='?', default=2025,
                        help='Year to process (default: 2025)')
    parser.add_argument('start_doy', type=int, nargs='?',
                        help='Starting day of year')
    parser.add_argument('end_doy', type=int, nargs='?',
                        help='Ending day of year (default: same as start)')
    parser.add_argument('--campaign', type=str, default=CAMPAIGN_PATH,
                        help=f'Campaign path (default: {CAMPAIGN_PATH})')
    parser.add_argument('--watch', action='store_true',
                        help='Watch mode: continuously monitor for new data')
    parser.add_argument('--interval', type=int, default=60,
                        help='Check interval in seconds for watch mode')
    parser.add_argument('--init-db', action='store_true',
                        help='Initialize database tables only')

    args = parser.parse_args()

    # Initialize database if requested
    if args.init_db:
        print("Initializing database tables...")
        db = QualityDatabase(DB_PATH)
        db.create_tables()
        db.close()
        print("Done!")
        return

    # Watch mode
    if args.watch:
        watch_mode(args.campaign, args.year, args.interval)
        return

    # Single day or range mode
    if args.start_doy is None:
        # If no DOY specified, find latest
        latest = get_latest_processed_doy(args.campaign, args.year)
        if latest == 0:
            print("No processed data found. Specify a DOY.")
            return
        args.start_doy = latest
        args.end_doy = latest

    if args.end_doy is None:
        args.end_doy = args.start_doy

    print(f"Processing year {args.year}, DOY {args.start_doy} to {args.end_doy}")
    print(f"Campaign path: {args.campaign}")

    populate_database(args.campaign, args.year, args.start_doy, args.end_doy)


if __name__ == "__main__":
    main()
