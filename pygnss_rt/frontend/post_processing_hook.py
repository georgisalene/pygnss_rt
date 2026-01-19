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
"""

import sys
import os
from pathlib import Path
from datetime import datetime

# Add parent to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from frontend.bernese_parser import parse_campaign_day
from frontend.db_models import QualityDatabase
from frontend.config import DB_PATH, CAMPAIGN_PATH


def update_quality_db(year: int, doy: int, campaign_path: str = CAMPAIGN_PATH,
                      verbose: bool = True) -> bool:
    """
    Update quality database after processing completes.

    Args:
        year: Processing year
        doy: Day of year
        campaign_path: Path to campaign directory
        verbose: Print progress messages

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

        # Parse and save results
        results = parse_campaign_day(campaign_path, year, doy, db)

        db.close()

        if verbose:
            print(f"Updated {len(results)} station solutions in database")

        return len(results) > 0

    except Exception as e:
        print(f"Error updating quality database: {e}")
        return False


def integrate_with_daily_ppp():
    """
    Example integration with daily_ppp.py processing.

    Add this to your daily_ppp.py after processing completes:

    ```python
    from frontend.post_processing_hook import update_quality_db

    class DailyPPPProcessor:
        def process_day(self, year: int, doy: int):
            # ... existing processing code ...

            # After successful processing, update quality DB
            update_quality_db(year, doy, self.campaign_path)
    ```
    """
    pass


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python post_processing_hook.py <year> <doy> [campaign_path]")
        print("Example: python post_processing_hook.py 2025 296")
        sys.exit(1)

    year = int(sys.argv[1])
    doy = int(sys.argv[2])
    campaign = sys.argv[3] if len(sys.argv) > 3 else CAMPAIGN_PATH

    success = update_quality_db(year, doy, campaign)
    sys.exit(0 if success else 1)
