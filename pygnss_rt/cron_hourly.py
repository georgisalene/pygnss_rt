#!/usr/bin/env python3
"""
Hourly GNSS Processing CRON Entry Point.

Main entry point for hourly GNSS processing designed to run from crontab.
Produces ZTD (Zenith Tropospheric Delay) and coordinates.

Replaces the Perl cron job entry points:
- /home/nrt105/bin/hourlyFuncs/runHourlyProc_BSW54.bash
- iGNSS_NRDDP_TRO_54_nrt_direct.pl (called by bash wrapper)

Usage from crontab:
    # Run every hour at minute 45 (for 3-hour latency)
    45 * * * * /home/ahunegnaw/Python_IGNSS/i-GNSS/venv/bin/python \
        /home/ahunegnaw/Python_IGNSS/i-GNSS/pygnss_rt/cron_hourly.py

    # Or with specific latency
    45 * * * * /path/to/python /path/to/cron_hourly.py --latency 3

Manual execution:
    python cron_hourly.py --date 2024-09-16 --hour 12
    python cron_hourly.py --date 2024/260 --hour 0 --verbose
    python cron_hourly.py --dry-run

Environment Variables:
    IGNSS_HOME: Base directory for i-GNSS (default: /home/ahunegnaw/Python_IGNSS/i-GNSS)
    IGNSS_DATA: Data root directory (default: /home/nrt105/data54)
    GPSUSER_DIR: GPSUSER directory (default: /home/ahunegnaw/GPSUSER54_LANT)
    IGNSS_CONFIG: Path to config file (optional)
    IGNSS_DRY_RUN: Set to "1" for dry-run mode
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any


def setup_paths() -> None:
    """Add pygnss_rt to Python path if not already present."""
    script_dir = Path(__file__).resolve().parent
    ignss_home = script_dir.parent  # i-GNSS directory

    if str(ignss_home) not in sys.path:
        sys.path.insert(0, str(ignss_home))


def parse_date(date_str: str) -> tuple[int, int, int]:
    """Parse date string to (year, month, day) or (year, doy, 0).

    Supports formats:
        - YYYY-MM-DD
        - YYYY/DOY
        - YYYYDOY

    Args:
        date_str: Date string

    Returns:
        Tuple of (year, month_or_doy, day_or_zero)

    Raises:
        ValueError: If date format is invalid
    """
    date_str = date_str.strip()

    # YYYY-MM-DD
    if "-" in date_str:
        parts = date_str.split("-")
        if len(parts) == 3:
            return int(parts[0]), int(parts[1]), int(parts[2])

    # YYYY/DOY
    if "/" in date_str:
        parts = date_str.split("/")
        if len(parts) == 2:
            return int(parts[0]), int(parts[1]), 0

    # YYYYDOY (7 digits)
    if len(date_str) == 7 and date_str.isdigit():
        return int(date_str[:4]), int(date_str[4:]), 0

    raise ValueError(
        f"Invalid date format: {date_str}. Use YYYY-MM-DD or YYYY/DOY"
    )


def get_args() -> argparse.Namespace:
    """Parse command line arguments.

    Returns:
        Parsed arguments
    """
    parser = argparse.ArgumentParser(
        description="Hourly GNSS Processing - CRON Entry Point",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Run in cron mode (auto-detect date/hour with 3-hour latency)
    python cron_hourly.py

    # Specific date and hour
    python cron_hourly.py --date 2024-09-16 --hour 12
    python cron_hourly.py --date 2024/260 --hour 0

    # Multiple hours
    python cron_hourly.py --date 2024-09-16 --start-hour 0 --end-hour 23

    # Dry run (no actual processing)
    python cron_hourly.py --dry-run --verbose

    # Skip product download (data already present)
    python cron_hourly.py --skip-products
        """
    )

    # Date/time options
    time_group = parser.add_argument_group("Date/Time Options")
    time_group.add_argument(
        "--date", "-d",
        type=str,
        default=None,
        help="Processing date (YYYY-MM-DD or YYYY/DOY). Default: auto from latency"
    )
    time_group.add_argument(
        "--hour", "-H",
        type=int,
        default=None,
        help="Processing hour (0-23). Default: auto from latency"
    )
    time_group.add_argument(
        "--start-hour",
        type=int,
        default=None,
        help="Start hour for range processing (0-23)"
    )
    time_group.add_argument(
        "--end-hour",
        type=int,
        default=None,
        help="End hour for range processing (0-23)"
    )
    time_group.add_argument(
        "--latency", "-l",
        type=int,
        default=3,
        help="Latency in hours for cron mode (default: 3)"
    )

    # Processing options
    proc_group = parser.add_argument_group("Processing Options")
    proc_group.add_argument(
        "--processing-type", "-t",
        type=str,
        choices=["nrddp_tro", "ppp_daily"],
        default="nrddp_tro",
        help="Processing type (default: nrddp_tro)"
    )
    proc_group.add_argument(
        "--skip-products",
        action="store_true",
        help="Skip product download (use existing)"
    )
    proc_group.add_argument(
        "--skip-data",
        action="store_true",
        help="Skip station data download"
    )
    proc_group.add_argument(
        "--skip-iwv",
        action="store_true",
        help="Skip ZTD to IWV conversion"
    )
    proc_group.add_argument(
        "--skip-dcm",
        action="store_true",
        help="Skip DCM (archive) step"
    )
    proc_group.add_argument(
        "--no-db",
        action="store_true",
        help="Disable database tracking"
    )

    # Station options
    station_group = parser.add_argument_group("Station Options")
    station_group.add_argument(
        "--stations", "-s",
        type=str,
        default=None,
        help="Comma-separated station list (default: all NRT stations)"
    )
    station_group.add_argument(
        "--exclude", "-x",
        type=str,
        default=None,
        help="Comma-separated stations to exclude"
    )
    station_group.add_argument(
        "--nrt-only",
        action="store_true",
        default=True,
        help="Only process NRT-capable stations (default: True)"
    )

    # Path options
    path_group = parser.add_argument_group("Path Options")
    path_group.add_argument(
        "--config", "-c",
        type=str,
        default=None,
        help="Path to configuration file"
    )
    path_group.add_argument(
        "--data-root",
        type=str,
        default=None,
        help="Data root directory (default: $IGNSS_DATA or /home/nrt105/data54)"
    )
    path_group.add_argument(
        "--ignss-dir",
        type=str,
        default=None,
        help="i-GNSS installation directory (default: $IGNSS_HOME)"
    )

    # Output options
    output_group = parser.add_argument_group("Output Options")
    output_group.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable verbose output"
    )
    output_group.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug mode"
    )
    output_group.add_argument(
        "--dry-run", "-n",
        action="store_true",
        help="Dry run - show what would be done without executing"
    )
    output_group.add_argument(
        "--quiet", "-q",
        action="store_true",
        help="Quiet mode - only show errors"
    )
    output_group.add_argument(
        "--log-file",
        type=str,
        default=None,
        help="Log output to file"
    )

    # Notification options
    notify_group = parser.add_argument_group("Notification Options")
    notify_group.add_argument(
        "--email",
        type=str,
        default=None,
        help="Email address for notifications"
    )
    notify_group.add_argument(
        "--email-on-error",
        action="store_true",
        help="Only send email on errors"
    )

    return parser.parse_args()


def configure_logging(args: argparse.Namespace) -> Any:
    """Configure logging based on arguments.

    Args:
        args: Parsed arguments

    Returns:
        Logger instance
    """
    from pygnss_rt.utils import setup_logging, get_logger
    from pathlib import Path

    level = "DEBUG" if args.debug else ("WARNING" if args.quiet else "INFO")

    # Determine log directory from log_file path
    log_dir = None
    if args.log_file:
        log_path = Path(args.log_file)
        log_dir = log_path.parent

    setup_logging(
        level=level,
        log_dir=log_dir,
        log_to_file=bool(args.log_file),
        log_to_console=not args.quiet,
    )

    return get_logger("cron_hourly")


def get_processing_time(args: argparse.Namespace) -> tuple[int, int, int, int, int]:
    """Determine processing date/hour from arguments.

    In cron mode (no date specified), calculates the processing time
    by subtracting latency hours from current UTC time.

    Args:
        args: Parsed arguments

    Returns:
        Tuple of (year, month, day, start_hour, end_hour)
        If DOY format used, returns (year, 0, doy, start_hour, end_hour)
    """
    if args.date:
        # Date explicitly specified
        year, month_or_doy, day = parse_date(args.date)

        if day == 0:
            # DOY format - convert to month/day
            from pygnss_rt.utils.dates import GNSSDate
            gnss_date = GNSSDate.from_doy(year, month_or_doy)
            year = gnss_date.year
            month = gnss_date.month
            day = gnss_date.day
        else:
            month = month_or_doy

        # Determine hour range
        if args.hour is not None:
            start_hour = end_hour = args.hour
        elif args.start_hour is not None and args.end_hour is not None:
            start_hour = args.start_hour
            end_hour = args.end_hour
        elif args.start_hour is not None:
            start_hour = args.start_hour
            end_hour = 23
        elif args.end_hour is not None:
            start_hour = 0
            end_hour = args.end_hour
        else:
            # Default to full day
            start_hour = 0
            end_hour = 23

    else:
        # Cron mode - calculate from current time and latency
        now = datetime.now(timezone.utc)
        proc_time = now - timedelta(hours=args.latency)

        year = proc_time.year
        month = proc_time.month
        day = proc_time.day
        start_hour = end_hour = proc_time.hour

    return year, month, day, start_hour, end_hour


def run_nrddp_tro(args: argparse.Namespace, logger: Any) -> bool:
    """Run NRDDP TRO processing.

    Args:
        args: Parsed arguments
        logger: Logger instance

    Returns:
        True if processing succeeded
    """
    from pygnss_rt.processing import (
        NRDDPTROProcessor,
        NRDDPTROArgs,
        NRDDPTROConfig,
        create_nrddp_tro_config,
    )
    from pygnss_rt.processing.station_merger import NRDDP_STATION_SOURCES
    from pygnss_rt.utils.dates import GNSSDate

    # Get processing time
    year, month, day, start_hour, end_hour = get_processing_time(args)

    # Create GNSSDate
    proc_date = GNSSDate(year, month, day)

    logger.info(f"Processing: {proc_date.year}/{proc_date.doy:03d} hours {start_hour}-{end_hour}")

    # Create config
    data_root = args.data_root or os.environ.get("IGNSS_DATA", "/home/nrt105/data54")
    ignss_dir = args.ignss_dir or os.environ.get(
        "IGNSS_HOME", "/home/ahunegnaw/Python_IGNSS/i-GNSS"
    )
    gpsuser_dir = os.environ.get("GPSUSER_DIR", "/home/ahunegnaw/GPSUSER54_LANT")

    config = create_nrddp_tro_config(
        data_root=data_root,
        ignss_dir=ignss_dir,
        gpsuser_dir=gpsuser_dir,
    )

    # Parse exclude stations
    exclude_stations = []
    if args.exclude:
        exclude_stations = [s.strip().upper() for s in args.exclude.split(",")]

    # Create processor arguments
    proc_args = NRDDPTROArgs(
        start_date=proc_date,
        end_date=proc_date,
        start_hour=start_hour,
        end_hour=end_hour,
        cron_mode=False,  # We've already calculated the date/hour
        latency_hours=args.latency,
        nrt_only=args.nrt_only,
        exclude_stations=exclude_stations,
        skip_products=args.skip_products,
        skip_data=args.skip_data,
        skip_iwv=args.skip_iwv,
        skip_dcm=args.skip_dcm,
        dry_run=args.dry_run,
        verbose=args.verbose,
    )

    # Create and run processor
    processor = NRDDPTROProcessor(config=config)
    results = processor.process(proc_args)

    # Check results
    success_count = sum(1 for r in results if r.success)
    total_count = len(results)

    if success_count == total_count:
        logger.info(f"All {total_count} hour(s) processed successfully")
        return True
    elif success_count > 0:
        logger.warning(f"{success_count}/{total_count} hours processed successfully")
        return True  # Partial success
    else:
        logger.error(f"All {total_count} hours failed")
        for r in results:
            if not r.success:
                logger.error(f"  {r.session_name}: {r.error_message}")
        return False


def send_notification(
    args: argparse.Namespace,
    success: bool,
    message: str,
    logger: Any,
) -> None:
    """Send notification email if configured.

    Args:
        args: Parsed arguments
        success: Processing success status
        message: Notification message
        logger: Logger instance
    """
    if not args.email:
        return

    if args.email_on_error and success:
        return

    try:
        from pygnss_rt.utils.monitoring import AlertManager, AlertLevel

        alert_manager = AlertManager()

        level = AlertLevel.INFO if success else AlertLevel.ERROR

        alert_manager.send_alert(
            level=level,
            message=message,
            recipients=[args.email],
        )

        logger.debug(f"Notification sent to {args.email}")
    except Exception as e:
        logger.warning(f"Failed to send notification: {e}")


def print_banner(dry_run: bool = False) -> None:
    """Print processing banner.

    Args:
        dry_run: Whether this is a dry run
    """
    from datetime import datetime

    now = datetime.now(timezone.utc)

    print()
    print("=" * 70)
    print("  PyGNSS-RT Hourly Processing")
    print(f"  Started: {now.strftime('%Y-%m-%d %H:%M:%S')} UTC")
    if dry_run:
        print("  Mode: DRY RUN (no actual processing)")
    print("=" * 70)
    print()


def print_summary(results: list, start_time: datetime, dry_run: bool = False) -> None:
    """Print processing summary.

    Args:
        results: List of processing results
        start_time: Processing start time
        dry_run: Whether this was a dry run
    """
    end_time = datetime.now(timezone.utc)
    duration = (end_time - start_time).total_seconds()

    success_count = sum(1 for r in results if getattr(r, 'success', False))
    total_count = len(results) if results else 0

    print()
    print("=" * 70)
    print("  Processing Summary")
    print("-" * 70)
    print(f"  Completed: {end_time.strftime('%Y-%m-%d %H:%M:%S')} UTC")
    print(f"  Duration:  {duration:.1f} seconds")
    if dry_run:
        print("  Mode:      DRY RUN")
    if total_count > 0:
        print(f"  Results:   {success_count}/{total_count} successful")
    print("=" * 70)
    print()


def main() -> int:
    """Main entry point.

    Returns:
        Exit code (0 for success, non-zero for failure)
    """
    # Setup Python path before imports
    setup_paths()

    # Check for environment dry-run override
    if os.environ.get("IGNSS_DRY_RUN") == "1":
        sys.argv.append("--dry-run")

    # Parse arguments
    args = get_args()

    # Print banner (unless quiet)
    if not args.quiet:
        print_banner(dry_run=args.dry_run)

    # Configure logging
    try:
        logger = configure_logging(args)
    except ImportError:
        # Fallback if pygnss_rt not fully available
        import logging
        logging.basicConfig(
            level=logging.DEBUG if args.debug else logging.INFO,
            format="%(asctime)s %(levelname)s: %(message)s",
        )
        logger = logging.getLogger("cron_hourly")

    start_time = datetime.now(timezone.utc)
    results = []
    success = False
    message = ""

    try:
        # Log start
        logger.info("Hourly processing started")

        if args.dry_run:
            logger.info("[DRY RUN MODE - no actual processing]")

        # Route to appropriate processor
        if args.processing_type == "nrddp_tro":
            success = run_nrddp_tro(args, logger)
        elif args.processing_type == "ppp_daily":
            logger.error("PPP daily processing not yet implemented in hourly cron")
            success = False
        else:
            logger.error(f"Unknown processing type: {args.processing_type}")
            success = False

        # Build summary message
        if success:
            message = f"Hourly processing completed successfully at {start_time.strftime('%Y-%m-%d %H:%M')}"
        else:
            message = f"Hourly processing failed at {start_time.strftime('%Y-%m-%d %H:%M')}"

    except KeyboardInterrupt:
        logger.warning("Processing interrupted by user")
        success = False
        message = "Processing interrupted"

    except Exception as e:
        logger.exception(f"Fatal error during processing: {e}")
        success = False
        message = f"Fatal error: {e}"

    finally:
        # Print summary
        if not args.quiet:
            print_summary(results, start_time, dry_run=args.dry_run)

        # Send notification
        send_notification(args, success, message, logger)

        # Log completion
        duration = (datetime.now(timezone.utc) - start_time).total_seconds()
        if success:
            logger.info(f"Processing completed in {duration:.1f}s")
        else:
            logger.error(f"Processing failed after {duration:.1f}s")

    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
