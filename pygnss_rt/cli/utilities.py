"""
Utility commands for PyGNSS-RT CLI.

Commands: info, list-networks, convert-date, alerts, test-email, met-maintain, ztd2iwv
"""

from __future__ import annotations

import sys
from pathlib import Path

import click

from . import cli
from ._helpers import parse_date as _parse_date


@cli.command("info")
@click.pass_context
def info(ctx: click.Context) -> None:
    """Show PyGNSS-RT system information.

    Displays version, configuration, and environment information.
    """
    import platform
    from pygnss_rt import __version__
    from pygnss_rt.utils.dates import GNSSDate

    now = GNSSDate.now()

    click.echo("PyGNSS-RT System Information")
    click.echo("=" * 50)
    click.echo()
    click.echo(f"Version: {__version__}")
    click.echo(f"Python: {platform.python_version()}")
    click.echo(f"Platform: {platform.platform()}")
    click.echo()
    click.echo("Current Time:")
    click.echo(f"  UTC: {now.datetime.strftime('%Y-%m-%d %H:%M:%S')}")
    click.echo(f"  Year/DOY: {now.year}/{now.doy:03d}")
    click.echo(f"  GPS Week: {now.gps_week}")
    click.echo(f"  MJD: {now.mjd:.3f}")
    click.echo()

    # Check for Bernese installation
    import os
    bsw_path = os.environ.get("C", "")
    if bsw_path:
        click.echo(f"Bernese Installation: {bsw_path}")
    else:
        click.echo("Bernese Installation: Not detected ($C not set)")

    # Check database
    from pygnss_rt.core.config import load_config
    config_path = ctx.obj.get("config")
    config = load_config(config_path) if config_path else {}
    db_path = Path(config.get("database", {}).get("path", "data/pygnss_rt.duckdb"))
    click.echo()
    click.echo(f"Database: {db_path}")
    click.echo(f"  Exists: {db_path.exists()}")
    if db_path.exists():
        size_mb = db_path.stat().st_size / (1024 * 1024)
        click.echo(f"  Size: {size_mb:.1f} MB")


@cli.command("list-networks")
def list_networks_cmd() -> None:
    """List available network profiles for daily PPP processing.

    Shows all configured GNSS station networks with their descriptions
    and alignment requirements.
    """
    from pygnss_rt.processing import list_networks

    click.echo("Available Networks for Daily PPP Processing")
    click.echo("=" * 60)
    click.echo()
    click.echo(f"{'ID':<4} {'Description':<45} {'Alignment':<10}")
    click.echo("-" * 60)

    for net in list_networks():
        click.echo(
            f"{net['id']:<4} {net['description']:<45} {net['requires_alignment']:<10}"
        )

    click.echo()
    click.echo("Notes:")
    click.echo("  - Networks with 'Yes' in Alignment require IGS (IG) to be processed first")
    click.echo("  - Use 'pygnss-rt daily-ppp ALL' to process all networks in correct order")


@cli.command("convert-date")
@click.argument("date_input")
@click.pass_context
def convert_date(ctx: click.Context, date_input: str) -> None:
    """Convert between date formats.

    Accepts various date formats and shows conversions:
    - YYYY-MM-DD (calendar date)
    - YYYY/DOY (year and day of year)
    - MJD (Modified Julian Date)
    - GPS week and day (WWWWD)

    Examples:

    \b
        # From calendar date
        pygnss-rt convert-date 2024-07-01

        # From year/DOY
        pygnss-rt convert-date 2024/183

        # From MJD
        pygnss-rt convert-date 60491.5

        # From GPS week/day
        pygnss-rt convert-date 23221
    """
    from pygnss_rt.utils.dates import GNSSDate

    try:
        # Try different formats
        gnss_date = None

        # Try YYYY-MM-DD
        if "-" in date_input:
            gnss_date = _parse_date(date_input)

        # Try YYYY/DOY
        elif "/" in date_input:
            gnss_date = _parse_date(date_input)

        # Try MJD (decimal number)
        elif "." in date_input:
            mjd = float(date_input)
            gnss_date = GNSSDate.from_mjd(mjd)

        # Try GPS week/day (5 digits) or YYYYDOY (7 digits)
        elif date_input.isdigit():
            if len(date_input) == 5:
                # GPS week/day
                gps_week = int(date_input[:4])
                day_of_week = int(date_input[4])
                gnss_date = GNSSDate.from_gps_week(gps_week, day_of_week)
            elif len(date_input) == 7:
                # YYYYDOY
                gnss_date = _parse_date(date_input)
            else:
                raise ValueError(f"Unknown format: {date_input}")

        if gnss_date is None:
            raise ValueError(f"Could not parse: {date_input}")

        click.echo("Date Conversions")
        click.echo("=" * 40)
        click.echo(f"Input: {date_input}")
        click.echo()
        click.echo(f"Calendar: {gnss_date.year}-{gnss_date.month:02d}-{gnss_date.day:02d}")
        click.echo(f"Year/DOY: {gnss_date.year}/{gnss_date.doy:03d}")
        click.echo(f"MJD: {gnss_date.mjd:.6f}")
        click.echo(f"GPS Week: {gnss_date.gps_week}")
        click.echo(f"Day of Week: {gnss_date.day_of_week}")
        click.echo(f"GPS Week/Day: {gnss_date.gps_week:04d}{gnss_date.day_of_week}")

    except Exception as e:
        click.echo(f"Error: {e}")
        sys.exit(1)


@cli.command("alerts")
@click.option(
    "--level", "-l",
    type=click.Choice(["FATAL", "CRITICAL", "WARNING", "INFO", "all"]),
    default="all",
    help="Filter by alert level",
)
@click.option(
    "--campaign", "-c",
    type=str,
    help="Filter by campaign",
)
@click.option(
    "--limit", "-n",
    type=int,
    default=20,
    help="Number of recent alerts to show",
)
@click.option(
    "--log-file",
    type=click.Path(path_type=Path),
    help="Alert log file to read",
)
@click.pass_context
def alerts(
    ctx: click.Context,
    level: str,
    campaign: str | None,
    limit: int,
    log_file: Path | None,
) -> None:
    """Show recent processing alerts.

    Displays alerts from the monitoring system, useful for troubleshooting
    processing failures.

    Examples:

    \b
        # Show recent alerts
        pygnss-rt alerts

        # Show only fatal/critical alerts
        pygnss-rt alerts -l FATAL

        # Show alerts for specific campaign
        pygnss-rt alerts -c IG2024189
    """
    from pygnss_rt.utils.monitoring import AlertLevel as AL, ALERT_CODES

    if log_file and log_file.exists():
        # Read from log file
        click.echo(f"Reading alerts from: {log_file}")
        click.echo()

        with open(log_file, "r") as f:
            lines = f.readlines()[-limit:]

        for line in lines:
            line = line.strip()
            if not line:
                continue

            # Filter by level if specified
            if level != "all" and level not in line:
                continue

            # Filter by campaign if specified
            if campaign and campaign not in line:
                continue

            click.echo(line)

    else:
        # Show available alert codes
        click.echo("Alert Codes Reference")
        click.echo("=" * 70)
        click.echo()
        click.echo(f"{'Code':<6} {'Type':<12} {'Level':<10} Description")
        click.echo("-" * 70)

        for code, info in sorted(ALERT_CODES.items()):
            if level != "all" and info["level"].value != level:
                continue
            click.echo(
                f"{code:<6} {info['type'].value:<12} {info['level'].value:<10} "
                f"{info['description']}"
            )

        click.echo()
        click.echo("Tip: Use --log-file to read alerts from an alert log")


@cli.command("test-email")
@click.option(
    "--to", "-t",
    type=str,
    required=True,
    help="Recipient email address",
)
@click.option(
    "--smtp-server",
    type=str,
    help="SMTP server (default: from config)",
)
@click.option(
    "--from-addr",
    type=str,
    default="pygnss-rt@localhost",
    help="From address",
)
@click.pass_context
def test_email(
    ctx: click.Context,
    to: str,
    smtp_server: str | None,
    from_addr: str,
) -> None:
    """Test email notification configuration.

    Sends a test email to verify the email alerting system is working.

    Examples:

    \b
        # Send test email
        pygnss-rt test-email -t admin@example.com

        # Specify SMTP server
        pygnss-rt test-email -t admin@example.com --smtp-server smtp.example.com
    """
    from pygnss_rt.utils.monitoring import AlertManager, EmailConfig

    if smtp_server is None:
        from pygnss_rt.core.config import load_config
        config_path = ctx.obj.get("config")
        config = load_config(config_path) if config_path else {}
        smtp_server = config.get("email", {}).get("smtp_server", "localhost")

    email_config = EmailConfig(
        smtp_server=smtp_server,
        from_address=from_addr,
        default_recipients=[to],
    )

    alerts = AlertManager(email_config=email_config)

    click.echo(f"Sending test email to {to} via {smtp_server}...")

    success = alerts.send_email_alert(
        subject="Test Alert from PyGNSS-RT",
        body="""
This is a test email from PyGNSS-RT.

If you received this message, your email alerting configuration is working correctly.

---
PyGNSS-RT Monitoring System
""",
        recipients=[to],
    )

    if success:
        click.echo("Test email sent successfully!")
    else:
        click.echo("Failed to send test email. Check SMTP configuration.")
        sys.exit(1)


@cli.command("met-maintain")
@click.option(
    "--late-day",
    type=int,
    default=3,
    help="Days threshold for 'too late' files",
)
@click.option(
    "--late-hour",
    type=int,
    default=0,
    help="Hours threshold for 'too late' files",
)
@click.option(
    "--download/--no-download",
    default=True,
    help="Download waiting MET files",
)
@click.option(
    "--met-dir",
    type=click.Path(path_type=Path),
    help="MET data directory (overrides config)",
)
@click.option(
    "--dry-run",
    is_flag=True,
    help="Show what would be done without executing",
)
@click.pass_context
def met_maintain(
    ctx: click.Context,
    late_day: int,
    late_hour: int,
    download: bool,
    met_dir: Path | None,
    dry_run: bool,
) -> None:
    """Maintain meteorological data tracking.

    Manages hourly MET file tracking for ZTD to IWV conversion:
    - Creates/updates hourly entries in the database
    - Fills gaps from interrupted cron jobs
    - Marks old files as 'Too Late'
    - Downloads waiting MET files

    This replaces the Perl call_MET_maintain.pl script.

    Examples:

        # Run full maintenance with defaults
        pygnss-rt met-maintain

        # Dry run to see what would happen
        pygnss-rt met-maintain --dry-run

        # Run without downloading files
        pygnss-rt met-maintain --no-download

        # Custom latency threshold (5 days, 12 hours)
        pygnss-rt met-maintain --late-day 5 --late-hour 12
    """
    from pygnss_rt.core.config import load_config
    from pygnss_rt.database.connection import init_db
    from pygnss_rt.database.met import MetManager
    from pygnss_rt.utils.dates import GNSSDate

    config_path = ctx.obj.get("config")
    verbose = ctx.obj.get("verbose", False)

    # Load configuration
    config = load_config(config_path) if config_path else {}

    # Get database path from config or use default
    db_path = Path(config.get("database", {}).get("path", "data/pygnss_rt.duckdb"))

    # Get MET directory
    if met_dir is None:
        met_dir = Path(config.get("data", {}).get("met_dir", "data/met"))

    click.echo("MET Data Maintenance")
    click.echo("=" * 40)
    click.echo(f"Database: {db_path}")
    click.echo(f"MET Dir: {met_dir}")
    click.echo(f"Late threshold: {late_day} days, {late_hour} hours")
    click.echo()

    if dry_run:
        click.echo("[DRY RUN MODE]")
        click.echo()

    # Initialize database
    db = init_db(db_path, create_schema=True)
    met = MetManager(db)

    # Ensure MET table exists
    met.ensure_table()
    if verbose:
        click.echo("MET table verified")

    # Get current time for reference
    now = GNSSDate.now()
    click.echo(f"Reference time: {now}")
    click.echo()

    # Step 1: Add current hour entry
    if not dry_run:
        added = met.maintain(now)
        if added:
            click.echo(f"Added new entry for {now.year}/{now.doy:03d} hour {now.hour_alpha}")
        elif verbose:
            click.echo("Current hour entry already exists")
    else:
        click.echo(f"Would add entry for {now.year}/{now.doy:03d} hour {now.hour_alpha}")

    # Step 2: Fill any gaps from interruptions
    if not dry_run:
        filled = met.fill_gap(late_day=late_day, late_hour=late_hour + 1, reference_date=now)
        if filled:
            click.echo(f"Filled {filled} gap entries")
    else:
        click.echo("Would fill gap entries if any exist")

    # Step 3: Mark old files as too late
    if not dry_run:
        marked = met.set_too_late_files(late_day=late_day, late_hour=late_hour, reference_date=now)
        if marked:
            click.echo(f"Marked {marked} files as 'Too Late'")
    else:
        click.echo("Would mark old waiting files as 'Too Late'")

    # Step 4: Get and download waiting files
    waiting = met.get_waiting_list()
    click.echo(f"\nWaiting files: {len(waiting)}")

    if download and waiting:
        if dry_run:
            click.echo("\nWould download:")
            for item in waiting[:10]:  # Show first 10
                click.echo(f"  - {item['year']}/{item['doy']:03d} hour {item['hour']}")
            if len(waiting) > 10:
                click.echo(f"  ... and {len(waiting) - 10} more")
        else:
            # Download MET files
            click.echo("\nDownloading MET files...")
            downloaded = _download_met_files(waiting, met_dir, config, verbose)

            if downloaded:
                # Update status for downloaded files
                updated = met.update_status(downloaded, late_day, late_hour, now)
                click.echo(f"Downloaded {len(downloaded)} files, updated {updated} entries")

    # Show status summary
    click.echo("\nStatus Summary:")
    summary = met.get_status_summary()
    for status, count in sorted(summary.items()):
        click.echo(f"  {status}: {count}")

    db.close()
    click.echo("\nMET maintenance complete")


def _download_met_files(
    waiting: list[dict],
    met_dir: Path,
    config: dict,
    verbose: bool = False,
) -> list[dict]:
    """Download waiting MET files via the MeteorologicalDataDownloader.

    Args:
        waiting: List of waiting file entries from MetManager.get_waiting_list()
        met_dir: Target directory for downloads
        config: Configuration dict
        verbose: Show verbose output

    Returns:
        List of successfully downloaded file entries
    """
    import click
    from pygnss_rt.data_access.met_downloader import MeteorologicalDataDownloader

    met_dir.mkdir(parents=True, exist_ok=True)

    downloader = MeteorologicalDataDownloader(
        download_dir=met_dir,
        verbose=verbose,
    )

    downloaded = []

    try:
        for item in waiting:
            try:
                result = downloader.download_hourly_met(
                    year=int(item["year"]),
                    doy=item["doy"],
                    hour=item["hour_int"],
                )

                if result.success:
                    downloaded.append(item)
                    if verbose:
                        click.echo(f"  Downloaded: {item.get('filename', '')} from {result.provider_used}")
                elif verbose:
                    click.echo(f"  Failed: {item.get('filename', '')} - {result.error}")
            except Exception as e:
                if verbose:
                    click.echo(f"  Error downloading {item.get('filename', '')}: {e}")
    finally:
        downloader.close()

    return downloaded


@cli.command()
@click.argument("ztd_file", type=click.Path(exists=True, path_type=Path))
@click.option(
    "--output", "-o",
    type=click.Path(path_type=Path),
    help="Output file path",
)
@click.option(
    "--format", "-f",
    type=click.Choice(["cost716", "csv"]),
    default="cost716",
    help="Output format",
)
@click.option(
    "--station-xml",
    type=click.Path(exists=True, path_type=Path),
    help="Station XML file for coordinates",
)
@click.pass_context
def ztd2iwv(
    ctx: click.Context,
    ztd_file: Path,
    output: Path | None,
    format: str,
    station_xml: Path | None,
) -> None:
    """Convert ZTD to IWV.

    Reads ZTD values from a TRP file and converts to Integrated Water Vapor.

    Examples:

        # Convert to COST-716 format
        pygnss-rt ztd2iwv output.TRP -o output.cost716

        # Convert to CSV
        pygnss-rt ztd2iwv output.TRP -f csv -o output.csv
    """
    from pygnss_rt.atmosphere.ztd2iwv import ZTD2IWV, read_ztd_file
    from pygnss_rt.stations.station import StationManager
    from pygnss_rt.utils.dates import GNSSDate

    # Load stations if provided (supports XML or YAML)
    station_manager = None
    if station_xml:
        station_manager = StationManager()
        station_manager.load(station_xml)  # Auto-detects XML or YAML

    # Read ZTD data
    ztd_data = read_ztd_file(ztd_file)
    click.echo(f"Read {len(ztd_data)} ZTD records from {ztd_file}")

    # Convert
    converter = ZTD2IWV(tm_method="bevis")

    for record in ztd_data:
        # Get station coordinates
        lat, lon, height = 0.0, 0.0, 0.0
        if station_manager:
            station = station_manager.get_station(record["station"])
            if station and station.latitude:
                lat = station.latitude
                lon = station.longitude or 0.0
                height = station.height or 0.0

        if lat == 0.0:
            click.echo(f"Warning: No coordinates for station {record['station']}")
            continue

        date = GNSSDate.from_mjd(record["mjd"])
        converter.process(
            station_id=record["station"],
            ztd=record["ztd"],
            ztd_sigma=record.get("ztd_sigma", 0.001),
            timestamp=date.datetime,
            latitude=lat,
            longitude=lon,
            height=height,
        )

    # Write output
    if output is None:
        output = ztd_file.with_suffix(f".{format}")

    if format == "cost716":
        converter.write_cost716_file(output)
    else:
        converter.write_csv(output)

    click.echo(f"Wrote {len(converter.results)} records to {output}")
