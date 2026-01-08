"""
Command-line interface for PyGNSS-RT.

Provides a modern CLI using Click for running GNSS processing,
data downloads, and other operations.
"""

from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path

import click

from pygnss_rt import __version__


@click.group()
@click.version_option(version=__version__, prog_name="PyGNSS-RT")
@click.option(
    "--config", "-c",
    type=click.Path(exists=True, path_type=Path),
    help="Path to configuration file",
)
@click.option(
    "--verbose", "-v",
    is_flag=True,
    help="Enable verbose output",
)
@click.option(
    "--debug",
    is_flag=True,
    help="Enable debug mode",
)
@click.pass_context
def cli(ctx: click.Context, config: Path | None, verbose: bool, debug: bool) -> None:
    """PyGNSS-RT: Python GNSS Real-Time Processing System

    A modern Python framework for real-time GNSS data processing and analysis,
    integrating with Bernese GNSS Software for PPP and tropospheric
    parameter estimation.
    """
    ctx.ensure_object(dict)
    ctx.obj["config"] = config
    ctx.obj["verbose"] = verbose
    ctx.obj["debug"] = debug

    if debug:
        import logging
        logging.basicConfig(level=logging.DEBUG)


@cli.command()
@click.option(
    "--start-date", "-s",
    type=str,
    help="Start date (YYYY-MM-DD or YYYY/DOY)",
)
@click.option(
    "--end-date", "-e",
    type=str,
    help="End date (YYYY-MM-DD or YYYY/DOY)",
)
@click.option(
    "--proc-type", "-t",
    type=click.Choice(["daily", "hourly", "subhourly"]),
    default="hourly",
    help="Processing type",
)
@click.option(
    "--stations", "-S",
    type=str,
    help="Comma-separated list of stations",
)
@click.option(
    "--network", "-n",
    type=str,
    help="Network filter (e.g., IGS20)",
)
@click.option(
    "--exclude", "-x",
    type=str,
    help="Comma-separated stations to exclude",
)
@click.option(
    "--cron",
    is_flag=True,
    help="Run in CRON mode (auto-detect dates)",
)
@click.option(
    "--latency",
    type=int,
    default=3,
    help="Latency in hours for CRON mode",
)
@click.option(
    "--no-iwv",
    is_flag=True,
    help="Skip IWV generation",
)
@click.option(
    "--dry-run",
    is_flag=True,
    help="Show what would be done without executing",
)
@click.pass_context
def process(
    ctx: click.Context,
    start_date: str | None,
    end_date: str | None,
    proc_type: str,
    stations: str | None,
    network: str | None,
    exclude: str | None,
    cron: bool,
    latency: int,
    no_iwv: bool,
    dry_run: bool,
) -> None:
    """Run GNSS processing.

    Examples:

        # Process hourly data for specific date range
        pygnss-rt process -s 2024-01-01 -e 2024-01-07 -t hourly

        # Run in CRON mode with 3-hour latency
        pygnss-rt process --cron --latency 3

        # Process specific stations
        pygnss-rt process -s 2024-01-01 -e 2024-01-01 -S algo,nrc1,dubo
    """
    from pygnss_rt.core.orchestrator import IGNSS, ProcessingArgs
    from pygnss_rt.utils.dates import GNSSDate

    config_path = ctx.obj.get("config")

    # Parse dates
    start = None
    end = None

    if start_date:
        start = _parse_date(start_date)
    if end_date:
        end = _parse_date(end_date)

    # Parse station lists
    station_list = stations.split(",") if stations else []
    exclude_list = exclude.split(",") if exclude else []

    # Build processing arguments
    args = ProcessingArgs(
        proc_type=proc_type,
        start_date=start,
        end_date=end,
        stations=station_list,
        network=network,
        exclude_stations=exclude_list,
        cron_mode=cron,
        latency_hours=latency,
        generate_iwv=not no_iwv,
    )

    if dry_run:
        click.echo("Dry run mode - would process:")
        click.echo(f"  Type: {proc_type}")
        click.echo(f"  Dates: {start} to {end}")
        click.echo(f"  Stations: {station_list or 'all from network'}")
        click.echo(f"  Network: {network or 'all'}")
        click.echo(f"  CRON mode: {cron}")
        return

    # Run processing
    with IGNSS(config_path=config_path) as processor:
        results = processor.process(args)

    # Report results
    success = sum(1 for r in results if r.success)
    click.echo(f"\nProcessing complete: {success}/{len(results)} epochs successful")

    if success < len(results):
        sys.exit(1)


@cli.command()
@click.option(
    "--product-type", "-p",
    type=click.Choice(["orbit", "erp", "clock", "dcb"]),
    required=True,
    help="Product type to download",
)
@click.option(
    "--provider",
    type=str,
    default="IGS",
    help="Product provider (IGS, CODE, etc.)",
)
@click.option(
    "--tier",
    type=click.Choice(["final", "rapid", "ultra"]),
    default="final",
    help="Product tier",
)
@click.option(
    "--start-date", "-s",
    type=str,
    required=True,
    help="Start date",
)
@click.option(
    "--end-date", "-e",
    type=str,
    help="End date (defaults to start date)",
)
@click.option(
    "--output-dir", "-o",
    type=click.Path(path_type=Path),
    help="Output directory",
)
@click.pass_context
def download(
    ctx: click.Context,
    product_type: str,
    provider: str,
    tier: str,
    start_date: str,
    end_date: str | None,
    output_dir: Path | None,
) -> None:
    """Download GNSS products.

    Examples:

        # Download final orbits for a week
        pygnss-rt download -p orbit --provider IGS --tier final \\
            -s 2024-01-01 -e 2024-01-07

        # Download CODE DCB files
        pygnss-rt download -p dcb --provider CODE -s 2024-01-01
    """
    from pygnss_rt.data_access.downloader import DataDownloader
    from pygnss_rt.database.models import ProductTier, ProductType
    from pygnss_rt.utils.dates import GNSSDate

    start = _parse_date(start_date)
    end = _parse_date(end_date) if end_date else start

    downloader = DataDownloader(
        download_dir=output_dir or Path("downloads")
    )

    pt = ProductType(product_type)
    pt_tier = ProductTier(tier)

    click.echo(f"Downloading {product_type} products from {provider} ({tier})")
    click.echo(f"Date range: {start} to {end}")

    current = start
    success = 0
    total = 0

    with click.progressbar(length=int(end.mjd - start.mjd) + 1) as bar:
        while current.mjd <= end.mjd:
            total += 1
            result = downloader.download_product(pt, provider, pt_tier, current)
            if result.success:
                success += 1
                click.echo(f"\n  Downloaded: {result.local_path}")
            current = current.add_days(1)
            bar.update(1)

    click.echo(f"\nDownloaded {success}/{total} files")
    downloader.close()


@cli.command()
@click.argument("xml_file", type=click.Path(exists=True, path_type=Path))
@click.option(
    "--network", "-n",
    type=str,
    help="Filter by network",
)
@click.option(
    "--nrt-only",
    is_flag=True,
    help="Show only NRT-enabled stations",
)
@click.option(
    "--format", "-f",
    type=click.Choice(["table", "csv", "json"]),
    default="table",
    help="Output format",
)
@click.pass_context
def stations(
    ctx: click.Context,
    xml_file: Path,
    network: str | None,
    nrt_only: bool,
    format: str,
) -> None:
    """List stations from XML configuration.

    Examples:

        # List all IGS20 stations
        pygnss-rt stations info/IGS20rh.xml -n IGS20

        # Export to CSV
        pygnss-rt stations info/IGS20rh.xml -f csv > stations.csv
    """
    from pygnss_rt.stations.station import StationManager

    manager = StationManager()
    manager.load_xml(xml_file)

    station_list = manager.get_stations(
        network=network,
        use_nrt=True if nrt_only else None,
    )

    if format == "table":
        click.echo(f"{'ID':<6} {'Name':<20} {'Network':<10} {'Lat':>8} {'Lon':>9} {'NRT':<4}")
        click.echo("-" * 60)
        for s in sorted(station_list, key=lambda x: x.station_id):
            lat = f"{s.latitude:.3f}" if s.latitude else "N/A"
            lon = f"{s.longitude:.3f}" if s.longitude else "N/A"
            nrt = "Yes" if s.use_nrt else "No"
            click.echo(f"{s.station_id.upper():<6} {(s.name or '')[:20]:<20} {(s.network or ''):<10} {lat:>8} {lon:>9} {nrt:<4}")

    elif format == "csv":
        click.echo("station_id,name,network,latitude,longitude,use_nrt")
        for s in sorted(station_list, key=lambda x: x.station_id):
            click.echo(f"{s.station_id},{s.name or ''},{s.network or ''},{s.latitude or ''},{s.longitude or ''},{s.use_nrt}")

    elif format == "json":
        import json
        data = [s.to_dict() for s in station_list]
        click.echo(json.dumps(data, indent=2))

    click.echo(f"\nTotal: {len(station_list)} stations")


@cli.command()
@click.option(
    "--db-path",
    type=click.Path(path_type=Path),
    default=Path("data/pygnss_rt.duckdb"),
    help="Database path",
)
@click.pass_context
def init(ctx: click.Context, db_path: Path) -> None:
    """Initialize the PyGNSS-RT database.

    Creates the DuckDB database with required schema.
    """
    from pygnss_rt.database.connection import init_db

    click.echo(f"Initializing database at {db_path}")
    db = init_db(db_path, create_schema=True)
    db.close()
    click.echo("Database initialized successfully")


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

    # Load stations if provided
    station_manager = None
    if station_xml:
        station_manager = StationManager()
        station_manager.load_xml(station_xml)

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
    """Download waiting MET files via FTP.

    Args:
        waiting: List of waiting file entries
        met_dir: Target directory for downloads
        config: Configuration dict
        verbose: Show verbose output

    Returns:
        List of successfully downloaded file entries
    """
    import click

    # Ensure met_dir exists
    met_dir.mkdir(parents=True, exist_ok=True)

    downloaded = []

    # TODO: Integrate with FTP module when available
    # For now, this is a placeholder that shows what would be downloaded
    # In the full implementation, this would use pygnss_rt.data_access.ftp

    if verbose:
        click.echo("Note: FTP download integration pending")
        click.echo("Files would be organized as: {met_dir}/{yyyy}/{yydoy}/")

    # Return empty list until FTP integration is complete
    return downloaded


@cli.command("daily-ppp")
@click.argument(
    "network",
    type=click.Choice(["IG", "EU", "GB", "RG", "SS", "ALL"], case_sensitive=False),
)
@click.option(
    "--start-date", "-s",
    type=str,
    help="Start date (YYYY-MM-DD or YYYY/DOY)",
)
@click.option(
    "--end-date", "-e",
    type=str,
    help="End date (defaults to start date)",
)
@click.option(
    "--cron",
    is_flag=True,
    help="Run in CRON mode (auto-detect date with latency)",
)
@click.option(
    "--latency",
    type=int,
    default=21,
    help="Latency in days for CRON mode (default: 21)",
)
@click.option(
    "--stations", "-S",
    type=str,
    help="Comma-separated list of stations (overrides network filter)",
)
@click.option(
    "--exclude", "-x",
    type=str,
    help="Comma-separated stations to exclude",
)
@click.option(
    "--skip-products",
    is_flag=True,
    help="Skip product download (assume already available)",
)
@click.option(
    "--skip-data",
    is_flag=True,
    help="Skip station data download (assume already available)",
)
@click.option(
    "--skip-dcm",
    is_flag=True,
    help="Skip DCM archiving after processing",
)
@click.option(
    "--dry-run",
    is_flag=True,
    help="Show what would be done without executing",
)
@click.pass_context
def daily_ppp(
    ctx: click.Context,
    network: str,
    start_date: str | None,
    end_date: str | None,
    cron: bool,
    latency: int,
    stations: str | None,
    exclude: str | None,
    skip_products: bool,
    skip_data: bool,
    skip_dcm: bool,
    dry_run: bool,
) -> None:
    """Run daily PPP processing for a GNSS network.

    Processes daily GNSS observations using Bernese GNSS Software for
    Precise Point Positioning. Supports 5 networks:

    \b
    - IG: IGS core stations (global reference network)
    - EU: EUREF stations (European reference network)
    - GB: Great Britain stations (OS active, scientific, IGS)
    - RG: RGP France stations (French permanent network)
    - SS: Supersites (Netherlands/European supersites)
    - ALL: Process all networks in order (IG first, then others)

    This replaces the Perl caller scripts:
    iGNSS_D_PPP_AR_*_IGS54_direct_NRT.pl

    Examples:

    \b
        # Process IGS network for a specific date
        pygnss-rt daily-ppp IG -s 2024-07-01

        # Process in CRON mode with 21-day latency
        pygnss-rt daily-ppp EU --cron --latency 21

        # Process date range for Great Britain
        pygnss-rt daily-ppp GB -s 2024-07-01 -e 2024-07-07

        # Dry run to see what would be processed
        pygnss-rt daily-ppp RG --cron --dry-run

        # Process all networks
        pygnss-rt daily-ppp ALL -s 2024-07-01
    """
    from pygnss_rt.processing import (
        DailyPPPProcessor,
        DailyPPPArgs,
        NetworkID,
        list_networks,
    )
    from pygnss_rt.utils.dates import GNSSDate

    config_path = ctx.obj.get("config")
    verbose = ctx.obj.get("verbose", False)

    # Parse dates
    start = None
    end = None

    if start_date:
        start = _parse_date(start_date)
    if end_date:
        end = _parse_date(end_date)
    elif start:
        end = start  # Default end to start if only start provided

    # Parse station lists
    station_list = [s.strip() for s in stations.split(",")] if stations else []
    exclude_list = [s.strip() for s in exclude.split(",")] if exclude else []

    # Determine which networks to process
    if network.upper() == "ALL":
        # Process all networks - IG first (required for alignment)
        network_ids = [NetworkID.IG, NetworkID.EU, NetworkID.GB, NetworkID.RG, NetworkID.SS]
    else:
        network_ids = [NetworkID(network.upper())]

    click.echo("Daily PPP Processing")
    click.echo("=" * 50)

    if dry_run:
        click.echo("[DRY RUN MODE]")
        click.echo()

    # Show network info
    if verbose:
        click.echo("\nAvailable networks:")
        for net in list_networks():
            marker = ">>>" if net["id"] in [n.value for n in network_ids] else "   "
            click.echo(f"  {marker} {net['id']}: {net['description']}")
        click.echo()

    # Show processing parameters
    click.echo(f"Networks: {', '.join(n.value for n in network_ids)}")
    if cron:
        click.echo(f"Mode: CRON (latency: {latency} days)")
    else:
        click.echo(f"Mode: Manual")
        if start:
            click.echo(f"Date range: {start} to {end}")
        else:
            click.echo("Error: Either --cron or --start-date must be specified")
            sys.exit(1)

    if station_list:
        click.echo(f"Stations: {', '.join(station_list)}")
    if exclude_list:
        click.echo(f"Excluded: {', '.join(exclude_list)}")

    click.echo()

    # Initialize processor
    processor = DailyPPPProcessor(config_path=config_path)

    all_results = []

    for net_id in network_ids:
        click.echo(f"\n{'='*50}")
        click.echo(f"Processing network: {net_id.value}")
        click.echo(f"{'='*50}")

        # Build arguments
        args = DailyPPPArgs(
            network_id=net_id,
            start_date=start,
            end_date=end,
            cron_mode=cron,
            latency_days=latency,
            stations=station_list,
            exclude_stations=exclude_list,
            skip_products=skip_products,
            skip_data=skip_data,
            skip_dcm=skip_dcm,
            dry_run=dry_run,
            verbose=verbose,
        )

        # Run processing
        results = processor.process(args)
        all_results.extend(results)

        # Report results for this network
        success = sum(1 for r in results if r.success)
        if results:
            click.echo(f"\n{net_id.value} results: {success}/{len(results)} days successful")
            for r in results:
                status = "OK" if r.success else "FAILED"
                click.echo(f"  {r.date}: {status}")
                if r.error_message:
                    click.echo(f"    Error: {r.error_message}")

    # Final summary
    click.echo("\n" + "=" * 50)
    click.echo("PROCESSING SUMMARY")
    click.echo("=" * 50)

    total_success = sum(1 for r in all_results if r.success)
    click.echo(f"Total: {total_success}/{len(all_results)} days successful")

    if total_success < len(all_results):
        failed = [r for r in all_results if not r.success]
        click.echo(f"\nFailed processing ({len(failed)}):")
        for r in failed:
            click.echo(f"  - {r.network_id} {r.date}: {r.error_message or 'Unknown error'}")
        sys.exit(1)
    else:
        click.echo("\nAll processing completed successfully!")


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


def _parse_date(date_str: str) -> "GNSSDate":
    """Parse date string to GNSSDate.

    Supports formats:
    - YYYY-MM-DD
    - YYYY/DOY
    - YYYYDOY
    """
    from pygnss_rt.utils.dates import GNSSDate

    # Try YYYY-MM-DD
    if "-" in date_str:
        parts = date_str.split("-")
        if len(parts) == 3:
            return GNSSDate(int(parts[0]), int(parts[1]), int(parts[2]))

    # Try YYYY/DOY
    if "/" in date_str:
        parts = date_str.split("/")
        if len(parts) == 2:
            return GNSSDate.from_doy(int(parts[0]), int(parts[1]))

    # Try YYYYDOY
    if len(date_str) == 7 and date_str.isdigit():
        return GNSSDate.from_doy(int(date_str[:4]), int(date_str[4:]))

    raise click.BadParameter(f"Invalid date format: {date_str}")


def main() -> None:
    """Main entry point."""
    cli(obj={})


if __name__ == "__main__":
    main()
