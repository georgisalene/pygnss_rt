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


@cli.command("nrddp-tro")
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
    "--start-hour",
    type=int,
    default=0,
    help="Start hour (0-23, default: 0)",
)
@click.option(
    "--end-hour",
    type=int,
    default=23,
    help="End hour (0-23, default: 23)",
)
@click.option(
    "--cron",
    is_flag=True,
    help="Run in CRON mode (auto-detect date/hour with latency)",
)
@click.option(
    "--latency",
    type=int,
    default=3,
    help="Latency in hours for CRON mode (default: 3)",
)
@click.option(
    "--exclude", "-x",
    type=str,
    help="Comma-separated stations to exclude",
)
@click.option(
    "--skip-products",
    is_flag=True,
    help="Skip product download",
)
@click.option(
    "--skip-data",
    is_flag=True,
    help="Skip station data download",
)
@click.option(
    "--skip-iwv",
    is_flag=True,
    help="Skip ZTD to IWV conversion",
)
@click.option(
    "--skip-dcm",
    is_flag=True,
    help="Skip DCM archiving",
)
@click.option(
    "--dry-run",
    is_flag=True,
    help="Show what would be done without executing",
)
@click.pass_context
def nrddp_tro(
    ctx: click.Context,
    start_date: str | None,
    end_date: str | None,
    start_hour: int,
    end_hour: int,
    cron: bool,
    latency: int,
    exclude: str | None,
    skip_products: bool,
    skip_data: bool,
    skip_iwv: bool,
    skip_dcm: bool,
    dry_run: bool,
) -> None:
    """Run NRDDP TRO (Near Real-Time Tropospheric) processing.

    Hourly processing for tropospheric parameter estimation combining
    stations from all available networks (IGS, EUREF, OS, RGP, etc.).
    Produces ZTD and IWV products.

    This replaces the Perl caller scripts:
    - iGNSS_NRDDP_TRO_54_nrt_direct.pl
    - iGNSS_NRDDP_TRO_BSW54_direct.pl

    Key features:
    - Hourly processing (vs daily for PPP)
    - Dynamic NRT coordinates (updated daily)
    - All-network station merging (10+ networks)
    - NEQ stacking (4-hour accumulation)
    - ZTD to IWV conversion

    Examples:

    \b
        # Process in CRON mode (3-hour latency)
        pygnss-rt nrddp-tro --cron --latency 3

        # Process specific date/hour range
        pygnss-rt nrddp-tro -s 2024-09-16 --start-hour 0 --end-hour 23

        # Process single hour
        pygnss-rt nrddp-tro -s 2024-09-16 --start-hour 12 --end-hour 12

        # Dry run to see what would be processed
        pygnss-rt nrddp-tro --cron --dry-run
    """
    from pygnss_rt.processing import (
        NRDDPTROProcessor,
        NRDDPTROArgs,
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
        end = start

    # Parse exclusion list
    exclude_list = [s.strip() for s in exclude.split(",")] if exclude else []

    click.echo("NRDDP TRO Processing")
    click.echo("=" * 60)

    if dry_run:
        click.echo("[DRY RUN MODE]")
        click.echo()

    # Show parameters
    if cron:
        click.echo(f"Mode: CRON (latency: {latency} hours)")
    else:
        if start:
            click.echo(f"Date range: {start} to {end}")
            click.echo(f"Hour range: {start_hour:02d}:00 - {end_hour:02d}:00 UTC")
        else:
            click.echo("Error: Either --cron or --start-date must be specified")
            sys.exit(1)

    if exclude_list:
        click.echo(f"Excluded: {', '.join(exclude_list)}")

    click.echo()

    # Build arguments
    args = NRDDPTROArgs(
        start_date=start,
        end_date=end,
        start_hour=start_hour,
        end_hour=end_hour,
        cron_mode=cron,
        latency_hours=latency,
        exclude_stations=exclude_list,
        skip_products=skip_products,
        skip_data=skip_data,
        skip_iwv=skip_iwv,
        skip_dcm=skip_dcm,
        dry_run=dry_run,
        verbose=verbose,
    )

    # Initialize processor
    processor = NRDDPTROProcessor(config_path=config_path)

    # Run processing
    results = processor.process(args)

    # Report results
    click.echo("\n" + "=" * 60)
    click.echo("NRDDP TRO SUMMARY")
    click.echo("=" * 60)

    success_count = sum(1 for r in results if r.success)
    click.echo(f"Total: {success_count}/{len(results)} hours successful")

    if success_count < len(results):
        failed = [r for r in results if not r.success]
        click.echo(f"\nFailed hours ({len(failed)}):")
        for r in failed:
            click.echo(f"  - {r.session_name}: {r.error_message or 'Unknown error'}")
        sys.exit(1)
    else:
        click.echo("\nAll processing completed successfully!")

    # Show IWV summary if generated
    total_iwv = sum(r.iwv_records for r in results)
    if total_iwv > 0:
        click.echo(f"\nIWV records generated: {total_iwv}")


# =============================================================================
# Database Maintenance Commands
# =============================================================================

@cli.command("db-maintain")
@click.option(
    "--table",
    type=click.Choice(["hourly", "daily", "orbit", "met", "all"]),
    default="all",
    help="Table to maintain (default: all)",
)
@click.option(
    "--fill-gaps/--no-fill-gaps",
    default=True,
    help="Fill gaps in tracking tables",
)
@click.option(
    "--mark-late/--no-mark-late",
    default=True,
    help="Mark old waiting files as 'Too Late'",
)
@click.option(
    "--late-days",
    type=int,
    default=30,
    help="Days threshold for marking as too late",
)
@click.option(
    "--cleanup/--no-cleanup",
    default=False,
    help="Remove old entries (default: off)",
)
@click.option(
    "--cleanup-days",
    type=int,
    default=180,
    help="Days of data to keep during cleanup",
)
@click.option(
    "--dry-run",
    is_flag=True,
    help="Show what would be done without executing",
)
@click.pass_context
def db_maintain(
    ctx: click.Context,
    table: str,
    fill_gaps: bool,
    mark_late: bool,
    late_days: int,
    cleanup: bool,
    cleanup_days: int,
    dry_run: bool,
) -> None:
    """Maintain database tracking tables.

    Performs maintenance operations on the data tracking tables:
    - Add entries for the current day/hour
    - Fill gaps from interrupted processing
    - Mark old waiting files as 'Too Late'
    - Clean up old entries (optional)

    This replaces the Perl call_*_maintain.pl scripts.

    Examples:

    \b
        # Maintain all tables with defaults
        pygnss-rt db-maintain

        # Maintain only hourly data table
        pygnss-rt db-maintain --table hourly

        # Cleanup old entries (180 days)
        pygnss-rt db-maintain --cleanup --cleanup-days 180

        # Dry run to see what would happen
        pygnss-rt db-maintain --dry-run
    """
    from pygnss_rt.core.config import load_config
    from pygnss_rt.database.connection import init_db
    from pygnss_rt.utils.dates import GNSSDate

    config_path = ctx.obj.get("config")
    verbose = ctx.obj.get("verbose", False)

    config = load_config(config_path) if config_path else {}
    db_path = Path(config.get("database", {}).get("path", "data/pygnss_rt.duckdb"))

    click.echo("Database Maintenance")
    click.echo("=" * 50)
    click.echo(f"Database: {db_path}")
    click.echo(f"Tables: {table}")
    click.echo()

    if dry_run:
        click.echo("[DRY RUN MODE]")
        click.echo()

    db = init_db(db_path, create_schema=True)
    now = GNSSDate.now()

    tables_to_maintain = []
    if table == "all":
        tables_to_maintain = ["hourly", "daily", "orbit", "met"]
    else:
        tables_to_maintain = [table]

    for tbl in tables_to_maintain:
        click.echo(f"\n--- Maintaining {tbl} table ---")

        if tbl == "hourly":
            from pygnss_rt.database.hourly_data import HourlyDataManager
            mgr = HourlyDataManager(db)
        elif tbl == "daily":
            from pygnss_rt.database.daily_data import DailyDataManager
            mgr = DailyDataManager(db)
        elif tbl == "orbit":
            from pygnss_rt.products.orbit import OrbitDataManager
            mgr = OrbitDataManager(db)
        elif tbl == "met":
            from pygnss_rt.database.met import MetManager
            mgr = MetManager(db)
        else:
            continue

        mgr.ensure_table()

        if not dry_run:
            # Add current entry
            added = mgr.maintain(now)
            if added:
                click.echo(f"  Added {added} new entries")

            # Fill gaps
            if fill_gaps:
                filled = mgr.fill_gap(late_day=late_days, reference_date=now)
                if filled:
                    click.echo(f"  Filled {filled} gap entries")

            # Mark too late
            if mark_late:
                marked = mgr.set_too_late_files(late_day=late_days, reference_date=now)
                if marked:
                    click.echo(f"  Marked {marked} entries as 'Too Late'")

            # Cleanup
            if cleanup and hasattr(mgr, 'cleanup_old_entries'):
                removed = mgr.cleanup_old_entries(days_to_keep=cleanup_days)
                if removed:
                    click.echo(f"  Removed {removed} old entries")
        else:
            click.echo("  Would add entries, fill gaps, mark late files")
            if cleanup:
                click.echo(f"  Would remove entries older than {cleanup_days} days")

    db.close()
    click.echo("\nMaintenance complete")


@cli.command("db-status")
@click.option(
    "--table",
    type=click.Choice(["hourly", "daily", "orbit", "met", "all"]),
    default="all",
    help="Table to show status for",
)
@click.option(
    "--format", "-f",
    type=click.Choice(["table", "json"]),
    default="table",
    help="Output format",
)
@click.pass_context
def db_status(
    ctx: click.Context,
    table: str,
    format: str,
) -> None:
    """Show database tracking status.

    Displays statistics about the data tracking tables:
    - Total entries by status (Waiting, Downloaded, Too Late)
    - Date range covered
    - Recent activity

    Examples:

    \b
        # Show status for all tables
        pygnss-rt db-status

        # Show status for hourly table only
        pygnss-rt db-status --table hourly

        # Output as JSON
        pygnss-rt db-status -f json
    """
    from pygnss_rt.core.config import load_config
    from pygnss_rt.database.connection import init_db

    config_path = ctx.obj.get("config")
    config = load_config(config_path) if config_path else {}
    db_path = Path(config.get("database", {}).get("path", "data/pygnss_rt.duckdb"))

    if not db_path.exists():
        click.echo(f"Database not found: {db_path}")
        click.echo("Run 'pygnss-rt init' to create the database")
        sys.exit(1)

    db = init_db(db_path)

    tables_to_check = []
    if table == "all":
        tables_to_check = ["hourly", "daily", "orbit", "met"]
    else:
        tables_to_check = [table]

    results = {}

    for tbl in tables_to_check:
        try:
            if tbl == "hourly":
                from pygnss_rt.database.hourly_data import HourlyDataManager
                mgr = HourlyDataManager(db)
            elif tbl == "daily":
                from pygnss_rt.database.daily_data import DailyDataManager
                mgr = DailyDataManager(db)
            elif tbl == "orbit":
                from pygnss_rt.products.orbit import OrbitDataManager
                mgr = OrbitDataManager(db)
            elif tbl == "met":
                from pygnss_rt.database.met import MetManager
                mgr = MetManager(db)
            else:
                continue

            if not mgr.table_exists():
                results[tbl] = {"exists": False}
                continue

            summary = mgr.get_status_summary() if hasattr(mgr, 'get_status_summary') else {}
            waiting = len(mgr.get_waiting_list()) if hasattr(mgr, 'get_waiting_list') else 0

            results[tbl] = {
                "exists": True,
                "summary": summary,
                "waiting": waiting,
            }

        except Exception as e:
            results[tbl] = {"exists": False, "error": str(e)}

    db.close()

    if format == "json":
        import json
        click.echo(json.dumps(results, indent=2))
    else:
        click.echo("Database Status")
        click.echo("=" * 60)
        click.echo(f"Database: {db_path}")
        click.echo()

        for tbl, info in results.items():
            click.echo(f"\n--- {tbl.upper()} ---")
            if not info.get("exists"):
                if "error" in info:
                    click.echo(f"  Error: {info['error']}")
                else:
                    click.echo("  Table does not exist")
                continue

            if info.get("summary"):
                for status, count in sorted(info["summary"].items()):
                    click.echo(f"  {status}: {count}")
            click.echo(f"  Waiting: {info.get('waiting', 0)}")


# =============================================================================
# Product Download Commands
# =============================================================================

@cli.command("download-products")
@click.option(
    "--date", "-d",
    type=str,
    required=True,
    help="Date to download products for (YYYY-MM-DD or YYYY/DOY)",
)
@click.option(
    "--products", "-p",
    type=str,
    default="orbit,erp,clock",
    help="Comma-separated products: orbit,erp,clock,dcb,ion (default: orbit,erp,clock)",
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
    "--output-dir", "-o",
    type=click.Path(path_type=Path),
    help="Output directory (default: from config)",
)
@click.option(
    "--dry-run",
    is_flag=True,
    help="Show what would be downloaded",
)
@click.pass_context
def download_products(
    ctx: click.Context,
    date: str,
    products: str,
    provider: str,
    tier: str,
    output_dir: Path | None,
    dry_run: bool,
) -> None:
    """Download GNSS products for processing.

    Downloads orbit, ERP, clock, and other products from IGS/CODE data centers.

    Examples:

    \b
        # Download default products (orbit, erp, clock)
        pygnss-rt download-products -d 2024-07-01

        # Download specific products
        pygnss-rt download-products -d 2024-07-01 -p orbit,clock

        # Download from CODE with rapid tier
        pygnss-rt download-products -d 2024-07-01 --provider CODE --tier rapid
    """
    from pygnss_rt.data_access import download_products_for_date
    from pygnss_rt.core.config import load_config
    from pygnss_rt.utils.dates import GNSSDate

    config_path = ctx.obj.get("config")
    config = load_config(config_path) if config_path else {}

    gnss_date = _parse_date(date)
    product_list = [p.strip() for p in products.split(",")]

    if output_dir is None:
        output_dir = Path(config.get("data", {}).get("products_dir", "data/products"))

    click.echo("Product Download")
    click.echo("=" * 50)
    click.echo(f"Date: {gnss_date}")
    click.echo(f"Products: {', '.join(product_list)}")
    click.echo(f"Provider: {provider}")
    click.echo(f"Tier: {tier}")
    click.echo(f"Output: {output_dir}")
    click.echo()

    if dry_run:
        click.echo("[DRY RUN MODE]")
        click.echo("Would download the following products:")
        for prod in product_list:
            click.echo(f"  - {prod} from {provider} ({tier})")
        return

    output_dir.mkdir(parents=True, exist_ok=True)

    results = download_products_for_date(
        date=gnss_date,
        provider=provider,
        products=product_list,
        destination=output_dir,
    )

    success = 0
    for prod, result in results.items():
        if result.success:
            click.echo(f"  {prod}: Downloaded to {result.local_path}")
            success += 1
        else:
            click.echo(f"  {prod}: FAILED - {result.error_message}")

    click.echo(f"\nDownloaded {success}/{len(product_list)} products")


@cli.command("download-gen")
@click.option(
    "--bsw-version",
    type=click.Choice(["52", "54"]),
    default="54",
    help="Bernese version (default: 54)",
)
@click.option(
    "--output-dir", "-o",
    type=click.Path(path_type=Path),
    help="Output directory",
)
@click.option(
    "--config-files/--no-config-files",
    default=True,
    help="Download configuration files",
)
@click.option(
    "--ref-files/--no-ref-files",
    default=True,
    help="Download reference files",
)
@click.option(
    "--antenna/--no-antenna",
    default=True,
    help="Download antenna files",
)
@click.option(
    "--dry-run",
    is_flag=True,
    help="Show what would be downloaded",
)
@click.pass_context
def download_gen(
    ctx: click.Context,
    bsw_version: str,
    output_dir: Path | None,
    config_files: bool,
    ref_files: bool,
    antenna: bool,
    dry_run: bool,
) -> None:
    """Download BSW GEN configuration files.

    Downloads Bernese GEN files (configuration, reference, antenna) from CODE FTP.

    This replaces the Perl genFilesDownloader*.pm scripts.

    Examples:

    \b
        # Download all GEN files for BSW54
        pygnss-rt download-gen

        # Download only antenna files
        pygnss-rt download-gen --no-config-files --no-ref-files --antenna

        # Download to specific directory
        pygnss-rt download-gen -o /path/to/gen
    """
    from pygnss_rt.data_access import (
        GENFilesDownloader,
        GENDownloaderConfig,
        download_gen_files,
    )

    click.echo("GEN Files Download")
    click.echo("=" * 50)
    click.echo(f"BSW Version: {bsw_version}")
    click.echo(f"Output: {output_dir or 'default'}")
    click.echo()

    if dry_run:
        click.echo("[DRY RUN MODE]")
        if config_files:
            click.echo("Would download configuration files")
        if ref_files:
            click.echo("Would download reference files")
        if antenna:
            click.echo("Would download antenna files")
        return

    result = download_gen_files(
        bsw_version=bsw_version,
        destination=output_dir,
        download_config=config_files,
        download_ref=ref_files,
    )

    click.echo(f"Downloaded: {result.downloaded_count} files")
    click.echo(f"Skipped: {result.skipped_count} files")
    if result.failed_count > 0:
        click.echo(f"Failed: {result.failed_count} files")

    for file_result in result.files:
        status = "OK" if file_result.success else "FAILED"
        click.echo(f"  {file_result.filename}: {status}")


# =============================================================================
# Alert Management Commands
# =============================================================================

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


# =============================================================================
# System Information Commands
# =============================================================================

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


# =============================================================================
# Station Management Commands (i-BSWSTA Port)
# =============================================================================

@cli.command("update-sta")
@click.option(
    "--source", "-s",
    type=click.Choice(["IGS", "EUREF", "OSGB", "all"]),
    default=["IGS"],
    multiple=True,
    help="Site log source(s) to download from (can specify multiple)",
)
@click.option(
    "--output", "-o",
    type=click.Path(path_type=Path),
    required=True,
    help="Output STA file path",
)
@click.option(
    "--work-dir", "-w",
    type=click.Path(path_type=Path),
    default=Path("/data/station_info"),
    help="Working directory for site logs",
)
@click.option(
    "--stations",
    type=str,
    help="Comma-separated list of stations to include",
)
@click.option(
    "--exclude",
    type=str,
    help="Comma-separated list of stations to exclude",
)
@click.option(
    "--use-domes/--no-domes",
    default=False,
    help="Include DOMES numbers in station names",
)
@click.option(
    "--overwrite/--no-overwrite",
    default=False,
    help="Overwrite existing site log downloads",
)
@click.option(
    "--skip-download",
    is_flag=True,
    help="Skip download, use existing site logs",
)
@click.option(
    "--backup/--no-backup",
    default=True,
    help="Create backup of existing STA file",
)
@click.option(
    "--dry-run",
    is_flag=True,
    help="Show what would be done without executing",
)
@click.pass_context
def update_sta(
    ctx: click.Context,
    source: tuple[str, ...],
    output: Path,
    work_dir: Path,
    stations: str | None,
    exclude: str | None,
    use_domes: bool,
    overwrite: bool,
    skip_download: bool,
    backup: bool,
    dry_run: bool,
) -> None:
    """Update Bernese STA file from IGS site logs.

    Downloads site logs from IGS/EUREF/OSGB, parses them, and generates
    a Bernese .STA station information file containing receiver/antenna
    history and eccentricities.

    This replaces the Perl scripts:
    - call_autoSta_NEWNRT52_IGS.pl
    - call_autoSta_OSGB_sftp_with_IGS20_54_name.pl

    Examples:

    \b
        # Update STA file from IGS site logs
        pygnss-rt update-sta -s IGS -o /path/to/STATIONS.STA

        # Download from multiple sources
        pygnss-rt update-sta -s IGS -s EUREF -o STATIONS.STA

        # Filter specific stations
        pygnss-rt update-sta -s IGS -o STATIONS.STA --stations algo,nrc1,dubo

        # Use existing downloads (skip FTP)
        pygnss-rt update-sta -s IGS -o STATIONS.STA --skip-download

        # Include DOMES numbers in station names
        pygnss-rt update-sta -s IGS -o STATIONS.STA --use-domes
    """
    from pygnss_rt.stations import (
        AutoStationProcessor,
        AutoStationConfig,
    )

    verbose = ctx.obj.get("verbose", False)

    # Parse station lists
    station_filter = [s.strip() for s in stations.split(",")] if stations else None
    exclude_list = [s.strip() for s in exclude.split(",")] if exclude else None

    # Expand "all" to include all sources
    sources = list(source)
    if "all" in sources:
        sources = ["IGS", "EUREF", "OSGB"]

    click.echo("Station Information Update (i-BSWSTA)")
    click.echo("=" * 60)
    click.echo(f"Sources: {', '.join(sources)}")
    click.echo(f"Output: {output}")
    click.echo(f"Work Dir: {work_dir}")
    click.echo(f"DOMES: {'Yes' if use_domes else 'No'}")
    if station_filter:
        click.echo(f"Stations: {', '.join(station_filter)}")
    if exclude_list:
        click.echo(f"Excluded: {', '.join(exclude_list)}")
    click.echo()

    if dry_run:
        click.echo("[DRY RUN MODE]")
        if not skip_download:
            click.echo(f"Would download site logs from: {', '.join(sources)}")
        click.echo(f"Would parse site logs from: {work_dir}")
        click.echo(f"Would generate STA file: {output}")
        if backup and output.exists():
            click.echo(f"Would create backup of existing file")
        return

    # Configure processor
    config = AutoStationConfig(
        work_dir=work_dir,
        use_domes=use_domes,
        sta_title="i-BSWSTA generated",
        verbose=verbose,
    )

    # Add bad stations (from original Perl scripts)
    config.bad_stations = [
        "dund", "str2", "sey2", "elat", "katz", "ohig",  # IGS bad stations
    ]

    processor = AutoStationProcessor(config=config)

    # Create backup if file exists
    if backup and output.exists():
        import shutil
        backup_path = output.with_suffix(
            f".bak.{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        )
        shutil.copy2(output, backup_path)
        click.echo(f"Created backup: {backup_path}")

    # Run processing
    if skip_download:
        click.echo("Skipping download, using existing site logs...")
        parsed = processor.parse_site_logs(station_filter=station_filter)
        click.echo(f"Parsed {parsed} site logs")
    else:
        click.echo("Downloading site logs...")
        download_results = processor.download_site_logs(
            sources=sources,
            station_filter=station_filter,
            exclude_stations=exclude_list,
            overwrite=overwrite,
        )

        for result in download_results:
            click.echo(
                f"  {result.source}: {result.downloaded} downloaded, "
                f"{result.skipped} skipped, {result.failed} failed"
            )
            if result.errors:
                for err in result.errors[:5]:  # Show first 5 errors
                    click.echo(f"    Error: {err}")

        click.echo("\nParsing site logs...")
        parsed = processor.parse_site_logs(station_filter=station_filter)
        click.echo(f"Parsed {parsed} site logs")

    # Generate STA file
    click.echo(f"\nGenerating STA file: {output}")
    written = processor.generate_sta_file(output, station_filter=station_filter)
    click.echo(f"Wrote {written} stations to STA file")

    if written > 0:
        click.echo("\nUpdate complete!")
    else:
        click.echo("\nWarning: No stations written to STA file")
        sys.exit(1)


@cli.command("parse-sitelogs")
@click.argument("directory", type=click.Path(exists=True, path_type=Path))
@click.option(
    "--output", "-o",
    type=click.Path(path_type=Path),
    help="Output STA file (optional)",
)
@click.option(
    "--use-domes/--no-domes",
    default=False,
    help="Include DOMES numbers in station names",
)
@click.option(
    "--format", "-f",
    type=click.Choice(["summary", "json", "csv"]),
    default="summary",
    help="Output format for parsed data",
)
@click.option(
    "--station",
    type=str,
    help="Show details for specific station",
)
@click.pass_context
def parse_sitelogs(
    ctx: click.Context,
    directory: Path,
    output: Path | None,
    use_domes: bool,
    format: str,
    station: str | None,
) -> None:
    """Parse site log files from a directory.

    Parses IGS-format ASCII site log files (.log) and optionally generates
    a Bernese .STA file.

    Examples:

    \b
        # Parse and show summary
        pygnss-rt parse-sitelogs /data/sitelogs

        # Parse and generate STA file
        pygnss-rt parse-sitelogs /data/sitelogs -o STATIONS.STA

        # Show details for specific station
        pygnss-rt parse-sitelogs /data/sitelogs --station ALGO

        # Export as JSON
        pygnss-rt parse-sitelogs /data/sitelogs -f json > stations.json
    """
    from pygnss_rt.stations import (
        parse_site_logs_directory,
        write_sta_file,
    )
    import json

    click.echo(f"Parsing site logs from: {directory}")

    # Parse all site logs
    parsed = parse_site_logs_directory(directory)
    click.echo(f"Found {len(parsed)} valid site logs")

    if station:
        # Show details for specific station
        station_data = parsed.get(station.lower())
        if not station_data:
            click.echo(f"Station not found: {station}")
            sys.exit(1)

        click.echo()
        click.echo(f"Station: {station_data.station_id}")
        click.echo("=" * 50)
        click.echo(f"Site Name: {station_data.site_identification.site_name}")
        click.echo(f"DOMES: {station_data.domes_number}")
        click.echo(f"Country: {station_data.site_location.country}")
        click.echo()
        click.echo(f"Receivers ({len(station_data.receivers)}):")
        for i, rec in enumerate(station_data.receivers, 1):
            click.echo(f"  {i}. {rec.receiver_type}")
            click.echo(f"     Serial: {rec.serial_number}")
            click.echo(f"     Installed: {rec.date_installed}")
            click.echo(f"     Removed: {rec.date_removed or 'Current'}")
        click.echo()
        click.echo(f"Antennas ({len(station_data.antennas)}):")
        for i, ant in enumerate(station_data.antennas, 1):
            click.echo(f"  {i}. {ant.antenna_type}")
            click.echo(f"     Radome: {ant.radome_type}")
            click.echo(f"     Serial: {ant.serial_number}")
            click.echo(f"     Eccentricities (N/E/U): {ant.marker_arp_north_ecc:.4f} / "
                      f"{ant.marker_arp_east_ecc:.4f} / {ant.marker_arp_up_ecc:.4f}")
            click.echo(f"     Installed: {ant.date_installed}")
            click.echo(f"     Removed: {ant.date_removed or 'Current'}")

    elif format == "summary":
        click.echo()
        click.echo(f"{'Station':<6} {'Name':<25} {'DOMES':<12} {'Rx':<3} {'Ant':<3}")
        click.echo("-" * 55)
        for sta_id in sorted(parsed.keys()):
            data = parsed[sta_id]
            name = (data.site_identification.site_name or "")[:25]
            domes = data.domes_number[:12] if data.domes_number else ""
            rx_count = len(data.receivers)
            ant_count = len(data.antennas)
            click.echo(f"{sta_id.upper():<6} {name:<25} {domes:<12} {rx_count:<3} {ant_count:<3}")

    elif format == "json":
        # Export as JSON
        export_data = {}
        for sta_id, data in parsed.items():
            export_data[sta_id] = {
                "site_name": data.site_identification.site_name,
                "domes": data.domes_number,
                "country": data.site_location.country,
                "receivers": len(data.receivers),
                "antennas": len(data.antennas),
                "current_receiver": data.current_receiver.receiver_type if data.current_receiver else None,
                "current_antenna": data.current_antenna.antenna_type if data.current_antenna else None,
            }
        click.echo(json.dumps(export_data, indent=2))

    elif format == "csv":
        click.echo("station,site_name,domes,country,receivers,antennas,current_receiver,current_antenna")
        for sta_id in sorted(parsed.keys()):
            data = parsed[sta_id]
            name = (data.site_identification.site_name or "").replace(",", ";")
            domes = data.domes_number or ""
            country = data.site_location.country or ""
            curr_rx = data.current_receiver.receiver_type if data.current_receiver else ""
            curr_ant = data.current_antenna.antenna_type if data.current_antenna else ""
            click.echo(f"{sta_id},{name},{domes},{country},{len(data.receivers)},"
                      f"{len(data.antennas)},{curr_rx},{curr_ant}")

    # Generate STA file if requested
    if output:
        click.echo()
        click.echo(f"Generating STA file: {output}")
        station_list = list(parsed.values())
        written = write_sta_file(output, station_list, use_domes=use_domes)
        click.echo(f"Wrote {written} stations")


@cli.command("download-sitelogs")
@click.option(
    "--source", "-s",
    type=click.Choice(["IGS", "EUREF", "OSGB", "IGS_HISTORICAL"]),
    default="IGS",
    help="Site log source",
)
@click.option(
    "--output-dir", "-o",
    type=click.Path(path_type=Path),
    required=True,
    help="Output directory for downloaded files",
)
@click.option(
    "--stations",
    type=str,
    help="Comma-separated list of stations to download",
)
@click.option(
    "--exclude",
    type=str,
    help="Comma-separated list of stations to exclude",
)
@click.option(
    "--overwrite/--no-overwrite",
    default=False,
    help="Overwrite existing files",
)
@click.option(
    "--list-only",
    is_flag=True,
    help="List available files without downloading",
)
@click.pass_context
def download_sitelogs(
    ctx: click.Context,
    source: str,
    output_dir: Path,
    stations: str | None,
    exclude: str | None,
    overwrite: bool,
    list_only: bool,
) -> None:
    """Download site log files from IGS/EUREF.

    Downloads IGS-format site log files from FTP servers for station
    metadata maintenance.

    Examples:

    \b
        # Download all IGS site logs
        pygnss-rt download-sitelogs -s IGS -o /data/sitelogs

        # Download specific stations
        pygnss-rt download-sitelogs -s IGS -o /data/sitelogs \\
            --stations algo,nrc1,dubo

        # List available files without downloading
        pygnss-rt download-sitelogs -s EUREF -o /data/sitelogs --list-only
    """
    from pygnss_rt.stations import (
        SiteLogDownloader,
        DEFAULT_SITE_LOG_SOURCES,
    )

    verbose = ctx.obj.get("verbose", False)

    # Parse station lists
    station_filter = [s.strip() for s in stations.split(",")] if stations else None
    exclude_list = [s.strip() for s in exclude.split(",")] if exclude else None

    click.echo(f"Site Log Download from {source}")
    click.echo("=" * 50)

    downloader = SiteLogDownloader(verbose=verbose)

    if list_only:
        click.echo(f"Listing files on {source}...")
        try:
            files = downloader.list_remote_files(source)
            click.echo(f"Found {len(files)} site log files")
            for f in sorted(files)[:50]:  # Show first 50
                click.echo(f"  {f}")
            if len(files) > 50:
                click.echo(f"  ... and {len(files) - 50} more")
        except Exception as e:
            click.echo(f"Error: {e}")
            sys.exit(1)
        return

    # Download
    click.echo(f"Downloading to: {output_dir}")
    result = downloader.download(
        source=source,
        destination=output_dir,
        station_filter=station_filter,
        exclude_stations=exclude_list,
        overwrite=overwrite,
    )

    click.echo()
    click.echo(f"Total files: {result.total_files}")
    click.echo(f"Downloaded: {result.downloaded}")
    click.echo(f"Skipped: {result.skipped}")
    click.echo(f"Failed: {result.failed}")
    click.echo(f"Filtered out: {result.filtered_out}")
    click.echo(f"Duration: {result.duration_seconds:.1f}s")

    if result.errors:
        click.echo()
        click.echo("Errors:")
        for err in result.errors[:10]:
            click.echo(f"  - {err}")


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
