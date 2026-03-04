"""
Processing commands for PyGNSS-RT CLI.

Commands: process, daily-ppp, nrddp-tro, daily-crd
"""

from __future__ import annotations

import sys
from pathlib import Path

import click

from . import cli
from ._helpers import parse_date as _parse_date


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
@click.option(
    "--debug",
    is_flag=True,
    help="Enable debug logging for BPE execution",
)
@click.option(
    "--local-rinex-dir",
    type=click.Path(exists=True, file_okay=False, dir_okay=True, path_type=Path),
    help="Local directory containing RINEX files (checked before network download)",
)
@click.option(
    "--local-only",
    is_flag=True,
    help="Only use local RINEX files, no network downloads (requires --local-rinex-dir)",
)
@click.option(
    "--min-stations",
    type=float,
    default=0.5,
    help="Minimum station success rate (0.0-1.0, default: 0.5 = 50%)",
)
@click.option(
    "--systems",
    type=click.Choice(["G", "GE", "GR", "GRE"], case_sensitive=False),
    default="GRE",
    show_default=True,
    help="GNSS systems for PPP-AR: G=GPS, GE=GPS+Galileo, GR=GPS+GLONASS, GRE=GPS+GLONASS+Galileo",
)
@click.option(
    "--tro-gradient-interval",
    type=click.Choice(["6", "12", "24"], case_sensitive=False),
    default=None,
    help="Troposphere horizontal gradient interval in hours (default: 24 from YAML config)",
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
    debug: bool,
    local_rinex_dir: Path | None,
    local_only: bool,
    min_stations: float,
    systems: str,
    tro_gradient_interval: str | None,
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

    # Enable debug logging if requested
    if debug:
        import logging
        logging.basicConfig(
            level=logging.DEBUG,
            format='%(asctime)s [%(levelname)-8s] %(name)s: %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        click.echo("[DEBUG MODE ENABLED]")

    config_path = ctx.obj.get("config")
    verbose = ctx.obj.get("verbose", False) or debug

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
    click.echo(f"Systems: {systems.upper()}")
    if tro_gradient_interval:
        click.echo(f"Tro gradient interval: {tro_gradient_interval}h")

    if station_list:
        click.echo(f"Stations: {', '.join(station_list)}")
    if exclude_list:
        click.echo(f"Excluded: {', '.join(exclude_list)}")

    # Validate and display local RINEX options
    if local_only and not local_rinex_dir:
        click.echo("Error: --local-only requires --local-rinex-dir to be specified")
        sys.exit(1)

    if local_rinex_dir:
        click.echo(f"Local RINEX: {local_rinex_dir}")
        if local_only:
            click.echo(f"  Mode: LOCAL ONLY (no network downloads)")
        else:
            click.echo(f"  Mode: Local first, then network fallback")

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
            local_rinex_dir=local_rinex_dir,
            local_only=local_only,
            min_stations_pct=min_stations,
            systems=systems.upper(),
            tro_gradient_interval=f"{int(tro_gradient_interval):02d} 00 00" if tro_gradient_interval else None,
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


@cli.command("daily-crd")
@click.option(
    "--year", "-y",
    type=int,
    help="Processing year",
)
@click.option(
    "--doy", "-d",
    type=int,
    multiple=True,
    help="Day of year (single value or range: -d START -d END)",
)
@click.option(
    "--cron",
    is_flag=True,
    help="Run in CRON mode (auto-detect date with latency)",
)
@click.option(
    "--latency",
    type=int,
    default=12,
    help="Latency in hours for CRON mode (default: 12)",
)
@click.option(
    "--output-dir", "-o",
    type=click.Path(path_type=Path),
    default=None,
    help="Output directory for CRD files (default: from PathConfig)",
)
@click.option(
    "--ppp-root",
    type=click.Path(path_type=Path),
    default=None,
    help="Root directory for archived PPP solutions (default: from PathConfig)",
)
@click.option(
    "--dry-run",
    is_flag=True,
    help="Show what would be done without executing",
)
@click.option(
    "--flat/--no-flat",
    default=True,
    help="Use flat directory structure (yyyy/doy/FIN_*.CRD). Use --no-flat for network archives",
)
@click.option(
    "--window",
    type=int,
    nargs=2,
    default=(51, 22),
    help="Averaging window as START END days before target (default: 51 22)",
)
@click.option(
    "--min-records",
    type=int,
    default=7,
    help="Minimum records per station (default: 7)",
)
@click.pass_context
def daily_crd(
    ctx: click.Context,
    year: int | None,
    doy: tuple[int, ...],
    cron: bool,
    latency: int,
    output_dir: Path,
    ppp_root: Path,
    dry_run: bool,
    flat: bool,
    window: tuple[int, int],
    min_records: int,
) -> None:
    """Generate daily NRT coordinates for NRDDP processing.

    Computes a-priori coordinates based on aligned solutions from daily PPP
    runs over a 21-50 day window. Uses iterative outlier rejection to produce
    robust mean coordinates.

    This replaces the Perl script: iGNSS_D_CRD_54.pl

    Output files:
    - DNR{YY}{DOY}0.CRD: Single-day coordinate solution
    - ANR{YY}{DOY}0.CRD: Combined solution for NRDDP (current + previous day)

    Examples:

    \b
        # Run in CRON mode (auto-detect date)
        pygnss-rt daily-crd --cron

        # Process specific date (single DOY)
        pygnss-rt daily-crd --year 2024 --doy 260

        # Process a range of days
        pygnss-rt daily-crd --year 2025 -d 357 -d 359

        # Custom output directory
        pygnss-rt daily-crd --cron -o /path/to/nrtCoord
    """
    from pygnss_rt.processing.daily_crd import (
        DailyCRDProcessor,
        DailyCRDConfig,
        NetworkArchive,
    )

    verbose = ctx.obj.get("verbose", False)

    click.echo("Daily NRT Coordinate Generation")
    click.echo("=" * 60)

    if dry_run:
        click.echo("[DRY RUN MODE]")
        click.echo()

    # Use defaults from dataclass if not explicitly provided
    from pygnss_rt.processing.daily_crd import (
        _get_default_nrt_coord_dir,
        _get_default_ppp_root,
    )

    actual_output_dir = output_dir if output_dir else _get_default_nrt_coord_dir()
    actual_ppp_root = ppp_root if ppp_root else _get_default_ppp_root()

    # Create configuration with network archives
    networks = [
        NetworkArchive(network_id="IG", root=actual_ppp_root, campaign_pattern="YYDOYIG", prefix="AIG"),
        NetworkArchive(network_id="EU", root=actual_ppp_root, campaign_pattern="YYDOYEU", prefix="AEU"),
        NetworkArchive(network_id="GB", root=actual_ppp_root, campaign_pattern="YYDOYGB", prefix="AGB"),
        NetworkArchive(network_id="IR", root=actual_ppp_root, campaign_pattern="YYDOYIR", prefix="AIR"),
        NetworkArchive(network_id="IS", root=actual_ppp_root, campaign_pattern="YYDOYIS", prefix="AIS"),
        NetworkArchive(network_id="RG", root=actual_ppp_root, campaign_pattern="YYDOYRG", prefix="ARG"),
        NetworkArchive(network_id="SS", root=actual_ppp_root, campaign_pattern="YYDOYSS", prefix="ASS"),
        NetworkArchive(network_id="CA", root=actual_ppp_root, campaign_pattern="YYDOYCA", prefix="ACA"),
    ]

    config = DailyCRDConfig(
        output_dir=actual_output_dir,
        ppp_root=actual_ppp_root,
        networks=networks,
        latency_hours=latency,
        use_flat_structure=flat,
        window_start_days=window[0],
        window_end_days=window[1],
        min_records=min_records,
    )

    click.echo(f"Output directory: {actual_output_dir}")
    click.echo(f"PPP archive root: {actual_ppp_root}")
    if flat:
        click.echo("Mode: Flat structure (FIN_*.CRD)")

    # Parse DOY range (supports single value or range)
    doy_start, doy_end = None, None
    if doy:
        if len(doy) == 1:
            doy_start = doy_end = doy[0]
        elif len(doy) >= 2:
            doy_start, doy_end = doy[0], doy[1]

    if cron:
        click.echo(f"Mode: CRON (latency: {latency} hours)")
    else:
        if year and doy_start is not None:
            if doy_start == doy_end:
                click.echo(f"Date: {year}/{doy_start:03d}")
            else:
                click.echo(f"Date range: {year}/{doy_start:03d} - {year}/{doy_end:03d}")
        else:
            click.echo("Error: Either --cron or both --year and --doy are required")
            sys.exit(1)

    click.echo()

    if dry_run:
        if cron:
            from datetime import datetime, timedelta, timezone
            now = datetime.now(timezone.utc)
            proc_time = now - timedelta(hours=latency)
            click.echo(f"Would process: {proc_time.year}/{proc_time.timetuple().tm_yday:03d}")
        else:
            if doy_start == doy_end:
                click.echo(f"Would process: {year}/{doy_start:03d}")
            else:
                click.echo(f"Would process: {year}/{doy_start:03d} to {year}/{doy_end:03d}")
        click.echo(f"Would collect coordinates from {len(networks)} networks")
        click.echo("Would generate DNR and ANR CRD files")
        return

    # Create processor and run
    processor = DailyCRDProcessor(config=config, verbose=verbose)

    if cron:
        result = processor.process_cron()
        # Report single result
        click.echo()
        if result.success:
            click.echo("SUCCESS")
            click.echo(f"  Stations: {result.n_stations}")
            click.echo(f"  Rejected: {result.n_rejected}")
            click.echo(f"  DNR file: {result.dnr_file}")
            click.echo(f"  ANR file: {result.anr_file}")
            click.echo(f"  Processing time: {result.processing_time:.1f}s")
        else:
            click.echo("FAILED")
            click.echo(f"  Error: {result.error_message}")
            sys.exit(1)
    else:
        # Process range of DOYs
        results = []
        for current_doy in range(doy_start, doy_end + 1):
            click.echo(f"Processing {year}/{current_doy:03d}...")
            result = processor.process(year=year, doy=current_doy)
            results.append((current_doy, result))

        # Report results
        click.echo()
        click.echo("=" * 60)
        click.echo("SUMMARY")
        click.echo("=" * 60)

        n_success = sum(1 for _, r in results if r.success)
        n_failed = len(results) - n_success

        for current_doy, result in results:
            status = "OK" if result.success else "FAILED"
            if result.success:
                click.echo(f"  {year}/{current_doy:03d}: {status} - {result.n_stations} stations")
            else:
                click.echo(f"  {year}/{current_doy:03d}: {status} - {result.error_message}")

        click.echo()
        click.echo(f"Total: {n_success} succeeded, {n_failed} failed")

        if n_failed > 0:
            sys.exit(1)
