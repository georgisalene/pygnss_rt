"""
Station management commands for PyGNSS-RT CLI.

Commands: stations, update-sta, add-stations, parse-sitelogs, check-blq
"""

from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path

import click

from . import cli


@cli.command()
@click.argument("station_file", type=click.Path(exists=True, path_type=Path))
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
    station_file: Path,
    network: str | None,
    nrt_only: bool,
    format: str,
) -> None:
    """List stations from station configuration file (XML or YAML).

    Examples:

        # List all IGS20 stations from YAML
        pygnss-rt stations station_data/IGS20gh.yaml -n IGS20

        # List from XML (legacy)
        pygnss-rt stations station_data/IGS20rh.xml -n IGS20

        # Export to CSV
        pygnss-rt stations station_data/IGS20gh.yaml -f csv > stations.csv
    """
    from pygnss_rt.stations.station import StationManager

    manager = StationManager()
    manager.load(station_file)  # Auto-detects XML or YAML

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


@cli.command("check-blq")
@click.option(
    "--output", "-o",
    type=click.Path(path_type=Path),
    help="Save ocean loading format to file",
)
@click.option(
    "--verbose", "-v",
    is_flag=True,
    help="Show detailed output",
)
@click.pass_context
def check_blq(ctx: click.Context, output: Path | None, verbose: bool) -> None:
    """Check BLQ (ocean loading) file coverage for all stations.

    Compares stations defined in YAML configuration files against entries
    in BLQ files and reports any missing stations.

    Missing stations are output in a format suitable for the Chalmers
    Ocean Loading Provider (http://holt.oso.chalmers.se/loading/).

    Examples:

        # Check for missing BLQ entries
        pygnss-rt check-blq

        # Save output to file for ocean loading website
        pygnss-rt check-blq -o missing_stations.txt
    """
    from pygnss_rt.stations.blq_checker import BLQChecker

    checker = BLQChecker()
    missing = checker.find_missing_stations()

    # Print report
    checker.print_report(missing, verbose=verbose)

    if missing:
        # Print format for ocean loading website
        checker.print_ocean_loading_format(missing)

        # Save to file if requested
        if output:
            ocean_format = checker.get_ocean_loading_format(missing)
            output.write_text(ocean_format)
            click.echo(f"\nSaved ocean loading format to: {output}")
            click.echo("Upload this file to http://holt.oso.chalmers.se/loading/")

        # Instructions
        click.echo("\n" + "=" * 70)
        click.echo("INSTRUCTIONS")
        click.echo("=" * 70)
        click.echo("""
1. Go to http://holt.oso.chalmers.se/loading/
2. Copy the station list above into the 'station list' input box
3. Select options:
   - Ocean model: FES2014b (recommended)
   - Output format: BLQ
   - CMC: Yes (center of mass correction)
4. Submit and download the result
5. Append the output to your BLQ file:
   pygnss_rt/station_data/IGS20_54.BLQ
""")
        sys.exit(1)  # Exit with error if stations are missing
    else:
        click.echo("\nAll stations have BLQ entries!")


@cli.command("add-stations")
@click.argument("input_file", type=click.Path(exists=True, path_type=Path))
@click.option(
    "--target", "-t",
    type=str,
    required=True,
    help="Target YAML file name (e.g., eurefgh.yaml) or full path",
)
@click.option(
    "--network", "-n",
    type=str,
    default="EUREF",
    help="Primary network name (default: EUREF)",
)
@click.option(
    "--provider", "-p",
    type=str,
    default="EUREF",
    help="Data provider (default: EUREF)",
)
@click.option(
    "--type", "station_type",
    type=str,
    default="EUREF",
    help="Station type (default: EUREF)",
)
@click.option(
    "--nrt/--no-nrt",
    default=True,
    help="Enable NRT processing (default: True)",
)
@click.option(
    "--dry-run", "-d",
    is_flag=True,
    help="Show what would be added without modifying files",
)
@click.option(
    "--list-targets", "-l",
    is_flag=True,
    help="List available target YAML files",
)
@click.pass_context
def add_stations(
    ctx: click.Context,
    input_file: Path,
    target: str,
    network: str,
    provider: str,
    station_type: str,
    nrt: bool,
    dry_run: bool,
    list_targets: bool,
) -> None:
    """Add new stations from input file to YAML station configuration.

    INPUT_FILE should contain station data in one of these formats:

    \b
    1. Simple format (one station per line):
       STATION_ID  X  Y  Z

    \b
    2. Extended format (with long ID mapping):
       LONG_NAME_MAP = {
           station_id  LONG_ID00XXX
       }
       STATION_ID  X  Y  Z

    \b
    3. YAML format:
       stations:
         - id: xxxx
           long_id: XXXX00XXX
           coordinates: {x: ..., y: ..., z: ...}

    Examples:

        # List available target YAML files
        pygnss-rt add-stations input.txt --target dummy --list-targets

        # Add stations to eurefgh.yaml
        pygnss-rt add-stations new_stations.txt --target eurefgh.yaml

        # Dry run to see what would be added
        pygnss-rt add-stations new_stations.txt --target eurefgh.yaml --dry-run

        # Add to IGS network with custom settings
        pygnss-rt add-stations new_sta.txt -t stationsgh.yaml -n IGS -p IGS --type IGS
    """
    from pygnss_rt.stations.station_adder import StationAdder

    adder = StationAdder()

    # List targets if requested
    if list_targets:
        click.echo("\nAvailable YAML station files:")
        click.echo("=" * 50)
        yaml_files = adder.list_yaml_files()
        for path in yaml_files:
            existing = len(adder.get_existing_stations(path))
            click.echo(f"  {path.name:<25} ({existing} stations)")
        return

    # Parse input file
    try:
        new_stations = adder.parse_input_file(input_file)
    except Exception as e:
        click.echo(f"Error parsing input file: {e}", err=True)
        sys.exit(1)

    if not new_stations:
        click.echo("No stations found in input file", err=True)
        sys.exit(1)

    click.echo(f"\nParsed {len(new_stations)} stations from {input_file.name}:")
    click.echo("-" * 60)
    click.echo(f"{'Station':<8} {'Long ID':<12} {'Country':<15} {'X':>14} {'Y':>14} {'Z':>14}")
    click.echo("-" * 60)
    for stn in new_stations:
        country = stn.country_info["name"][:15]
        click.echo(f"{stn.id:<8} {stn.long_id:<12} {country:<15} {stn.x:>14.4f} {stn.y:>14.4f} {stn.z:>14.4f}")

    # Add stations
    try:
        num_added, added, skipped = adder.add_stations(
            input_file=input_file,
            target_yaml=target,
            primary_net=network,
            provider=provider,
            station_type=station_type,
            use_nrt=nrt,
            dry_run=dry_run,
        )
    except FileNotFoundError as e:
        click.echo(f"\nError: {e}", err=True)
        click.echo("\nUse --list-targets to see available YAML files")
        sys.exit(1)

    # Report results
    click.echo(f"\n{'=' * 60}")
    if dry_run:
        click.echo("DRY RUN - No changes made")
    click.echo(f"{'=' * 60}")

    click.echo(f"\nTarget: {target}")
    click.echo(f"Network: {network}")
    click.echo(f"Added: {num_added} stations")

    if added:
        click.echo(f"  New: {', '.join(added)}")
    if skipped:
        click.echo(f"  Skipped (already exist): {', '.join(skipped)}")

    if num_added > 0 and not dry_run:
        click.echo(f"\nStations successfully added to {target}")
        click.echo("\nNext steps:")
        click.echo("  1. Run 'pygnss-rt check-blq' to check for missing BLQ entries")
        click.echo("  2. Generate BLQ entries at http://holt.oso.chalmers.se/loading/")
        click.echo("  3. Add stations to STA file if needed")


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
