"""
Download commands for PyGNSS-RT CLI.

Commands: download, download-products, download-gen, download-sitelogs, download-m3g
"""

from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path

import click

from . import cli
from ._helpers import parse_date as _parse_date


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


@cli.command("download-m3g")
@click.option(
    "--stations", "-s",
    type=str,
    required=False,
    help="Comma-separated list of 9-char station IDs (e.g., BASC00LUX,ECH200LUX)",
)
@click.option(
    "--country", "-c",
    type=str,
    help="3-letter country code for predefined station lists (e.g., LUX)",
)
@click.option(
    "--output-dir", "-o",
    type=click.Path(path_type=Path),
    default=Path("site_logs"),
    help="Output directory for downloaded files",
)
@click.option(
    "--sta-output",
    type=click.Path(path_type=Path),
    help="Generate STA file from downloaded logs",
)
@click.option(
    "--merge-sta",
    type=click.Path(path_type=Path, exists=True),
    help="Merge into existing STA file",
)
@click.option(
    "--format", "-f",
    type=click.Choice(["log", "xml"]),
    default="log",
    help="Download format (log=ASCII, xml=GeodesyML)",
)
@click.pass_context
def download_m3g(
    ctx: click.Context,
    stations: str | None,
    country: str | None,
    output_dir: Path,
    sta_output: Path | None,
    merge_sta: Path | None,
    format: str,
) -> None:
    """Download site logs from gnss-metadata.eu (M3G).

    Downloads IGS site logs from the European GNSS metadata portal.
    Can generate or update Bernese STA files automatically.

    Examples:

    \b
        # Download specific stations
        pygnss-rt download-m3g -s BASC00LUX,ECH200LUX -o ./site_logs

        # Download all Luxembourg stations (predefined list)
        pygnss-rt download-m3g -c LUX -o ./site_logs

        # Download and generate STA file
        pygnss-rt download-m3g -c LUX -o ./site_logs --sta-output LUXEMBOURG.STA

        # Download and merge into existing STA file
        pygnss-rt download-m3g -s BASC00LUX --merge-sta IGS20_54.STA
    """
    from pygnss_rt.stations.gnss_metadata_eu import (
        GNSSMetadataDownloader,
        LUXEMBOURG_STATIONS,
        EUREF_CORE_STATIONS,
    )
    from pygnss_rt.stations.site_log_parser import parse_site_log
    from pygnss_rt.stations.sta_file_writer import write_sta_file

    verbose = ctx.obj.get("verbose", False)

    click.echo("M3G Site Log Download (gnss-metadata.eu)")
    click.echo("=" * 50)

    # Determine station list
    station_list = []
    if stations:
        station_list = [s.strip().upper() for s in stations.split(",")]
    elif country:
        country = country.upper()
        if country == "LUX":
            station_list = LUXEMBOURG_STATIONS
            click.echo(f"Using predefined Luxembourg stations: {len(station_list)}")
        elif country == "EUREF":
            station_list = EUREF_CORE_STATIONS
            click.echo(f"Using EUREF core stations: {len(station_list)}")
        else:
            click.echo(f"Error: No predefined list for country '{country}'")
            click.echo("Available: LUX, EUREF")
            click.echo("Or provide specific stations with -s option")
            sys.exit(1)
    else:
        click.echo("Error: Provide --stations or --country")
        sys.exit(1)

    click.echo(f"Stations to download: {', '.join(station_list)}")
    click.echo(f"Output directory: {output_dir}")
    click.echo()

    # Download
    downloader = GNSSMetadataDownloader()
    output_dir.mkdir(parents=True, exist_ok=True)
    results = downloader.download_stations(station_list, output_dir, format=format)

    # Report results
    success = sum(1 for p in results.values() if p is not None)
    failed = len(results) - success
    click.echo(f"Downloaded: {success}/{len(results)}")
    if failed > 0:
        click.echo(f"Failed: {failed}")
        for sta, path in results.items():
            if path is None:
                click.echo(f"  - {sta}")

    # Parse downloaded logs
    parsed_data = []
    for station_id, log_path in results.items():
        if log_path and log_path.exists():
            try:
                data = parse_site_log(log_path)
                if data and data.station_id:
                    parsed_data.append(data)
                    if verbose:
                        click.echo(f"Parsed: {data.station_id} - {data.site_identification.site_name}")
            except Exception as e:
                click.echo(f"Parse error {log_path}: {e}")

    # Generate STA file if requested
    if sta_output and parsed_data:
        click.echo()
        click.echo(f"Generating STA file: {sta_output}")
        count = write_sta_file(sta_output, parsed_data, title=f"M3G Download {datetime.now():%Y-%m-%d}")
        click.echo(f"Wrote {count} stations to STA file")

    # Merge into existing STA if requested
    if merge_sta and parsed_data:
        click.echo()
        click.echo(f"Merging into: {merge_sta}")
        from pygnss_rt.stations.gnss_metadata_eu import download_and_add_to_sta
        # We already downloaded, so just add the parsed data
        from pygnss_rt.stations.sta_file_writer import STAFileWriter

        # Read existing STA to check for duplicates
        try:
            existing_content = merge_sta.read_text(encoding='utf-8')
        except UnicodeDecodeError:
            existing_content = merge_sta.read_text(encoding='latin-1')
        existing_ids = set()
        for line in existing_content.split('\n'):
            import re
            if re.match(r'^[A-Z0-9]{4}\s+001\s+\d{4}', line):
                station_name = line[:4].strip().upper()
                existing_ids.add(station_name)

        # Filter out duplicates
        new_stations = [s for s in parsed_data if s.station_id.upper() not in existing_ids]
        if new_stations:
            # Simple append approach - add to existing file
            writer = STAFileWriter(use_domes=False)
            stations_info = writer._build_station_info(new_stations)

            # Append TYPE 001 and TYPE 002 entries
            lines = existing_content.split('\n')

            # Find insertion points
            type002_start = -1
            type003_start = -1
            for i, line in enumerate(lines):
                if line.startswith("TYPE 002:"):
                    type002_start = i
                if line.startswith("TYPE 003:"):
                    type003_start = i

            if type002_start > 0 and type003_start > 0:
                # Insert TYPE 001 entries before TYPE 002
                new_type001 = []
                for station in stations_info:
                    name = station.station_name_no_domes
                    old_name = f"{station.station_id.lower()}*"
                    new_type001.append(
                        f"{name:<16}      001  {'':19}  {'':19}  {old_name.upper():<16}      "
                        f"{'M3G download':<24}"
                    )

                # Insert TYPE 002 entries before TYPE 003
                new_type002 = []
                for station in stations_info:
                    name = station.station_name_no_domes
                    for event in station.events:
                        start = event.start_date
                        end = event.end_date
                        start_str = f"{start.year:4d} {start.month:02d} {start.day:02d} " \
                                   f"{start.hour:02d} {start.minute:02d} {start.second:02d}"
                        if end.year >= 2099:
                            end_str = " " * 19
                        else:
                            end_str = f"{end.year:4d} {end.month:02d} {end.day:02d} " \
                                     f"{end.hour:02d} {end.minute:02d} {end.second:02d}"

                        line = f"{name:<16}      001  "
                        line += f"{start_str}  "
                        line += f"{end_str}  "
                        line += f"{event.receiver_type:<20}  "
                        line += f"{event.receiver_serial:>20}  "
                        line += f"{event.receiver_serial[-6:]:>6}  "
                        line += f"{event.antenna_type:<20}  "
                        line += f"{event.antenna_serial:>20}  "
                        line += f"{event.antenna_serial[-6:]:>6}  "
                        line += f"{event.north_ecc:8.4f}  "
                        line += f"{event.east_ecc:8.4f}  "
                        line += f"{event.up_ecc:8.4f}  "
                        line += f"{event.site_name:<22}  "
                        line += "M3G download"
                        new_type002.append(line)

                # Reconstruct file
                lines = lines[:type002_start-1] + new_type001 + lines[type002_start-1:]
                # Recalculate type003_start
                type003_start += len(new_type001)
                for i, line in enumerate(lines):
                    if line.startswith("TYPE 003:"):
                        type003_start = i
                        break

                lines = lines[:type003_start-1] + new_type002 + lines[type003_start-1:]

                merge_sta.write_text('\n'.join(lines))
                click.echo(f"Added {len(new_stations)} new stations")
            else:
                click.echo("Error: Could not find TYPE sections in existing STA file")
        else:
            click.echo("All stations already exist in STA file")
