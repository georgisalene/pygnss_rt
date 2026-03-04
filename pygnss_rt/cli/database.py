"""
Database commands for PyGNSS-RT CLI.

Commands: init, db-maintain, db-status
"""

from __future__ import annotations

import sys
from pathlib import Path

import click

from . import cli


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
