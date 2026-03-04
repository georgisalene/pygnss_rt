"""
Command-line interface for PyGNSS-RT.

Provides a modern CLI using Click for running GNSS processing,
data downloads, and other operations.
"""

from __future__ import annotations

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


# Import submodules to register commands
from . import processing  # noqa: F401, E402
from . import download  # noqa: F401, E402
from . import stations  # noqa: F401, E402
from . import database  # noqa: F401, E402
from . import utilities  # noqa: F401, E402


def main() -> None:
    """Main entry point."""
    cli(obj={})


__all__ = ["cli", "main"]
