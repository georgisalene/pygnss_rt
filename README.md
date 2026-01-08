# PyGNSS-RT: Python GNSS Real-Time Processing System

A modern Python framework for real-time GNSS (Global Navigation Satellite System) data processing, integrating with Bernese GNSS Software (BSW) for Precise Point Positioning (PPP) and tropospheric parameter estimation.

## Overview

PyGNSS-RT automates the real-time processing of GNSS observations to generate:
- **ZTD** (Zenith Tropospheric Delay) estimates
- **IWV** (Integrated Water Vapor) products
- **Coordinates** for GNSS stations

This Python implementation replaces the legacy Perl system while maintaining full compatibility with Bernese GNSS Software v5.4.

## Features

- **DuckDB Database**: Embedded analytical database for efficient data management
- **Multi-Protocol Downloads**: FTP, SFTP, and HTTPS/Curl support for data retrieval
- **Modern Python**: Type hints, Pydantic validation, structured logging
- **Flexible Configuration**: YAML-based configuration with environment variable support
- **COST-716 Output**: Standard format for tropospheric products
- **CLI Interface**: Full command-line interface using Click

## Installation

```bash
# Clone the repository
git clone https://github.com/yourusername/pygnss-rt.git
cd pygnss-rt

# Create virtual environment
python -m venv venv
source venv/bin/activate  # Linux/Mac
# or: venv\Scripts\activate  # Windows

# Install dependencies
pip install -e .

# For development
pip install -e ".[dev]"
```

## Quick Start

### Initialize Database

```bash
pygnss-rt init --db-path data/pygnss_rt.duckdb
```

### Run Processing

```bash
# CRON mode (automatic date detection)
pygnss-rt process --cron --latency 3

# Manual date range
pygnss-rt process -s 2024-01-01 -e 2024-01-07 -t hourly

# Specific stations
pygnss-rt process -s 2024-01-01 -S algo,nrc1,dubo
```

### Download Products

```bash
# Download IGS final orbits
pygnss-rt download -p orbit --provider IGS --tier final -s 2024-01-01 -e 2024-01-07
```

### List Stations

```bash
pygnss-rt stations config/stations/IGS20rh.xml -n IGS20
```

## Project Structure

```
pygnss-rt/
├── pygnss_rt/                  # Main Python package
│   ├── core/                   # Orchestration & configuration
│   ├── database/               # DuckDB integration
│   ├── data_access/            # FTP/SFTP/HTTP clients
│   ├── stations/               # Station management
│   ├── atmosphere/             # ZTD to IWV conversion
│   ├── bsw/                    # Bernese interface
│   └── utils/                  # Date/time, logging utilities
├── callers/                    # Processing scripts
├── config/                     # Configuration files
├── tests/                      # Test suite
└── pyproject.toml              # Project metadata
```

## Configuration

Copy and customize the configuration:

```bash
cp config/settings.example.yaml config/settings.yaml
```

Key configuration sections:
- `database`: DuckDB path and settings
- `bsw`: Bernese GNSS Software paths
- `data`: Data directory locations
- `processing`: Default processing options
- `ftp`: FTP server definitions

## Database Schema

The system uses DuckDB with the following tables:

| Table | Purpose |
|-------|---------|
| `products` | GNSS products (orbits, ERP, clocks, DCB) |
| `hourly_data` | Observation data tracking |
| `stations` | Station metadata |
| `processing_runs` | Processing history |
| `ztd_results` | ZTD/IWV results |

## Dependencies

- Python 3.10+
- DuckDB 1.0+
- Bernese GNSS Software 5.4 (for full processing)

See `pyproject.toml` for complete dependency list.

## Migration from Legacy Perl System

This Python implementation replaces the following Perl modules:

| Perl Module | Python Replacement |
|-------------|-------------------|
| IGNSS.pm | `pygnss_rt/core/orchestrator.py` |
| DATES.pm, GMTTIME.pm | `pygnss_rt/utils/dates.py` |
| FTP.pm | `pygnss_rt/data_access/ftp_client.py` |
| DB.pm, PROD.pm | `pygnss_rt/database/` |
| STA.pm | `pygnss_rt/stations/station.py` |
| ZTD2IWV.pm | `pygnss_rt/atmosphere/ztd2iwv.py` |
| LOADENV.pm | `pygnss_rt/bsw/environment.py` |

## License

MIT License - See LICENSE file for details.

## Contributing

Contributions are welcome! Please read the contributing guidelines before submitting PRs.
