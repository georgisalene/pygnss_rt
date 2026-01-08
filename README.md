# PyGNSS-RT: Python GNSS Real-Time Processing System

**Version 1.1.0**

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

---

## Perl to Python Conversion Status

### Summary

| Metric | Count |
|--------|-------|
| **Total Perl Modules** | 18 |
| **Fully Converted** | 14 |
| **Partially Converted** | 2 |
| **Not Yet Converted** | 2 |
| **Perl Lines of Code** | 17,537 |
| **Python Lines of Code** | 8,057 |

### Detailed Conversion Matrix

| Perl Module | Lines | Python Replacement | Lines | Status | Notes |
|-------------|-------|-------------------|-------|--------|-------|
| **IGNSS.pm** | 4,700 | `core/orchestrator.py` | 1,363 | ✅ Complete | Main processing orchestrator with full workflow |
| **DATES.pm** | 1,416 | `utils/dates.py` | 330 | ✅ Complete | GNSSDate class, GPS week, MJD, DOY conversions |
| **GMTTIME.pm** | 312 | `utils/dates.py` | (incl.) | ✅ Complete | Merged into dates.py |
| **DB.pm** | 1,628 | `database/connection.py` | 211 | ✅ Complete | DuckDB replaces PostgreSQL |
| | | `database/models.py` | 157 | | Product/Station models |
| | | `database/products.py` | 267 | | Product management |
| **FTP.pm** | 3,027 | `data_access/ftp_client.py` | 274 | ✅ Complete | FTP/SFTP with retry logic |
| | | `data_access/http_client.py` | 241 | | HTTPS/Curl support |
| | | `data_access/downloader.py` | 300 | | Unified download manager |
| **FTPCONF.pm** | 556 | `data_access/ftp_config.py` | 714 | ✅ Complete | Full XML configuration parsing |
| **ZTD2IWV.pm** | 1,609 | `atmosphere/ztd2iwv.py` | 916 | ✅ Complete | ZTD to IWV with met station lookup |
| **STA.pm** | 676 | `stations/station.py` | 202 | ✅ Complete | Station management with XML loading |
| **BSWSTA.pm** | 265 | `stations/bswsta.py` | 312 | ✅ Complete | BSW station file parsing (.STA, .CRD, .ABB) |
| **CRD.pm** | 201 | `stations/coordinates.py` | 511 | ✅ Complete | ITRS/ETRS89 transforms, Cartesian/Ellipsoidal |
| **FORMAT.pm** | 353 | `utils/format.py` | 284 | ✅ Complete | Hour-to-alpha, zero padding, time formats |
| **UTIL.pm** | 707 | `utils/rinex.py` | 488 | ✅ Complete | RINEX 2.x/3.x filename parsing |
| **LOADENV.pm** | 61 | `bsw/environment.py` | 148 | ✅ Complete | BSW environment loading |
| **PROD.pm** | 495 | `database/products.py` | (incl.) | ✅ Complete | Merged into database module |
| **PRINT.pm** | 119 | `utils/logging.py` | 91 | ✅ Complete | Structured logging with structlog |
| **ORBIT.pm** | 405 | `data_access/downloader.py` | (incl.) | ⚠️ Partial | Basic orbit handling; SP3 parsing TODO |
| **INX2TEC.pm** | 835 | - | - | ❌ Not Started | IONEX to TEC conversion |
| **TIVOLI2.pm** | 172 | - | - | ❌ Not Started | Tivoli archiving (legacy, may not be needed) |

### New Python Modules (No Perl Equivalent)

| Python Module | Lines | Purpose |
|---------------|-------|---------|
| `core/config.py` | 172 | Pydantic-based settings management |
| `core/exceptions.py` | 93 | Custom exception hierarchy |
| `bsw/interface.py` | 338 | BSW campaign management and BPE runner |
| `cli.py` | 493 | Click-based command line interface |

### Conversion Details by Category

#### Core Processing
```
Perl: IGNSS.pm (4,700 lines)
  └── Python: core/orchestrator.py (1,363 lines)

Features converted:
  ✅ Processing types (daily, hourly, subhourly)
  ✅ Session naming conventions (yydoyHA format)
  ✅ Product management (orbit, ERP, clock, BIA, ION, DCB)
  ✅ Station data acquisition
  ✅ BSW campaign directory setup
  ✅ RINEX preprocessing (clockprep, cc2noncc)
  ✅ BSW processing integration
  ✅ ZTD to IWV generation
  ✅ DCM archiving (Delete, Compress, Move)
```

#### Date/Time Handling
```
Perl: DATES.pm (1,416 lines) + GMTTIME.pm (312 lines) = 1,728 lines
  └── Python: utils/dates.py (330 lines)

Features converted:
  ✅ GNSSDate class with GPS week, DOY, MJD
  ✅ Julian date conversions
  ✅ GPS week and day-of-week calculations
  ✅ Year/DOY to calendar date
  ✅ Leap year handling
  ✅ Date arithmetic (add_days, add_hours)
```

#### Database Layer
```
Perl: DB.pm (1,628 lines) + PROD.pm (495 lines) = 2,123 lines
  └── Python: database/ (635 lines total)
      ├── connection.py (211 lines) - DuckDB connection management
      ├── models.py (157 lines) - Enums and data models
      └── products.py (267 lines) - Product tracking

Features converted:
  ✅ Product storage and retrieval
  ✅ Station data management
  ✅ Processing run tracking
  ✅ Hourly/subhourly data tables

Improvements:
  ✅ PostgreSQL → DuckDB (embedded, no server needed)
  ✅ Type-safe enums for ProductType, ProductTier
```

#### FTP/Download System
```
Perl: FTP.pm (3,027 lines) + FTPCONF.pm (556 lines) = 3,583 lines
  └── Python: data_access/ (1,529 lines total)
      ├── ftp_client.py (274 lines) - FTP/SFTP client
      ├── ftp_config.py (714 lines) - XML configuration
      ├── http_client.py (241 lines) - HTTPS/Curl
      └── downloader.py (300 lines) - Unified manager

Features converted:
  ✅ FTP with passive mode
  ✅ SFTP with key authentication
  ✅ HTTPS/Curl support
  ✅ Retry logic with exponential backoff
  ✅ Full FTPCONF.xml parsing
  ✅ Multi-server configuration
  ✅ Product tier selection
```

#### Troposphere Processing
```
Perl: ZTD2IWV.pm (1,609 lines)
  └── Python: atmosphere/ztd2iwv.py (916 lines)

Features converted:
  ✅ ZTD to IWV conversion
  ✅ Meteorological data interpolation
  ✅ WMO station database loading
  ✅ COST-716 output format
  ✅ Saastamoinen model
  ✅ Pressure/temperature models
```

#### Station Management
```
Perl: STA.pm (676 lines) + BSWSTA.pm (265 lines) + CRD.pm (201 lines) = 1,142 lines
  └── Python: stations/ (1,025 lines total)
      ├── station.py (202 lines) - Station management
      ├── bswsta.py (312 lines) - BSW file parsing
      └── coordinates.py (511 lines) - Coordinate transforms

Features converted:
  ✅ XML station list loading
  ✅ BSW .STA file parsing
  ✅ BSW .CRD file parsing (coordinates)
  ✅ BSW .ABB file parsing (abbreviations)
  ✅ ITRS to ETRS89 transformation
  ✅ Cartesian to Ellipsoidal conversion
  ✅ WGS84 and IGb00 ellipsoids
  ✅ Haversine distance calculation
```

#### Utilities
```
Perl: FORMAT.pm (353 lines) + UTIL.pm (707 lines) = 1,060 lines
  └── Python: utils/ (1,193 lines total)
      ├── dates.py (330 lines) - Date/time
      ├── format.py (284 lines) - Formatting
      ├── rinex.py (488 lines) - RINEX parsing
      └── logging.py (91 lines) - Logging

Features converted:
  ✅ Hour to alpha conversion (a-x)
  ✅ Subhour notation (a00, a15, etc.)
  ✅ Zero padding utilities
  ✅ RINEX 2.x filename parsing
  ✅ RINEX 3.x long filename parsing
  ✅ Orbit/ERP/DCB/BIA filename parsing
```

### Not Yet Converted

| Module | Lines | Reason |
|--------|-------|--------|
| **INX2TEC.pm** | 835 | IONEX to TEC conversion - specialized ionosphere processing |
| **TIVOLI2.pm** | 172 | Legacy Tivoli tape archiving - may be obsolete |

---

## Installation

```bash
# Clone the repository
git clone https://github.com/georgisalene/pygnss_rt.git
cd pygnss_rt

# Create virtual environment
python -m venv venv
source venv/bin/activate  # Linux/Mac

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

## Project Structure

```
pygnss_rt/
├── pygnss_rt/                  # Main Python package
│   ├── core/                   # Orchestration & configuration
│   │   ├── orchestrator.py     # Main IGNSS workflow (replaces IGNSS.pm)
│   │   ├── config.py           # Pydantic settings
│   │   └── exceptions.py       # Custom exceptions
│   ├── database/               # DuckDB integration (replaces DB.pm)
│   │   ├── connection.py       # Database manager
│   │   ├── models.py           # Data models
│   │   └── products.py         # Product tracking
│   ├── data_access/            # FTP/SFTP/HTTP clients (replaces FTP.pm)
│   │   ├── ftp_client.py       # FTP/SFTP client
│   │   ├── ftp_config.py       # XML config (replaces FTPCONF.pm)
│   │   ├── http_client.py      # HTTPS/Curl
│   │   └── downloader.py       # Unified downloader
│   ├── stations/               # Station management (replaces STA.pm)
│   │   ├── station.py          # Station manager
│   │   ├── bswsta.py           # BSW file parsing (replaces BSWSTA.pm)
│   │   └── coordinates.py      # Transforms (replaces CRD.pm)
│   ├── atmosphere/             # Troposphere (replaces ZTD2IWV.pm)
│   │   └── ztd2iwv.py          # ZTD to IWV conversion
│   ├── bsw/                    # Bernese interface (replaces LOADENV.pm)
│   │   ├── environment.py      # BSW environment
│   │   └── interface.py        # Campaign & BPE runner
│   ├── utils/                  # Utilities
│   │   ├── dates.py            # Date/time (replaces DATES.pm, GMTTIME.pm)
│   │   ├── format.py           # Formatting (replaces FORMAT.pm)
│   │   ├── rinex.py            # RINEX parsing (replaces UTIL.pm)
│   │   └── logging.py          # Logging (replaces PRINT.pm)
│   └── cli.py                  # Command-line interface
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

## Dependencies

- Python 3.10+
- DuckDB 1.0+
- Bernese GNSS Software 5.4 (for full processing)

See `pyproject.toml` for complete dependency list.

## License

MIT License - See LICENSE file for details.

## Contributing

Contributions are welcome! Please read the contributing guidelines before submitting PRs.
