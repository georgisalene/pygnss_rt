#!/usr/bin/env python3
"""
Ambiguity Resolution Report Generator

Parses Bernese AMB_*.SUM files to display ambiguity resolution
statistics for GPS, Galileo, and combined GPS+Galileo.

Usage:
    # View report only
    python -m frontend.amb_report 296 297

    # Save to database
    python -m frontend.amb_report 296 297 --save

    # Query from database
    python -m frontend.amb_report --query 2025 296 297
"""

import sys
import os
from pathlib import Path

from .db_models import QualityDatabase, AmbiguityResolution
from .bernese_parser import find_ig_campaign_dir
from .config import CAMPAIGN_PATH, DB_PATH


def parse_amb_file(filepath, doy, save_to_db=False, db=None):
    """Parse and display ambiguity resolution summary"""

    with open(filepath) as f:
        content = f.read()

    print("=" * 100)
    print(f"AMBIGUITY RESOLUTION REPORT - DOY {doy}")
    print("=" * 100)

    # Find all lines with station data
    wl_lines = [l for l in content.split("\n") if "#AR_WL" in l and "Tot:" not in l]
    nl_lines = [l for l in content.split("\n") if "#AR_NL" in l and "Tot:" not in l]

    # Parse WL data by station
    # Format: " 05312960 0531                    69  0.0     5  0.4  92.8 G       0.215  0.096..."
    # Indices after split():  0       1                      2   3    4   5    6    7
    stations = {}
    for line in wl_lines:
        parts = line.split()
        if len(parts) >= 8:
            sta = parts[1]  # Station name (4 char)
            rate = parts[6]  # Resolution rate (%)
            sys_type = parts[7]  # G or E
            sys2 = parts[8] if len(parts) > 8 else ""

            if sta not in stations:
                stations[sta] = {"WL_G": "-", "WL_E": "-", "WL_GE": "-",
                                "NL_G": "-", "NL_E": "-", "NL_GE": "-", "receiver": ""}

            if sys_type == "G" and sys2 != "E":
                stations[sta]["WL_G"] = rate
            elif sys_type == "E":
                stations[sta]["WL_E"] = rate
            elif sys_type == "G" and sys2 == "E":
                stations[sta]["WL_GE"] = rate
                # Extract receiver
                for i, p in enumerate(parts):
                    if p in ["LEICA", "TRIMBLE", "SEPT", "JAVAD"]:
                        stations[sta]["receiver"] = " ".join(parts[i:i+2])
                        break

    # Parse NL data
    for line in nl_lines:
        parts = line.split()
        if len(parts) >= 8:
            sta = parts[1]
            rate = parts[6]
            sys_type = parts[7]
            sys2 = parts[8] if len(parts) > 8 else ""

            if sta in stations:
                if sys_type == "G" and sys2 != "E":
                    stations[sta]["NL_G"] = rate
                elif sys_type == "E":
                    stations[sta]["NL_E"] = rate
                elif sys_type == "G" and sys2 == "E":
                    stations[sta]["NL_GE"] = rate

    # Print results
    print()
    print("Station  Receiver          | ------ WIDELANE ------ | ----- NARROWLANE -----")
    print("                           |   GPS     GAL    G+E   |   GPS     GAL    G+E")
    print("-" * 80)

    for sta in sorted(stations.keys()):
        s = stations[sta]
        print(f"{sta:8} {s['receiver']:17} | {s['WL_G']:>6}  {s['WL_E']:>6}  {s['WL_GE']:>6} | {s['NL_G']:>6}  {s['NL_E']:>6}  {s['NL_GE']:>6}")

    print("-" * 80)

    # Print totals from file
    print()
    print("TOTALS (from Bernese output):")
    for line in content.split("\n"):
        if line.startswith("Tot:"):
            print("  " + line.strip())

    # Save to database if requested
    if save_to_db and db:
        # Extract year from filepath
        year = 2025  # Default
        filename = os.path.basename(filepath)
        if "AMB_" in filename:
            try:
                year = int(filename[4:8])
            except:
                pass

        count = 0
        for sta, s in stations.items():
            amb = AmbiguityResolution(
                station_id=sta,
                year=year,
                doy=doy,
                receiver=s['receiver'] if s['receiver'] else None,
                wl_gps=float(s['WL_G']) if s['WL_G'] != '-' else None,
                wl_gal=float(s['WL_E']) if s['WL_E'] != '-' else None,
                wl_combined=float(s['WL_GE']) if s['WL_GE'] != '-' else None,
                nl_gps=float(s['NL_G']) if s['NL_G'] != '-' else None,
                nl_gal=float(s['NL_E']) if s['NL_E'] != '-' else None,
                nl_combined=float(s['NL_GE']) if s['NL_GE'] != '-' else None,
            )
            db.insert_ambiguity(amb)
            count += 1
        print(f"\n>> Saved {count} stations to database")

    return stations


def query_ambiguity(year, start_doy, end_doy):
    """Query and display ambiguity data from database"""
    db = QualityDatabase(DB_PATH)
    db.connect()
    db.create_tables()

    data = db.get_ambiguity(year=year, start_doy=start_doy, end_doy=end_doy)

    if not data:
        print(f"No ambiguity data found for {year} DOY {start_doy}-{end_doy}")
        return

    print("=" * 110)
    print(f"AMBIGUITY RESOLUTION FROM DATABASE - {year} DOY {start_doy} to {end_doy}")
    print("=" * 110)
    print()
    print("Station  DOY  Receiver          | ------ WIDELANE ------ | ----- NARROWLANE -----")
    print("                                |   GPS     GAL    G+E   |   GPS     GAL    G+E")
    print("-" * 90)

    for row in data:
        wl_g = f"{row['wl_gps']:.1f}" if row['wl_gps'] else "-"
        wl_e = f"{row['wl_gal']:.1f}" if row['wl_gal'] else "-"
        wl_ge = f"{row['wl_combined']:.1f}" if row['wl_combined'] else "-"
        nl_g = f"{row['nl_gps']:.1f}" if row['nl_gps'] else "-"
        nl_e = f"{row['nl_gal']:.1f}" if row['nl_gal'] else "-"
        nl_ge = f"{row['nl_combined']:.1f}" if row['nl_combined'] else "-"
        rcv = row['receiver'] or ""

        print(f"{row['station_id']:8} {row['doy']:3}  {rcv:17} | {wl_g:>6}  {wl_e:>6}  {wl_ge:>6} | {nl_g:>6}  {nl_e:>6}  {nl_ge:>6}")

    print("-" * 90)
    print(f"Total: {len(data)} records")
    db.close()


def main():
    campaign_path = CAMPAIGN_PATH

    # Check for query mode
    if len(sys.argv) > 1 and sys.argv[1] == "--query":
        if len(sys.argv) < 5:
            print("Usage: python -m frontend.amb_report --query <year> <start_doy> <end_doy>")
            sys.exit(1)
        year = int(sys.argv[2])
        start_doy = int(sys.argv[3])
        end_doy = int(sys.argv[4])
        query_ambiguity(year, start_doy, end_doy)
        return

    # Check for --save flag
    save_to_db = "--save" in sys.argv
    args = [a for a in sys.argv[1:] if a != "--save"]

    # Initialize database if saving
    db = None
    if save_to_db:
        db = QualityDatabase(DB_PATH)
        db.connect()
        db.create_tables()

    # Get DOYs from command line or auto-detect
    if args:
        doys = [int(d) for d in args]
    else:
        # Default: find available DOYs
        doys = []
        for d in os.listdir(campaign_path):
            # Match IG, IG_G, IG_GE, IG_GRE, etc.
            if "IG" in d and len(d) >= 7:
                try:
                    doy = int(d[2:5])
                    doys.append(doy)
                except ValueError:
                    pass
        doys = sorted(set(doys))

    for doy in doys:
        session_path = find_ig_campaign_dir(campaign_path, 2025, doy)
        if not session_path:
            print(f"Session dir not found for DOY {doy}")
            continue
        amb_file = os.path.join(session_path, "OUT", f"AMB_2025{doy:03d}0.SUM")

        if os.path.exists(amb_file):
            parse_amb_file(amb_file, doy, save_to_db=save_to_db, db=db)
            print("\n")
        else:
            print(f"AMB file not found for DOY {doy}: {amb_file}")

    if db:
        db.close()


if __name__ == "__main__":
    main()
