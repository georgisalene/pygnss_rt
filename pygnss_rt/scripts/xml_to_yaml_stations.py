#!/usr/bin/env python3
"""Convert station XML files to YAML format.

Converts the i-GNSS station XML files to a cleaner YAML format.

Usage:
    python xml_to_yaml_stations.py [--input-dir DIR] [--output-dir DIR]

Author: Addisu Hunegnaw
Date: January 2026
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from xml.etree import ElementTree

import yaml


def parse_station_xml(xml_path: Path) -> dict:
    """Parse station XML file and return structured data.

    Args:
        xml_path: Path to XML file

    Returns:
        Dictionary with metadata and stations list
    """
    tree = ElementTree.parse(xml_path)
    root = tree.getroot()

    # Extract metadata
    metadata = {
        "source_file": xml_path.name,
        "ref_epoch": root.findtext("ref_ep", ""),
        "datum": root.findtext("datum", ""),
    }

    stations = []
    for station_elem in root.findall(".//station"):
        station = {}

        # Core identification
        station["id"] = station_elem.findtext("fourCharName", "").lower()
        station["domes"] = station_elem.findtext("DOMES", "")
        station["name"] = station_elem.findtext("fullName", "")

        # Network/type info
        station["primary_net"] = station_elem.findtext("primaryNet", "")
        station["provider"] = station_elem.findtext("provider", "")
        station["type"] = station_elem.findtext("type", "")

        # NRT capability
        use_nrt_text = station_elem.findtext("use_nrt", "no").lower()
        station["use_nrt"] = use_nrt_text in ("yes", "1", "true")

        # Location
        station["country"] = station_elem.findtext("country", "")
        station["iso"] = station_elem.findtext("ISO", "")

        # Coordinates (ECEF)
        x = station_elem.findtext("approximate_X")
        y = station_elem.findtext("approximate_Y")
        z = station_elem.findtext("approximate_Z")

        if x and y and z:
            station["coordinates"] = {
                "x": float(x),
                "y": float(y),
                "z": float(z),
            }

        # Only add station if it has an ID
        if station["id"]:
            # Remove empty fields
            station = {k: v for k, v in station.items() if v or v is False}
            stations.append(station)

    return {
        "metadata": metadata,
        "stations": stations,
    }


def convert_xml_to_yaml(xml_path: Path, output_dir: Path | None = None) -> Path:
    """Convert a single XML file to YAML.

    Args:
        xml_path: Path to input XML file
        output_dir: Output directory (defaults to same as input)

    Returns:
        Path to created YAML file
    """
    # Parse XML
    data = parse_station_xml(xml_path)

    # Determine output path
    if output_dir is None:
        output_dir = xml_path.parent

    yaml_path = output_dir / xml_path.with_suffix(".yaml").name

    # Write YAML with custom formatting
    with open(yaml_path, "w") as f:
        f.write(f"# Station configuration converted from {xml_path.name}\n")
        f.write(f"# Stations: {len(data['stations'])}\n")
        f.write("#\n")
        f.write("# Fields:\n")
        f.write("#   id: 4-character station ID (lowercase)\n")
        f.write("#   domes: DOMES number\n")
        f.write("#   name: Full station name\n")
        f.write("#   primary_net: Primary network (IGS20, EUREF, etc.)\n")
        f.write("#   type: Station type (core, EUREF, OS active, etc.)\n")
        f.write("#   use_nrt: NRT processing enabled (true/false)\n")
        f.write("#   coordinates: ECEF coordinates (x, y, z in meters)\n")
        f.write("\n")

        # Write metadata section
        yaml.dump({"metadata": data["metadata"]}, f, default_flow_style=False, sort_keys=False)
        f.write("\n")

        # Write stations section
        f.write("stations:\n")
        for station in data["stations"]:
            # Write each station as a compact block
            f.write(f"  - id: {station['id']}\n")
            for key, value in station.items():
                if key == "id":
                    continue
                if key == "coordinates":
                    f.write(f"    coordinates: {{x: {value['x']}, y: {value['y']}, z: {value['z']}}}\n")
                elif isinstance(value, bool):
                    f.write(f"    {key}: {str(value).lower()}\n")
                elif isinstance(value, str) and value:
                    # Quote strings that might have special chars
                    if any(c in value for c in ":#[]{}"):
                        f.write(f"    {key}: \"{value}\"\n")
                    else:
                        f.write(f"    {key}: {value}\n")
                elif value is not None:
                    f.write(f"    {key}: {value}\n")

    return yaml_path


def main():
    parser = argparse.ArgumentParser(
        description="Convert station XML files to YAML format"
    )
    parser.add_argument(
        "--input-dir", "-i",
        type=Path,
        default=Path(__file__).parent.parent / "station_data",
        help="Input directory containing XML files",
    )
    parser.add_argument(
        "--output-dir", "-o",
        type=Path,
        default=None,
        help="Output directory for YAML files (defaults to input dir)",
    )
    parser.add_argument(
        "--file", "-f",
        type=str,
        default=None,
        help="Convert only this specific file (e.g., IGS20gh.xml)",
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Verbose output",
    )

    args = parser.parse_args()

    input_dir = args.input_dir
    output_dir = args.output_dir or input_dir

    if not input_dir.exists():
        print(f"Error: Input directory not found: {input_dir}")
        sys.exit(1)

    output_dir.mkdir(parents=True, exist_ok=True)

    # Find XML files to convert
    if args.file:
        xml_files = [input_dir / args.file]
        if not xml_files[0].exists():
            print(f"Error: File not found: {xml_files[0]}")
            sys.exit(1)
    else:
        xml_files = sorted(input_dir.glob("*.xml"))

    if not xml_files:
        print(f"No XML files found in {input_dir}")
        sys.exit(1)

    print(f"Converting {len(xml_files)} XML files to YAML...")
    print(f"Input: {input_dir}")
    print(f"Output: {output_dir}")
    print()

    for xml_path in xml_files:
        try:
            yaml_path = convert_xml_to_yaml(xml_path, output_dir)
            data = parse_station_xml(xml_path)
            nrt_count = sum(1 for s in data["stations"] if s.get("use_nrt", False))
            print(f"  {xml_path.name} -> {yaml_path.name}")
            print(f"    {len(data['stations'])} stations ({nrt_count} NRT-enabled)")
        except Exception as e:
            print(f"  ERROR: {xml_path.name}: {e}")

    print()
    print("Conversion complete!")


if __name__ == "__main__":
    main()
