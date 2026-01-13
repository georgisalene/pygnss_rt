#!/usr/bin/env python3
"""
Convert BSW options XML files to YAML format.

This script converts the Perl-era XML configuration files to Python-friendly YAML format.
The YAML files have the same structure but are more readable and maintainable.

Usage:
    python convert_xml_to_yaml.py [--all] [--file FILE]
"""

import argparse
import sys
import xml.etree.ElementTree as ET
from pathlib import Path

import yaml


def xml_to_dict(element: ET.Element) -> dict | str:
    """Convert XML element to dictionary recursively."""
    result = {}

    # If element has no children, return its text content
    if len(element) == 0:
        return (element.text or "").strip()

    # Process children
    for child in element:
        child_data = xml_to_dict(child)

        # Handle duplicate keys (shouldn't happen in BSW configs)
        if child.tag in result:
            # Convert to list if not already
            if not isinstance(result[child.tag], list):
                result[child.tag] = [result[child.tag]]
            result[child.tag].append(child_data)
        else:
            result[child.tag] = child_data

    return result


def convert_bsw_xml_to_yaml(xml_path: Path) -> dict:
    """Convert BSW options XML file to YAML-compatible dict.

    Args:
        xml_path: Path to XML file

    Returns:
        Dictionary suitable for YAML serialization
    """
    tree = ET.parse(xml_path)
    root = tree.getroot()

    # Build header from recipe attributes
    result = {
        "recipe": {
            "target": root.get("target", "Processor"),
            "version": root.get("version", "1.0"),
            "author": root.get("author", ""),
        },
        "bern_options": {},
    }

    # Parse bernOptions section
    bern_options = root.find("bernOptions")
    if bern_options is not None:
        for step_elem in bern_options:
            step_name = step_elem.tag
            step_data = {}

            for prog_elem in step_elem:
                program_name = prog_elem.tag
                program_data = {}

                for opt_elem in prog_elem:
                    opt_name = opt_elem.tag
                    opt_value = (opt_elem.text or "").strip()
                    program_data[opt_name] = opt_value

                step_data[program_name] = program_data

            result["bern_options"][step_name] = step_data

    return result


def save_yaml(data: dict, yaml_path: Path) -> None:
    """Save dictionary to YAML file with nice formatting."""

    # Custom representer for multi-line strings
    def str_representer(dumper, data):
        if '\n' in data:
            return dumper.represent_scalar('tag:yaml.org,2002:str', data, style='|')
        return dumper.represent_scalar('tag:yaml.org,2002:str', data)

    yaml.add_representer(str, str_representer)

    with open(yaml_path, 'w') as f:
        yaml.dump(
            data,
            f,
            default_flow_style=False,
            allow_unicode=True,
            sort_keys=False,
            width=120,
        )


def convert_file(xml_path: Path, output_dir: Path | None = None) -> Path:
    """Convert a single XML file to YAML.

    Args:
        xml_path: Path to input XML file
        output_dir: Optional output directory (defaults to same as input)

    Returns:
        Path to created YAML file
    """
    if output_dir is None:
        output_dir = xml_path.parent

    yaml_path = output_dir / xml_path.with_suffix('.yaml').name

    print(f"Converting: {xml_path.name}")
    data = convert_bsw_xml_to_yaml(xml_path)
    save_yaml(data, yaml_path)
    print(f"  -> {yaml_path.name}")

    return yaml_path


def main():
    parser = argparse.ArgumentParser(
        description="Convert BSW options XML files to YAML"
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Convert all XML files in the bsw_configs directory"
    )
    parser.add_argument(
        "--file", "-f",
        type=Path,
        help="Convert a specific XML file"
    )
    parser.add_argument(
        "--output", "-o",
        type=Path,
        help="Output directory (defaults to same as input)"
    )

    args = parser.parse_args()

    # Determine script directory
    script_dir = Path(__file__).parent

    if args.file:
        xml_path = args.file if args.file.is_absolute() else script_dir / args.file
        convert_file(xml_path, args.output)
    elif args.all:
        xml_files = list(script_dir.glob("*.xml"))
        if not xml_files:
            print("No XML files found in bsw_configs directory")
            sys.exit(1)

        print(f"Converting {len(xml_files)} XML files...")
        for xml_path in xml_files:
            convert_file(xml_path, args.output)
        print(f"\nDone! Converted {len(xml_files)} files.")
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
