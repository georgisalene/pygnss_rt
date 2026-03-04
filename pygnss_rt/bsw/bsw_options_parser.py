"""
BSW options file parser.

Parses Bernese processing options from YAML or XML configuration files.
Extracted from bpe_runner.py for modularity.
"""

from __future__ import annotations

from pathlib import Path


def parse_bsw_options_file(config_path: Path) -> dict[str, dict[str, dict[str, str]]]:
    """Parse BSW options from YAML or XML file.

    Supports both YAML (preferred) and XML (legacy) formats.

    YAML format:
        bern_options:
            D_PPPGEN:
                CODSPP:
                    key1: value1

    XML format:
        <recipe>
            <bernOptions>
                <D_PPPGEN>
                    <CODSPP>
                        <key1>value1</key1>
                    </CODSPP>
                </D_PPPGEN>
            </bernOptions>
        </recipe>

    Args:
        config_path: Path to YAML or XML file

    Returns:
        Nested dict: opt_dir -> inp_file -> key -> value
    """
    path = Path(config_path)

    # Try YAML first
    yaml_path = path.with_suffix('.yaml')
    xml_path = path.with_suffix('.xml')

    if yaml_path.exists():
        return _parse_bsw_options_yaml(yaml_path)
    elif path.suffix == '.yaml' and path.exists():
        return _parse_bsw_options_yaml(path)
    elif xml_path.exists():
        return _parse_bsw_options_xml(xml_path)
    elif path.exists():
        if path.suffix == '.yaml':
            return _parse_bsw_options_yaml(path)
        else:
            return _parse_bsw_options_xml(path)

    return {}


def _parse_bsw_options_yaml(yaml_path: Path) -> dict[str, dict[str, dict[str, str]]]:
    """Parse BSW options from YAML file.

    Args:
        yaml_path: Path to YAML file

    Returns:
        Nested dict: opt_dir -> inp_file -> key -> value
    """
    import yaml

    if not yaml_path.exists():
        return {}

    with open(yaml_path) as f:
        data = yaml.safe_load(f)

    result: dict[str, dict[str, dict[str, str]]] = {}

    bern_opts = data.get("bern_options", {})

    for opt_name, opt_data in bern_opts.items():
        result[opt_name] = {}
        if isinstance(opt_data, dict):
            for inp_name, inp_data in opt_data.items():
                result[opt_name][inp_name] = {}
                if isinstance(inp_data, dict):
                    for key_name, key_value in inp_data.items():
                        # Convert to string for consistency
                        result[opt_name][inp_name][key_name] = str(key_value) if key_value is not None else ""

    return result


def _parse_bsw_options_xml(xml_path: Path) -> dict[str, dict[str, dict[str, str]]]:
    """Parse BSW options from XML file (legacy format).

    Args:
        xml_path: Path to XML file

    Returns:
        Nested dict: opt_dir -> inp_file -> key -> value
    """
    from xml.etree import ElementTree

    if not xml_path.exists():
        return {}

    tree = ElementTree.parse(xml_path)
    root = tree.getroot()

    result: dict[str, dict[str, dict[str, str]]] = {}

    # Find bernOptions element
    bern_opts = root.find(".//bernOptions")
    if bern_opts is None:
        # Try recipe/bernOptions
        bern_opts = root.find("recipe/bernOptions")

    if bern_opts is None:
        return {}

    # Iterate over OPT directories
    for opt_elem in bern_opts:
        opt_name = opt_elem.tag
        result[opt_name] = {}

        # Iterate over INP files
        for inp_elem in opt_elem:
            inp_name = inp_elem.tag
            result[opt_name][inp_name] = {}

            # Iterate over keys
            for key_elem in inp_elem:
                key_name = key_elem.tag
                key_value = key_elem.text or ""
                result[opt_name][inp_name][key_name] = key_value.strip()

    return result


# Backward compatibility alias
def parse_bsw_options_xml(xml_path: Path) -> dict[str, dict[str, dict[str, str]]]:
    """Parse BSW options file (backward compatibility alias).

    Now supports both YAML and XML formats.
    """
    return parse_bsw_options_file(xml_path)
