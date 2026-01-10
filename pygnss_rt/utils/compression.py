"""
RINEX and data compression utilities.

Provides comprehensive compression/decompression support for GNSS data files:
- Hatanaka compression/decompression (CRX <-> RNX)
- Standard compression (gzip, compress, zip)
- Legacy Z-file support
- Batch processing capabilities

This module replaces compression-related functions from Perl UTIL.pm and IGNSS.pm.

Usage:
    from pygnss_rt.utils.compression import (
        decompress_hatanaka,
        compress_hatanaka,
        decompress_file,
        compress_file,
    )

    # Decompress Hatanaka-compressed RINEX
    rnx_file = decompress_hatanaka("/path/to/file.crx.gz")

    # Auto-detect and decompress any format
    output = decompress_file("/path/to/file.24o.Z")
"""

from __future__ import annotations

import gzip
import logging
import os
import shutil
import subprocess
import tempfile
import zipfile
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


class CompressionFormat(str, Enum):
    """Supported compression formats."""

    GZIP = "gz"
    COMPRESS = "Z"
    ZIP = "zip"
    BZIP2 = "bz2"
    NONE = ""


class HatanakaFormat(str, Enum):
    """Hatanaka compression state."""

    COMPRESSED = "crx"  # Hatanaka compressed
    UNCOMPRESSED = "rnx"  # Standard RINEX


@dataclass
class CompressionResult:
    """Result of compression/decompression operation."""

    success: bool
    input_path: Path
    output_path: Optional[Path] = None
    original_size: int = 0
    final_size: int = 0
    compression_ratio: float = 0.0
    error: str = ""


# =============================================================================
# Hatanaka Compression (CRX <-> RNX)
# =============================================================================

def find_hatanaka_tools() -> tuple[Optional[Path], Optional[Path]]:
    """Find CRX2RNX and RNX2CRX executables.

    Searches in:
    1. System PATH
    2. BSW environment ($X/EXE)
    3. Common installation locations

    Returns:
        Tuple of (crx2rnx_path, rnx2crx_path) or (None, None) if not found
    """
    crx2rnx = None
    rnx2crx = None

    # Tool names (case variations)
    crx2rnx_names = ["crx2rnx", "CRX2RNX", "crx2rnx.exe", "CRX2RNX.exe"]
    rnx2crx_names = ["rnx2crx", "RNX2CRX", "rnx2crx.exe", "RNX2CRX.exe"]

    # Search paths
    search_paths = []

    # 1. Check system PATH
    path_dirs = os.environ.get("PATH", "").split(os.pathsep)
    search_paths.extend(path_dirs)

    # 2. BSW installation
    if os.environ.get("X"):
        search_paths.append(f"{os.environ['X']}/EXE")
    if os.environ.get("BPE"):
        search_paths.append(os.environ["BPE"])

    # 3. Common locations
    common_paths = [
        "/usr/local/bin",
        "/usr/bin",
        "/opt/rnx2crx",
        Path.home() / "bin",
        Path.home() / ".local" / "bin",
    ]
    search_paths.extend(str(p) for p in common_paths)

    # Search for tools
    for search_dir in search_paths:
        if not search_dir:
            continue
        search_path = Path(search_dir)
        if not search_path.exists():
            continue

        for name in crx2rnx_names:
            tool_path = search_path / name
            if tool_path.exists() and os.access(tool_path, os.X_OK):
                crx2rnx = tool_path
                break

        for name in rnx2crx_names:
            tool_path = search_path / name
            if tool_path.exists() and os.access(tool_path, os.X_OK):
                rnx2crx = tool_path
                break

        if crx2rnx and rnx2crx:
            break

    # Also try shutil.which
    if not crx2rnx:
        for name in crx2rnx_names:
            found = shutil.which(name)
            if found:
                crx2rnx = Path(found)
                break

    if not rnx2crx:
        for name in rnx2crx_names:
            found = shutil.which(name)
            if found:
                rnx2crx = Path(found)
                break

    return crx2rnx, rnx2crx


def decompress_hatanaka(
    input_path: Path | str,
    output_path: Optional[Path | str] = None,
    keep_original: bool = True,
    timeout: int = 60,
) -> CompressionResult:
    """Decompress Hatanaka-compressed RINEX file (CRX -> RNX).

    Uses CRX2RNX tool to convert compact RINEX to standard RINEX format.

    Args:
        input_path: Path to .crx file (may also be gzipped)
        output_path: Output path (auto-generated if None)
        keep_original: Keep the original file
        timeout: Command timeout in seconds

    Returns:
        CompressionResult with operation status
    """
    input_path = Path(input_path)
    result = CompressionResult(
        success=False,
        input_path=input_path,
        original_size=input_path.stat().st_size if input_path.exists() else 0,
    )

    if not input_path.exists():
        result.error = f"Input file not found: {input_path}"
        return result

    # Find CRX2RNX tool
    crx2rnx, _ = find_hatanaka_tools()
    if not crx2rnx:
        result.error = "CRX2RNX tool not found in PATH or BSW installation"
        return result

    # Handle gzip compression first
    temp_crx = None
    working_file = input_path

    if str(input_path).lower().endswith(".gz"):
        # Decompress gzip first
        temp_crx = input_path.parent / input_path.stem
        try:
            with gzip.open(input_path, "rb") as f_in:
                with open(temp_crx, "wb") as f_out:
                    shutil.copyfileobj(f_in, f_out)
            working_file = temp_crx
        except Exception as e:
            result.error = f"Failed to decompress gzip: {e}"
            return result

    # Determine output path
    if output_path:
        output_path = Path(output_path)
    else:
        # Replace .crx with .rnx (or .??d with .??o for RINEX 2)
        name = working_file.name
        if name.lower().endswith(".crx"):
            output_name = name[:-4] + ".rnx"
        elif len(name) > 3 and name[-1].lower() == "d":
            # RINEX 2 format: ssssdddf.yyd -> ssssdddf.yyo
            output_name = name[:-1] + "o"
        else:
            output_name = name + ".rnx"
        output_path = working_file.parent / output_name

    # Run CRX2RNX
    try:
        # CRX2RNX typically outputs to stdout or creates output file
        cmd = [str(crx2rnx), str(working_file)]

        # Check if tool supports -f (force overwrite) flag
        proc = subprocess.run(
            cmd,
            capture_output=True,
            timeout=timeout,
            cwd=working_file.parent,
        )

        # CRX2RNX may create output file automatically
        # or we may need to redirect stdout
        if proc.returncode != 0:
            # Try with output redirection
            cmd = [str(crx2rnx), "-", str(working_file)]
            with open(output_path, "wb") as f_out:
                proc = subprocess.run(
                    cmd,
                    stdout=f_out,
                    stderr=subprocess.PIPE,
                    timeout=timeout,
                    cwd=working_file.parent,
                )

        if proc.returncode != 0:
            result.error = f"CRX2RNX failed: {proc.stderr.decode()}"
            return result

        # Check if output was created (might be auto-named)
        if not output_path.exists():
            # Try to find auto-generated output
            possible_outputs = list(working_file.parent.glob(
                working_file.stem.replace(".crx", "") + "*.rnx"
            ))
            if possible_outputs:
                # Move to expected output path
                shutil.move(possible_outputs[0], output_path)

        if output_path.exists():
            result.success = True
            result.output_path = output_path
            result.final_size = output_path.stat().st_size
            if result.original_size > 0:
                result.compression_ratio = result.final_size / result.original_size

    except subprocess.TimeoutExpired:
        result.error = f"CRX2RNX timed out after {timeout}s"
    except Exception as e:
        result.error = f"CRX2RNX failed: {e}"
    finally:
        # Cleanup temp file
        if temp_crx and temp_crx.exists():
            temp_crx.unlink()

        # Remove original if requested
        if result.success and not keep_original and input_path.exists():
            input_path.unlink()

    return result


def compress_hatanaka(
    input_path: Path | str,
    output_path: Optional[Path | str] = None,
    keep_original: bool = True,
    timeout: int = 60,
) -> CompressionResult:
    """Compress RINEX file using Hatanaka compression (RNX -> CRX).

    Uses RNX2CRX tool to convert standard RINEX to compact format.

    Args:
        input_path: Path to .rnx/.??o file
        output_path: Output path (auto-generated if None)
        keep_original: Keep the original file
        timeout: Command timeout in seconds

    Returns:
        CompressionResult with operation status
    """
    input_path = Path(input_path)
    result = CompressionResult(
        success=False,
        input_path=input_path,
        original_size=input_path.stat().st_size if input_path.exists() else 0,
    )

    if not input_path.exists():
        result.error = f"Input file not found: {input_path}"
        return result

    # Find RNX2CRX tool
    _, rnx2crx = find_hatanaka_tools()
    if not rnx2crx:
        result.error = "RNX2CRX tool not found in PATH or BSW installation"
        return result

    # Determine output path
    if output_path:
        output_path = Path(output_path)
    else:
        name = input_path.name
        if name.lower().endswith(".rnx"):
            output_name = name[:-4] + ".crx"
        elif len(name) > 3 and name[-1].lower() == "o":
            # RINEX 2 format: ssssdddf.yyo -> ssssdddf.yyd
            output_name = name[:-1] + "d"
        else:
            output_name = name + ".crx"
        output_path = input_path.parent / output_name

    # Run RNX2CRX
    try:
        cmd = [str(rnx2crx), str(input_path)]

        proc = subprocess.run(
            cmd,
            capture_output=True,
            timeout=timeout,
            cwd=input_path.parent,
        )

        if proc.returncode != 0:
            # Try with output redirection
            with open(output_path, "wb") as f_out:
                proc = subprocess.run(
                    [str(rnx2crx), "-", str(input_path)],
                    stdout=f_out,
                    stderr=subprocess.PIPE,
                    timeout=timeout,
                )

        if proc.returncode != 0:
            result.error = f"RNX2CRX failed: {proc.stderr.decode()}"
            return result

        # Check for auto-generated output
        if not output_path.exists():
            possible_outputs = list(input_path.parent.glob(
                input_path.stem.replace(".rnx", "") + "*.crx"
            ))
            if possible_outputs:
                shutil.move(possible_outputs[0], output_path)

        if output_path.exists():
            result.success = True
            result.output_path = output_path
            result.final_size = output_path.stat().st_size
            if result.original_size > 0:
                result.compression_ratio = result.final_size / result.original_size

    except subprocess.TimeoutExpired:
        result.error = f"RNX2CRX timed out after {timeout}s"
    except Exception as e:
        result.error = f"RNX2CRX failed: {e}"
    finally:
        if result.success and not keep_original and input_path.exists():
            input_path.unlink()

    return result


# =============================================================================
# Standard Compression (gzip, Z, zip, bz2)
# =============================================================================

def decompress_gzip(
    input_path: Path | str,
    output_path: Optional[Path | str] = None,
    keep_original: bool = True,
) -> CompressionResult:
    """Decompress gzip file.

    Args:
        input_path: Path to .gz file
        output_path: Output path (auto-generated if None)
        keep_original: Keep the original file

    Returns:
        CompressionResult
    """
    input_path = Path(input_path)
    result = CompressionResult(
        success=False,
        input_path=input_path,
        original_size=input_path.stat().st_size if input_path.exists() else 0,
    )

    if not input_path.exists():
        result.error = f"Input file not found: {input_path}"
        return result

    if output_path:
        output_path = Path(output_path)
    else:
        # Remove .gz extension
        output_path = input_path.parent / input_path.stem

    try:
        with gzip.open(input_path, "rb") as f_in:
            with open(output_path, "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)

        result.success = True
        result.output_path = output_path
        result.final_size = output_path.stat().st_size
        if result.original_size > 0:
            result.compression_ratio = result.final_size / result.original_size

        if not keep_original:
            input_path.unlink()

    except Exception as e:
        result.error = f"Gzip decompression failed: {e}"

    return result


def compress_gzip(
    input_path: Path | str,
    output_path: Optional[Path | str] = None,
    keep_original: bool = True,
    compression_level: int = 9,
) -> CompressionResult:
    """Compress file with gzip.

    Args:
        input_path: Path to input file
        output_path: Output path (auto-generated if None)
        keep_original: Keep the original file
        compression_level: Compression level (1-9)

    Returns:
        CompressionResult
    """
    input_path = Path(input_path)
    result = CompressionResult(
        success=False,
        input_path=input_path,
        original_size=input_path.stat().st_size if input_path.exists() else 0,
    )

    if not input_path.exists():
        result.error = f"Input file not found: {input_path}"
        return result

    if output_path:
        output_path = Path(output_path)
    else:
        output_path = input_path.parent / (input_path.name + ".gz")

    try:
        with open(input_path, "rb") as f_in:
            with gzip.open(output_path, "wb", compresslevel=compression_level) as f_out:
                shutil.copyfileobj(f_in, f_out)

        result.success = True
        result.output_path = output_path
        result.final_size = output_path.stat().st_size
        if result.original_size > 0:
            result.compression_ratio = result.final_size / result.original_size

        if not keep_original:
            input_path.unlink()

    except Exception as e:
        result.error = f"Gzip compression failed: {e}"

    return result


def decompress_z_file(
    input_path: Path | str,
    output_path: Optional[Path | str] = None,
    keep_original: bool = True,
) -> CompressionResult:
    """Decompress Unix compress (.Z) file.

    Uses system 'uncompress' or 'gzip -d' command.

    Args:
        input_path: Path to .Z file
        output_path: Output path (auto-generated if None)
        keep_original: Keep the original file

    Returns:
        CompressionResult
    """
    input_path = Path(input_path)
    result = CompressionResult(
        success=False,
        input_path=input_path,
        original_size=input_path.stat().st_size if input_path.exists() else 0,
    )

    if not input_path.exists():
        result.error = f"Input file not found: {input_path}"
        return result

    if output_path:
        output_path = Path(output_path)
    else:
        # Remove .Z extension
        if str(input_path).upper().endswith(".Z"):
            output_path = input_path.parent / input_path.name[:-2]
        else:
            output_path = input_path.parent / (input_path.name + ".uncompressed")

    # Try different decompression methods
    methods = [
        # Method 1: uncompress command
        ["uncompress", "-c", str(input_path)],
        # Method 2: gzip can handle .Z files
        ["gzip", "-d", "-c", str(input_path)],
        # Method 3: zcat
        ["zcat", str(input_path)],
    ]

    for cmd in methods:
        if not shutil.which(cmd[0]):
            continue

        try:
            with open(output_path, "wb") as f_out:
                proc = subprocess.run(
                    cmd,
                    stdout=f_out,
                    stderr=subprocess.PIPE,
                    timeout=120,
                )

            if proc.returncode == 0 and output_path.exists():
                result.success = True
                result.output_path = output_path
                result.final_size = output_path.stat().st_size
                if result.original_size > 0:
                    result.compression_ratio = result.final_size / result.original_size

                if not keep_original:
                    input_path.unlink()
                return result

        except Exception as e:
            logger.debug(f"Decompression method {cmd[0]} failed: {e}")
            continue

    result.error = "No working decompression method found for .Z file"
    return result


def compress_z_file(
    input_path: Path | str,
    output_path: Optional[Path | str] = None,
    keep_original: bool = True,
) -> CompressionResult:
    """Compress file using Unix compress (.Z format).

    Uses system 'compress' command.

    Args:
        input_path: Path to input file
        output_path: Output path (auto-generated if None)
        keep_original: Keep the original file

    Returns:
        CompressionResult
    """
    input_path = Path(input_path)
    result = CompressionResult(
        success=False,
        input_path=input_path,
        original_size=input_path.stat().st_size if input_path.exists() else 0,
    )

    if not input_path.exists():
        result.error = f"Input file not found: {input_path}"
        return result

    compress_cmd = shutil.which("compress")
    if not compress_cmd:
        result.error = "compress command not found"
        return result

    if output_path:
        output_path = Path(output_path)
    else:
        output_path = input_path.parent / (input_path.name + ".Z")

    try:
        with open(output_path, "wb") as f_out:
            proc = subprocess.run(
                ["compress", "-c", str(input_path)],
                stdout=f_out,
                stderr=subprocess.PIPE,
                timeout=120,
            )

        if proc.returncode == 0:
            result.success = True
            result.output_path = output_path
            result.final_size = output_path.stat().st_size
            if result.original_size > 0:
                result.compression_ratio = result.final_size / result.original_size

            if not keep_original:
                input_path.unlink()
        else:
            result.error = f"compress failed: {proc.stderr.decode()}"

    except Exception as e:
        result.error = f"Compression failed: {e}"

    return result


def decompress_zip(
    input_path: Path | str,
    output_dir: Optional[Path | str] = None,
    keep_original: bool = True,
) -> CompressionResult:
    """Decompress ZIP archive.

    Args:
        input_path: Path to .zip file
        output_dir: Output directory (uses input dir if None)
        keep_original: Keep the original file

    Returns:
        CompressionResult (output_path is directory for multi-file archives)
    """
    input_path = Path(input_path)
    result = CompressionResult(
        success=False,
        input_path=input_path,
        original_size=input_path.stat().st_size if input_path.exists() else 0,
    )

    if not input_path.exists():
        result.error = f"Input file not found: {input_path}"
        return result

    if output_dir:
        output_dir = Path(output_dir)
    else:
        output_dir = input_path.parent

    output_dir.mkdir(parents=True, exist_ok=True)

    try:
        with zipfile.ZipFile(input_path, "r") as zf:
            zf.extractall(output_dir)
            extracted = zf.namelist()

        result.success = True
        # If single file, return its path
        if len(extracted) == 1:
            result.output_path = output_dir / extracted[0]
        else:
            result.output_path = output_dir

        result.final_size = sum(
            (output_dir / f).stat().st_size
            for f in extracted
            if (output_dir / f).exists()
        )
        if result.original_size > 0:
            result.compression_ratio = result.final_size / result.original_size

        if not keep_original:
            input_path.unlink()

    except Exception as e:
        result.error = f"ZIP extraction failed: {e}"

    return result


# =============================================================================
# Auto-Detection Functions
# =============================================================================

def detect_compression(filepath: Path | str) -> tuple[CompressionFormat, bool]:
    """Detect compression format and Hatanaka status.

    Args:
        filepath: Path to file

    Returns:
        Tuple of (CompressionFormat, is_hatanaka_compressed)
    """
    filepath = Path(filepath)
    name = filepath.name.lower()

    # Check standard compression
    compression = CompressionFormat.NONE
    if name.endswith(".gz"):
        compression = CompressionFormat.GZIP
        name = name[:-3]
    elif name.endswith(".z"):
        compression = CompressionFormat.COMPRESS
        name = name[:-2]
    elif name.endswith(".zip"):
        compression = CompressionFormat.ZIP
        name = name[:-4]
    elif name.endswith(".bz2"):
        compression = CompressionFormat.BZIP2
        name = name[:-4]

    # Check Hatanaka
    is_hatanaka = False
    if name.endswith(".crx"):
        is_hatanaka = True
    elif len(name) > 3 and name[-1] == "d" and name[-2].isdigit():
        # RINEX 2 compact format: ssssdddf.yyd
        is_hatanaka = True

    return compression, is_hatanaka


def decompress_file(
    input_path: Path | str,
    output_path: Optional[Path | str] = None,
    keep_original: bool = True,
    auto_hatanaka: bool = True,
) -> CompressionResult:
    """Auto-detect and decompress file.

    Handles all supported compression formats and Hatanaka compression.

    Args:
        input_path: Path to compressed file
        output_path: Output path (auto-generated if None)
        keep_original: Keep the original file
        auto_hatanaka: Also decompress Hatanaka if detected

    Returns:
        CompressionResult
    """
    input_path = Path(input_path)
    compression, is_hatanaka = detect_compression(input_path)

    # First handle standard compression
    if compression == CompressionFormat.GZIP:
        result = decompress_gzip(input_path, output_path, keep_original)
    elif compression == CompressionFormat.COMPRESS:
        result = decompress_z_file(input_path, output_path, keep_original)
    elif compression == CompressionFormat.ZIP:
        result = decompress_zip(input_path, output_path, keep_original)
    elif compression == CompressionFormat.BZIP2:
        # Use bz2 module
        import bz2
        result = CompressionResult(success=False, input_path=input_path)
        try:
            if output_path:
                out = Path(output_path)
            else:
                out = input_path.parent / input_path.stem

            with bz2.open(input_path, "rb") as f_in:
                with open(out, "wb") as f_out:
                    shutil.copyfileobj(f_in, f_out)

            result.success = True
            result.output_path = out
            result.final_size = out.stat().st_size
        except Exception as e:
            result.error = str(e)
    else:
        # No standard compression
        result = CompressionResult(
            success=True,
            input_path=input_path,
            output_path=input_path,
            original_size=input_path.stat().st_size,
            final_size=input_path.stat().st_size,
        )

    # Handle Hatanaka if needed
    if result.success and auto_hatanaka and is_hatanaka:
        working_file = result.output_path or input_path
        hat_result = decompress_hatanaka(working_file, keep_original=keep_original)

        if hat_result.success:
            # Cleanup intermediate file if different from input
            if keep_original and working_file != input_path and working_file.exists():
                working_file.unlink()
            return hat_result
        else:
            # Return Hatanaka error but preserve standard decompression
            result.error = f"Standard decompression OK, Hatanaka failed: {hat_result.error}"

    return result


def compress_file(
    input_path: Path | str,
    output_path: Optional[Path | str] = None,
    format: CompressionFormat = CompressionFormat.GZIP,
    use_hatanaka: bool = False,
    keep_original: bool = True,
) -> CompressionResult:
    """Compress file with specified format.

    Args:
        input_path: Path to input file
        output_path: Output path (auto-generated if None)
        format: Compression format to use
        use_hatanaka: Apply Hatanaka compression for RINEX files
        keep_original: Keep the original file

    Returns:
        CompressionResult
    """
    input_path = Path(input_path)
    working_path = input_path

    # Apply Hatanaka first if requested
    if use_hatanaka:
        hat_result = compress_hatanaka(input_path, keep_original=keep_original)
        if hat_result.success:
            working_path = hat_result.output_path
        else:
            return hat_result

    # Apply standard compression
    if format == CompressionFormat.GZIP:
        return compress_gzip(working_path, output_path, keep_original)
    elif format == CompressionFormat.COMPRESS:
        return compress_z_file(working_path, output_path, keep_original)
    else:
        # No compression or unsupported format
        return CompressionResult(
            success=True,
            input_path=input_path,
            output_path=working_path,
            original_size=input_path.stat().st_size,
            final_size=working_path.stat().st_size,
        )


# =============================================================================
# Batch Processing
# =============================================================================

def decompress_directory(
    input_dir: Path | str,
    output_dir: Optional[Path | str] = None,
    pattern: str = "*",
    recursive: bool = False,
    keep_original: bool = True,
    auto_hatanaka: bool = True,
) -> list[CompressionResult]:
    """Decompress all matching files in a directory.

    Args:
        input_dir: Input directory
        output_dir: Output directory (uses input dir if None)
        pattern: Glob pattern for files to process
        recursive: Process subdirectories
        keep_original: Keep original files
        auto_hatanaka: Also decompress Hatanaka

    Returns:
        List of CompressionResults
    """
    input_dir = Path(input_dir)
    output_dir = Path(output_dir) if output_dir else input_dir

    results = []

    if recursive:
        files = input_dir.rglob(pattern)
    else:
        files = input_dir.glob(pattern)

    for filepath in files:
        if not filepath.is_file():
            continue

        # Determine relative output path
        rel_path = filepath.relative_to(input_dir)
        out_path = output_dir / rel_path.parent / rel_path.stem

        result = decompress_file(
            filepath,
            out_path,
            keep_original=keep_original,
            auto_hatanaka=auto_hatanaka,
        )
        results.append(result)

    return results


# =============================================================================
# Convenience Functions
# =============================================================================

def is_compressed(filepath: Path | str) -> bool:
    """Check if file is compressed.

    Args:
        filepath: Path to check

    Returns:
        True if file has compression extension
    """
    compression, is_hatanaka = detect_compression(filepath)
    return compression != CompressionFormat.NONE or is_hatanaka


def get_uncompressed_name(filepath: Path | str) -> str:
    """Get the uncompressed filename.

    Args:
        filepath: Path to compressed file

    Returns:
        Filename with compression extensions removed
    """
    filepath = Path(filepath)
    name = filepath.name

    # Remove standard compression extension
    for ext in [".gz", ".Z", ".zip", ".bz2"]:
        if name.lower().endswith(ext.lower()):
            name = name[:-len(ext)]
            break

    # Handle Hatanaka
    if name.lower().endswith(".crx"):
        name = name[:-4] + ".rnx"
    elif len(name) > 3 and name[-1].lower() == "d" and name[-2].isdigit():
        name = name[:-1] + "o"

    return name
