#!/usr/bin/env python3
"""Audit the native compatibility boundary of a DogPaddle release archive."""

from __future__ import annotations

import argparse
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile

from release_archive import extract

LINUX_GLIBC_BASELINE = (2, 28)
MACOS_DEPLOYMENT_TARGET = (11, 0)
LINUX_SYSTEM_LIBRARIES = {
    "ld-linux-aarch64.so.1",
    "ld-linux-x86-64.so.2",
    "libc.so.6",
    "libdl.so.2",
    "libm.so.6",
    "libpthread.so.0",
    "libresolv.so.2",
    "librt.so.1",
    "libutil.so.1",
}
VERSIONED_SYMBOL = re.compile(r"\b(GLIBC|GLIBCXX|CXXABI)_([0-9]+(?:\.[0-9]+)+)\b")


MAX_TOOL_OUTPUT = 1024 * 1024
MACHO_MAGICS = {
    bytes.fromhex(value)
    for value in (
        "feedface",
        "feedfacf",
        "cefaedfe",
        "cffaedfe",
        "cafebabe",
        "cafebabf",
        "bebafeca",
        "bfbafeca",
    )
}


def run(*command: str) -> str:
    result = subprocess.run(
        command,
        check=True,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=30,
    )
    if len(result.stdout) > MAX_TOOL_OUTPUT:
        raise RuntimeError(f"tool output exceeded limit: {command[0]}")
    return result.stdout


def version(value: str) -> tuple[int, ...]:
    return tuple(int(component) for component in value.split("."))


def files(root: Path) -> list[Path]:
    return sorted(path for path in root.rglob("*") if path.is_file())


def inside(root: Path, path: Path) -> bool:
    resolved_root = root.resolve()
    resolved_path = path.resolve()
    return resolved_path == resolved_root or resolved_root in resolved_path.parents


def elf_search_directory(root: Path, binary: Path, entry: str) -> Path:
    if entry.startswith("${ORIGIN}"):
        suffix = entry[len("${ORIGIN}") :]
    elif entry.startswith("$ORIGIN"):
        suffix = entry[len("$ORIGIN") :]
    else:
        raise RuntimeError(
            f"{binary.relative_to(root)} has non-portable library path {entry}"
        )
    if suffix and not suffix.startswith("/"):
        raise RuntimeError(
            f"{binary.relative_to(root)} has malformed ORIGIN path {entry}"
        )
    directory = (binary.parent / suffix.removeprefix("/")).resolve()
    if not inside(root, directory):
        raise RuntimeError(
            f"{binary.relative_to(root)} has library path outside the archive: {entry}"
        )
    return directory


def audit_linux(root: Path, target: str) -> list[str]:
    readelf = shutil.which("readelf")
    if readelf is None:
        raise RuntimeError("Linux release audit requires readelf")
    elf_files = []
    expected_machine = {
        "x86_64-unknown-linux-gnu": "Advanced Micro Devices X86-64",
        "aarch64-unknown-linux-gnu": "AArch64",
    }[target]
    for path in files(root):
        with path.open("rb") as source:
            if source.read(4) != b"\x7fELF":
                continue
        header = run(readelf, "--file-header", str(path))
        match = re.search(r"^\s*Machine:\s*(.+)$", header, re.MULTILINE)
        if match is None or match.group(1) != expected_machine:
            actual = match.group(1) if match else "unknown"
            raise RuntimeError(
                f"{path.relative_to(root)} has machine {actual}, expected {expected_machine}"
            )
        elf_files.append(path)
    if not elf_files:
        raise RuntimeError("release archive contains no ELF files")

    provided = set()
    details: dict[Path, str] = {}
    search_directories: dict[Path, list[Path]] = {}
    maximum_glibc = (0,)
    maximum_glibc_text = "none"
    for path in elf_files:
        dynamic = run(readelf, "--dynamic", "--wide", str(path))
        details[path] = dynamic
        search_paths = re.findall(r"\((?:RPATH|RUNPATH)\).*\[([^]]+)]", dynamic)
        directories = []
        for search_path in search_paths:
            for entry in search_path.split(":"):
                directories.append(elf_search_directory(root, path, entry))
        search_directories[path] = directories
        sonames = re.findall(r"\(SONAME\).*\[([^]]+)]", dynamic)
        provided.update(sonames)
        symbols = run(readelf, "--version-info", "--wide", str(path))
        for family, text in VERSIONED_SYMBOL.findall(symbols):
            if family == "GLIBC" and version(text) > maximum_glibc:
                maximum_glibc = version(text)
                maximum_glibc_text = text
    if maximum_glibc > LINUX_GLIBC_BASELINE:
        raise RuntimeError(
            f"archive requires GLIBC_{maximum_glibc_text}, above supported GLIBC_2.28"
        )

    executable = root / "bin/dogpaddle"
    if executable not in details:
        raise RuntimeError("bin/dogpaddle is not an ELF executable")
    executable_needs = set(re.findall(r"\(NEEDED\).*\[([^]]+)]", details[executable]))
    forbidden = executable_needs & {"libgcc_s.so.1", "libstdc++.so.6"}
    if forbidden:
        raise RuntimeError(
            "bin/dogpaddle dynamically links non-system compiler runtimes: "
            + ", ".join(sorted(forbidden))
        )
    unexpected = executable_needs - provided - LINUX_SYSTEM_LIBRARIES
    if unexpected:
        raise RuntimeError(
            "bin/dogpaddle has unpackaged native dependencies: "
            + ", ".join(sorted(unexpected))
        )
    for needed in executable_needs & provided:
        candidates = [directory / needed for directory in search_directories[executable]]
        if not any(candidate.is_file() for candidate in candidates):
            raise RuntimeError(f"bin/dogpaddle cannot resolve packaged dependency {needed}")

    all_needs = set()
    for dynamic in details.values():
        all_needs.update(re.findall(r"\(NEEDED\).*\[([^]]+)]", dynamic))
    optional = sorted(all_needs - provided - LINUX_SYSTEM_LIBRARIES)
    return [
        "format: ELF",
        "glibc baseline: 2.28",
        f"highest required glibc symbol: {maximum_glibc_text}",
        "dogpaddle host libraries: " + ", ".join(sorted(executable_needs - provided)),
        "optional JRE host libraries: " + (", ".join(optional) if optional else "none"),
        f"audited ELF files: {len(elf_files)}",
    ]


def macos_minos(path: Path) -> tuple[int, ...] | None:
    output = run("otool", "-l", str(path))
    match = re.search(r"\n\s+minos\s+([0-9.]+)", output)
    if match:
        return version(match.group(1))
    match = re.search(r"\n\s+version\s+([0-9.]+)", output)
    return version(match.group(1)) if match else None


def expand_macos_path(root: Path, binary: Path, value: str) -> Path:
    variables = {
        "@loader_path": binary.parent,
        "@executable_path": root / "bin",
    }
    for variable, base in variables.items():
        if value == variable:
            candidate = base.resolve()
            break
        prefix = f"{variable}/"
        if value.startswith(prefix):
            candidate = (base / value[len(prefix) :]).resolve()
            break
    else:
        raise RuntimeError(
            f"{binary.relative_to(root)} has unsupported loader path {value}"
        )
    if not inside(root, candidate):
        raise RuntimeError(
            f"{binary.relative_to(root)} has loader path outside the archive: {value}"
        )
    return candidate


def audit_macos(root: Path, target: str) -> list[str]:
    if shutil.which("otool") is None:
        raise RuntimeError("macOS release audit requires otool")
    macho_files = []
    expected_architecture = {
        "x86_64-apple-darwin": "x86_64",
        "aarch64-apple-darwin": "arm64",
    }[target]
    for path in files(root):
        with path.open("rb") as source:
            if source.read(4) not in MACHO_MAGICS:
                continue
        description = run("file", "-b", str(path))
        if "Mach-O" not in description:
            continue
        if expected_architecture not in description:
            raise RuntimeError(
                f"{path.relative_to(root)} does not contain {expected_architecture} code"
            )
        macho_files.append(path)
    if not macho_files:
        raise RuntimeError("release archive contains no Mach-O files")

    bundled_paths = {path.resolve() for path in macho_files}
    bundled_names = {path.name for path in macho_files}
    maximum = (0,)
    external = set()
    for path in macho_files:
        minimum = macos_minos(path)
        if minimum is not None:
            maximum = max(maximum, minimum)
            if minimum > MACOS_DEPLOYMENT_TARGET:
                text = ".".join(map(str, minimum))
                raise RuntimeError(f"{path.relative_to(root)} requires macOS {text}")
        dependencies = run("otool", "-L", str(path)).splitlines()[1:]
        for line in dependencies:
            dependency = line.strip().split(" ", 1)[0]
            if dependency.startswith(("/System/Library/", "/usr/lib/")):
                external.add(dependency)
            elif dependency.startswith(("@loader_path", "@executable_path")):
                candidate = expand_macos_path(root, path, dependency)
                if candidate not in bundled_paths:
                    raise RuntimeError(
                        f"{path.relative_to(root)} references missing {dependency}"
                    )
            elif dependency.startswith("@rpath/"):
                relative = dependency[len("@rpath/") :]
                if (
                    Path(relative).is_absolute()
                    or ".." in Path(relative).parts
                    or Path(relative).name not in bundled_names
                ):
                    raise RuntimeError(
                        f"{path.relative_to(root)} references missing or unsafe {dependency}"
                    )
            else:
                raise RuntimeError(
                    f"{path.relative_to(root)} has non-portable dependency {dependency}"
                )
    maximum_text = ".".join(map(str, maximum))
    return [
        "format: Mach-O",
        "macOS deployment target: 11.0",
        f"highest minimum OS in archive: {maximum_text}",
        f"system library references: {len(external)}",
        f"audited Mach-O files: {len(macho_files)}",
    ]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("target")
    parser.add_argument("archive", type=Path)
    parser.add_argument("report", type=Path)
    arguments = parser.parse_args()
    if not arguments.archive.is_file():
        parser.error(f"archive does not exist: {arguments.archive}")

    with tempfile.TemporaryDirectory(prefix="dogpaddle-release-audit.") as temporary:
        root = extract(arguments.archive, Path(temporary))
        executable = root / "bin/dogpaddle"
        runtime = root / "libexec/dogpaddle/debezium"
        if not executable.is_file() or not os.access(executable, os.X_OK):
            raise RuntimeError("release archive has no executable bin/dogpaddle")
        if not runtime.is_dir():
            raise RuntimeError("release archive has no bundled Debezium runtime")
        if arguments.target.endswith("linux-gnu"):
            lines = audit_linux(root, arguments.target)
        elif arguments.target.endswith("apple-darwin"):
            lines = audit_macos(root, arguments.target)
        else:
            raise RuntimeError(f"unsupported release target: {arguments.target}")

    arguments.report.parent.mkdir(parents=True, exist_ok=True)
    arguments.report.write_text(
        "\n".join([f"target: {arguments.target}", *lines, "status: PASS", ""]),
        encoding="utf-8",
    )
    print(f"PASS release compatibility audit: {arguments.report}")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (OSError, RuntimeError, subprocess.SubprocessError) as error:
        print(f"release audit failed: {error}", file=sys.stderr)
        sys.exit(1)
