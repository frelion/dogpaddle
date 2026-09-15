#!/usr/bin/env python3
"""Safely extract one DogPaddle release archive."""

from __future__ import annotations

import argparse
from pathlib import Path
import sys
import tarfile


MAX_ARCHIVE_ENTRIES = 4_096
MAX_ARCHIVE_FILE_SIZE = 512 * 1024 * 1024
MAX_ARCHIVE_EXPANDED_SIZE = 2 * 1024 * 1024 * 1024


def extract(archive: Path, destination: Path) -> Path:
    destination.mkdir(parents=True, exist_ok=True)
    resolved_destination = destination.resolve()
    paths: set[Path] = set()
    expanded_size = 0
    entries = 0
    with tarfile.open(archive, "r|gz") as source:
        for member in source:
            entries += 1
            if entries > MAX_ARCHIVE_ENTRIES:
                raise RuntimeError("release archive contains too many entries")
            candidate = (destination / member.name).resolve()
            if candidate in paths:
                raise RuntimeError(f"archive contains duplicate path: {member.name}")
            paths.add(candidate)
            if candidate == resolved_destination or resolved_destination not in candidate.parents:
                raise RuntimeError(f"archive path escapes its root: {member.name}")
            if not (member.isdir() or member.isfile()):
                raise RuntimeError(f"archive contains a non-regular entry: {member.name}")
            if member.isfile():
                if member.size > MAX_ARCHIVE_FILE_SIZE:
                    raise RuntimeError(f"archive contains oversized file: {member.name}")
                expanded_size += member.size
                if expanded_size > MAX_ARCHIVE_EXPANDED_SIZE:
                    raise RuntimeError("release archive expands beyond the size limit")
            if hasattr(tarfile, "data_filter"):
                source.extract(member, destination, filter="data")
            else:
                source.extract(member, destination)
    roots = list(destination.iterdir())
    if len(roots) != 1 or not roots[0].is_dir():
        raise RuntimeError("release archive must contain exactly one root directory")
    return roots[0]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("archive", type=Path)
    parser.add_argument("destination", type=Path)
    arguments = parser.parse_args()
    if not arguments.archive.is_file():
        parser.error(f"archive does not exist: {arguments.archive}")
    print(extract(arguments.archive, arguments.destination))
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (OSError, RuntimeError, tarfile.TarError) as error:
        print(f"release extraction failed: {error}", file=sys.stderr)
        sys.exit(1)
