#!/usr/bin/env python3
"""Safely extract one DogPaddle release archive."""

from __future__ import annotations

import argparse
import gzip
from pathlib import Path
import sys
import tarfile


MAX_ARCHIVE_ENTRIES = 4_096
MAX_ARCHIVE_COMPRESSED_SIZE = 512 * 1024 * 1024
MAX_ARCHIVE_FILE_SIZE = 512 * 1024 * 1024
MAX_ARCHIVE_EXPANDED_SIZE = 2 * 1024 * 1024 * 1024
MAX_EXTENDED_HEADER_SIZE = 64 * 1024
EXTENDED_HEADER_TYPES = {b"g", b"x", b"K", b"L"}


def _tar_size(header: bytes) -> int:
    value = header[124:136]
    if value[0] & 0x80:
        raise RuntimeError("release archive uses unsupported base-256 sizes")
    value = value.rstrip(b"\0 ").lstrip(b" ") or b"0"
    try:
        return int(value, 8)
    except ValueError as error:
        raise RuntimeError("release archive contains an invalid size") from error


def _read_exact(source: gzip.GzipFile, size: int) -> bytes:
    chunks = []
    remaining = size
    while remaining:
        chunk = source.read(min(remaining, 64 * 1024))
        if not chunk:
            raise RuntimeError("release archive ended unexpectedly")
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


def _discard_exact(source: gzip.GzipFile, size: int) -> None:
    remaining = size
    while remaining:
        chunk = source.read(min(remaining, 64 * 1024))
        if not chunk:
            raise RuntimeError("release archive ended unexpectedly")
        remaining -= len(chunk)


def preflight(archive: Path) -> None:
    if archive.stat().st_size > MAX_ARCHIVE_COMPRESSED_SIZE:
        raise RuntimeError("release archive exceeds the compressed size limit")
    entries = 0
    stream_size = 0
    with gzip.open(archive, "rb") as source:
        while True:
            header = _read_exact(source, 512)
            stream_size += 512
            if header == bytes(512):
                break
            entries += 1
            if entries > MAX_ARCHIVE_ENTRIES * 2:
                raise RuntimeError("release archive contains too many raw entries")
            size = _tar_size(header)
            entry_type = header[156:157]
            if entry_type in EXTENDED_HEADER_TYPES and size > MAX_EXTENDED_HEADER_SIZE:
                raise RuntimeError("release archive has an oversized extended header")
            if entry_type not in {b"\0", b"0", b"5", *EXTENDED_HEADER_TYPES}:
                raise RuntimeError("release archive contains an unsupported entry type")
            padded_size = (size + 511) // 512 * 512
            stream_size += padded_size
            if stream_size > MAX_ARCHIVE_EXPANDED_SIZE:
                raise RuntimeError("release archive stream exceeds the size limit")
            _discard_exact(source, padded_size)


def extract(archive: Path, destination: Path) -> Path:
    preflight(archive)
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
