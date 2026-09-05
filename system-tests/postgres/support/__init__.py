"""Small process and PostgreSQL-fixture primitives for the two PG gates.

This module deliberately knows nothing about DogPaddle host protocols, SQL
oracles, or scenario state.  D1 is a separate fixture and does not import it.
"""

from __future__ import annotations

import os
import shlex
import shutil
import socket
import subprocess
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional, Sequence


def run(
    command: Sequence[str],
    *,
    cwd: Optional[Path] = None,
    env: Optional[dict[str, str]] = None,
    timeout: int = 60,
) -> str:
    """Run one bounded subprocess and return stripped stdout."""

    result = subprocess.run(
        command,
        cwd=cwd,
        env=env,
        check=True,
        capture_output=True,
        text=True,
        timeout=timeout,
    )
    return result.stdout.strip()


def free_loopback_port() -> int:
    """Ask the kernel for an unused TCP port on loopback."""

    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


def absolute_existing(value: Path, label: str) -> Path:
    """Resolve an explicitly supplied absolute path and require it to exist."""

    if not value.is_absolute():
        raise ValueError(f"{label} must be an absolute path")
    return value.resolve(strict=True)


def require_executables(directory: Path, names: Sequence[str]) -> None:
    """Require named executable files below one already-resolved directory."""

    for name in names:
        executable = (directory / name).resolve(strict=True)
        if not executable.is_file() or not os.access(executable, os.X_OK):
            raise ValueError(f"required executable is not executable: {executable}")


@dataclass(frozen=True)
class ArtifactDirectory:
    """A caller-selected or temporary root and its retention policy."""

    root: Path
    retain: bool

    @classmethod
    def create(
        cls, supplied: Optional[Path], *, prefix: str, keep_temporary: bool
    ) -> "ArtifactDirectory":
        if supplied is None:
            return cls(Path(tempfile.mkdtemp(prefix=prefix)), keep_temporary)
        if not supplied.is_absolute():
            raise ValueError("--artifacts-dir must be an absolute path")
        supplied.mkdir(parents=True, exist_ok=True)
        return cls(supplied.resolve(strict=True), True)

    def remove(self) -> None:
        """Remove a successfully stopped temporary fixture when not retained."""

        if not self.retain:
            shutil.rmtree(self.root)


class PostgresCluster:
    """Own one initdb-created, loopback-only PostgreSQL process."""

    def __init__(self, root: Path, pg_bin: Path, *, socket_directory: Path) -> None:
        self.root = root
        self.pg_bin = pg_bin
        self.data = root / "data"
        self.log = root / "postgres.log"
        self.socket_directory = socket_directory
        self.port = free_loopback_port()
        self.started = False

    def start(
        self,
        *,
        initdb_arguments: Sequence[str],
        server_options: Sequence[str],
    ) -> None:
        """Initialize and start the cluster with explicit fixture options."""

        self.socket_directory.mkdir(parents=True, exist_ok=True)
        run(
            [
                str(self.pg_bin / "initdb"),
                "-D",
                str(self.data),
                *initdb_arguments,
            ]
        )
        options = shlex.join(
            [
                "-h",
                "127.0.0.1",
                "-p",
                str(self.port),
                "-k",
                str(self.socket_directory),
                *server_options,
            ]
        )
        run(
            [
                str(self.pg_bin / "pg_ctl"),
                "-D",
                str(self.data),
                "-l",
                str(self.log),
                "-o",
                options,
                "-w",
                "start",
            ]
        )
        self.started = True

    def stop(self) -> None:
        """Immediately stop the owned cluster, including a partially started one."""

        if self.started or (self.data / "postmaster.pid").exists():
            run(
                [
                    str(self.pg_bin / "pg_ctl"),
                    "-D",
                    str(self.data),
                    "-m",
                    "immediate",
                    "-w",
                    "stop",
                ]
            )
        self.started = False


def print_log_tails(paths: Iterable[Path], *, byte_limit: int = 16_384) -> None:
    """Print bounded UTF-8-lossy tails from any logs that were created."""

    for log in paths:
        if not log.exists() or not log.is_file():
            continue
        try:
            with log.open("rb") as stream:
                stream.seek(0, os.SEEK_END)
                size = stream.tell()
                stream.seek(max(0, size - byte_limit))
                tail = stream.read().decode("utf-8", errors="replace")
        except OSError as error:
            print(f"could not read diagnostic log {log}: {error}", file=os.sys.stderr)
        else:
            print(f"--- {log.name} (last {byte_limit} bytes) ---\n{tail}", file=os.sys.stderr)
