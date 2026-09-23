#!/usr/bin/env python3
"""Run the MySQL 8.4 CDC -> DogPaddle Store crash/reopen acceptance gate.

Only a fresh, uniquely named Compose project, container and volume are used.
Pass a target-matched runtime bundle containing the MySQL Debezium connector.
The Rust system host is built by default, or supply an already built --host.
Host stderr is discarded to avoid retaining upstream credentials; state is
retained on failure for private local inspection. Python 3.9+ and a Docker or
Podman CLI with Compose support are required.
"""

from __future__ import annotations

import argparse
import json
import os
import queue
import secrets
import shutil
import socket
import stat
import subprocess
import sys
import tempfile
import threading
import time
import traceback
from pathlib import Path
from typing import Any, Callable, Optional


DATABASE = "dogpaddle_gate"
TABLE = "direct_events"
INITIAL = [[1, 10, 10, "pre-existing"]]
STREAMING = [[-1, 10, 10, "pre-existing"], [1, 10, 11, "updated"],
             [1, 20, 20, "after-reopen"]]
SUCCESSOR = [1, 30, 30, "last-witness"]
COMPOSE_FILE = Path(__file__).resolve().with_name("compose.yaml")
REPOSITORY = Path(__file__).resolve().parents[2]


def absolute_existing(value: Path, label: str) -> Path:
    if not value.is_absolute():
        raise ValueError(f"{label} must be an absolute path")
    return value.resolve(strict=True)


def command(
    argv: list[str], *, cwd: Optional[Path] = None,
    env: Optional[dict[str, str]] = None, input_text: Optional[str] = None,
    timeout: int = 90, secret: str = "",
) -> str:
    try:
        result = subprocess.run(argv, cwd=cwd, env=env, input=input_text,
                                text=True, capture_output=True, timeout=timeout)
    except subprocess.TimeoutExpired as error:
        raise RuntimeError(f"command timed out after {timeout}s: {argv[:3]}") from error
    if result.returncode:
        detail = (result.stderr or result.stdout).strip()[-4000:]
        raise RuntimeError(f"command failed (exit {result.returncode}, {argv[:3]}): "
                           f"{detail.replace(secret, '<redacted>') if secret else detail}")
    return result.stdout.strip()


def free_port() -> int:
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


def until(description: str, observe: Callable[[], bool], *, seconds: int = 180) -> None:
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if observe():
            return
        time.sleep(0.1)
    raise RuntimeError(f"timed out after {seconds}s: {description}")


class Container:
    def __init__(self, engine: str, port: int, password: str) -> None:
        suffix = secrets.token_hex(9)
        self.project = f"dogpaddle-mysql-cdc-{suffix}"
        self.container = f"{self.project}-mysql"
        self.volume = f"{self.project}-data"
        self.port = port
        self.password = password
        self.argv = [engine, "compose", "--project-name", self.project,
                     "--file", str(COMPOSE_FILE)]
        self.env = dict(os.environ, DOGPADDLE_GATE_CONTAINER=self.container,
                        DOGPADDLE_GATE_VOLUME=self.volume,
                        DOGPADDLE_GATE_PORT=str(port), DOGPADDLE_GATE_PASSWORD=password)
        # Explicit options own this project/file; inherited Compose settings must
        # not cause another project or service to be joined or removed.
        for name in ("COMPOSE_PROJECT_NAME", "COMPOSE_FILE", "COMPOSE_PROFILES",
                     "COMPOSE_ENV_FILES"):
            self.env.pop(name, None)
        self.started = False

    def compose(self, *args: str, timeout: int = 90,
                input_text: Optional[str] = None) -> str:
        return command([*self.argv, *args], env=self.env, input_text=input_text,
                       timeout=timeout, secret=self.password)

    def sql(self, statement: str, *, timeout: int = 30) -> str:
        # The password is only expanded inside our disposable container and SQL
        # travels on stdin, never in a CLI argument or logged command string.
        return self.compose("exec", "-T", "mysql", "sh", "-c",
                            'MYSQL_PWD="$MYSQL_ROOT_PASSWORD" exec mysql '
                            '--protocol=socket -uroot --batch --skip-column-names',
                            input_text=statement + "\n", timeout=timeout)

    def start(self) -> None:
        self.started = True  # Also clean up containers after a partly failed up.
        self.compose("up", "--detach", timeout=240)
        until("isolated MySQL responds through its socket",
              lambda: self.ready(), seconds=180)
        until("isolated MySQL loopback port is published", self.port_ready,
              seconds=30)

    def ready(self) -> bool:
        try:
            return self.sql("SELECT 1", timeout=10) == "1"
        except (RuntimeError, subprocess.SubprocessError):
            return False

    def port_ready(self) -> bool:
        try:
            with socket.create_connection(("127.0.0.1", self.port), timeout=2):
                return True
        except OSError:
            return False

    def stop(self) -> None:
        if self.started:
            # This random project owns its sole service and uniquely named volume.
            # In particular, do not use --remove-orphans or global prune commands.
            self.compose("down", "--volumes", timeout=120)
            self.started = False

    def diagnostics(self) -> None:
        try:
            logs = self.compose("logs", "--no-color", "--tail", "100", timeout=20)
            print(f"--- isolated MySQL logs ---\n{logs.replace(self.password, '<redacted>')}",
                  file=sys.stderr)
        except RuntimeError as error:
            print(f"could not retrieve isolated MySQL logs: {error}", file=sys.stderr)


class Host:
    def __init__(self, binary: Path, root: Path, bundle: Path, port: int,
                 password: str, session: int) -> None:
        self.log = root / f"session-{session}.log"
        self.password = password
        # A system host can emit upstream diagnostics; keep its temporary log private.
        self.stderr = os.fdopen(os.open(self.log, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600),
                                "w", encoding="utf-8")
        try:
            self.process = subprocess.Popen(
                [str(binary), str(root), str(bundle), str(port), TABLE],
                stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=self.stderr,
                text=True, bufsize=1,
                env=dict(os.environ, DOGPADDLE_GATE_PASSWORD=password),
            )
        except OSError:
            self.stderr.close()
            raise
        self.responses: queue.Queue[Any] = queue.Queue()
        self.reader = threading.Thread(target=self._read, daemon=True)
        self.reader.start()

    def __enter__(self) -> Host:
        try:
            if self.receive() != {"kind": "ready"}:
                raise RuntimeError("unexpected MySQL host startup response")
        except BaseException:
            self.__exit__()
            raise
        return self

    def __exit__(self, *_: Any) -> None:
        if self.process.poll() is None:
            self.process.kill()
        self.process.wait(timeout=15)
        self.reader.join(timeout=5)
        if self.process.stdin:
            self.process.stdin.close()
        if self.process.stdout:
            self.process.stdout.close()
        self.stderr.close()
        # Host/upstream diagnostics are not an oracle. Remove their private
        # log rather than retaining a potentially echoed credential, including
        # on failure; the runner reports bounded/redacted protocol diagnostics.
        self.log.unlink(missing_ok=True)

    def _read(self) -> None:
        assert self.process.stdout is not None
        try:
            for line in self.process.stdout:
                if line.strip():
                    self.responses.put(json.loads(line))
        except (ValueError, OSError) as error:
            self.responses.put(error)
        finally:
            self.responses.put(RuntimeError("MySQL host stdout closed"))

    def receive(self) -> dict[str, Any]:
        # Connector startup alone may legally take 60 seconds; discovery,
        # polling and the Store transaction also need time in the same turn.
        response = self.responses.get(timeout=120)
        if isinstance(response, BaseException):
            raise response
        if response.get("kind") == "error":
            raise RuntimeError(str(response["message"]).replace(self.password, "<redacted>"))
        return response

    def request(self, action: str) -> dict[str, Any]:
        assert self.process.stdin is not None
        self.process.stdin.write(action + "\n")
        self.process.stdin.flush()
        return self.receive()


def assert_rows(host: Host, expected: list[list[Any]], *, phase: int,
                spool_nonempty: bool = False) -> None:
    result = host.request("read")
    wanted = {"kind": "rows", "rows": expected, "checkpoint_present": True,
              "phase": phase, "spool_nonempty": spool_nonempty}
    if result != wanted:
        raise RuntimeError(f"unexpected durable scan rows/phase: {result!r}")


def gate(container: Container, binary: Path, root: Path, bundle: Path) -> None:
    # Table and its committed snapshot row must exist before the first host run.
    # The random hex password contains no SQL metacharacters.
    container.sql(
        f"CREATE DATABASE {DATABASE};\n"
        f"CREATE USER '{DATABASE}'@'%' IDENTIFIED WITH mysql_native_password "
        f"BY '{container.password}';\n"
        "GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, "
        f"REPLICATION CLIENT, PROCESS ON *.* TO '{DATABASE}'@'%';\n"
        f"CREATE TABLE {DATABASE}.{TABLE} (id BIGINT PRIMARY KEY, "
        "tx_seq INT NOT NULL, payload VARCHAR(255) NOT NULL) ENGINE=InnoDB;\n"
        f"INSERT INTO {DATABASE}.{TABLE} VALUES (10,10,'pre-existing');"
    )
    print("seeded isolated MySQL snapshot row before starting the Rust host", flush=True)

    with Host(binary, root, bundle, container.port, container.password, 1) as host:
        def terminal_capture() -> bool:
            response = host.request("crash-terminal-capture")
            if response.get("kind") != "durable-terminal-capture":
                if response.get("kind") not in ("idle", "advance"):
                    raise RuntimeError(f"unexpected capture response: {response}")
                return False
            if response != {"kind": "durable-terminal-capture",
                            "checkpoint_present": True, "commits": 1}:
                raise RuntimeError(f"terminal capture is not durable: {response}")
            return True

        until("terminal MySQL snapshot committed before ACK", terminal_capture)
        if host.process.wait(timeout=15) != 76:
            raise RuntimeError("terminal capture did not exit at the pre-ACK boundary (76)")

    with Host(binary, root, bundle, container.port, container.password, 2) as host:
        assert_rows(host, [], phase=2, spool_nonempty=True)
        restored = host.request("advance")
        if restored != {"kind": "advance", "output": False,
                        "checkpoint_present": True, "commits": 1}:
            raise RuntimeError(f"publishing restore changed durable output: {restored}")
        assert_rows(host, [], phase=2, spool_nonempty=True)

        def publish() -> bool:
            response = host.request("advance")
            if response.get("output"):
                if response != {"kind": "advance", "output": True,
                                "checkpoint_present": True, "commits": 1}:
                    raise RuntimeError(f"snapshot publish did not commit atomically: {response}")
                return True
            return False

        until("private snapshot spool publishes one initial row", publish)
        assert_rows(host, INITIAL, phase=3)
        # Update produces [-1 old, +1 new] and INSERT follows in this transaction.
        container.sql(f"START TRANSACTION; UPDATE {DATABASE}.{TABLE} "
                      "SET tx_seq=11,payload='updated' WHERE id=10; "
                      f"INSERT INTO {DATABASE}.{TABLE} VALUES (20,20,'after-reopen'); COMMIT")

        def streaming_crash() -> bool:
            response = host.request("crash-before-ack")
            if response.get("kind") != "durable-before-ack":
                if response.get("output"):
                    raise RuntimeError("streaming output committed without the requested pre-ACK crash")
                if response.get("kind") not in ("idle", "advance"):
                    raise RuntimeError(f"unexpected streaming response: {response}")
                assert_rows(host, INITIAL, phase=3)
                return False
            if response != {"kind": "durable-before-ack", "output": True,
                            "checkpoint_present": True, "commits": 1}:
                raise RuntimeError(f"streaming delivery did not atomically commit: {response}")
            return True

        until("streaming UPDATE/INSERT committed before ACK", streaming_crash)
        if host.process.wait(timeout=15) != 74:
            raise RuntimeError("streaming delivery did not exit at the pre-ACK boundary (74)")

    with Host(binary, root, bundle, container.port, container.password, 3) as host:
        state = host.request("read")
        prefix = state.get("rows")
        expected = INITIAL + STREAMING
        if (state.get("kind") != "rows" or state.get("phase") != 3
                or not state.get("checkpoint_present") or state.get("spool_nonempty")
                or not isinstance(prefix, list) or not 1 < len(prefix) <= len(expected)
                or prefix != expected[:len(prefix)]):
            raise RuntimeError(f"streaming pre-ACK prefix is not durable and ordered: {state}")
        restored = host.request("advance")
        if restored != {"kind": "advance", "output": False,
                        "checkpoint_present": True, "commits": 1}:
            raise RuntimeError(f"streaming restore re-emitted an ACKed row: {restored}")
        assert_rows(host, prefix, phase=3)
        container.sql(f"INSERT INTO {DATABASE}.{TABLE} VALUES (30,30,'last-witness')")
        expected.append(SUCCESSOR)

        def replay_and_follow() -> bool:
            response = host.request("advance")
            if response.get("output") and response != {
                "kind": "advance", "output": True,
                "checkpoint_present": True, "commits": 1,
            }:
                raise RuntimeError(f"replay data turn did not commit atomically: {response}")
            state = host.request("read")
            rows = state.get("rows")
            if (state.get("kind") != "rows" or state.get("phase") != 3
                    or state.get("spool_nonempty") or not state.get("checkpoint_present")
                    or not isinstance(rows, list) or rows != expected[:len(rows)]):
                raise RuntimeError(f"CDC restart duplicated, reordered or skipped an event: {state}")
            return rows == expected

        until("streaming replay and successor follow without missing or duplicate rows",
              replay_and_follow)
        assert_rows(host, expected, phase=3)
    print("PASS MySQL snapshot private spool, terminal pre-ACK recovery, "
          "streaming pre-ACK UPDATE/INSERT replay and successor")


def check_password_not_persisted(root: Path, password: str) -> None:
    needle = password.encode()
    state = root / "scan"
    if not state.is_dir():
        raise RuntimeError("MySQL host did not persist Scan state")
    for path in state.rglob("*"):
        if not path.is_file():
            continue
        with path.open("rb") as stream:
            tail = b""
            while chunk := stream.read(1024 * 1024):
                combined = tail + chunk
                if needle in combined:
                    raise RuntimeError(f"MySQL credential persisted in Store file: {path}")
                tail = combined[-len(needle):]
    print("PASS MySQL credential absent from persisted Store bytes")


def resolve_host(parser: argparse.ArgumentParser, supplied: Optional[Path]) -> Path:
    if supplied is not None:
        try:
            binary = absolute_existing(supplied, "--host")
        except (OSError, ValueError) as error:
            parser.error(str(error))
    else:
        print("building dogpaddle-mysql-system-host release mysql_cdc binary", flush=True)
        command(["cargo", "build", "--release", "--locked", "-p",
                 "dogpaddle-mysql-system-host", "--bin", "mysql_cdc"],
                cwd=REPOSITORY, timeout=1200)
        metadata = json.loads(command(["cargo", "metadata", "--locked", "--no-deps",
                                       "--format-version", "1"], cwd=REPOSITORY))
        binary = (Path(metadata["target_directory"]) / "release" / "mysql_cdc").resolve(strict=True)
    if not binary.is_file() or not os.access(binary, os.X_OK):
        parser.error(f"MySQL CDC host is not executable: {binary}")
    return binary


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bundle", type=Path, required=True)
    parser.add_argument("--host", type=Path)
    parser.add_argument("--engine", default=os.environ.get("CONTAINER_ENGINE", "docker"),
                        help="Docker/Podman executable with Compose support (default: docker)")
    parser.add_argument("--artifacts-dir", type=Path,
                        help="new or empty absolute directory retained for local diagnostics")
    parser.add_argument("--keep", action="store_true",
                        help="retain this run's private Store state after container cleanup")
    args = parser.parse_args()
    try:
        bundle = absolute_existing(args.bundle, "--bundle")
        if not bundle.is_dir():
            raise ValueError(f"--bundle is not a directory: {bundle}")
        mysql_connector = bundle / "debezium/lib/debezium-connector-mysql-3.6.2.Final.jar"
        if not mysql_connector.is_file():
            raise ValueError(f"--bundle lacks the pinned MySQL connector: {bundle}")
        if not COMPOSE_FILE.is_file():
            raise ValueError(f"missing Compose fixture: {COMPOSE_FILE}")
        if shutil.which(args.engine) is None:
            raise ValueError(f"container engine not on PATH: {args.engine}")
        if args.artifacts_dir is not None:
            if not args.artifacts_dir.is_absolute():
                raise ValueError("--artifacts-dir must be an absolute path")
            if args.artifacts_dir.exists():
                if args.artifacts_dir.is_symlink() or not args.artifacts_dir.is_dir():
                    raise ValueError("--artifacts-dir must be a real directory")
                if any(args.artifacts_dir.iterdir()):
                    raise ValueError("--artifacts-dir must be new or empty")
                if stat.S_IMODE(args.artifacts_dir.stat().st_mode) & 0o077:
                    raise ValueError("--artifacts-dir must not be accessible to other users")
    except (OSError, ValueError) as error:
        parser.error(str(error))
    binary = resolve_host(parser, args.host)
    password = secrets.token_hex(32)
    port = free_port()
    container = Container(args.engine, port, password)
    if args.artifacts_dir is None:
        root = Path(tempfile.mkdtemp(prefix="dogpaddle-mysql-cdc-"))
        retain = args.keep
    else:
        if not args.artifacts_dir.exists():
            args.artifacts_dir.mkdir(mode=0o700, parents=True)
        root = args.artifacts_dir.resolve(strict=True)
        if stat.S_IMODE(root.stat().st_mode) & 0o077:
            parser.error("--artifacts-dir must not be accessible to other users")
        retain = True
    print(f"isolated fixture: {root} (Compose project {container.project}, port {port})", flush=True)
    passed = False
    stopped = False
    failure: Optional[BaseException] = None
    try:
        container.start()
        gate(container, binary, root, bundle)
        check_password_not_persisted(root, password)
        passed = True
    except BaseException as error:
        failure = error
        container.diagnostics()
        raise
    finally:
        cleanup_error: Optional[BaseException] = None
        try:
            container.stop()
            stopped = True
        except BaseException as error:
            cleanup_error = error
            print(f"failed to stop isolated Compose project {container.project}; "
                  "container/volume may require manual removal", file=sys.stderr)
            traceback.print_exception(type(error), error, error.__traceback__, file=sys.stderr)
        if passed and stopped and not retain:
            shutil.rmtree(root)
            print("removed this run's temporary state")
        else:
            print(f"private state retained at {root}; host stderr discarded", file=sys.stderr)
        if cleanup_error is not None and failure is None:
            raise cleanup_error


if __name__ == "__main__":
    main()
