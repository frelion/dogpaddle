#!/usr/bin/env python3
"""Real SQL frontend PostgreSQL CDC-to-PostgreSQL gate (Python 3.9+).

The gate owns one temporary loopback-only PostgreSQL cluster. Ordinary Cargo
commands compile the Rust host but never run this script.
"""

from __future__ import annotations

import argparse
import json
import os
import queue
import subprocess
import threading
import time
import traceback
from pathlib import Path
from typing import Any, Callable, Optional

from support import (
    ArtifactDirectory,
    PostgresCluster,
    absolute_existing,
    print_log_tails,
    require_executables,
    run,
)


PASSWORD = "dogpaddle-postgres-sql-gate"


def until(description: str, observe: Callable[[], bool]) -> None:
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        if observe():
            return
        time.sleep(0.05)
    raise RuntimeError(f"timed out: {description}")


class Host:
    def __init__(self, binary: Path, mode: str, sql: Path, flow: Path,
                 log: Path) -> None:
        self.stderr = log.open("w", encoding="utf-8")
        try:
            self.process = subprocess.Popen(
                [str(binary), mode, str(sql), str(flow)],
                stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=self.stderr,
                text=True, bufsize=1,
                env=dict(os.environ, DOGPADDLE_SQL_GATE_PASSWORD=PASSWORD),
            )
        except BaseException:
            self.stderr.close()
            raise
        self.responses: queue.Queue[Any] = queue.Queue()
        self.reader = threading.Thread(target=self._read, daemon=True)
        self.reader.start()
        try:
            ready = self.receive()
            if ready != {"kind": "ready", "mode": mode}:
                raise RuntimeError(f"unexpected host startup: {ready}")
        except BaseException:
            self.close()
            raise

    def __enter__(self) -> Host:
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()

    def _read(self) -> None:
        assert self.process.stdout is not None
        try:
            for line in self.process.stdout:
                if line.strip():
                    self.responses.put(json.loads(line))
        except (OSError, ValueError) as error:
            self.responses.put(error)
        finally:
            self.responses.put(RuntimeError("host stdout closed"))

    def receive(self) -> dict[str, Any]:
        try:
            response = self.responses.get(timeout=60)
        except queue.Empty as error:
            raise RuntimeError("timed out waiting for the SQL host") from error
        if isinstance(response, BaseException):
            raise response
        return response

    def advance(self) -> dict[str, Any]:
        assert self.process.stdin is not None
        self.process.stdin.write("advance\n")
        self.process.stdin.flush()
        response = self.receive()
        if response.get("kind") == "error":
            raise RuntimeError(response["message"])
        if response.get("kind") != "advance":
            raise RuntimeError(f"unexpected host response: {response}")
        return response

    def kill(self) -> None:
        if self.process.poll() is None:
            self.process.kill()
        self.process.wait(timeout=15)

    def close(self) -> None:
        self.kill()
        self.reader.join(timeout=5)
        if self.process.stdin:
            self.process.stdin.close()
        if self.process.stdout:
            self.process.stdout.close()
        self.stderr.close()


class Gate:
    def __init__(self, root: Path, pg_bin: Path, bundle: Path,
                 binary: Path, cluster: PostgresCluster) -> None:
        self.root, self.pg_bin = root, pg_bin
        self.bundle, self.binary = bundle, binary
        self.cluster = cluster
        self.flow = root / "flow"
        self.program = root / "pipeline.sql"
        self.port = cluster.port
        self.pg_env = dict(os.environ, PGPASSWORD=PASSWORD)
        self.psql = [str(pg_bin / "psql"), "-X", "-h", "127.0.0.1",
                     "-p", str(self.port), "-U", "dogpaddle_gate",
                     "-d", "postgres", "-v", "ON_ERROR_STOP=1", "-At"]

    def start(self) -> None:
        password_file = self.root / "password"
        password_file.write_text(PASSWORD + "\n", encoding="utf-8")
        password_file.chmod(0o600)
        try:
            self.cluster.start(
                initdb_arguments=(
                    "-U",
                    "dogpaddle_gate",
                    "--pwfile",
                    str(password_file),
                    "--auth-local=trust",
                    "--auth-host=scram-sha-256",
                    "--no-instructions",
                    "--locale=C",
                    "-E",
                    "UTF8",
                ),
                server_options=(
                    "-c",
                    "wal_level=logical",
                    "-c",
                    "max_replication_slots=4",
                    "-c",
                    "max_wal_senders=4",
                    "-c",
                    "fsync=on",
                    "-c",
                    "synchronous_commit=on",
                    "-c",
                    "log_statement=all",
                    "-c",
                    "log_parameter_max_length=0",
                ),
            )
        finally:
            password_file.unlink(missing_ok=True)

    def stop(self) -> None:
        self.cluster.stop()

    def sql(self, statement: str) -> str:
        return run([*self.psql, "-c", statement], env=self.pg_env)

    def prepare(self) -> None:
        self.sql(
            "CREATE SCHEMA source; CREATE SCHEMA target; "
            "CREATE TABLE source.events ("
            "id BIGINT PRIMARY KEY, tx_seq INTEGER NOT NULL, payload TEXT NOT NULL); "
            "ALTER TABLE source.events REPLICA IDENTITY FULL; "
            "CREATE PUBLICATION events_publication FOR TABLE source.events"
        )
        self.sql(
            "SELECT * FROM pg_create_logical_replication_slot("
            "'events_slot', 'pgoutput')"
        )
        bundle = str(self.bundle).replace("'", "''")
        self.program.write_text(
            f"""-- One SQL file defines scan, transforms, and sink.
INSERT INTO postgres(
    sink_id => 'sql_gate_sink',
    host => '127.0.0.1',
    port => {self.port},
    database => 'postgres',
    user => 'dogpaddle_gate',
    password => env('DOGPADDLE_SQL_GATE_PASSWORD'),
    schema => 'target',
    table => 'events'
)
WITH even_events AS (
    SELECT id, tx_seq, payload
    FROM postgres_cdc(
        engine_name => 'sql_gate_scan',
        runtime_bundle => '{bundle}',
        host => '127.0.0.1',
        port => {self.port},
        database => 'postgres',
        user => 'dogpaddle_gate',
        password => env('DOGPADDLE_SQL_GATE_PASSWORD'),
        schema => 'source',
        table => 'events',
        slot => 'events_slot',
        publication => 'events_publication'
    )
    WHERE tx_seq % 2 = 0
)
SELECT id, tx_seq, payload
FROM even_events
WHERE tx_seq % 4 = 0
UNION ALL
SELECT id, tx_seq,
       CASE WHEN tx_seq % 4 = 2 THEN payload ELSE NULL END AS payload
FROM even_events
WHERE tx_seq % 4 = 2;
""",
            encoding="utf-8",
        )

    def slot_active(self) -> bool:
        return self.sql(
            "SELECT active FROM pg_replication_slots "
            "WHERE slot_name = 'events_slot'"
        ) == "t"

    def rows(self) -> list[tuple[int, int, int, str]]:
        if self.sql("SELECT to_regclass('target.events') IS NOT NULL") != "t":
            return []
        values = self.sql(
            'SELECT "$dogpaddle.id", id, tx_seq, convert_from(payload, \'UTF8\') '
            'FROM target.events ORDER BY id'
        )
        if not values:
            return []
        return [
            (int(technical_id), int(row_id), int(tx_seq), payload)
            for technical_id, row_id, tx_seq, payload in
            (line.split("|", 3) for line in values.splitlines())
        ]

    def target_insert_count(self) -> int:
        log = (self.root / "postgres.log").read_text(
            encoding="utf-8", errors="replace"
        )
        return log.count('INSERT INTO "target"."events" (')

    def host(self, mode: str, session: int) -> Host:
        return Host(self.binary, mode, self.program, self.flow,
                    self.root / f"host-{session}.log")

    def drive(self, host: Host, description: str,
              done: Callable[[], bool]) -> None:
        def advance() -> bool:
            host.advance()
            return done()

        until(description, advance)

    def settle(self, host: Host, description: str) -> None:
        def settled() -> bool:
            response = host.advance()
            if response["outcome"] != "Idle":
                return False
            if response["sink"]["cursor"] != response["sink"]["tail"]:
                raise RuntimeError("idle SQL sink still has an input backlog")
            return True

        until(description, settled)

    def run_gate(self) -> None:
        first = [(1, 2, 2, "even")]
        with self.host("build", 1) as host:
            self.drive(host, "SQL CDC connector starts", self.slot_active)
            self.sql(
                "INSERT INTO source.events VALUES "
                "(1, 1, 'odd'), (2, 2, 'even')"
            )

            def first_target_commit() -> bool:
                response = host.advance()
                rows = self.rows()
                sink = response["sink"]
                if rows and (
                    response["outcome"] != "Progressed"
                    or sink["cursor"] >= sink["tail"]
                    or rows != first
                ):
                    raise RuntimeError(f"invalid first filtered target batch: {rows}")
                return rows == first

            until("first filtered target batch commits", first_target_commit)
            first_insert_count = self.target_insert_count()
            if first_insert_count == 0:
                raise RuntimeError("PostgreSQL did not log the first target INSERT")
            # The target write committed during the command above. No next Flow
            # round settles its durable Prepared state before this process exit.
            host.kill()

        until("killed SQL host releases its slot", lambda: not self.slot_active())
        with self.host("open", 2) as host:
            replay = host.advance()
            if (
                replay["outcome"] != "Progressed"
                or replay["sink"]["cursor"] >= replay["sink"]["tail"]
                or self.rows() != first
            ):
                raise RuntimeError("Prepared replay duplicated or changed the first target row")
            until(
                "Prepared target INSERT is replayed",
                lambda: self.target_insert_count() > first_insert_count,
            )
            self.drive(host, "reopened SQL CDC connector starts", self.slot_active)
            self.settle(host, "reopened Flow settles")
            if self.rows() != first:
                raise RuntimeError("settling Prepared replay changed the target relation")

            self.sql(
                "BEGIN; "
                "UPDATE source.events SET tx_seq = 4, payload = 'became-even' WHERE id = 1; "
                "UPDATE source.events SET tx_seq = 3, payload = 'became-odd' WHERE id = 2; "
                "INSERT INTO source.events VALUES (3, 6, 'after-reopen'); "
                "COMMIT"
            )
            expected = [
                (2, 1, 4, "became-even"),
                (3, 3, 6, "after-reopen"),
            ]
            self.drive(host, "filtered updates reach the PostgreSQL sink",
                       lambda: self.rows() == expected)
            self.settle(host, "updated Flow settles")
            if self.rows() != expected:
                raise RuntimeError("settling the updated Flow changed its target rows")
            if self.sql(
                "SELECT schemaname || '.' || tablename FROM pg_publication_tables "
                "WHERE pubname = 'events_publication'"
            ) != "source.events":
                raise RuntimeError("CDC publication captured the SQL sink target")

        print(
            "PASS SQL postgres_cdc -> CTE/filter/nullable UNION ALL -> postgres "
            "build, filtered updates, PG-committed/Prepared crash, idempotent "
            "reopen, and successor"
        )


def resolve_host(parser: argparse.ArgumentParser, supplied: Optional[Path]) -> Path:
    if supplied is not None:
        try:
            binary = absolute_existing(supplied, "--host")
        except (OSError, ValueError) as error:
            parser.error(str(error))
    else:
        repository = Path(__file__).resolve().parents[2]
        print("building dogpaddle-postgres-system-hosts release binaries", flush=True)
        run(
            [
                "cargo",
                "build",
                "--locked",
                "--release",
                "-p",
                "dogpaddle-postgres-system-hosts",
                "--bins",
            ],
            cwd=repository,
            timeout=1_200,
        )
        metadata = json.loads(
            run(
                [
                    "cargo",
                    "metadata",
                    "--locked",
                    "--no-deps",
                    "--format-version",
                    "1",
                ],
                cwd=repository,
            )
        )
        binary = Path(metadata["target_directory"]) / "release" / "postgres_sql"
        binary = binary.resolve(strict=True)
    if not binary.is_file() or not os.access(binary, os.X_OK):
        parser.error(f"PostgreSQL SQL host is not executable: {binary}")
    return binary


def report_stop_failure(root: Path, error: BaseException) -> None:
    print(
        "failed to stop the isolated PostgreSQL cluster; a process may still be "
        f"running and the fixture is retained at {root}",
        file=os.sys.stderr,
    )
    traceback.print_exception(type(error), error, error.__traceback__, file=os.sys.stderr)


def main() -> None:
    if not __debug__:
        raise RuntimeError("this correctness gate requires assertions; run Python without -O")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bundle", type=Path, required=True)
    parser.add_argument("--host", type=Path)
    parser.add_argument("--postgres-bin", type=Path, required=True)
    parser.add_argument("--artifacts-dir", type=Path)
    parser.add_argument(
        "--keep",
        action="store_true",
        help="retain this run's temporary cluster and logs after stopping it",
    )
    args = parser.parse_args()
    try:
        bundle = absolute_existing(args.bundle, "--bundle")
        pg_bin = absolute_existing(args.postgres_bin, "--postgres-bin")
        if not bundle.is_dir():
            raise ValueError(f"--bundle is not a directory: {bundle}")
        require_executables(pg_bin, ("initdb", "pg_ctl", "postgres", "psql"))
    except (OSError, ValueError) as error:
        parser.error(str(error))
    binary = resolve_host(parser, args.host)
    try:
        artifacts = ArtifactDirectory.create(
            args.artifacts_dir,
            prefix="dogpaddle-pg-sql-",
            keep_temporary=args.keep,
        )
    except (OSError, ValueError) as error:
        parser.error(str(error))

    print(run([str(pg_bin / "postgres"), "--version"]))
    root = artifacts.root
    if (root / "data").exists():
        raise RuntimeError(f"refusing to overwrite an existing SQL fixture: {root}")
    print(f"isolated fixture: {root}", flush=True)
    cluster = PostgresCluster(root, pg_bin, socket_directory=root / "socket")
    gate = Gate(root, pg_bin, bundle, binary, cluster)
    failure: Optional[BaseException] = None
    passed = False
    stopped = False
    try:
        gate.start()
        gate.prepare()
        gate.run_gate()
        passed = True
    except BaseException as error:
        failure = error
        print_log_tails([cluster.log, *sorted(root.glob("host-*.log"))])
        raise
    finally:
        try:
            gate.stop()
            stopped = True
        except BaseException as error:
            report_stop_failure(root, error)
            if failure is None:
                raise
        if passed and stopped and not artifacts.retain:
            artifacts.remove()
            print("removed the temporary PostgreSQL cluster and Flow")
        else:
            print(f"fixture retained at {root}", file=os.sys.stderr)


if __name__ == "__main__":
    main()
