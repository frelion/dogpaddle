#!/usr/bin/env python3
"""Explicit native PostgreSQL -> DogPaddle -> SQLite recovery gate (Python 3.9+).

Only an initdb-owned temporary cluster is used; existing databases and services
are never contacted. Supply the already built, target-matched runtime payload.
The PostgreSQL version used is printed, not presented as the pinned D1 matrix.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import queue
import shutil
import socket
import sqlite3
import struct
import subprocess
import tempfile
import threading
import time
from pathlib import Path
from typing import Any, Callable


PASSWORD = "gate-secret-must-not-be-persisted"


def run(command: list[str]) -> str:
    result = subprocess.run(command, check=True, capture_output=True, text=True, timeout=60)
    return result.stdout.strip()


class Host:
    def __init__(self, command: list[str], log: Path) -> None:
        self.stderr = log.open("w", encoding="utf-8")
        try:
            self.process = subprocess.Popen(
                command,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=self.stderr,
                text=True,
                bufsize=1,
                env=dict(os.environ, DOGPADDLE_GATE_PASSWORD=PASSWORD),
            )
        except OSError:
            self.stderr.close()
            raise
        self.responses: queue.Queue[Any] = queue.Queue()
        self.reader = threading.Thread(target=self._read, daemon=True)
        self.reader.start()

    def __enter__(self) -> Host:
        try:
            response = self.receive()
            if response != {"kind": "ready"}:
                raise RuntimeError(f"unexpected host startup: {response}")
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

    def _read(self) -> None:
        assert self.process.stdout is not None
        try:
            for line in self.process.stdout:
                if line.strip():
                    self.responses.put(json.loads(line))
        except (ValueError, OSError) as error:
            self.responses.put(error)
        finally:
            self.responses.put(RuntimeError("host stdout closed"))

    def receive(self) -> dict[str, Any]:
        response = self.responses.get(timeout=60)
        if isinstance(response, BaseException):
            raise response
        if response.get("kind") == "error":
            raise RuntimeError(response["message"])
        return response

    def request(self, command: str) -> dict[str, Any]:
        assert self.process.stdin is not None
        self.process.stdin.write(command + "\n")
        self.process.stdin.flush()
        return self.receive()


def until(description: str, observe: Callable[[], bool]) -> None:
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        if observe():
            return
        time.sleep(0.05)
    raise RuntimeError(f"timed out: {description}")


def drive(host: Host, done: Callable[[], bool], description: str) -> None:
    def advance() -> bool:
        host.request("advance")
        return done()

    until(description, advance)


def crash_after_output(host: Host) -> bool:
    response = host.request("crash-before-ack")
    if response["kind"] != "durable-before-ack":
        return False
    if response != {"kind": "durable-before-ack", "output": True,
                    "checkpoint_present": True, "commits": 1}:
        raise RuntimeError(f"delivery did not atomically commit checkpoint and output: {response}")
    return True


class Fixture:
    def __init__(self, root: Path, pg_bin: Path, bundle: Path, binary: Path) -> None:
        self.root, self.pg_bin, self.bundle, self.binary = root, pg_bin, bundle, binary
        with socket.socket() as listener:
            listener.bind(("127.0.0.1", 0))
            self.port = listener.getsockname()[1]
        self.psql = [str(pg_bin / "psql"), "-X", "-h", "127.0.0.1", "-p", str(self.port),
                     "-U", "dogpaddle_gate", "-d", "postgres", "-v", "ON_ERROR_STOP=1", "-At"]

    def sql(self, sql: str) -> str:
        return run([*self.psql, "-c", sql])

    def create_table(self, table: str, *, rich: bool = False) -> None:
        # Names are fixed literals in this gate, never external SQL fragments.
        extra = (", flag BOOLEAN, small SMALLINT, real_value REAL, double_value DOUBLE PRECISION, "
                 "amount NUMERIC(10,2), day DATE, stamp TIMESTAMP, instant TIMESTAMPTZ, bytes BYTEA") if rich else ""
        self.sql(f"CREATE TABLE public.{table} (id BIGINT PRIMARY KEY, tx_seq INTEGER NOT NULL, "
                 f"payload TEXT NOT NULL{extra}); "
                 f"ALTER TABLE public.{table} REPLICA IDENTITY FULL; "
                 f"CREATE PUBLICATION {table}_pub FOR TABLE public.{table}")
        self.sql(f"SELECT * FROM pg_create_logical_replication_slot('{table}_slot', 'pgoutput')")

    def active(self, table: str) -> bool:
        return self.sql(f"SELECT active FROM pg_replication_slots WHERE slot_name = '{table}_slot'") == "t"

    def host(self, mode: str, table: str, session: int) -> Host:
        root = self.root / table
        root.mkdir(exist_ok=True)
        return Host([str(self.binary), mode, str(root), str(self.bundle), str(self.port),
                     table, table + "_slot", table + "_pub"], root / f"session-{session}.log")

    def sqlite_rows(self, table: str) -> list[tuple[int, int, str]]:
        path = self.root / table / "sink.sqlite"
        if not path.exists():
            return []
        with sqlite3.connect(f"file:{path}?mode=ro", uri=True) as connection:
            if not connection.execute("SELECT 1 FROM sqlite_schema WHERE name = 'events' AND type = 'table'").fetchone():
                return []
            return connection.execute("SELECT id, tx_seq, payload FROM events ORDER BY id").fetchall()

    def flow_gate(self) -> None:
        table = "flow_events"
        self.create_table(table, rich=True)
        toasted = "".join(hashlib.sha256(str(i).encode()).hexdigest() for i in range(400))
        with self.host("flow", table, 1) as host:
            drive(host, lambda: self.active(table), "Flow connector starts")
            self.sql(f"INSERT INTO {table} VALUES (1, 1, '{toasted}', true, -123, 1.25, 2.5, -123.45, "
                     "'1970-01-03', '1970-01-01 00:00:01.123456', '1970-01-01 08:00:01.123456+08', "
                     f"decode('0001ff','hex')); INSERT INTO {table}(id,tx_seq,payload) VALUES (2,2,'beta')")
            drive(host, lambda: self.sqlite_rows(table) == [(1, 1, toasted), (2, 2, "beta")],
                  "ordered inserts reach SQLite")
            with sqlite3.connect(self.root / table / "sink.sqlite") as connection:
                values = connection.execute("SELECT flag,small,real_value,double_value,amount,day,stamp,instant,bytes "
                                            "FROM events WHERE id = 1").fetchone()
            expected = (1, -123, struct.pack(">f", 1.25), struct.pack(">d", 2.5),
                        (-12345).to_bytes(16, "big", signed=True), 2, 1123456, 1123456, b"\x00\x01\xff")
            if values != expected:
                raise RuntimeError(f"lossless PostgreSQL/Arrow/SQLite type mapping differs: {values!r}")
            self.sql(f"UPDATE {table} SET tx_seq = 11 WHERE id = 1")
            drive(host, lambda: self.sqlite_rows(table) == [(1, 11, toasted), (2, 2, "beta")],
                  "FULL replica identity preserves unchanged toasted text")
            self.sql(f"BEGIN; UPDATE {table} SET payload = 'updated', amount = NULL, flag = NULL WHERE id = 1; "
                     f"DELETE FROM {table} WHERE id = 2; COMMIT")
            drive(host, lambda: self.sqlite_rows(table) == [(1, 11, "updated")],
                  "update retracts its old row and delete retracts its row")
            with sqlite3.connect(self.root / table / "sink.sqlite") as connection:
                if connection.execute("SELECT amount,flag FROM events").fetchone() != (None, None):
                    raise RuntimeError("nullable transition was not preserved")
        until("killed Flow releases its slot", lambda: not self.active(table))
        with self.host("flow", table, 2) as host:
            drive(host, lambda: self.active(table), "reopened Flow connector starts")
            self.sql(f"INSERT INTO {table}(id,tx_seq,payload) VALUES (3, 3, 'after-reopen')")
            drive(host, lambda: self.sqlite_rows(table) == [(1, 11, "updated"), (3, 3, "after-reopen")],
                  "reopened Flow continues without duplicate or missing rows")
            self.sql(f"TRUNCATE TABLE {table}")

            def rejects_truncate() -> bool:
                try:
                    host.request("advance")
                except RuntimeError as error:
                    if "expected a streaming insert, update, or delete" in str(error):
                        return True
                    raise
                return False

            until("truncate is rejected instead of silently skipped", rejects_truncate)
            if self.sqlite_rows(table) != [(1, 11, "updated"), (3, 3, "after-reopen")]:
                raise RuntimeError("rejected truncate changed the SQLite relation")
        print("PASS real Flow -> SQLite type mappings, null transition, unchanged TOAST, update/delete, truncate rejection and process-kill/reopen")

    def source_gate(self) -> None:
        table = "direct_events"
        self.create_table(table)

        def rejected_delivery(host: Host, command: str) -> bool:
            response = host.request(command)
            kind = "backpressure" if command == "backpressure" else "rollback"
            if response["kind"] != kind:
                return False
            if response != {"kind": kind, "output": True, "checkpoint_unchanged": True,
                            "output_unchanged": True, "commits": 0}:
                raise RuntimeError(f"rejected delivery changed durable state: {response}")
            return True

        with self.host("direct", table, 1) as host:
            drive(host, lambda: self.active(table), "direct Source starts")
            self.sql(f"INSERT INTO {table} VALUES (10, 10, 'before-ack')")
            until("actual delivery rollback", lambda: rejected_delivery(host, "rollback"))
            if host.request("read")["rows"]:
                raise RuntimeError("rolled-back Source emitted durable output")
            until("same delivery commits checkpoint and output before ACK", lambda: crash_after_output(host))
            if host.process.wait(timeout=15) != 74:
                raise RuntimeError("crash window did not terminate with the expected code")
        until("crashed Source releases its slot", lambda: not self.active(table))
        with self.host("direct", table, 2) as host:
            if host.request("read") != {"kind": "rows", "rows": [[1, 10, 10, "before-ack"]],
                                        "checkpoint_present": True}:
                raise RuntimeError("output and checkpoint were not both durable before ACK")
            restored = host.request("advance")
            if restored != {"kind": "advance", "output": False, "checkpoint_present": True, "commits": 1}:
                raise RuntimeError(f"first turn did not only restore the checkpoint: {restored}")
            if self.active(table):
                raise RuntimeError("checkpoint restoration unexpectedly connected to PostgreSQL")
            drive(host, lambda: self.active(table), "reopened Source restores checkpoint")
            self.sql(f"INSERT INTO {table} VALUES (20, 20, 'after-reopen')")
            until("full output rejects checkpoint and output together",
                  lambda: rejected_delivery(host, "backpressure"))
            if host.request("read")["rows"] != [[1, 10, 10, "before-ack"]]:
                raise RuntimeError("backpressure changed durable output")
        until("backpressured Source releases its slot", lambda: not self.active(table))
        with self.host("direct", table, 3) as host:
            if host.request("read")["rows"] != [[1, 10, 10, "before-ack"]]:
                raise RuntimeError("backpressured output became durable despite rollback")
            expected = [[1, 10, 10, "before-ack"], [1, 20, 20, "after-reopen"]]

            def replay() -> bool:
                response = host.request("advance")
                if not response.get("output", False):
                    return False
                if response.get("commits") != 1 or not response.get("checkpoint_present"):
                    raise RuntimeError(f"data turn did not use one atomic commit: {response}")
                if host.request("read")["rows"] != expected:
                    raise RuntimeError("restart repeated an ACKed row or lost the backpressured row")
                return True

            until("PostgreSQL replays the unacknowledged backpressured delivery", replay)
            self.sql(f"INSERT INTO {table} VALUES (30, 30, 'last-witness')")
            expected.append([1, 30, 30, "last-witness"])
            drive(host, lambda: host.request("read")["rows"] == expected,
                  "successor witnesses ordered replay without duplicates")
        print("PASS real Source atomic checkpoint/output commit, rollback, pre-ACK process exit, backpressure replay and witness")

    def split_transaction_gate(self) -> None:
        table = "chunked_events"
        self.create_table(table)
        count = 2050  # Exceeds two configured max.batch.size=1024 deliveries.
        expected = [[1, row, row, f"row-{row}"] for row in range(1, count + 1)]
        with self.host("direct", table, 1) as host:
            drive(host, lambda: self.active(table), "split-transaction Source starts")
            self.sql(f"BEGIN; INSERT INTO {table} SELECT value, value, 'row-' || value "
                     f"FROM generate_series(1, {count}) AS value ORDER BY value; COMMIT")
            until("first partial transaction delivery commits before ACK", lambda: crash_after_output(host))
            if host.process.wait(timeout=15) != 74:
                raise RuntimeError("split transaction did not stop in the pre-ACK window")
        until("split-transaction crash releases its slot", lambda: not self.active(table))
        with self.host("direct", table, 2) as host:
            committed = host.request("read")
            prefix = committed["rows"]
            if not committed["checkpoint_present"] or not 0 < len(prefix) <= 1024:
                raise RuntimeError("crash did not cut the PostgreSQL transaction between deliveries")
            if prefix != expected[:len(prefix)]:
                raise RuntimeError("first committed delivery is not the ordered transaction prefix")
            self.sql(f"INSERT INTO {table} VALUES ({count + 1}, {count + 1}, 'last-witness')")
            expected.append([1, count + 1, count + 1, "last-witness"])

            def replay_suffix() -> bool:
                response = host.request("advance")
                if response.get("output") and (response.get("commits") != 1
                                               or not response.get("checkpoint_present")):
                    raise RuntimeError("resumed data delivery was not one atomic checkpoint/output commit")
                rows = host.request("read")["rows"]
                if rows != expected[:len(rows)]:
                    raise RuntimeError("transaction split/reopen duplicated, reordered, or skipped events")
                return rows == expected

            until("transaction suffix and witness resume without duplicate or missing events", replay_suffix)
        print("PASS 2050-row PostgreSQL transaction split across deliveries with first-delivery pre-ACK crash and ordered witness")

    def check_no_password(self) -> None:
        needle = PASSWORD.encode()
        for directory in [self.root / "flow_events" / "flow", self.root / "direct_events" / "source",
                          self.root / "chunked_events" / "source"]:
            for path in directory.rglob("*"):
                if not path.is_file():
                    continue
                with path.open("rb") as stream:
                    tail = b""
                    while chunk := stream.read(1024 * 1024):
                        combined = tail + chunk
                        if needle in combined:
                            raise RuntimeError(f"runtime password was persisted in {path}")
                        tail = combined[-len(needle):]
        print("PASS runtime credential absent from persisted Store bytes")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bundle", type=Path, required=True)
    parser.add_argument("--postgres-bin", type=Path, required=True)
    parser.add_argument("--keep", action="store_true", help="retain this run's temporary cluster and logs after stopping it")
    args = parser.parse_args()
    repo = Path(__file__).resolve().parents[1]
    bundle, pg_bin = args.bundle.resolve(strict=True), args.postgres_bin.resolve(strict=True)
    print(run([str(pg_bin / "postgres"), "--version"]))
    print(run(["rustc", "--version"]))
    subprocess.run(["cargo", "build", "--locked", "-p", "dogpaddle-flow", "--example", "postgres_cdc"],
                   cwd=repo, check=True)
    root = Path(tempfile.mkdtemp(prefix="dogpaddle-pg-cdc-"))
    print(f"isolated fixture: {root}", flush=True)
    fixture = Fixture(root, pg_bin, bundle, repo / "target" / "debug" / "examples" / "postgres_cdc")
    data, started, passed = root / "data", False, False
    try:
        run([str(pg_bin / "initdb"), "-D", str(data), "-U", "dogpaddle_gate", "--auth=trust", "--no-instructions", "--locale=C", "-E", "UTF8"])
        options = f"-h 127.0.0.1 -p {fixture.port} -k {root} -c wal_level=logical -c max_replication_slots=8 -c max_wal_senders=8"
        run([str(pg_bin / "pg_ctl"), "-D", str(data), "-l", str(root / "postgres.log"), "-o", options, "-w", "start"])
        started = True
        fixture.flow_gate()
        fixture.source_gate()
        fixture.split_transaction_gate()
        fixture.check_no_password()
        passed = True
    finally:
        if started or (data / "postmaster.pid").exists():
            run([str(pg_bin / "pg_ctl"), "-D", str(data), "-m", "immediate", "-w", "stop"])
        if passed and not args.keep:
            shutil.rmtree(root)
            print("removed this run's temporary cluster and logs")
        else:
            print(f"stopped fixture and logs retained at {root}")


if __name__ == "__main__":
    main()
