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
                 log: Path, environment: dict[str, str]) -> None:
        self.stderr = log.open("w", encoding="utf-8")
        try:
            host_environment = dict(os.environ)
            host_environment.update(environment)
            self.process = subprocess.Popen(
                [str(binary), mode, str(sql), str(flow)],
                stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=self.stderr,
                text=True, bufsize=1,
                env=host_environment,
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
        repository = Path(__file__).resolve().parents[2]
        self.program = (
            repository / "crates/sql/examples/postgres_etl.sql"
        ).resolve(strict=True)
        self.port = cluster.port
        self.pg_env = dict(os.environ, PGPASSWORD=PASSWORD)
        self.psql = [str(pg_bin / "psql"), "-X", "-h", "127.0.0.1",
                     "-p", str(self.port), "-U", "dogpaddle_gate",
                     "-d", "postgres", "-v", "ON_ERROR_STOP=1", "-At"]
        self.host_environment = {
            "DOGPADDLE_POSTGRES_ETL_BUNDLE": str(bundle),
            "DOGPADDLE_POSTGRES_ETL_PORT": str(self.port),
            "DOGPADDLE_POSTGRES_ETL_USER": "dogpaddle_gate",
            "DOGPADDLE_POSTGRES_ETL_PASSWORD": PASSWORD,
        }

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
            "CREATE SCHEMA sales; CREATE SCHEMA analytics; "
            "CREATE TABLE sales.orders ("
            "order_id BIGINT PRIMARY KEY, customer TEXT NOT NULL, "
            "region TEXT NOT NULL, status TEXT NOT NULL, "
            "quantity INTEGER NOT NULL, unit_price_cents BIGINT NOT NULL, "
            "discount_pct INTEGER NOT NULL); "
            "ALTER TABLE sales.orders REPLICA IDENTITY FULL; "
            "CREATE PUBLICATION orders_publication FOR TABLE sales.orders"
        )
        self.sql(
            "SELECT * FROM pg_create_logical_replication_slot("
            "'orders_slot', 'pgoutput')"
        )

    def slot_active(self) -> bool:
        return self.sql(
            "SELECT active FROM pg_replication_slots "
            "WHERE slot_name = 'orders_slot'"
        ) == "t"

    def rows(self) -> list[tuple[int, int, str, str, int, int, str,
                               Optional[str]]]:
        if self.sql(
            "SELECT to_regclass('analytics.order_insights') IS NOT NULL"
        ) != "t":
            return []
        values = self.sql(
            'SELECT "$dogpaddle.id", order_id, '
            "convert_from(customer, 'UTF8'), convert_from(market, 'UTF8'), "
            "gross_cents, net_cents, convert_from(lane, 'UTF8'), "
            "attention IS NULL, COALESCE(convert_from(attention, 'UTF8'), '') "
            "FROM analytics.order_insights ORDER BY order_id"
        )
        if not values:
            return []
        rows = []
        for line in values.splitlines():
            (technical_id, order_id, customer, market, gross_cents,
             net_cents, lane, attention_is_null, attention) = line.split("|", 8)
            if attention_is_null not in {"t", "f"}:
                raise RuntimeError(
                    f"invalid PostgreSQL NULL marker: {attention_is_null}"
                )
            if attention_is_null == "t" and attention:
                raise RuntimeError("NULL attention unexpectedly has a value")
            rows.append((
                int(technical_id),
                int(order_id),
                customer,
                market,
                int(gross_cents),
                int(net_cents),
                lane,
                None if attention_is_null == "t" else attention,
            ))
        return rows

    def logical_rows(self) -> list[tuple[int, str, str, int, int, str,
                                        Optional[str]]]:
        return [row[1:] for row in self.rows()]

    def technical_ids(self) -> dict[int, int]:
        rows = self.rows()
        ids = {row[1]: row[0] for row in rows}
        if len(ids) != len(rows):
            raise RuntimeError("target contains duplicate logical order IDs")
        return ids

    def target_insert_count(self) -> int:
        log = (self.root / "postgres.log").read_text(
            encoding="utf-8", errors="replace"
        )
        return log.count('INSERT INTO "analytics"."order_insights" (')

    def host(self, mode: str, session: int) -> Host:
        return Host(self.binary, mode, self.program, self.flow,
                    self.root / f"host-{session}.log", self.host_environment)

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
        first = [
            (103, "Nova", "China", 32_000, 24_000, "standard", "regional"),
            (104, "Kite", "Global", 50_000, 40_000, "priority", "review"),
        ]
        with self.host("build", 1) as host:
            self.drive(host, "SQL CDC connector starts", self.slot_active)
            self.sql(
                "INSERT INTO sales.orders VALUES "
                "(101, 'Acme', 'cn-east', 'new', 3, 5000, 10), "
                "(102, 'Orbit', 'eu-west', 'paid', 2, 4000, 0), "
                "(103, 'Nova', 'cn-south', 'paid', 4, 8000, 25), "
                "(104, 'Kite', 'us-west', 'paid', 5, 10000, 20)"
            )

            def first_target_commit() -> bool:
                response = host.advance()
                rows = self.logical_rows()
                sink = response["sink"]
                if len(rows) != len(set(rows)) or not set(rows).issubset(first):
                    raise RuntimeError(f"invalid partial ETL target relation: {rows}")
                if rows != first:
                    return False
                if (
                    response["outcome"] != "Progressed"
                    or sink["cursor"] >= sink["tail"]
                ):
                    raise RuntimeError(
                        "first ETL result was not observed at the Prepared crash point"
                    )
                return True

            until("first multi-stage ETL result commits", first_target_commit)
            first_rows = self.rows()
            first_ids = self.technical_ids()
            if (
                set(first_ids) != {103, 104}
                or len(set(first_ids.values())) != len(first_ids)
            ):
                raise RuntimeError(f"invalid initial technical IDs: {first_ids}")
            first_insert_count = self.target_insert_count()
            if first_insert_count == 0:
                raise RuntimeError("PostgreSQL did not log the first target INSERT")
            if self.sql(
                "SELECT is_nullable FROM information_schema.columns "
                "WHERE table_schema = 'analytics' "
                "AND table_name = 'order_insights' "
                "AND column_name = 'attention'"
            ) != "YES":
                raise RuntimeError("UNION common Schema did not widen attention")
            # The target write committed during the command above. No next Flow
            # round settles its durable Prepared state before this process exit.
            host.kill()

        until("killed SQL host releases its slot", lambda: not self.slot_active())
        with self.host("open", 2) as host:
            replay = host.advance()
            if (
                replay["outcome"] != "Progressed"
                or replay["sink"]["cursor"] >= replay["sink"]["tail"]
                or self.rows() != first_rows
            ):
                raise RuntimeError("Prepared replay duplicated or changed the ETL result")
            until(
                "Prepared target INSERT is replayed",
                lambda: self.target_insert_count() > first_insert_count,
            )
            self.drive(host, "reopened SQL CDC connector starts", self.slot_active)
            self.settle(host, "reopened Flow settles")
            if self.rows() != first_rows or self.technical_ids() != first_ids:
                raise RuntimeError(
                    "settling Prepared replay changed rows or stable technical IDs"
                )

            self.sql(
                "UPDATE sales.orders SET status = 'paid' WHERE order_id = 101"
            )
            entered = [
                (101, "Acme", "China", 15_000, 13_500, "standard", "regional"),
                *first,
            ]
            self.drive(
                host,
                "an update makes a previously filtered order qualify",
                lambda: self.logical_rows() == entered,
            )
            self.settle(host, "newly qualified order settles")
            if self.logical_rows() != entered:
                raise RuntimeError("settling changed the newly qualified result")
            entered_ids = self.technical_ids()
            if (
                entered_ids[103] != first_ids[103]
                or entered_ids[104] != first_ids[104]
                or entered_ids[101] in first_ids.values()
            ):
                raise RuntimeError(
                    f"unrelated rows changed identity after qualifying update: {entered_ids}"
                )

            self.sql(
                "UPDATE sales.orders SET quantity = 6, discount_pct = 0 "
                "WHERE order_id = 103"
            )
            repriced = [
                entered[0],
                (103, "Nova", "China", 48_000, 48_000, "priority", "regional"),
                entered[2],
            ]
            self.drive(
                host,
                "arithmetic and CASE results change after repricing",
                lambda: self.logical_rows() == repriced,
            )
            self.settle(host, "repriced order settles")
            if self.logical_rows() != repriced:
                raise RuntimeError("settling changed the repriced result")
            repriced_ids = self.technical_ids()
            if (
                repriced_ids[101] != entered_ids[101]
                or repriced_ids[104] != entered_ids[104]
                or repriced_ids[103] <= max(entered_ids.values())
            ):
                raise RuntimeError(
                    f"repricing did not replace exactly one stable relation row: {repriced_ids}"
                )

            self.sql("DELETE FROM sales.orders WHERE order_id = 104")
            deleted = repriced[:2]
            self.drive(
                host,
                "deleting a source order retracts its Global result",
                lambda: self.logical_rows() == deleted,
            )
            self.settle(host, "source deletion settles")
            if self.logical_rows() != deleted:
                raise RuntimeError("settling changed the source deletion result")
            if self.technical_ids() != {
                101: repriced_ids[101],
                103: repriced_ids[103],
            }:
                raise RuntimeError("source deletion changed an unrelated technical ID")

            self.sql(
                "INSERT INTO sales.orders VALUES "
                "(105, 'Lumen', 'us-west', 'paid', 2, 7000, 0)"
            )
            final = [
                *deleted,
                (105, "Lumen", "Global", 14_000, 14_000, "standard", None),
            ]
            self.drive(
                host,
                "a successor INSERT reaches the Global UNION ALL branch",
                lambda: self.logical_rows() == final,
            )
            self.settle(host, "successor INSERT settles")
            if self.logical_rows() != final:
                raise RuntimeError("settling changed the successor INSERT result")
            final_ids = self.technical_ids()
            if (
                final_ids[101] != repriced_ids[101]
                or final_ids[103] != repriced_ids[103]
                or final_ids[105] <= max(repriced_ids.values())
            ):
                raise RuntimeError(
                    f"successor INSERT changed existing IDs or reused the frontier: {final_ids}"
                )
            if self.sql(
                "SELECT schemaname || '.' || tablename FROM pg_publication_tables "
                "WHERE pubname = 'orders_publication'"
            ) != "sales.orders":
                raise RuntimeError("CDC publication captured the SQL sink target")

        print(
            "PASS checked-in SQL postgres_cdc -> four CTEs/arithmetic/CASE/filter/"
            "nullable UNION ALL -> postgres build, INSERT/UPDATE/DELETE, "
            "PG-committed/Prepared crash, idempotent reopen, and successor"
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
