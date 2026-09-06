#!/usr/bin/env python3
"""Real same-PostgreSQL fulfillment queue gate (Python 3.9+).

The gate owns one temporary loopback-only PostgreSQL cluster. Ordinary Cargo
commands compile the Rust host but never run this script.
"""

from __future__ import annotations

import argparse
import json
import os
import queue
import subprocess
import tempfile
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
                 binary: Path, cluster: PostgresCluster,
                 capture_trace: bool) -> None:
        self.root, self.pg_bin = root, pg_bin
        self.bundle, self.binary = bundle, binary
        self.cluster = cluster
        self.capture_trace = capture_trace
        self.scenes: list[dict[str, Any]] = []
        self.flow = root / "flow"
        repository = Path(__file__).resolve().parents[2]
        self.program = (
            repository / "crates/sql/examples/fulfillment.sql"
        ).resolve(strict=True)
        self.port = cluster.port
        self.pg_env = dict(os.environ, PGPASSWORD=PASSWORD)
        self.psql = [str(pg_bin / "psql"), "-X", "-h", "127.0.0.1",
                     "-p", str(self.port), "-U", "dogpaddle_gate",
                     "-d", "postgres", "-v", "ON_ERROR_STOP=1", "-At"]
        self.host_environment = {
            "DOGPADDLE_FULFILLMENT_BUNDLE": str(bundle),
            "DOGPADDLE_FULFILLMENT_PORT": str(self.port),
            "DOGPADDLE_FULFILLMENT_USER": "dogpaddle_gate",
            "DOGPADDLE_FULFILLMENT_PASSWORD": PASSWORD,
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
            "CREATE SCHEMA sales; CREATE SCHEMA ops; "
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

    def source_rows(self) -> list[dict[str, Any]]:
        values = self.sql(
            "SELECT order_id, customer, region, status, quantity, "
            "unit_price_cents, discount_pct "
            "FROM sales.orders ORDER BY order_id"
        )
        if not values:
            return []
        rows = []
        for line in values.splitlines():
            (order_id, customer, region, status, quantity,
             unit_price_cents, discount_pct) = line.split("|", 6)
            rows.append({
                "order_id": int(order_id),
                "customer": customer,
                "region": region,
                "status": status,
                "quantity": int(quantity),
                "unit_price_cents": int(unit_price_cents),
                "discount_pct": int(discount_pct),
            })
        return rows

    def rows(self) -> list[tuple[int, int, str, int, int, str, str,
                               Optional[str]]]:
        if self.sql(
            "SELECT to_regclass('ops.fulfillment_queue') IS NOT NULL"
        ) != "t":
            return []
        values = self.sql(
            'SELECT "$dogpaddle.id", order_id, '
            "convert_from(customer, 'UTF8'), subtotal_cents, payable_cents, "
            "convert_from(fulfillment_center, 'UTF8'), "
            "convert_from(handling_lane, 'UTF8'), handling_reason IS NULL, "
            "COALESCE(convert_from(handling_reason, 'UTF8'), '') "
            "FROM ops.fulfillment_queue ORDER BY order_id"
        )
        if not values:
            return []
        rows = []
        for line in values.splitlines():
            (technical_id, order_id, customer, subtotal_cents,
             payable_cents, fulfillment_center, handling_lane,
             reason_is_null, handling_reason) = line.split("|", 8)
            if reason_is_null not in {"t", "f"}:
                raise RuntimeError(
                    f"invalid PostgreSQL NULL marker: {reason_is_null}"
                )
            if reason_is_null == "t" and handling_reason:
                raise RuntimeError(
                    "NULL handling_reason unexpectedly has a value"
                )
            rows.append((
                int(technical_id),
                int(order_id),
                customer,
                int(subtotal_cents),
                int(payable_cents),
                fulfillment_center,
                handling_lane,
                None if reason_is_null == "t" else handling_reason,
            ))
        return rows

    def logical_rows(self) -> list[tuple[int, str, int, int, str, str,
                                        Optional[str]]]:
        return [row[1:] for row in self.rows()]

    def technical_ids(self) -> dict[int, int]:
        rows = self.rows()
        ids = {row[1]: row[0] for row in rows}
        if len(ids) != len(rows):
            raise RuntimeError("target contains duplicate logical order IDs")
        return ids

    def target_trace_rows(self) -> list[dict[str, Any]]:
        return [
            {
                "rid": technical_id,
                "order_id": order_id,
                "customer": customer,
                "subtotal_cents": subtotal_cents,
                "payable_cents": payable_cents,
                "fulfillment_center": fulfillment_center,
                "handling_lane": handling_lane,
                "handling_reason": handling_reason,
            }
            for (
                technical_id,
                order_id,
                customer,
                subtotal_cents,
                payable_cents,
                fulfillment_center,
                handling_lane,
                handling_reason,
            ) in self.rows()
        ]

    def capture(self, name: str) -> None:
        if not self.capture_trace:
            return
        self.scenes.append({
            "name": name,
            "source": self.source_rows(),
            "target": self.target_trace_rows(),
        })

    def target_delete_count(self) -> int:
        log = (self.root / "postgres.log").read_text(
            encoding="utf-8", errors="replace"
        )
        return log.count('DELETE FROM ONLY "ops"."fulfillment_queue" ')

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

    def run_gate(self) -> list[dict[str, Any]]:
        inserted = [
            (
                103, "Nova", 32_000, 24_000,
                "CN-HUB", "standard", "regional_route",
            ),
        ]
        paid = [
            (
                101, "Acme", 15_000, 13_500,
                "CN-HUB", "standard", "regional_route",
            ),
            *inserted,
        ]
        repriced = [
            paid[0],
            (
                103, "Nova", 48_000, 48_000,
                "CN-HUB", "priority", "regional_route",
            ),
        ]
        after_delete = [repriced[1]]

        with self.host("build", 1) as host:
            self.drive(host, "SQL CDC connector starts", self.slot_active)
            self.capture("connected")

            self.sql(
                "INSERT INTO sales.orders VALUES "
                "(101, 'Acme', 'cn-east', 'new', 3, 5000, 10), "
                "(102, 'Orbit', 'eu-west', 'paid', 2, 4000, 0), "
                "(103, 'Nova', 'cn-south', 'paid', 4, 8000, 25)"
            )
            self.drive(
                host,
                "the first qualified order enters fulfillment",
                lambda: self.logical_rows() == inserted,
            )
            self.settle(host, "initial fulfillment result settles")
            if self.logical_rows() != inserted:
                raise RuntimeError("settling changed the initial fulfillment result")
            inserted_ids = self.technical_ids()
            if inserted_ids != {103: 1}:
                raise RuntimeError(f"invalid initial technical IDs: {inserted_ids}")
            if self.sql(
                "SELECT is_nullable FROM information_schema.columns "
                "WHERE table_schema = 'ops' "
                "AND table_name = 'fulfillment_queue' "
                "AND column_name = 'handling_reason'"
            ) != "YES":
                raise RuntimeError(
                    "UNION common Schema did not widen handling_reason"
                )
            self.capture("inserted")

            self.sql(
                "UPDATE sales.orders SET status = 'paid' WHERE order_id = 101"
            )
            self.drive(
                host,
                "a paid order becomes eligible for fulfillment",
                lambda: self.logical_rows() == paid,
            )
            self.settle(host, "paid order settles")
            if self.logical_rows() != paid:
                raise RuntimeError("settling changed the paid order result")
            paid_ids = self.technical_ids()
            if paid_ids != {101: 2, 103: 1}:
                raise RuntimeError(
                    f"qualifying an order changed existing identity: {paid_ids}"
                )
            self.capture("paid")

            self.sql(
                "UPDATE sales.orders SET quantity = 6, discount_pct = 0 "
                "WHERE order_id = 103"
            )
            self.drive(
                host,
                "repricing promotes an order to priority",
                lambda: self.logical_rows() == repriced,
            )
            self.settle(host, "repriced order settles")
            if self.logical_rows() != repriced:
                raise RuntimeError("settling changed the repriced result")
            repriced_ids = self.technical_ids()
            if repriced_ids != {101: 2, 103: 3}:
                raise RuntimeError(
                    "repricing did not replace exactly one relation row: "
                    f"{repriced_ids}"
                )
            self.capture("repriced")

            delete_count_before = self.target_delete_count()
            self.sql("DELETE FROM sales.orders WHERE order_id = 101")
            if self.logical_rows() != repriced:
                raise RuntimeError("target changed before the Flow advanced deletion")

            def deletion_target_commit() -> bool:
                response = host.advance()
                rows = self.logical_rows()
                if rows != repriced and rows != after_delete:
                    raise RuntimeError(
                        f"invalid partial fulfillment deletion: {rows}"
                    )
                if rows != after_delete:
                    return False
                sink = response["sink"]
                if (
                    response["outcome"] != "Progressed"
                    or sink["cursor"] >= sink["tail"]
                ):
                    raise RuntimeError(
                        "deletion was not observed at the Prepared crash point"
                    )
                return True

            until("fulfillment deletion commits", deletion_target_commit)
            crashed_rows = self.rows()
            crashed_ids = self.technical_ids()
            if crashed_ids != {103: 3}:
                raise RuntimeError(
                    f"deletion changed the surviving technical ID: {crashed_ids}"
                )
            crash_delete_count = self.target_delete_count()
            if crash_delete_count <= delete_count_before:
                raise RuntimeError("PostgreSQL did not log the target deletion")
            self.capture("deleted")
            # PostgreSQL has committed the delete, while the durable Sink cursor
            # still points at the same input. Kill before its settlement turn.
            host.kill()

        until("killed SQL host releases its slot", lambda: not self.slot_active())
        self.capture("crashed")

        with self.host("open", 2) as host:
            replay = host.advance()
            if (
                replay["outcome"] != "Progressed"
                or replay["sink"]["cursor"] >= replay["sink"]["tail"]
                or self.rows() != crashed_rows
                or self.technical_ids() != crashed_ids
            ):
                raise RuntimeError(
                    "Prepared replay duplicated or changed the fulfillment queue"
                )
            until(
                "Prepared target DELETE is replayed",
                lambda: self.target_delete_count() > crash_delete_count,
            )
            self.drive(host, "reopened SQL CDC connector starts", self.slot_active)
            self.settle(host, "reopened Flow settles")
            if self.rows() != crashed_rows or self.technical_ids() != crashed_ids:
                raise RuntimeError(
                    "settling Prepared replay changed rows or stable technical IDs"
                )
            self.capture("recovered")

            self.sql(
                "INSERT INTO sales.orders VALUES "
                "(104, 'Kestrel', 'us-west', 'paid', 5, 10000, 20)"
            )
            resumed = [
                *after_delete,
                (
                    104, "Kestrel", 50_000, 40_000,
                    "GLOBAL-HUB", "priority", "export_review",
                ),
            ]
            self.drive(
                host,
                "a high-value global order enters export review",
                lambda: self.logical_rows() == resumed,
            )
            self.settle(host, "resumed global order settles")
            if self.logical_rows() != resumed:
                raise RuntimeError("settling changed the resumed result")
            resumed_ids = self.technical_ids()
            if resumed_ids != {103: 3, 104: 4}:
                raise RuntimeError(
                    f"resuming changed an existing ID or reused one: {resumed_ids}"
                )
            self.capture("resumed")

            self.sql(
                "INSERT INTO sales.orders VALUES "
                "(105, 'Lumen', 'us-west', 'paid', 2, 7000, 0)"
            )
            final = [
                *resumed,
                (
                    105, "Lumen", 14_000, 14_000,
                    "GLOBAL-HUB", "standard", None,
                ),
            ]
            self.drive(
                host,
                "a standard global order preserves nullable handling reason",
                lambda: self.logical_rows() == final,
            )
            self.settle(host, "nullable global order settles")
            if self.logical_rows() != final:
                raise RuntimeError("settling changed the nullable global result")
            final_ids = self.technical_ids()
            if final_ids != {103: 3, 104: 4, 105: 5}:
                raise RuntimeError(
                    f"final INSERT changed existing IDs or reused one: {final_ids}"
                )
            if self.sql(
                "SELECT schemaname || '.' || tablename FROM pg_publication_tables "
                "WHERE pubname = 'orders_publication'"
            ) != "sales.orders":
                raise RuntimeError("CDC publication captured the SQL sink target")

        expected_scenes = [
            "connected", "inserted", "paid", "repriced",
            "deleted", "crashed", "recovered", "resumed",
        ]
        if self.capture_trace and [
            scene["name"] for scene in self.scenes
        ] != expected_scenes:
            raise RuntimeError("fulfillment trace scenes are incomplete")
        return self.scenes


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


def write_trace(path: Path, scenes: list[dict[str, Any]]) -> Path:
    destination = path.expanduser().resolve()
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary: Optional[Path] = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=destination.parent,
            prefix=f".{destination.name}.",
            suffix=".tmp",
            delete=False,
        ) as output:
            temporary = Path(output.name)
            json.dump({"scenes": scenes}, output, indent=2)
            output.write("\n")
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary, destination)
        temporary = None
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)
    return destination


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
        "--trace-output",
        type=Path,
        help="atomically write real fulfillment scene data after the gate passes",
    )
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
    gate = Gate(
        root, pg_bin, bundle, binary, cluster,
        capture_trace=args.trace_output is not None,
    )
    failure: Optional[BaseException] = None
    passed = False
    stopped = False
    scenes: list[dict[str, Any]] = []
    try:
        gate.start()
        gate.prepare()
        scenes = gate.run_gate()
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

    if args.trace_output is not None:
        trace_path = write_trace(args.trace_output, scenes)
        print(f"fulfillment trace: {trace_path}")
    print(
        "PASS checked-in fulfillment SQL: postgres_cdc -> subtotal/discount/"
        "eligibility/routing -> PostgreSQL, INSERT/UPDATE/DELETE, nullable "
        "UNION ALL, PG-committed crash, idempotent reopen, and stable IDs"
    )


if __name__ == "__main__":
    main()
