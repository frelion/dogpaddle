# System tests

This directory owns validation that needs a packaged runtime, PostgreSQL,
MySQL, a JVM, or another process boundary. It is intentionally outside the product crates.

- `debezium-runtime/host/` is a non-publishable root-workspace package that
  contains only `bundled_runtime_probe`; `debezium-runtime/probe/` owns the
  test-only Java connector and the build/install/verify scripts for a relocated
  runtime bundle.
- `postgres/hosts/` is the non-publishable root-workspace package for the four
  PostgreSQL CDC, Sink, Sink-recovery, and SQL host binaries.
  `postgres/check_cdc.py`, `postgres/check_sink.py`, and `postgres/check_sql.py`
  own their disposable PostgreSQL scenarios and build those release binaries
  when no prebuilt host paths are supplied.
- `mysql/host/` is a non-publishable root-workspace package containing only the
  MySQL CDC direct-Operation crash-window host. `mysql/check_cdc.py` owns a
  disposable, pinned MySQL Compose fixture and verifies both terminal snapshot
  and streaming Store-commit-before-ACK recovery with a MySQL-capable bundle.
- `debezium-postgres/` is the isolated D1 black-box gate. Its Rust host
  remains a separate workspace with its own byte-for-byte preserved
  `Cargo.lock`; `scripts/check.sh` builds its inputs and runs the full local gate,
  while `scripts/run.sh` accepts only already-built absolute artifact paths.
- `warehouse-sinks/` owns disposable official ClickHouse and Doris containers.
  Its gate runs the ignored live adapter cases for target ownership, layout,
  convergent replay, stale-write suppression, exact row validation, and Doris
  multi-statement transaction splitting.

Normal `cargo test --workspace` does not start Java, PostgreSQL, MySQL, or containers.
The exact local and CI commands are documented in [`../TESTING.md`](../TESTING.md).
