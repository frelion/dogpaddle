# System tests

This directory owns validation that needs a packaged runtime, PostgreSQL, a JVM,
or another process boundary. It is intentionally outside the product crates.

- `debezium-runtime/host/` is a non-publishable root-workspace package that
  contains only `bundled_runtime_probe`; `debezium-runtime/probe/` owns the
  test-only Java connector and the build/install/verify scripts for a relocated
  runtime bundle.
- `postgres/hosts/` is the non-publishable root-workspace package for the three
  PostgreSQL CDC, Sink, and Sink-recovery host binaries. `postgres/check_cdc.py`
  and `postgres/check_sink.py` own their disposable PostgreSQL scenarios and
  build those release binaries when no prebuilt host paths are supplied.
- `debezium-postgres/` is the isolated D1 black-box gate. Its Rust host
  remains a separate workspace with its own byte-for-byte preserved
  `Cargo.lock`; `scripts/check.sh` builds its inputs and runs the full local gate,
  while `scripts/run.sh` accepts only already-built absolute artifact paths.

Normal `cargo test --workspace` does not start Java, PostgreSQL, or containers.
The exact local and CI commands are documented in [`../TESTING.md`](../TESTING.md).
