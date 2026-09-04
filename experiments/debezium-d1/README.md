# Debezium D1 product black-box gate

D1 is the real PostgreSQL fixture for the product `dogpaddle-debezium` crate.
It no longer owns a Java bridge or a JNI host implementation. The small Rust
JSONL host depends on the product crate and is built separately from the
reusable Linux x86_64 runtime payload, which contains the product Debezium
distribution and pinned Temurin JRE. The runner mounts both read-only into one
process, and the product loads only the payload's JVM.

The gate answers one narrow question:

> Can a caller persist the product runtime's opaque pre-ACK checkpoint, ACK
> only after taking durable ownership, and recover correctly with no competing
> Java offset file?

This remains a WAL-only, one-table PostgreSQL pilot. It is not yet a
DogPaddle Source Operation, does not use MDBX, and makes no snapshot, schema
evolution, transaction-framing, or Arrow mapping claim.

## Boundary under test

The fixture exercises only the public Rust surface:

```text
DebeziumRuntime::open(bundle_root)
  -> DebeziumRuntime::start(config, optional_checkpoint)
  -> Connector::poll(timeout)
  -> Delivery::{records, checkpoint}
  -> Delivery::ack()
  -> Connector::stop(timeout)
```

The host copies record bytes and the opaque `Checkpoint` only to render
diagnostic JSON. A returned `Delivery` then falls out of scope without ACK;
polling again must yield the same records and checkpoint. The JSON `token` is
allocated by this test host and is run-local. It is not a product delivery ID
and is never passed through the product boundary.

The host protocol deliberately splits durability from acknowledgement:

1. `poll` observes a delivery and its pre-ACK checkpoint.
2. `save` atomically replaces `/state/checkpoint.bin` with those opaque bytes.
3. `ack TOKEN` re-polls the same product `Delivery`, verifies its bytes and
   checkpoint are unchanged, and consumes `Delivery::ack()`.
4. `stop` never ACKs an outstanding delivery.

`Delivery::ack()` settles the exact Kafka Connect offset-store update. It does
not promise that PostgreSQL's `confirmed_flush_lsn` has already changed when
the Rust call returns: Debezium's Engine can defer the connector task commit,
and Debezium 3.6.2 does not surface every `SourceTask.commit()` failure through
`markBatchFinished`. D1 therefore observes PostgreSQL advancement only after a
subsequent source poll and/or graceful stop. This is eventual WAL-retention
feedback, not part of DogPaddle's durability decision.

The flat checkpoint file is a fixture stand-in for the future MDBX transaction.
It is the only durable accepted-offset truth. The connector fixture contains no
`offset.*` property; the product bridge owns its in-memory Kafka Connect offset
store and restores it solely from `Checkpoint`.

## Pinned environment

- Debezium `3.6.2.Final`, upstream commit
  `02810e25b19c04e5095b2b6fbbdcbae549a69f19`.
- Kafka Connect `4.3.0`.
- PostgreSQL `16.15`, using `pgoutput`, one publication, and one persistent
  logical replication slot.
- Java 17 bridge bytecode; Eclipse Temurin JRE `21.0.12.1+1` inside the runtime
  payload. The D1 runner invokes the product's unchanged local-only distribution
  builder inside its digest-pinned Maven/JDK image.
- Linux GNU x86_64 runtime. The four-platform JVM/bridge smoke matrix is owned
  by the separate `Debezium runtime bundles` workflow; the real PostgreSQL
  fixture is intentionally not duplicated on macOS or Linux arm64.
- `snapshot.mode=no_data` and `lsn.flush.mode=connector`.

`connector_and_driver` remains forbidden because pgjdbc automatic LSN
flushing would bypass the application ACK boundary.

## Run the gate

From the repository root:

```bash
experiments/debezium-d1/scripts/run.sh
```

The command performs, in order:

1. an audit of the exact upstream Debezium commit path;
2. the product bridge tests and distribution build in the pinned Maven/JDK
   image;
3. Rust format, unit tests, Clippy, and a release host build;
4. a self-contained runtime payload containing pinned Temurin, the PostgreSQL
   connector, manifests, SBOMs and notices;
5. a disposable real PostgreSQL black-box recovery matrix using the separately
   built host and payload in one process.

Required local tools are Rust 1.96 or newer with Clippy, Podman with a working
Compose provider, `git`, `psql`, Python 3, `curl`, `tar`, `rg`, `flock`, and
`sha256sum` or `shasum`. Network access is needed for the pinned source audit,
Maven dependencies and checksum-pinned Temurin assets. No host JDK, Maven or
`unzip` is required. Connector invocation clears Java-related environment and
loader paths, so the image's Java cannot replace the payload runtime. Port
`55432` must be free.

The runner owns an exclusive fixture lock and refuses to overwrite an existing
Compose project. Set `D1_KEEP_ARTIFACTS=1` to retain the sole checkpoint and
stderr log, or `D1_KEEP_POSTGRES=1` to retain PostgreSQL.

## Required exit gates

The real connector run must prove all of these:

1. A normal poll timeout returns idle, and a later PostgreSQL transaction is
   still delivered.
2. Dropping the borrowed `Delivery` and polling again returns identical
   record bytes and checkpoint; no implicit ACK occurs.
3. An unsaved, unacknowledged batch is replayed by a fresh Engine.
4. Saving the batch's candidate checkpoint before ACK, stopping without ACK,
   and starting a fresh Engine from that checkpoint skips the taken-over batch.
5. `ack` is rejected until the exact candidate checkpoint is durable.
6. After explicit ACK, a subsequent source poll and/or graceful stop eventually
   advances PostgreSQL `confirmed_flush_lsn`; ACK return alone is not treated
   as a synchronous PostgreSQL flush guarantee.
7. Stopping with another outstanding delivery neither saves nor ACKs it; a
   fresh Engine from the last accepted checkpoint replays it.
8. Rows written by one multi-row transaction retain their source order.
9. `/state/checkpoint.bin` is the only state file; no
   `FileOffsetBackingStore` or other Java offset file exists.
10. The executable and runtime payload are mounted separately;
    `DebeziumRuntime::open` validates the payload and explicitly loads its
    contained `libjvm`, not Java from the container environment.

Within one live connector, byte-for-byte stability includes the opaque
checkpoint and every encoded key, value, and header. Across a fresh Engine, the
replay oracle excludes only outer `SourceRecord.timestamp`, the envelope's
processing `ts_ms`/`ts_us`/`ts_ns`, and the
`__debezium.context.runId` header. The JSON remains diagnostic and is not a
product protocol.

## Source audit

Run the audit independently with:

```bash
experiments/debezium-d1/scripts/audit-debezium-source.sh
```

It pins the upstream tag and verifies the public path:

```text
RecordCommitter.markProcessed
  -> OffsetStorageWriter.offset
RecordCommitter.markBatchFinished
  -> OffsetCommitPolicy
  -> offset-store flush
  -> SourceTask.commit
  -> PostgresConnectorTask.performCommit
  -> replicationStream.flushLsn
```

The audit also checks that the product bridge does not copy or shadow
`io.debezium.*`, declares no Java-to-Rust native callback, and that the D1
fixture itself contains neither Java bridge source nor a direct `jni`
dependency.

The product's native CI separately relocates each Linux/macOS x86_64/aarch64
archive and completes the same public runtime handshake with an empty `PATH`
and invalid Java home variables. That is the direct no-system-Java proof; this
D1 fixture owns the deeper real PostgreSQL recovery proof.

## Fixture lifecycle

PostgreSQL listens only on `127.0.0.1:55432`. To remove an intentionally
retained fixture:

```bash
experiments/debezium-d1/scripts/clean.sh
```

The current evidence and remaining boundaries are recorded in
[`D1_REPORT.md`](D1_REPORT.md).

## References

- [Debezium Engine API](https://debezium.io/documentation/reference/3.6/development/engine.html)
- [Debezium 3.6.2 release](https://debezium.io/releases/3.6/release-notes#release-3.6.2-final)
- [PostgreSQL connector LSN flush modes](https://debezium.io/documentation/reference/3.6/connectors/postgresql.html)
- [RisingWave incident: uncheckpointed LSN advancement](https://github.com/risingwavelabs/risingwave/issues/25071)
