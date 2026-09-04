# Debezium D1 product-runtime revalidation

- **Decision:** GREEN
- **Revalidated:** 2026-09-04 (Asia/Shanghai)
- **Roadmap:** [GitHub #2](https://github.com/frelion/dogpaddle/issues/2)
- **D2 ledger:** [GitHub #5](https://github.com/frelion/dogpaddle/issues/5)

## Result

D1 now tests the product `dogpaddle-debezium` crate rather than carrying a
second implementation. The fixture has no Java source, Maven project, JNI
wrapper, offset-store file, or Debezium dependency inventory of its own. Its
Rust JSONL host is packaged with the product distribution and pinned Temurin
JRE in one Linux x86_64 bundle, then uses only this public product sequence:

```text
DebeziumRuntime::open(bundle_root)
  -> DebeziumRuntime::start
  -> Connector::poll
  -> Delivery::{records, checkpoint}
  -> Delivery::ack
  -> Connector::stop
```

The PostgreSQL run establishes that Rust can take durable ownership of the
opaque pre-ACK `Checkpoint`, then ACK the exact delivery. Dropping a borrowed
`Delivery` does not acknowledge it, and the next poll returns the same records
and checkpoint. A fresh Engine restores solely from Rust-owned checkpoint
bytes; there is no competing `FileOffsetBackingStore`.

This closes the old D1-to-product gap. The same real connector fixture now
exercises the reusable D2 runtime boundary that future source integrations will
call.

## Reproduction

From the repository root:

```bash
experiments/debezium-d1/scripts/run.sh
```

The command exits non-zero on the first failed gate. It audits the exact
upstream Debezium source, checks that D1 has no duplicate bridge or JNI layer,
runs the Rust fixture gates, invokes the product distribution and runtime-bundle
builders, and executes the recovery matrix against a disposable PostgreSQL
instance. The D1 host runs from the bundle's `bin/` and the runtime explicitly
loads the bundle's `libjvm`; it does not use `JAVA_HOME` or a system-Java
fallback. The command owns an exclusive fixture lock and removes its PostgreSQL
volume, network, state, and logs after success.

The accepted self-contained connector run passed:

```text
Product Java bridge tests: 46 passed, 0 failed
D1 Rust host tests:         5 passed, 0 failed
D1 Rust format:             passed
D1 Rust Clippy:             passed with -D warnings
D1 Rust release build:      passed
Self-contained host bundle: passed
Real PostgreSQL gate:       passed
```

That run consumed the product-built distribution unchanged. Its manifest pins
Debezium `3.6.2.Final`, Kafka Connect `4.3.0`, and bridge protocol `1`, and it
passed its SHA-256 inventory, Java protocol handshake, bridge JAR namespace
audit, required charset/timezone/TLS/DNS resource probe, and CycloneDX SBOM
generation. The distribution and D1 host were nested in an outer Linux GNU
x86_64 bundle whose manifest pins Eclipse Temurin JRE `21.0.12.1+1`. The host
ran with `PATH`, `JAVA_HOME` and `JDK_HOME` pointing at nonexistent locations,
all Java option variables unset, and Linux/macOS loader search paths empty.
Therefore the real PostgreSQL gate, not only a synthetic handshake, proves that
the process used the validated bundle `libjvm` without a system-Java fallback.

The accepted host bundle was `208106326` bytes unpacked and `90584226` bytes as
`tar.gz`; that run's archive SHA-256 was
`3027777f73133ae1ab4f8eb359594fd021f71898b85aca0d103756096566f9a8`.
Its normalized tree contained no symbolic links.

## Real PostgreSQL evidence

The black-box recovery matrix passed these transitions:

1. A poll timeout returned ordinary idle and left the connector usable.
2. One PostgreSQL transaction was delivered in row order `[101, 102, 103]`.
3. ACK was rejected until the delivery's exact opaque checkpoint had been
   durably saved.
4. Repeatedly dropping and polling the borrowed delivery returned identical
   record bytes, checkpoint bytes, and host-local diagnostic token; PostgreSQL
   `confirmed_flush_lsn` remained unchanged.
5. Stopping with that unsaved delivery saved nothing, acknowledged nothing,
   and a fresh Engine replayed the same source semantics and checkpoint.
6. Saving the candidate checkpoint and stopping without ACK still did not
   advance PostgreSQL. A fresh Engine initialized only from that checkpoint,
   skipped `[101, 102, 103]`, and next delivered row `[201]`.
7. After row `[201]` was saved and ACKed, PostgreSQL advancement was verified
   across the next source poll and graceful stop. Restart from the accepted
   checkpoint remained correct.
8. Stopping with outstanding row `[301]` neither changed the saved checkpoint
   nor acknowledged the row. Restart replayed identical source semantics and
   candidate checkpoint; save, ACK, and graceful stop then advanced PostgreSQL.
9. The state directory ended with exactly one file:
   `checkpoint.bin`.

The accepted run observed an initial candidate checkpoint of `253` bytes.
Repeatedly dropping its first delivery preserved checkpoint SHA-256
`c5e1fba24d1377255323a8e3255be9409d624ef13b672579e73834617ad440e5`.
Before the first accepted ACK, `confirmed_flush_lsn` stayed at byte position
`27156344`; the accepted position reached `27156992`. In this particular run it
was already visible at ACK return, but the gate still crossed the next-poll and
graceful-stop boundary because immediate visibility is not the API contract.
The final row reached position `27157240`. The replayed row `[301]` kept the
same candidate checkpoint SHA-256 on both sides of stop/start:
`8ecfcb1a32a2cc356a066fb3c2f70f0ef63800ad96b93fa16abd788048644018`.

The diagnostic token is deliberately allocated by the D1 host and is never
passed through the product API. The opaque connector-bound checkpoint plus the
retained record payload form the recovery witness exercised here; D1 does not
name either one a durable delivery identity. The minimum replay identity, if
one is needed at all, remains a D3 fault-matrix decision rather than an
`AppendLog` offset or a PostgreSQL-specific LSN exposed to Rust.

## The ACK/LSN distinction

`Delivery::ack()` waits for Debezium's handler to apply the exact previewed
Kafka Connect offset state. That is the boundary needed for DogPaddle's safety:
the candidate checkpoint was already durable before ACK, so a crash can resume
after the accepted batch.

It is intentionally **not** claimed that PostgreSQL's
`confirmed_flush_lsn` has changed when `Delivery::ack()` returns. Debezium
3.6.2 may defer the connector task commit until another source-poll lifecycle
or shutdown, and its Engine can consume a failed `SourceTask.commit()` without
surfacing it through `markBatchFinished`. The gate therefore requires eventual
LSN advancement after a subsequent poll and/or graceful stop. Delayed server
feedback can retain extra WAL, but it cannot lose accepted events because the
Rust checkpoint is already authoritative.

This operational distinction must remain visible in metrics and the PostgreSQL
runbook; it must not be promoted into a false synchronous ACK guarantee.

## Exact pinned environment

| Component | Baseline |
| --- | --- |
| Debezium | `3.6.2.Final`, tag `v3.6.2.Final`, commit `02810e25b19c04e5095b2b6fbbdcbae549a69f19` |
| Kafka Connect | `4.3.0` |
| Java build target | Java 17 bytecode |
| Java build image | Eclipse Temurin 21 / Maven 3.9.11, pinned digest in the scripts |
| Configured runtime | Eclipse Temurin JRE `21.0.12.1+1`, Linux GNU x86_64 archive and upstream SBOM locked by SHA-256 |
| Rust | `1.96.0` or newer; unsafe code forbidden |
| PostgreSQL | `16.15`, pinned image digest in `compose.yaml` |
| PostgreSQL CDC | `pgoutput`, persistent slot, `snapshot.mode=no_data` |
| LSN ownership | `lsn.flush.mode=connector`; driver auto-flush forbidden |

## Static source evidence

The source audit pins the upstream revision and verifies this path:

```text
RecordCommitter.markProcessed
  -> OffsetStorageWriter.offset
RecordCommitter.markBatchFinished
  -> offset-store flush
  -> SourceTask.commit
PostgresStreamingChangeEventSource.commitOffset
  -> replicationStream.flushLsn
```

It also mechanically rejects:

- a local `io.debezium.*` source or class shadow;
- a Java-to-Rust native callback;
- any remaining D1 Java bridge source or Maven manifest;
- a direct D1 host `jni` dependency or JNI module;
- PostgreSQL `connector_and_driver` LSN flushing;
- caller-controlled Kafka Connect offset-store properties.

## Remaining boundaries

D1 remains a focused fixture, not a production Source Operation. Its durable
file is only a crash-safe stand-in for the D3 MDBX transaction. It does not yet
prove Flow backpressure integration, Arrow `Change` mapping, snapshots, schema
evolution, transaction framing, auxiliary schema-history state, or a second
connector.

The real PostgreSQL fixture owns Linux GNU x86_64 only. A separate native CI
matrix builds and relocates runtime-only bundles on Linux GNU and macOS,
x86_64/aarch64, then completes the public JVM/bridge handshake with an empty
system `PATH` and invalid Java home variables. Those macOS artifacts are
unsigned development bundles; Developer ID signing and notarization remain a
D5 release responsibility. The repository also has no final DogPaddle product
executable yet—the optional D1 host demonstrates the generic `bin/` packaging
mechanism rather than shipping such a binary.

The record key/value/header bytes are schemas-enabled Kafka Connect JSON. That
is a connector-neutral owned transport representation, but it is not yet the
DogPaddle logical schema or Change codec. Fresh-Engine replay comparisons
exclude only source-record processing timestamps and Debezium's run-id header;
the checkpoint itself must remain byte-identical for the same offset state.

The next milestone is D3: persist each delivery payload and candidate
checkpoint atomically in MDBX, then call `Delivery::ack()` only after that
transaction commits. D3 should reuse this runtime as-is rather than introduce
another Debezium or PostgreSQL-specific process boundary.
