# Debezium D1 black-box gate

This experiment answers one narrow question before PostgreSQL CDC is allowed
to enter DogPaddle's product crates:

> Can the unmodified Debezium 3.6.2 PostgreSQL connector, embedded in the
> process through its public Engine API, keep PostgreSQL's acknowledged LSN
> behind an application-controlled acknowledgement?

It is deliberately a WAL-only, one-table pilot. It does not implement a
DogPaddle Operation and it does not make snapshot, schema evolution, or
transaction-atomicity claims.

## Pinned environment

- Debezium `3.6.2.Final`, upstream commit
  `02810e25b19c04e5095b2b6fbbdcbae549a69f19`.
- PostgreSQL `16.15` (Debian build `16.15-1.pgdg12+2`), from the multi-arch
  `quay.io/debezium/postgres:16` image pinned at manifest digest
  `sha256:114cbe1e4f38055e83c9b567a7e0988fb80837b8eb500203b25c0f784a075b92`,
  and configured with `wal_level=logical`.
- Eclipse Temurin OpenJDK `21.0.9+10` and Maven `3.9.11` from the linux/amd64
  Maven image pinned at
  `sha256:6fdc855a6ed81d288ca7ca37ac6ff5e9308b612485c0801d70b25a858c83d237`.
- `pgoutput`, one publication, one persistent replication slot.
- `snapshot.mode=no_data`.
- Kafka Connect's public `FileOffsetBackingStore` for the restart witness.
- `lsn.flush.mode=connector`; `connector_and_driver` is forbidden because it
  enables pgjdbc automatic LSN flushing.

The connector configuration sets `offset.flush.interval.ms=500`; the
unacknowledged test holds a delivery for four such wall-clock periods. Because
the handler is deliberately blocked before `markBatchFinished`, this is an
observation window, not a claim that four offset-store flush attempts occurred.

## One-command gate

From the repository root, run:

```bash
experiments/debezium-d1/scripts/run.sh
```

The command audits the exact Debezium source revision, builds and tests the
Java bridge in the pinned Maven/JDK container, runs Rust format, tests, Clippy,
and the release build, creates a disposable PostgreSQL volume, and executes the
process-level JSONL runner. It exits non-zero on the first failed gate.

The host needs Rust 1.96 or newer with Clippy, Podman with a working Compose
provider, `git`, `psql`, Python 3, `rg`, `unzip`, and `flock`; network access is
needed to fetch the pinned source and Maven dependencies. Port `55432` must be
available on loopback. The runner takes an exclusive fixture lock and refuses
to delete or replace an existing Compose project with the same name. Its
cleanup failure is itself a failed run.

Set `D1_KEEP_ARTIFACTS=1` to retain the file offset store and host stderr log
printed by the cleanup hook, or `D1_KEEP_POSTGRES=1` to leave the fixture
running for inspection.

## Required exit gates

The black-box runner must prove all of the following against a real PostgreSQL
slot:

1. Holding a delivery unacknowledged across at least three configured
   wall-clock observation intervals changes neither `confirmed_flush_lsn` nor
   the standard file offset bytes.
2. Acknowledging that delivery advances both `confirmed_flush_lsn` and the file
   offset bytes.
3. A fresh Engine using the same `FileOffsetBackingStore` and persistent
   PostgreSQL slot skips ACKed rows and delivers later rows. This is a combined
   restart witness: it does not isolate the client file from the server-side
   slot, and does not prove that captured JSON offsets can initialize an Engine.
4. Rows written by one multi-row PostgreSQL transaction retain their source
   order.
5. Exactly one delivery is outstanding: repeated polls before ACK cannot expose
   a later batch, and the outstanding token and bytes remain identical.
6. Rust and HotSpot report the same OS process ID; fresh Engine handles retain
   one JVM identity, and stop/start leaves at most one active slot consumer.
7. A too-small `max_bytes`, wrong token, repeated token, and stale cross-handle
   token all fail closed; a poll timeout is ordinary idle.
8. On a running Engine, a zero stop deadline returns promptly (either already
   stopped or with a timeout) while a one-shot daemon worker finishes
   `engine.close()` and the Engine-thread join; shutdown failure is observable.
9. An unsafe PostgreSQL configuration and a missing connector class become
   structured host errors without crossing JNI or crashing the process.
10. Stopping with an outstanding delivery does not ACK it; a fresh handle
    replays the same source semantics under a new globally monotonic token.

Byte-for-byte stability applies while an Engine handle remains alive. Across a
fresh Engine, the replay oracle deliberately excludes three kinds of run-local
metadata: the outer `SourceRecord.timestamp`, the envelope's top-level
`ts_ms`/`ts_us`/`ts_ns`, and the `__debezium.context.runId` header. It still
compares the complete source partition/offset, Connect schemas, keys, rows,
all metadata actually emitted in the source block (including PostgreSQL
`txId`), and every other header. The fixture disables Debezium's separate
transaction-metadata records, so D1 makes no claim about them. A product
delivery ID must not hash the full serialized envelope.

The D1 JSON is diagnostic. Key/value/header data use Kafka Connect's
`JsonConverter` with schemas enabled and explicit null preservation. Source
partition/offset maps preserve every PostgreSQL field used by this gate, but
JSON does not distinguish every Java numeric runtime type. D3 must define a
typed, opaque, connector-neutral encoding and restore it through a public
offset-store SPI; this JSON must not be treated as that production codec.

Any failure is a D1 red result. In particular, increasing the flush interval is
not an acceptable remedy for an LSN that advances without ACK.

## Source audit

Run:

```bash
experiments/debezium-d1/scripts/audit-debezium-source.sh
```

The audit clones the exact upstream tag and checks the public commit path that
the black-box test is intended to validate:

```text
RecordCommitter.markProcessed
  -> OffsetStorageWriter.offset(complete partition, complete offset)
RecordCommitter.markBatchFinished
  -> OffsetCommitPolicy
  -> offset-store flush
  -> SourceTask.commit
  -> PostgresConnectorTask.performCommit
  -> replicationStream.flushLsn
```

In Debezium 3.6.2, elapsed time alone does not add an unprocessed record to the
offset writer. The black-box test remains mandatory because this source reading
does not prove runtime configuration, pgjdbc behavior, shutdown behavior, or
future-version behavior.

The source and build gates also reject any local Java source declaring an
`io.debezium.*` package and any bridge JAR containing `io/debezium/**` classes.
That makes “stock” mechanically checkable rather than relying on directory
layout or manual review.

## PostgreSQL fixture

The fixture is intentionally local and disposable. Start it with:

```bash
podman compose \
  --project-name dogpaddle-debezium-d1 \
  --file experiments/debezium-d1/compose.yaml \
  up --detach postgres
```

It listens only on `127.0.0.1:55432`. Remove the scoped test
database and volume with:

```bash
experiments/debezium-d1/scripts/clean.sh
```

The executable black-box runner and host are kept outside `crates/`: passing
D1 is evidence for the architecture decision, not a product implementation,
an isolated `FileOffsetBackingStore` recovery proof, or a claim about future
MDBX-backed recovery. Direct opaque-offset injection belongs to D3.

The accepted 2026-09-04 run and its exact observations are recorded in
[`D1_REPORT.md`](D1_REPORT.md).

## References

- [Debezium Engine API](https://debezium.io/documentation/reference/3.6/development/engine.html)
- [Debezium 3.6.2 release](https://debezium.io/releases/3.6/release-notes#release-3.6.2-final)
- [PostgreSQL connector LSN flush modes](https://debezium.io/documentation/reference/3.6/connectors/postgresql.html)
- [RisingWave incident: uncheckpointed LSN advancement](https://github.com/risingwavelabs/risingwave/issues/25071)
