# dogpaddle-debezium

`dogpaddle-debezium` embeds the stock Debezium Engine in `DogPaddle`'s Rust
process and presents it as a small pull/ACK library. Its Rust API, checkpoint
protocol, and Java bridge are connector-neutral: they know how to run an Engine
and preserve Kafka Connect offsets, but they do not know Arrow, `Change`, MDBX,
Operation, Flow, or connector-specific position types. The reference D2 bundle
includes the `PostgreSQL` connector as its first real pilot; that packaging
choice does not introduce a `PostgreSQL` branch into the runtime.

The intended call sequence is:

```no_run
use std::time::Duration;

use dogpaddle_debezium::{ConnectorConfig, DebeziumRuntime};

# fn persist(_: &[dogpaddle_debezium::Record], _: &[u8]) -> Result<(), Box<dyn std::error::Error>> { Ok(()) }
# fn should_stop() -> bool { true }
# fn main() -> Result<(), Box<dyn std::error::Error>> {
let runtime = DebeziumRuntime::open("/opt/dogpaddle-debezium")?;
let config = ConnectorConfig::new(
    "orders",
    "io.debezium.connector.postgresql.PostgresConnector",
)?
.property("database.hostname", "127.0.0.1")?
.property("database.user", "cdc")?;

let mut connector = runtime.start(config, None)?;
loop {
    if should_stop() {
        break;
    }
    let Some(delivery) = connector.poll(Duration::from_secs(1))? else {
        continue;
    };
    persist(delivery.records(), delivery.checkpoint().as_bytes())?;
    delivery.ack()?;
}
connector.stop(Duration::from_secs(30))?;
# Ok(())
# }
```

`Delivery` is a linear guard. Its records and checkpoint are ordinary owned
Rust allocations, while its lifetime reserves exclusive access to the
connector. `ack` consumes the guard. Dropping it never acknowledges anything;
the same outstanding delivery can be polled again.

## Durability boundary

Before Java exposes a delivery, it uses Kafka Connect's public
`OffsetStorageWriter` with the exact offset converters used by Debezium 3.6.2
to preview every `SourceRecord` partition/offset in the batch. It merges that
raw delta with the complete accepted in-memory offset map and returns the
candidate map as a versioned [`Checkpoint`](Checkpoint). Only after the caller
has made both records and checkpoint durable does `Delivery::ack` let Debezium
call its real `RecordCommitter`. The actual backing-store bytes must equal the
preview.

The checkpoint is bound to the stable Engine `name` and connector class. It is
opaque to Rust and can contain multiple Kafka Connect source partitions. It is
an offset-store image, not a delivery ID and not a `PostgreSQL` LSN.
`Checkpoint::from_bytes` is for reopening bytes previously emitted by this
exact bridge protocol, not for synthesizing connector offsets by hand.

The Engine `name` is therefore durable source identity, not a display label.
Keep it stable across restarts and never reuse it for a different database,
replication slot, or logical source. Starting with a checkpoint whose name or
connector class differs fails before an Engine is created.

An ACK proves that Debezium's handler settled and that the runtime's actual
Kafka Connect offset-store write exactly matched the checkpoint returned before
the ACK. It does not promise that every connector has synchronously published
its own external progress marker. In Debezium 3.6.2, `PostgreSQL` schedules its
`confirmed_flush_lsn` update from `SourceTask.commit()`; a healthy task is
expected to perform and eventually expose the flush at the beginning of a
subsequent poll (or during stop). That observation is not part of ACK success.
Generic Debezium task commit failures may also be reported internally rather
than thrown through the Engine handler. The persisted checkpoint remains the
recovery boundary; lag in connector-specific external progress is an
operational/WAL-retention concern and is covered by the real-connector gate.

Some Debezium connectors also require durable schema history. The first
`PostgreSQL` pilot does not; this crate does not yet claim that its offset
checkpoint alone is sufficient for every connector.

## Runtime and packaging

One OS process has at most one `HotSpot` JVM. Reopening the same canonical
distribution reuses it only when the validated manifest and JAR fingerprint
are unchanged; another path or changed contents fail explicitly. The
distribution layout is:

```text
distribution/
├── MANIFEST
├── SHA256SUMS
├── bom.json
├── THIRD-PARTY-NOTICES.md
└── lib/
    ├── dogpaddle-debezium-bridge.jar
    └── pinned runtime dependency JARs...
```

Treat that directory as immutable after `DebeziumRuntime::open`. Before the JVM
starts, the runtime checks the exact pinned manifest, requires an exact
checksum-listed set of regular JAR files, verifies every SHA-256 digest, and
performs a bridge-protocol handshake. These checks catch incomplete, mixed, or
corrupted bundles; they are not a signature or a substitute for artifact
provenance.

`MANIFEST`, `SHA256SUMS`, and `lib/` are required at runtime. The official
builder additionally emits `bom.json` and `THIRD-PARTY-NOTICES.md` as review
material for the development bundle.

`cargo build` never invokes Maven, downloads Java artifacts, or requires a JDK
at compile time. The Java bridge has its own pinned build under `bridge/`; its
distribution and real-connector gates are explicit commands. JARs and JDKs are
not committed to the repository.

Build and test the pinned Java distribution with:

```bash
crates/debezium/scripts/build-distribution.sh
```

The script runs Maven in a digest-pinned JDK container, emits the `CycloneDX`
SBOM and checksums beside the bundle, and rejects a bridge JAR that shadows any
`io.debezium` class. The `PostgreSQL` pilot fixture consumes that distribution
through this crate's public Rust API; it does not carry another JNI host or
offset file.

The runtime currently fixes one task, one outstanding delivery, ordered batch
handling, no Debezium SMTs, and a caller-selected encoded delivery bound. These
are correctness constraints, not a generic Source framework. The bound limits
the completed bridge frame copied over JNI and is checked before Rust allocates
it; it is not a total JVM heap quota because Kafka Connect conversion can first
materialize individual field values. Size the JVM independently and treat a
too-large delivery as a terminal connector error that requires reconfiguration
and restart.

Call `Connector::stop` for deterministic shutdown and handle its deadline.
Dropping a `Delivery` never ACKs it. Dropping a `Connector` starts best-effort,
non-blocking cleanup so Rust destructors cannot hang; applications that need to
reuse the same Engine name immediately must stop explicitly first.

## Version boundary

The current bridge pins Debezium `3.6.2.Final`, Kafka Connect `4.3.0`, Java 17
bytecode, a JDK 21 runtime baseline, and `jni-rs` `0.22.4`. Offset conversion,
checkpoint framing, bridge framing, and ACK behavior are version-reviewed
boundaries. Upgrades require preview-versus-actual, restore, JNI, and real
connector regression gates.
