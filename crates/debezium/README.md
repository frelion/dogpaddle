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

`DebeziumRuntime::open` accepts the root of a self-contained, platform-specific
bundle. It loads `libjvm` by its validated absolute path; it never searches
`PATH`, `JAVA_HOME`, `JDK_HOME`, or a system Java installation. One OS process
still has at most one `HotSpot` JVM. Reopening the same canonical bundle reuses it
only when the validated contents are unchanged; a different path or contents
fail explicitly. `DogPaddle` must be the first and only JVM initializer during
process startup; another JNI component must neither initialize a JVM first nor
race `open`.

```text
dogpaddle-debezium-runtime-<target>/
├── MANIFEST
├── SHA256SUMS
├── runtime-sbom.json
├── TEMURIN-NOTICE.md
├── runtime/              # pinned Eclipse Temurin JRE
├── debezium/             # bridge, connector and dependency JARs
└── bin/                  # optional native host executable(s)
```

Before starting the JVM, the runtime verifies the exact bundle manifest,
platform target, complete regular-file inventory and every SHA-256 digest. It
then validates the nested Debezium distribution and performs the bridge
protocol handshake. Install the bundle in a directory that untrusted users
cannot modify and treat it as immutable from before `open` until process exit;
the JVM and classloader necessarily reopen validated paths later. These checks
fail closed on incomplete, mixed or corrupted bundles; they do not replace
release signing or artifact provenance.

The builder currently supports exactly:

- `x86_64-unknown-linux-gnu`;
- `aarch64-unknown-linux-gnu`;
- `x86_64-apple-darwin`;
- `aarch64-apple-darwin`.

Linux means GNU/glibc, not musl or Alpine. The macOS archives are unsigned
development artifacts. Public macOS distribution still requires a Developer
ID signature and Apple notarization; that release gate belongs to D5.
The native bundle CI currently proves Ubuntu 24.04 and macOS 15. A lower
glibc/macOS deployment baseline and the native host's complete dynamic-library
closure are D5 release decisions, not broader compatibility claims in D2.

Normal `cargo build`, `cargo test` and `cargo xtask check` never invoke Maven,
download Java artifacts or require a local Java installation. Building a
bundle is an explicit release/development action. First build the pinned Java
distribution:

```bash
crates/debezium/scripts/build-distribution.sh
```

The default Maven mode is `auto`: it prefers a running Podman or Docker engine
with the digest-pinned Maven/JDK image and otherwise uses local `mvn` plus a
JDK. Select the path explicitly with `DOGPADDLE_MAVEN_MODE=container` or
`DOGPADDLE_MAVEN_MODE=local`. The build runs the Java component tests, emits
the nested Debezium `CycloneDX` BOM and checksums, and rejects a bridge JAR that
shadows any `io.debezium` class.

Then assemble a runtime-only archive for one supported target:

```bash
crates/debezium/scripts/build-runtime-bundle.sh x86_64-unknown-linux-gnu
```

An application can place its already-built native executable in the same
archive without changing the runtime format:

```bash
crates/debezium/scripts/build-runtime-bundle.sh \
  x86_64-unknown-linux-gnu \
  target/release/my-host \
  my-host
```

The script downloads the exact checksum-pinned Temurin JRE and its upstream
`CycloneDX` SBOM, verifies the JRE release metadata, validates the complete
nested distribution, rejects a native host for the wrong OS/architecture,
normalizes the runtime tree, preserves notices, and emits
`.tar.gz` plus an archive checksum under `bridge/target/bundles/`. This
explicit bundle step requires `curl`, `tar`, Python 3, and either `sha256sum`
or `shasum`; none is a runtime dependency. This
repository does not yet contain `DogPaddle`'s final application binary, so the
runtime-only archive and optional-host form are the reusable packaging
mechanism rather than a claim that a final CLI has shipped.

The `PostgreSQL` D1 fixture packages its Rust host through this same optional
`bin/` path and runs it against the bundled JVM. It does not carry another JNI
host, Java bridge, offset store, or system-Java fallback.

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
bytecode, Eclipse Temurin JRE `21.0.12.1+1`, and `jni-rs` `0.22.4`. Offset
conversion, checkpoint framing, bridge framing, JVM bundle layout and ACK
behavior are version-reviewed boundaries. Upgrades require
preview-versus-actual, restore, JNI, native-platform bundle and real connector
regression gates.
