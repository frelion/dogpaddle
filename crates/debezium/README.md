# dogpaddle-debezium

`dogpaddle-debezium` embeds the stock Debezium Engine in `DogPaddle`'s Rust
process and exposes a small connector-neutral pull/ACK API. It knows Debezium
and Kafka Connect offsets, but not Arrow, `Change`, MDBX, Operation, Flow, or a
connector-specific position type. `PostgreSQL` is only the first connector in the
reference distribution.

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

`Delivery<'_>` is the outstanding-delivery capability. Its records and
checkpoint are owned Rust allocations, but its lifetime exclusively borrows
the connector. `ack(self)` consumes it; dropping it does not ACK, and the next
poll returns the same outstanding bytes. There is no separate delivery token.

## Durability boundary

Before exposing a delivery, the bridge previews every record's Kafka Connect
partition/offset with the same `OffsetStorageWriter` and converters used by the
Engine. It merges that delta with the complete accepted offset map and returns
the candidate map as an opaque, versioned [`Checkpoint`](Checkpoint). The
caller must make both records and checkpoint durable before ACK. The bridge
then runs the real Debezium committer and requires its actual offset-store
update to equal the preview.

The checkpoint is bound to the stable Engine name and connector class. It can
contain multiple source partitions; it is neither a delivery ID nor a
`PostgreSQL` LSN. Keep the Engine name stable across restart and never reuse it
for another logical source. A fresh Engine restores only from checkpoint bytes;
the bridge does not maintain a Java offset file.

ACK success means that the Engine handler settled and the offset-store image
matched. It does not mean that every connector has synchronously published an
external progress marker. With Debezium 3.6.2, `PostgreSQL` may expose
`confirmed_flush_lsn` during a later poll or stop. That lag is a WAL-retention
and monitoring concern covered by the real-connector gate, not part of the
durability decision.

Some connectors also need durable schema history. The `PostgreSQL` pilot does
not; this crate does not claim that offsets alone restore every connector.

## Runtime bundle

`DebeziumRuntime::open` accepts one platform-specific runtime payload and loads
its contained `libjvm` by validated absolute path. It never searches `PATH`,
`JAVA_HOME`, `JDK_HOME`, or a system Java installation. One process has at most
one `HotSpot` JVM; reopening the same canonical bundle path reuses it, while a
different path fails.

```text
dogpaddle-debezium-runtime-<target>/
├── MANIFEST
├── runtime-sbom.json
├── TEMURIN-NOTICE.md
├── runtime/              # pinned Eclipse Temurin JRE
└── debezium/             # bridge, connector and dependency JARs
```

`open` checks the exact target manifest, Temurin release metadata, required
runtime/security/legal resources, the nested distribution's exact JAR set and
hashes, and that `libjvm` resolves inside the bundle. It deliberately does not
traverse and hash the whole JRE on every process start. The emitted archive
digest and a trusted, immutable installation are the integrity boundary;
runtime preflight is not release signing or provenance verification. Install
the extracted payload where untrusted users cannot modify it and keep it
unchanged until process exit.

Supported payload targets are:

- `x86_64-unknown-linux-gnu`;
- `aarch64-unknown-linux-gnu`;
- `x86_64-apple-darwin`;
- `aarch64-apple-darwin`.

Linux means GNU/glibc, not musl or Alpine. The `Debezium runtime bundles`
workflow uses native Ubuntu 24.04 and macOS 15 runners for all four targets.
After relocating each archive to a path containing spaces, it removes access
to system Java and exercises the full public lifecycle:
`open -> start -> poll(position 1) -> drop/redeliver -> ack -> stop ->
checkpoint-only restart -> poll(position 2 witness) -> ack -> stop`.
The probe also checks the owned topic, partition, timestamp, key, value,
headers and pre-ACK checkpoint, and proves that the fresh connector restores
the accepted position before producing the next witness. macOS
archives are unsigned development artifacts; deployment baselines, native
dependency closure, signing and notarization remain D5 work.

Normal Cargo gates never invoke Maven, download Java artifacts, or require a
JDK. Bundle construction is explicit. First use a local JDK and Maven to build
and test the pinned Java distribution and the separate test-only connector:

```bash
crates/debezium/scripts/build-distribution.sh
```

Then build one runtime payload:

```bash
crates/debezium/scripts/build-runtime-bundle.sh x86_64-unknown-linux-gnu
```

The second script verifies checksum-pinned Temurin assets, normalizes the JRE,
adds the Java distribution and notices, and emits a `.tar.gz` plus its digest
under `bridge/target/bundles/`. It requires `curl`, `tar`, Python 3 with secure
tar extraction filters, and `sha256sum` or `shasum`.

The payload intentionally contains no native host or `bin/` packaging contract.
`DogPaddle`'s future release packager will combine the real application
executable with this reusable runtime payload. The lifecycle workflow builds a
deterministic connector from the separate `bridge/probe/` Maven project.
`scripts/install-lifecycle-probe.sh` injects it only into the relocated test
copy, and `examples/bundled_runtime_probe.rs` drives the Rust API; neither the
product distribution nor the uploaded runtime archive contains that connector
or host. The D1 `PostgreSQL` gate keeps its diagnostic host separate and mounts
both into one test process.

The independent `Debezium PostgreSQL recovery` workflow runs
`experiments/debezium-d1/scripts/run.sh` on Ubuntu 24.04 for relevant pull
requests and `main` changes, on a weekly schedule, and on manual dispatch. It
owns the real connector matrix: idle recovery, drop/redelivery, unacknowledged
restart replay, checkpoint-only takeover, durable-before-ACK, eventual
`confirmed_flush_lsn`, outstanding-stop replay, row order, absence of a Java
offset file, and payload-JVM isolation. Artifact upload runs on both success
and failure and includes whatever environment, retained fixture state and logs
were produced. This Linux `PostgreSQL` gate is kept
separate from the deterministic four-platform lifecycle probe.

## Runtime constraints

The private JNI surface is synchronous: `start(handle, timeout)` either reaches
polling or fails, while poll, ACK and stop have explicit deadlines. Java
exceptions carry ordinary failures; the narrow `failureKind(handle)` query only
distinguishes an oversized delivery after a failed poll. There is no status
JSON protocol. The development-v1 delivery wire (`DPDBDV01`, version `1`)
contains only the checkpoint and ordered records; linear Rust borrowing supplies
the outstanding capability. This layout replaces the earlier unshipped v1
bytes without compatibility or migration; rebuild old development bundles and
discard saved test fixtures.

The runtime fixes one task, one outstanding delivery, ordered batches, no SMTs,
and a caller-selected encoded-delivery bound. The bound limits the completed
frame copied over JNI, not total JVM heap use. An oversized delivery is a
terminal connector error requiring reconfiguration and restart.

Use `Connector::stop` for deterministic shutdown. Dropping a connector starts
best-effort non-blocking cleanup; a caller that must reuse the same Engine name
immediately should stop it explicitly.

## Version boundary

The bridge pins Debezium `3.6.2.Final`, Kafka Connect `4.3.0`, Java 17
bytecode, Eclipse Temurin JRE `21.0.12.1+1`, and `jni-rs` `0.22.4`. Offset
conversion, checkpoint framing, delivery wire, JNI commands, bundle layout and
ACK semantics are reviewed version boundaries. Upgrades require
preview-versus-actual, restore, native-bundle and real-connector regression
gates.
