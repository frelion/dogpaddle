# Debezium D1 evidence report

- **Decision:** GREEN
- **Run date:** 2026-09-04 (Asia/Shanghai)
- **Roadmap:** [GitHub #2](https://github.com/frelion/dogpaddle/issues/2)
- **D1 ledger:** [GitHub #3](https://github.com/frelion/dogpaddle/issues/3)
- **DogPaddle base commit:** `2fb7c561a19a31be35aedae366085010ca44ad30`

## What D1 establishes

An unmodified Debezium `3.6.2.Final` PostgreSQL connector can run in the same
OS process as the Rust host, inside one embedded HotSpot JVM, and be controlled
through a small public-API Java bridge. Rust pulls owned delivery bytes and
decides when to ACK. Before ACK, neither the PostgreSQL replication slot nor
the standard file offset bytes advance through the outstanding batch; ACK
allows both to advance.

The bridge maintains one outstanding delivery, runs every
`RecordCommitter` call on Debezium's handler thread, gives shutdown a real
deadline, and converts Java configuration/connector exceptions into host
errors without terminating the process. Fresh Engine handles share the same
JVM and cannot reuse an earlier delivery token.

This is a go/no-go result for the architecture. It is not a production Source,
does not modify any DogPaddle product crate, and does not establish MDBX-backed
recovery, snapshot correctness, schema evolution, a supported type mapping, or
an isolated proof that `FileOffsetBackingStore` alone determines restart
position.

## Reproduction

From the repository root:

```bash
experiments/debezium-d1/scripts/run.sh
```

The accepted run started from a clean D1 PostgreSQL volume and exited `0`. The
single command performed the pinned source audit, Java tests/package, bridge
JAR audit, Rust format/tests/Clippy/release build, and real PostgreSQL black-box
test. It then removed its owned container, volume, network, temporary state,
and logs; follow-up label queries found no D1 Compose resource.

The runner now takes an exclusive fixture lock, refuses to overwrite an
existing Compose project, and propagates cleanup failures. This avoids a failed
or concurrent run deleting somebody else's fixture.

## Exact environment

| Component | Accepted baseline |
| --- | --- |
| Debezium | `3.6.2.Final`, tag `v3.6.2.Final`, commit `02810e25b19c04e5095b2b6fbbdcbae549a69f19` |
| Java build/runtime | Java 17 bytecode on Eclipse Temurin `21.0.9+10` |
| Maven | `3.9.11` |
| Maven/JDK image | linux/amd64 `docker.io/library/maven@sha256:6fdc855a6ed81d288ca7ca37ac6ff5e9308b612485c0801d70b25a858c83d237` |
| JNI | `jni-rs 0.22.4`, safe Invocation API |
| PostgreSQL | `16.15` (`16.15-1.pgdg12+2`) |
| PostgreSQL image | `quay.io/debezium/postgres:16@sha256:114cbe1e4f38055e83c9b567a7e0988fb80837b8eb500203b25c0f784a075b92` |
| Host tools | Rust/Cargo `1.96.0`, Podman `4.9.3`, podman-compose `1.0.6`, psql `16.15`, Python `3.12.3` |

The Maven build produced 69 runtime dependency JARs occupying approximately
39 MiB, excluding the JDK. This is evidence of the packaging and dependency
surface that D3/D5 must own, not a proposed product bundle.

## Static and component gates

The exact upstream source audit passed all of these checks:

```text
PASS markProcessed records the complete partition and offset
PASS markBatchFinished gates offset-store flush through the public commit policy
PASS a successful offset-store flush requests the connector commit callback
PASS the driver-managed LSN mode exists and must be excluded by D1
PASS connector-only LSN flushing is the PostgreSQL connector default
PASS only connector_and_driver enables pgjdbc automatic LSN flush
PASS pgjdbc automatic LSN flush is selected by connector configuration
PASS the PostgreSQL commit callback advances the replication stream LSN
PASS the experiment does not copy or shadow io.debezium classes
PASS no Java source declares an io.debezium package
PASS the Java bridge declares no native callback into Rust
PASS the Rust host crate forbids unsafe code
```

The final bridge artifact also contained no `io/debezium/**` class. Component
verification from the same command passed:

```text
Maven clean package: BUILD SUCCESS
Java tests:          11 passed, 0 failed, 0 errors, 0 skipped
Rust tests:          7 passed
Rust fmt:            passed
Rust Clippy:         passed with -D warnings
Rust release build:  passed
```

The repository-level `cargo xtask check` also exited `0` after the D1 changes,
covering debug/release correctness, Clippy, Rustdoc, and documentation tests for
the existing product workspace.

The bridge uses only public Debezium Engine/API/SPI and public Kafka Connect
types. `RecordCommitter` never crosses into Rust: JNI ACK only signals the
decision and waits, while the original Debezium handler thread calls every
`markProcessed` in order and then `markBatchFinished`.

Component tests additionally prove that `engine.close()` plus Engine-thread
join run once in a daemon worker under the caller's total deadline, and that an
explicit Connect `null` remains null even when its schema has a default.

## Real PostgreSQL black-box evidence

The final one-command run produced these process-level observations:

| Gate | Observation |
| --- | --- |
| Unsafe configuration | `lsn.flush.mode=connector_and_driver` was rejected through JNI; the original created handle remained usable |
| Same process/JVM | Java PID `1` equalled Rust PID `1`; one JVM UUID remained stable across all fresh Engine handles |
| Idle poll | Poll timeout returned ordinary `idle` |
| Single consumer | One active slot consumer; initial PostgreSQL backend PID `80` |
| Source order | One PostgreSQL transaction arrived as IDs `[101, 102, 103]` |
| Complete position | Partition `{"server":"dogpaddle_d1"}`; first final-event offset LSN `27156800` retained |
| Fail-closed API | `max_bytes=1` and wrong ACK token failed; token `1` and the outstanding bytes remained unchanged |
| No implicit ACK | Across `4 × 500 ms` of wall-clock observation, `confirmed_flush_lsn` stayed at `27156344` and offset-file bytes stayed empty |
| Explicit ACK | LSN advanced `27156344 → 27156800`; offset SHA-256 changed from `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855` to `855494b4aeb8d5b60ca0fdd5c54374795b3810ff9214b0bde682c0d6b5ccf4e7` |
| Token rejection | Stale token `1` and repeated token `2` failed closed; the second ACK advanced LSN to `27156992` and offset SHA-256 to `0563d364e850304c3a20b86e028d7d6ac2a8aae27a6fa13602fa3e70e53f02e9` |
| Combined restart witness | With the same file store and persistent slot, a fresh Engine skipped ACKed rows and moved offset LSN `27156992 → 27157184` |
| Lifecycle | PostgreSQL backend PID changed `80 → 117`, with exactly one active slot consumer |
| Stop deadline | `stop(0)` returned a structured timeout in `1 ms`; status subsequently reached `stopped` |
| Unacked restart | Stop did not advance LSN or file bytes; offset LSN `27157184` replayed under token `4` rather than old token `3`, then ACK advanced offset SHA-256 to `84ead2985eaaeb6438e5bbc8b2cea5938ea72d28419e49269853c8f414e90b0c` |
| Clean stop | Active slot consumers returned to `0`; final confirmed position was `27157184` |
| Connector loading failure | A missing connector class became a structured JNI error; status stayed `stopped` and the host remained responsive |

Within one live Engine, repeated polls before ACK returned exactly the same
token and byte-for-byte delivery. A later row was not exposed until the first
delivery was ACKed. Wrong, repeated, stale, and stale-across-handle ACK tokens
all preserved the actual outstanding state.

The four 500 ms periods are deliberately described as wall-clock observation
periods. Because the handler is blocked before `markBatchFinished`, this does
not claim four scheduled offset-store flush attempts.

## Evidence boundaries discovered during review

### Restart position is a combined witness

The accepted restart uses both Kafka Connect's standard
`FileOffsetBackingStore` and the same persistent PostgreSQL replication slot.
The slot also retains `confirmed_flush_lsn`, so “fresh Engine skipped ACKed
rows” cannot causally isolate which of the two restored the start position.

D1 therefore makes two narrower claims:

1. byte comparison proves that the file offset does not change before ACK and
   does change after ACK;
2. the stock file-store-plus-slot combination restarts without replaying the
   ACKed rows.

It does **not** claim that the captured JSON partition/offset can independently
initialize an Engine. D3 must provide a public offset-store SPI adapter and
prove restore from MDBX opaque bytes without a competing Java file.

### The JSON envelope is diagnostic

The D1 envelope retains all PostgreSQL partition/offset fields observed here,
and Connect key/value/header JSON retains schemas and explicit nulls. Its
canonical JSON map is not type-injective for arbitrary Java numeric runtime
types, however. It is not the production opaque checkpoint codec; that wire
format remains a D3 decision.

### Restart replay has documented exclusions

A fresh Engine can regenerate run-local metadata. The replay oracle excludes
exactly:

- outer `SourceRecord.timestamp`;
- top-level envelope `ts_ms`, `ts_us`, and `ts_ns` processing timestamps;
- the `__debezium.context.runId` header.

It compares the complete source partition/offset, Connect key/value schemas and
payloads, operation, row meaning, all metadata actually present in the source
block (including PostgreSQL `txId`), and all other headers. The fixture sets
`provide.transaction.metadata=false`, so this run does not claim coverage of
Debezium's separate transaction-metadata records.

A production delivery ID must not hash the complete serialized `SourceRecord`
envelope. D3 must bind durable identity to source generation plus a complete,
typed opaque partition/offset while retaining the full record as payload.

### Lifecycle remains spike-grade

D1 proves bounded stop for a fully running Engine, including one with an
outstanding delivery. It does not prove stop at every internal Debezium startup
phase. Some phases can temporarily reject `close`; D3 needs a retryable,
phase-aware shutdown state machine and handle reclamation rather than promoting
the D1 one-shot worker unchanged. Any eventual close failure is observable as
`failed`, not falsely reported as `stopped`. The D1 static runtime map also
does not remove replaced handles, so repeated long-lived reconfiguration would
retain configuration and memory; D3 needs an explicit dispose/removal path.

## Explicit stop line

D2 has not started. D1 remains entirely under `experiments/`; no product crate
contains Java, JNI, Debezium, PostgreSQL, or new Source code. MDBX must become
the single durable accepted-offset truth before Java is ACKed, which belongs to
D2/D3 and requires a new owner decision after this report.
