# `DogPaddle` benchmark protocol

`dogpaddle-bench-protocol` is an internal, non-publishable workspace crate for the
mechanical parts shared by benchmark executables. It has no dependency on any
`DogPaddle` product crate and deliberately contains no fixture, workload, timing
boundary, result oracle, Store lifecycle, or human-readable report policy.

The crate owns five small protocol concerns:

- one common `smoke`/`reference` profile and optional filesystem run root;
- reproducibility metadata for rustc, OS/kernel, CPU, git, and an optional filesystem;
- typed JSONL records with validated workload fields, an exact sample count, and exact observation series;
- deterministic duration summaries and nearest-rank percentiles;
- explicit alternating or four-round-counterbalanced A/B execution order.

Benchmark owners keep their semantic structs locally and put only additional
machine-readable values in [`Fields`]. Every raw sample has one stable `series`
identity; derived summaries are computed for human output and are not duplicated
in the machine stream. Non-duration raw facts use `ObservationRecord`; configuration
declares every stable series and exact count in `required_observations`, while the
generic gate validates only identity and continuous indices without interpreting
owner payload. This is an internal executable harness: malformed
settings, records, fields, statistics, or output fail immediately at the caller
with a stage-and-source diagnostic instead of exposing recoverable error types.
Optional host probes remain best-effort and serialize `unavailable: ...`. Record constructors reject collisions
with protocol-owned keys, so a workload cannot silently replace fields such as
`record`, `benchmark`, `series`, `sample`, or `elapsed_ns`. The sealed record set is
`environment | configuration | sample | observation | completion`; arbitrary owner
discriminators and retired derived summaries cannot enter the machine stream.

```rust
use std::time::Duration;

use dogpaddle_bench_protocol::{
    Fields, JsonlWriter, SampleRecord,
};

let mut output = Vec::new();
let mut writer = JsonlWriter::new(&mut output);

let work = Fields::new()
    .with("operations", 64_usize)
    .with("logical_bytes", 4_096_usize);
writer.write(&SampleRecord::new(
    "example",
    "warm_scan",
    0,
    Duration::from_micros(30),
    work,
));
```

The JSONL writer emits exactly one compact JSON object followed by `\n` for each
record. Human-readable tables remain the responsibility of each benchmark target.
