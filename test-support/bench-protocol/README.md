# `DogPaddle` benchmark protocol

`dogpaddle-bench-protocol` is an internal, non-publishable workspace crate for the
mechanical parts shared by benchmark executables. It has no dependency on any
`DogPaddle` product crate and deliberately contains no fixture, workload, timing
boundary, result oracle, Store lifecycle, or human-readable report policy.

The crate owns five small protocol concerns:

- strict parsing for scalar/list settings, duplicate-free positive dimensions, and Cargo/run profiles;
- reproducibility metadata for rustc, OS/kernel, CPU, git, and an optional filesystem;
- typed JSONL records with validated extension fields;
- deterministic duration summaries and nearest-rank percentiles;
- explicit alternating or four-round-counterbalanced A/B execution order.

Benchmark owners keep their semantic structs locally and put only additional
machine-readable values in [`Fields`]. The record constructors reject collisions
with protocol-owned keys, so a workload cannot silently replace fields such as
`record`, `benchmark`, `elapsed_ns`, or `median_ns`. `ExtensionRecord` additionally
rejects the five core discriminators (`environment`, `configuration`, `sample`,
`summary`, and `pair_summary`) so a custom schema cannot masquerade as a standard
record.

```rust
use std::time::Duration;

use dogpaddle_bench_protocol::{
    DurationSummary, Fields, JsonlWriter, SampleRecord, SummaryRecord,
};

let mut output = Vec::new();
let mut writer = JsonlWriter::new(&mut output);

let work = Fields::new()
    .with("operations", 64_usize)?
    .with("logical_bytes", 4_096_usize)?;
writer.write(&SampleRecord::new(
    "example",
    "warm_scan",
    0,
    Duration::from_micros(30),
    work,
)?)?;

let samples = [Duration::from_micros(30), Duration::from_micros(20)];
let summary = DurationSummary::from_samples(&samples)?;
writer.write(&SummaryRecord::new(
    "example",
    "warm_scan",
    summary,
    Fields::new().with("operations", 64_usize)?,
)?)?;

# Ok::<(), Box<dyn std::error::Error>>(())
```

The JSONL writer emits exactly one compact JSON object followed by `\n` for each
record. Human-readable tables remain the responsibility of each benchmark target.
