# `DogPaddle` benchmark harness

`dogpaddle-bench-protocol` is the internal, non-publishable harness shared by
the ten release benchmark executables. Product crates do not depend on it.

The crate owns only mechanical policy:

- the fixed `smoke`/`reference` profiles and optional reference filesystem;
- rustc, host, git, and filesystem reproducibility metadata;
- a concrete `Run` that assigns case IDs and zero-based sample indices;
- a fixture-free, profile-specific `Plan` frozen before execution;
- deterministic paired execution order;
- the sole `Serialize + Deserialize` JSONL `Record` schema;
- a protocol-owned `RunValidator` and a report derived from validated raw facts.

Owners still own every fixture, workload, warmup, timing boundary, and oracle.
They return an already-timed and already-validated `Measurement`; the harness
never starts a clock around owner setup or validation.

One run consists of:

```text
run { environment, configuration, exact cases and observations }
sample { compact case id, index, elapsed_ns, dynamic fields } *
observation { compact observation id, index, fields } *
completion
```

Stable series, pair identity, sample counts, and static work facts occur once in
the run plan rather than in every sample. `completion` is written only after the
owner's final oracle succeeds. Human summaries are views of the same validated
artifact; no derived summary record enters the machine stream.

Cases and observations use one canonical lexicographic order. Every adjacent
`<target>.plan.json` records the smoke and reference case count, observation
count, canonical byte length, and FNV-1a-128 digest. `cargo xtask
bench-plan-check` asks each executable to construct only its pure Plan for both
profiles—without creating fixtures or starting measurements—and compares it to
that independent golden. A normal run consumes the same frozen IDs, and
`finish` rejects any missing or extra sample or observation.

`fnv1a-128-canonical-json-v1` hashes the UTF-8 canonical JSON bytes for
`protocol, benchmark, profile, configuration, cases, observations` in that
order. It starts at `6c62272e07bb014262b821756295c58d`; for each byte it XORs
then multiplies modulo 2¹²⁸ by `0000000001000000000000000000013b`.
Configuration/field maps are key-ordered, and case/observation arrays are
series-ordered. Locked empty/`a`/`foobar` vectors protect the implementation.

`Run::memory` is for benchmarks without persistent fixtures.
`Run::persistent` applies the common temporary/fixed-filesystem rules and owns
fresh per-sample directories. Ordinary cases use `Run::samples`, paired cases
use `Run::paired`, and unusual multi-way or endurance protocols use `push` and
`observe` with IDs returned while constructing the Plan. There is no
cross-owner scenario trait or workload DSL. The single deserialized `Record`
enum rejects unknown fields and invalid labels; `RunValidator` owns both full
artifact validation and plan-only golden validation.
