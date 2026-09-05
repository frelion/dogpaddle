# `DogPaddle` performance context

`dogpaddle-perf-context` is a deliberately small, non-publishable helper used by
benchmark executables. It owns the two workload profiles, fresh filesystem
roots, host metadata, and the release-build guard for measured runs.

It does not own benchmark cases, workload plans, sampling order, result schemas,
statistics, validation, or reports. Those semantics belong to the crate that
owns each benchmark. Criterion benchmarks use Criterion's native artifacts;
custom runners emit owner-specific JSONL.
