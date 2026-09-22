# Performance baselines

The performance framework reset makes every earlier result incomparable with current output. The first
`reference` run from the commit that lands this refactor establishes the new baseline epoch; record that commit,
Rust version, host, filesystem and the owner-specific artifact directory together.

There is no cross-target result schema or migration format. Compare only runs from the same target, baseline epoch,
workload profile and reference environment. Target ownership and commands are defined in [`TESTING.md`](../../TESTING.md).

The [2026-08-27 pre-reset report](./2026-08-27-pre-reset-reference.md) is retained only as a design-history record.

The [2026-09-20 architecture smoke comparison](./2026-09-20-architecture-smoke.md) records local
diagnostic results and their limitations; it does not establish a reference baseline.

The [2026-09-22 issue 30 smoke comparison](./2026-09-22-issue-30-smoke.md) records same-host
projection and ASOF before/after timings, turn counts, Rust heap peaks and output bounds.
It includes the former Project fast-path comparison and remains a local diagnostic.
