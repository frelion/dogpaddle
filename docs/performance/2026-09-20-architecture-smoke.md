# Architecture simplification smoke comparison — 2026-09-20

This is a local diagnostic, not a reference baseline or an end-to-end throughput claim.
Both runs used Rust 1.96.0, release builds, the smoke profile, Apple M5 / macOS aarch64,
and APFS. The working tree was based on `f7495e137592b0513e392f660554ce1401b3dea9`.
The before runs included the new benchmark workloads but preceded the corresponding
BoundProjection and EquiJoin preparation changes. Cargo checks overlapped some runs;
short sampling windows and outliers make timing estimates noisy.

## Projection

The same pure-column workloads measure Atomic apply and output drop. Binding, fixture
construction, transaction creation and Store commit are excluded. This does not measure
computed expressions, binding speed, external systems or complete Flow throughput.

Criterion intervals below are in microseconds, shown as lower / point / upper estimates.

| Case (columns × rows) | Before | After |
| --- | --- | --- |
| select/8x1 | 1.8379 / 1.9930 / 2.1607 | 2.6308 / 4.6773 / 7.2289 |
| align/8x1 | 1.6469 / 1.7075 / 1.7832 | 2.1994 / 2.4872 / 2.7003 |
| select/8x256 | 2.1871 / 2.3573 / 2.5109 | 2.1207 / 2.2166 / 2.3926 |
| align/8x256 | 2.0051 / 2.0606 / 2.1659 | 1.9052 / 2.0557 / 2.1835 |
| select/128x1 | 33.038 / 33.941 / 34.984 | 18.167 / 20.622 / 23.760 |
| align/128x1 | 32.592 / 33.245 / 34.154 | 15.658 / 15.888 / 16.223 |
| select/128x256 | 31.619 / 32.467 / 33.257 | 15.834 / 16.936 / 17.621 |
| align/128x256 | 32.399 / 33.154 / 33.715 | 17.634 / 18.443 / 19.660 |
| select/512x1 | 288.97 / 298.70 / 306.16 | 80.173 / 91.140 / 99.327 |
| align/512x1 | 389.00 / 455.99 / 518.36 | 77.459 / 90.633 / 110.65 |
| select/512x256 | 310.68 / 374.42 / 453.91 | 86.886 / 97.631 / 118.13 |
| align/512x256 | 300.84 / 319.09 / 335.96 | 75.282 / 107.16 / 147.15 |

Wide projections improved in this run, consistent with checking the complete input
Schema once instead of once per expression. The 8-column / 1-row cases were slower;
this smoke comparison does not establish whether that difference persists under
controlled reference sampling. No universal speedup is claimed.

Local artifact directories (temporary, not repository fixtures):

- Before: `/private/tmp/dogpaddle-refactor-perf/dogpaddle-projection-run-m635y2`
- After: `/private/tmp/dogpaddle-refactor-perf/dogpaddle-projection-run-QLgbHB`

## EquiJoin preparation

The computed-key workload uses 257 rows and 32 computed key expressions. The complete
Claim's Rust allocator peak fell from 227,952 to 188,346 bytes (17.4%); total allocated
bytes fell from 1,670,119 to 1,669,159. The single-key wide-row workload's peak remained
322,757 bytes. All semantic workload oracles passed before and after.

Coverage excludes fixture/seed/input Arrow allocations and RocksDB native heap. These
figures are not RSS or an input-memory hard limit. The implementation still retains a
complete prepared Claim; it now evaluates and releases one key array at a time before
encoding full rows.

## Reproduce

```sh
DOGPADDLE_PERF_PROFILE=smoke DOGPADDLE_PERF_ROOT=/absolute/artifact/root cargo bench --locked -p dogpaddle-operation --bench projection --bench equi_join_resources
```

Use the owner-specific artifacts and the reference protocol in `TESTING.md` for a
controlled performance baseline. Persistence format, transaction commit and recovery
semantics are unchanged by these kernel optimizations.

## Follow-up: one Flow declaration graph

A second change moved automatic Station partitioning from SQL into Flow and removed
SQL's private arena. Before binaries were copied out of `target/release/deps` before
rebuilding, so both runs use the same benchmark protocol and semantic workload.
The after benchmark expresses unfused outputs with `materialize` and fused outputs
with ordinary `operation` declarations. It does not reduce the benchmark workload.

All six runtime smoke cases passed their semantic oracles before and after. For the
same seven completed advances, the pure-chain comparison remains:

| Physical layout | Stations | Output logs | Input completions | IPC Change appends |
| --- | ---: | ---: | ---: | ---: |
| Explicit materialization | 7 | 6 | 42 | 42 |
| Fused (now planned automatically) | 2 | 1 | 7 | 7 |

Automatic planning adds no work to `advance` or `open`. These results confirm the
intended physical execution structures, not a new runtime speedup over the previously
hand-fused layout. Runtime smoke has only three measured advances per case; it is
insufficient for a throughput regression claim.

Lifecycle Criterion intervals below are milliseconds (lower / point / upper). This
run also overlapped Cargo verification and has substantial timing noise; it does not
establish improved build/reopen latency or absence of a small regression.

| Case | Before | After |
| --- | --- | --- |
| fresh_durable_build/2 | 25.381 / 27.663 / 30.470 | 21.960 / 33.652 / 47.809 |
| warm_reopen/2 | 21.955 / 23.237 / 24.699 | 19.212 / 22.512 / 25.795 |
| fresh_durable_build/3 | 40.223 / 63.548 / 94.645 | 27.969 / 37.866 / 54.633 |
| warm_reopen/3 | 32.328 / 40.364 / 48.540 | 22.016 / 25.007 / 29.057 |

Runtime artifacts (local temporary directories):

- Before: `/private/tmp/dogpaddle-auto-flow-perf/dogpaddle-flow-runtime-run-AavG53`
- After: `/private/tmp/dogpaddle-auto-flow-perf/dogpaddle-flow-runtime-run-dsMqiS`

The canonical physical Flow format remains intact. SQL keeps the development v1 compiler identity domain. Logical Operation numbering
now determines head Station IDs; current v1 golden fixtures are updated in place.
Old development state must be discarded and rebuilt, with no legacy-version
recognition, migration, or compatibility branches.
